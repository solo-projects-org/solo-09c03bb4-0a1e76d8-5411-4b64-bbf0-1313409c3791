"""震级更正写回源目录：整行核对 + 固定请求号幂等 + 回执随同事务复制。

写回流程（对源库的一笔事务）：
  1. 先查 corrections 回执表：同请求号已存在 —— 内容相同则直接返回原结果
     （幂等重放，不重复改写），内容不同则拒绝（ConflictContent）；
  2. 否则 SELECT ... FOR UPDATE 锁定源库该编号当前整行，与网页所见五字段
     逐字段核对：行不存在 -> rejected_deleted；整行不符 -> rejected_mismatch；
     相符 -> UPDATE 震级，outcome=applied；
  3. 同事务 INSERT corrections 回执并提交 —— 更正与回执在同一源事务，
     经逻辑复制在镜像端同笔落库，网页凭镜像中的回执判定“已采回”。

结果不明：commit 阶段异常抛 UnknownOutcome（源库可能已提交而响应丢失）。
调用方展示“待确认”；凭原请求号与原内容重试 —— 已提交则返回原结果，
未提交（已回滚）则重新执行，两种情形都不会重复改写。

本模块不依赖 psycopg2：连接由调用方注入，便于用假连接做单元测试。
"""
import json
import re
import uuid
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation

from canonical import norm_ts

OUTCOME_APPLIED = "applied"
OUTCOME_MISMATCH = "rejected_mismatch"
OUTCOME_DELETED = "rejected_deleted"

MAG_LO, MAG_HI = Decimal("-99.9"), Decimal("99.9")   # numeric(3,1)


class ValidationError(ValueError):
    """网页提交内容不合法。"""


class UnknownOutcome(Exception):
    """commit 阶段连接中断：源库可能已提交也可能已回滚，调用方不得断言结果。"""


class ConflictContent(Exception):
    """同请求号、不同内容：拒绝。stored 为已存记录。"""

    def __init__(self, stored):
        super().__init__("同请求号已存在且内容不同")
        self.stored = stored


class CorrectionRequest:
    """一次更正的完整内容：固定请求号 + 所见五字段 + 新震级。"""

    def __init__(self, request_id, expected, new_magnitude):
        self.request_id = request_id
        self.expected = dict(expected)          # 规范化五字段
        self.event_id = self.expected["event_id"]
        self.new_magnitude = new_magnitude      # 规范化文本，如 '4.5'


# ---------------- 规范化与校验 ----------------

_REQUEST_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9-]{3,63}$")


def new_request_id(now=None):
    now = now or datetime.now(timezone.utc)
    return f"RQ{now:%Y%m%d}-{uuid.uuid4().hex[:8]}"


def validate_request_id(text):
    text = (text or "").strip()
    if not _REQUEST_ID_RE.match(text):
        raise ValidationError(f"请求号格式不正确：{text!r}")
    return text


def _norm_decimal(text, places, lo, hi, label):
    s = (text or "").strip()
    try:
        v = Decimal(s)
    except InvalidOperation:
        raise ValidationError(f"{label}不是有效数字：{s!r}")
    if not v.is_finite():
        raise ValidationError(f"{label}不是有效数字：{s!r}")
    try:
        q = v.quantize(Decimal(1).scaleb(-places))
    except InvalidOperation:
        raise ValidationError(f"{label}位数过多：{s!r}")
    if q != v:
        raise ValidationError(f"{label}最多允许 {places} 位小数：{s!r}")
    if q < lo or q > hi:
        raise ValidationError(f"{label}超出范围 [{lo}, {hi}]：{s!r}")
    return f"{q:.{places}f}"


def norm_magnitude(text):
    return _norm_decimal(text, 1, MAG_LO, MAG_HI, "震级")


def build_expected(event_id, origin_time, latitude, longitude, magnitude):
    """网页所见五字段 -> 规范化 dict（与源库整行核对的基准）。"""
    eid = (event_id or "").strip()
    if not eid:
        raise ValidationError("震例号不能为空")
    try:
        ts = norm_ts(origin_time)
    except Exception:
        raise ValidationError(f"发震时刻格式不正确：{origin_time!r}")
    return {
        "event_id": eid,
        "origin_time": ts,
        "latitude": _norm_decimal(latitude, 3, Decimal("-90"), Decimal("90"), "纬度"),
        "longitude": _norm_decimal(longitude, 3, Decimal("-180"), Decimal("180"), "经度"),
        "magnitude": _norm_decimal(magnitude, 1, MAG_LO, MAG_HI, "当前震级"),
    }


def expected_json(expected):
    """五字段的规范 JSON 文本：同内容必得同串，用于“同号同内容”判定。"""
    return json.dumps(expected, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def row_to_expected(row):
    """源库整行 (event_id, origin_time, latitude, longitude, magnitude) -> 规范化五字段。"""
    event_id, origin_time, latitude, longitude, magnitude = row
    return {
        "event_id": str(event_id),
        "origin_time": norm_ts(origin_time),
        "latitude": f"{Decimal(str(latitude)):.3f}",
        "longitude": f"{Decimal(str(longitude)):.3f}",
        "magnitude": f"{Decimal(str(magnitude)):.1f}",
    }


def decide(expected, row):
    """核对源库当前整行：返回 (outcome, 源库实际五字段或 None)。"""
    if row is None:
        return OUTCOME_DELETED, None
    actual = row_to_expected(row)
    if actual != expected:
        return OUTCOME_MISMATCH, actual
    return OUTCOME_APPLIED, actual


# ---------------- 回执记录 ----------------

_SELECT_CORR_SQL = (
    "SELECT request_id, event_id, new_magnitude, expected, outcome, source_row, submitted_at "
    "FROM corrections WHERE request_id=%s"
)
_LOCK_ROW_SQL = (
    "SELECT event_id, origin_time, latitude, longitude, magnitude "
    "FROM seismic_events WHERE event_id=%s FOR UPDATE"
)
_UPDATE_SQL = "UPDATE seismic_events SET magnitude=%s WHERE event_id=%s"
_INSERT_SQL = (
    "INSERT INTO corrections(request_id, event_id, new_magnitude, expected, outcome, source_row) "
    "VALUES (%s, %s, %s, %s, %s, %s)"   # submitted_at 由源库 DEFAULT now() 落时间
)


def record_from_row(row):
    """corrections 表行（列序见 _SELECT_CORR_SQL）-> dict。"""
    expected = json.loads(row[3])
    return {
        "request_id": row[0],
        "event_id": row[1],
        "new_magnitude": f"{Decimal(str(row[2])):.1f}",
        "expected": expected,
        "expected_json": expected_json(expected),
        "outcome": row[4],
        "source_row": json.loads(row[5]) if row[5] else None,
        "submitted_at": norm_ts(row[6]),
        "replay": False,
    }


def fetch_record(cur, request_id):
    cur.execute(_SELECT_CORR_SQL, (request_id,))
    row = cur.fetchone()
    return record_from_row(row) if row else None


def _same_content(rec, req):
    return (
        rec["event_id"] == req.event_id
        and rec["new_magnitude"] == req.new_magnitude
        and rec["expected_json"] == expected_json(req.expected)
    )


def _replay_or_conflict(rec, req):
    if _same_content(rec, req):
        rec["replay"] = True
        return rec
    raise ConflictContent(rec)


# ---------------- 提交 ----------------

def submit_correction(connect, req, integrity_errors=()):
    """执行更正，返回 corrections 记录 dict；结果不明抛 UnknownOutcome。

    connect: 返回 DB-API 连接（autocommit 关闭）的可调用对象；
    integrity_errors: 源库唯一约束异常类型（如 psycopg2.IntegrityError），
                      用于“同号并发提交”竞态：插入撞上已存记录时回滚重读。
    """
    conn = connect()
    try:
        cur = conn.cursor()
        try:
            rec = fetch_record(cur, req.request_id)
            if rec is not None:                       # 同号已存在：幂等返回或拒绝
                conn.rollback()                       # 只读事务，结束
                return _replay_or_conflict(rec, req)
            cur.execute(_LOCK_ROW_SQL, (req.event_id,))
            outcome, actual = decide(req.expected, cur.fetchone())
            if outcome == OUTCOME_APPLIED:
                cur.execute(_UPDATE_SQL, (req.new_magnitude, req.event_id))
            cur.execute(_INSERT_SQL, (
                req.request_id, req.event_id, req.new_magnitude,
                expected_json(req.expected), outcome,
                expected_json(actual) if actual is not None else None,
            ))
        except integrity_errors:
            # 同号并发：另一方已抢先提交 —— 回滚后读已存记录，按同号规则处理
            conn.rollback()
            rec = fetch_record(cur, req.request_id)
            if rec is None:
                raise
            conn.rollback()
            return _replay_or_conflict(rec, req)
        try:
            conn.commit()
        except Exception as exc:
            # 提交阶段连接中断：源库可能已提交而响应丢失 —— 结果不明
            raise UnknownOutcome(
                f"请求号 {req.request_id} 提交阶段连接中断，源库结果不明"
            ) from exc
        rec = fetch_record(cur, req.request_id)
        conn.rollback()
        if rec is None:                               # 刚提交的回执读不到：按不明处理
            raise UnknownOutcome(f"请求号 {req.request_id} 提交后回执读取失败")
        return rec
    finally:
        try:
            conn.close()
        except Exception:
            pass


# ---------------- 镜像采回状态 ----------------

def receipt_state(mirror_ready, mirror_rec):
    """镜像采回状态：collected=已采回 / pending=采回中 / rebuilding=镜像未就绪。

    判定依据只有一条：就绪镜像的 corrections 表是否存在该请求号的回执
    （回执与更正同一源事务、同笔落库）；不以震级相等或 WAL 位置推断。
    镜像未就绪（含重建）时，旧镜像内容即将被丢弃，采回状态待新镜像重新判定。
    """
    if not mirror_ready:
        return "rebuilding"
    return "collected" if mirror_rec is not None else "pending"
