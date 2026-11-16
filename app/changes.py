"""wal2json（format-version 1）行变更 -> SQLite 镜像。

wal2json format 1：一条消息 = 一笔源事务，形如
  {"change": [ {"kind": "insert"|"update"|"delete", ...}, ... ], "xid": ..., ...}

复制两张源表：
- public.seismic_events -> events（震例目录）；
- public.corrections    -> corrections（震级更正回执）。回执与对应的震例更正
  在同一源事务提交，故在镜像端同笔落库 —— 回执到达即更正到达。

应用规则：
- insert/update：按主键 upsert（幂等 —— 源库重发时重复应用结果不变）；
- update 若改了主键：先按 oldkeys 删旧行再 upsert 新行；
- delete：按 oldkeys 主键删除（幂等 —— 删不存在的行是无害空操作）；
- 只处理行变更：truncate 等其他消息、未复制的表，记录并忽略。

变更只可能按 LSN 顺序被应用（单槽单消费者），且每条 wal2json 消息携带的是
该事务提交时的整行新值，因此旧值永远不会盖回新值。
"""
from canonical import norm_ts
from store import utcnow

UPSERT_SQL = """
INSERT INTO events(event_id, origin_time, latitude, longitude, magnitude)
VALUES (?, ?, ?, ?, ?)
ON CONFLICT(event_id) DO UPDATE SET
    origin_time = excluded.origin_time,
    latitude    = excluded.latitude,
    longitude   = excluded.longitude,
    magnitude   = excluded.magnitude
"""

DELETE_SQL = "DELETE FROM events WHERE event_id = ?"

CORR_UPSERT_SQL = """
INSERT INTO corrections(request_id, event_id, new_magnitude, expected, outcome,
                        source_row, submitted_at, mirrored_at)
VALUES (?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(request_id) DO UPDATE SET
    event_id      = excluded.event_id,
    new_magnitude = excluded.new_magnitude,
    expected      = excluded.expected,
    outcome       = excluded.outcome,
    source_row    = excluded.source_row,
    submitted_at  = excluded.submitted_at,
    mirrored_at   = excluded.mirrored_at
"""

CORR_DELETE_SQL = "DELETE FROM corrections WHERE request_id = ?"


def _old_key(change, name):
    keys = change.get("oldkeys") or {}
    names = keys.get("keynames") or []
    if name in names:
        return keys["keyvalues"][names.index(name)]
    return None


def _apply_event_change(cur, change):
    kind = change.get("kind")
    if kind in ("insert", "update"):
        cols = dict(zip(change["columnnames"], change["columnvalues"]))
        if kind == "update":
            old_id = _old_key(change, "event_id")
            if old_id is not None and old_id != cols["event_id"]:
                cur.execute(DELETE_SQL, (old_id,))   # 主键被改：先删旧行
        cur.execute(
            UPSERT_SQL,
            (
                cols["event_id"],
                norm_ts(cols["origin_time"]),
                float(cols["latitude"]),
                float(cols["longitude"]),
                float(cols["magnitude"]),
            ),
        )
        return kind
    if kind == "delete":
        old_id = _old_key(change, "event_id")
        if old_id is not None:
            cur.execute(DELETE_SQL, (old_id,))
        return kind
    return None  # truncate 等：只处理行变更


def _apply_correction_change(cur, change):
    kind = change.get("kind")
    if kind in ("insert", "update"):
        cols = dict(zip(change["columnnames"], change["columnvalues"]))
        if kind == "update":
            old_id = _old_key(change, "request_id")
            if old_id is not None and old_id != cols["request_id"]:
                cur.execute(CORR_DELETE_SQL, (old_id,))
        cur.execute(
            CORR_UPSERT_SQL,
            (
                cols["request_id"],
                cols["event_id"],
                float(cols["new_magnitude"]),
                cols["expected"],
                cols["outcome"],
                cols.get("source_row"),
                norm_ts(cols["submitted_at"]),
                utcnow(),
            ),
        )
        return kind
    if kind == "delete":
        old_id = _old_key(change, "request_id")
        if old_id is not None:
            cur.execute(CORR_DELETE_SQL, (old_id,))
        return kind
    return None


def apply_change(cur, change):
    """应用一条行变更，返回 'insert'/'update'/'delete'；非行变更/未复制的表返回 None。"""
    table = change.get("table")
    if table == "seismic_events":
        return _apply_event_change(cur, change)
    if table == "corrections":
        return _apply_correction_change(cur, change)
    return None


def apply_transaction(cur, payload):
    """把一笔源事务的全部行变更应用到（调用方持有的）同一个 SQLite 事务里。

    调用方负责提交/回滚 —— 整笔源事务要么整笔可见，要么完全不可见。
    """
    counts = {"insert": 0, "update": 0, "delete": 0, "other": 0}
    for change in payload.get("change", []):
        kind = apply_change(cur, change)
        counts[kind if kind else "other"] += 1
    return counts
