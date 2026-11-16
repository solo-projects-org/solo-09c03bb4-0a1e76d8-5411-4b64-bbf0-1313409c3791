#!/usr/bin/env python3
"""核心逻辑单元测试（仅标准库，本机可直接运行）：

  python3 -I tests/test_core.py

覆盖：LSN 换算、时间规范化、wal2json 事务应用的幂等与原子性、
     读事务快照一致性（每次取数只读同一已提交状态）、
     震级更正的整行核对 / 固定请求号幂等 / 结果不明重试不重复改写、
     更正回执随同事务落入镜像。
"""
import os
import sys
import tempfile
import unittest
from datetime import datetime, timezone
from decimal import Decimal

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "app"))

from canonical import canonical_line, norm_ts            # noqa: E402
from changes import apply_transaction                    # noqa: E402
from correction import (                                 # noqa: E402
    ConflictContent,
    CorrectionRequest,
    UnknownOutcome,
    ValidationError,
    build_expected,
    decide,
    expected_json,
    new_request_id,
    norm_magnitude,
    receipt_state,
    submit_correction,
    validate_request_id,
)
from lsnutil import int_to_lsn, lsn_to_int               # noqa: E402
from store import Store                                  # noqa: E402

TS = "2026-11-14 08:00:00.123456+00"


def change(kind, eid, ts=TS, lat=34.567, lon=100.123, mag=4.5):
    if kind == "delete":
        return {
            "kind": "delete", "schema": "public", "table": "seismic_events",
            "oldkeys": {"keynames": ["event_id"], "keytypes": ["text"], "keyvalues": [eid]},
        }
    return {
        "kind": kind, "schema": "public", "table": "seismic_events",
        "columnnames": ["event_id", "origin_time", "latitude", "longitude", "magnitude"],
        "columntypes": ["text", "timestamp with time zone", "numeric", "numeric", "numeric"],
        "columnvalues": [eid, ts, lat, lon, mag],
    }


def change_rename(eid_old, eid_new):
    ch = change("update", eid_new)
    ch["oldkeys"] = {"keynames": ["event_id"], "keytypes": ["text"], "keyvalues": [eid_old]}
    return ch


def corr_change(kind, rid, eid="A", mag="6.6", expected='{"event_id":"A"}',
                outcome="applied", source_row=None):
    """corrections 回执表的 wal2json 行变更。"""
    if kind == "delete":
        return {
            "kind": "delete", "schema": "public", "table": "corrections",
            "oldkeys": {"keynames": ["request_id"], "keytypes": ["text"], "keyvalues": [rid]},
        }
    return {
        "kind": kind, "schema": "public", "table": "corrections",
        "columnnames": ["request_id", "event_id", "new_magnitude", "expected",
                        "outcome", "source_row", "submitted_at"],
        "columntypes": ["text", "text", "numeric", "text", "text", "text",
                        "timestamp with time zone"],
        "columnvalues": [rid, eid, mag, expected, outcome, source_row, TS],
    }


class LsnTest(unittest.TestCase):
    def test_roundtrip(self):
        for s in ("0/0", "0/1B3C4D0", "1B3/4A0", "FF/FFFFFFFF", "2/9A08C800"):
            self.assertEqual(int_to_lsn(lsn_to_int(s)), s)
        self.assertLess(lsn_to_int("0/10"), lsn_to_int("1/0"))


class NormTsTest(unittest.TestCase):
    def test_load_path_equals_replication_path(self):
        from_load = norm_ts(datetime(2026, 11, 14, 8, 0, 0, 123456, tzinfo=timezone.utc))
        from_wal = norm_ts("2026-11-14 08:00:00.123456+00")
        self.assertEqual(from_load, from_wal)
        self.assertEqual(from_wal, "2026-11-14T08:00:00.123456Z")

    def test_other_offsets_normalized_to_utc(self):
        self.assertEqual(
            norm_ts("2026-11-14 16:00:00+08:00"), "2026-11-14T08:00:00.000000Z"
        )

    def test_canonical_line_decimal_float_equal(self):
        a = canonical_line("EQ1", "T", Decimal("34.500"), Decimal("100.100"), Decimal("4.0"))
        b = canonical_line("EQ1", "T", 34.5, 100.1, 4.0)
        self.assertEqual(a, b)
        self.assertEqual(a, "EQ1|T|34.500|100.100|4.0")


class StoreTestBase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.store = Store(os.path.join(self.tmp.name, "mirror.db"))

    def tearDown(self):
        self.tmp.cleanup()

    def apply(self, payload, lsn):
        with self.store.write_txn() as cur:
            counts = apply_transaction(cur, payload)
            Store.set_meta(cur, "last_applied_lsn", lsn)
            return counts

    def rows(self):
        return self.store.read(
            "SELECT event_id, origin_time, latitude, longitude, magnitude FROM events ORDER BY event_id"
        )


class ApplyTest(StoreTestBase):
    def test_insert_update_delete_flow(self):
        self.apply({"change": [change("insert", "A"), change("insert", "B", mag=6.1)]}, "0/10")
        self.assertEqual([r[0] for r in self.rows()], ["A", "B"])

        self.apply({"change": [change("update", "A", mag=5.5)]}, "0/20")
        row_a = [r for r in self.rows() if r[0] == "A"][0]
        self.assertEqual(row_a[4], 5.5)
        self.assertEqual(row_a[1], "2026-11-14T08:00:00.123456Z")   # 时间已规范化

        self.apply({"change": [change("delete", "B")]}, "0/30")
        self.assertEqual([r[0] for r in self.rows()], ["A"])
        self.assertEqual(self.store.get_meta("last_applied_lsn"), "0/30")

    def test_replay_is_idempotent(self):
        """保存后未确认 -> 源库重发 -> 幂等重放，最终状态不变、不重。"""
        txns = [
            ({"change": [change("insert", "A"), change("insert", "B")]}, "0/10"),
            ({"change": [change("update", "A", mag=7.2)]}, "0/20"),
            ({"change": [change("delete", "B")]}, "0/30"),
        ]
        for payload, lsn in txns:
            self.apply(payload, lsn)
        before = self.rows()
        for payload, lsn in txns:          # 模拟崩溃重启后整段重发
            self.apply(payload, lsn)
        self.assertEqual(self.rows(), before)

    def test_old_value_never_overwrites_new(self):
        """按 LSN 顺序应用：旧事务重放只会被后续新事务再次覆盖为新值。"""
        self.apply({"change": [change("insert", "A", mag=3.0)]}, "0/10")
        self.apply({"change": [change("update", "A", mag=8.8)]}, "0/20")
        # 崩溃重放：旧 insert 又来一遍，随后新 update 也重放，最终仍是新值
        self.apply({"change": [change("insert", "A", mag=3.0)]}, "0/10")
        self.apply({"change": [change("update", "A", mag=8.8)]}, "0/20")
        self.assertEqual(self.rows()[0][4], 8.8)

    def test_primary_key_rename(self):
        self.apply({"change": [change("insert", "OLD")]}, "0/10")
        self.apply({"change": [change_rename("OLD", "NEW")]}, "0/20")
        self.assertEqual([r[0] for r in self.rows()], ["NEW"])

    def test_truncate_ignored(self):
        self.apply({"change": [change("insert", "A")]}, "0/10")
        counts = self.apply({"change": [{"kind": "truncate", "schema": "public", "table": "seismic_events"}]}, "0/20")
        self.assertEqual(counts["other"], 1)
        self.assertEqual(len(self.rows()), 1)

    def test_transaction_atomicity_on_failure(self):
        """一笔源事务应用一半失败 -> 整体回滚，数据与位置都不前移。"""
        with self.assertRaises(RuntimeError):
            with self.store.write_txn() as cur:
                apply_transaction(cur, {"change": [change("insert", "A"), change("insert", "B")]})
                Store.set_meta(cur, "last_applied_lsn", "0/10")
                raise RuntimeError("模拟写入失败")
        self.assertEqual(self.rows(), [])
        self.assertIsNone(self.store.get_meta("last_applied_lsn"))

    def test_whole_transaction_visible_together(self):
        """一笔多行事务提交后整笔可见；读事务只看到提交前或提交后。"""
        with self.store.write_txn() as cur:
            apply_transaction(cur, {"change": [change("insert", f"E{i:02d}") for i in range(20)]})
            Store.set_meta(cur, "last_applied_lsn", "0/10")
        self.assertEqual(len(self.rows()), 20)


class SnapshotReadTest(StoreTestBase):
    def test_read_txn_sees_one_committed_state(self):
        self.apply({"change": [change("insert", "A")]}, "0/10")
        txn = self.store.read_txn()
        try:
            n1 = txn.execute("SELECT count(*) FROM events").fetchone()[0]
            # 读事务打开期间，写者提交了新事务
            self.apply({"change": [change("insert", "B")]}, "0/20")
            n2 = txn.execute("SELECT count(*) FROM events").fetchone()[0]
        finally:
            txn.close()
        self.assertEqual((n1, n2), (1, 1))          # 同一已提交状态
        self.assertEqual(self.store.read("SELECT count(*) FROM events")[0][0], 2)  # 新读见新态


class CorrectionApplyTest(StoreTestBase):
    """回执经 wal2json 落入镜像：与更正同笔事务、幂等重放。"""

    def corr_rows(self):
        return self.store.read(
            "SELECT request_id, event_id, new_magnitude, outcome, submitted_at, mirrored_at "
            "FROM corrections ORDER BY request_id"
        )

    def test_correction_and_event_land_together(self):
        """更正与回执在同一源事务 => 镜像同笔落库，凭回执即可判定已采回。"""
        self.apply({"change": [change("insert", "A", mag=4.5)]}, "0/10")
        self.apply({
            "change": [
                change("update", "A", mag=6.6),
                corr_change("insert", "RQ1", "A", "6.6"),
            ]
        }, "0/20")
        row = [r for r in self.rows() if r[0] == "A"][0]
        self.assertEqual(row[4], 6.6)                       # 更正已落镜像
        corr = self.corr_rows()
        self.assertEqual(len(corr), 1)                      # 回执同笔到达
        self.assertEqual(corr[0][0], "RQ1")
        self.assertEqual(corr[0][2], 6.6)
        self.assertEqual(corr[0][4], "2026-11-14T08:00:00.123456Z")  # 源库提交时间已规范化
        self.assertTrue(corr[0][5])                         # 镜像落库时间已记录

    def test_correction_replay_idempotent(self):
        payload = {"change": [change("insert", "A"), corr_change("insert", "RQ1")]}
        self.apply(payload, "0/10")
        before = (self.rows(), self.corr_rows())
        self.apply(payload, "0/10")                         # 崩溃后源库重发
        after = (self.rows(), self.corr_rows())
        self.assertEqual(before[0], after[0])
        self.assertEqual(len(after[1]), 1)
        self.assertEqual(before[1][0][:5], after[1][0][:5])  # 内容不变（mirrored_at 可刷新）

    def test_correction_delete(self):
        self.apply({"change": [corr_change("insert", "RQ1")]}, "0/10")
        self.apply({"change": [corr_change("delete", "RQ1")]}, "0/20")
        self.assertEqual(self.corr_rows(), [])

    def test_unknown_table_ignored(self):
        self.apply({"change": [change("insert", "A")]}, "0/10")
        counts = self.apply({"change": [{
            "kind": "insert", "schema": "public", "table": "something_else",
            "columnnames": ["x"], "columntypes": ["text"], "columnvalues": ["1"],
        }]}, "0/20")
        self.assertEqual(counts["other"], 1)
        self.assertEqual(len(self.rows()), 1)


# ---------------- 震级更正：纯逻辑 ----------------

def make_expected(eid="A", mag="4.5"):
    return build_expected(eid, "2026-11-14T08:00:00.123456Z", "34.567", "100.123", mag)


def make_req(rid="RQ20261116-abcdef01", eid="A", new_mag="6.6", expected=None):
    return CorrectionRequest(rid, expected or make_expected(eid), norm_magnitude(new_mag))


def source_event_row(eid="A", mag="4.5"):
    return (
        eid,
        datetime(2026, 11, 14, 8, 0, 0, 123456, tzinfo=timezone.utc),
        Decimal("34.567"), Decimal("100.123"), Decimal(mag),
    )


class ValidationTest(unittest.TestCase):
    def test_norm_magnitude(self):
        self.assertEqual(norm_magnitude("4.5"), "4.5")
        self.assertEqual(norm_magnitude("4"), "4.0")
        self.assertEqual(norm_magnitude("4.50"), "4.5")
        self.assertEqual(norm_magnitude("-99.9"), "-99.9")
        self.assertEqual(norm_magnitude(" 9.9 "), "9.9")
        for bad in ("", "abc", "4.55", "100", "-100", "1e2", "NaN"):
            with self.assertRaises(ValidationError, msg=bad):
                norm_magnitude(bad)

    def test_request_id_validation(self):
        rid = new_request_id()
        self.assertEqual(validate_request_id(rid), rid)
        self.assertEqual(validate_request_id("  RQ20261116-abcdef01 "), "RQ20261116-abcdef01")
        for bad in ("", "ab", "has space", "中文号", "-lead-dash", "x" * 65):
            with self.assertRaises(ValidationError, msg=bad):
                validate_request_id(bad)

    def test_build_expected_normalizes(self):
        e = make_expected()
        self.assertEqual(e["origin_time"], "2026-11-14T08:00:00.123456Z")
        self.assertEqual(e["latitude"], "34.567")
        self.assertEqual(e["magnitude"], "4.5")
        with self.assertRaises(ValidationError):
            build_expected("A", "not-a-time", "34.567", "100.123", "4.5")
        with self.assertRaises(ValidationError):
            build_expected("A", "2026-11-14T08:00:00Z", "91", "100.123", "4.5")

    def test_expected_json_canonical(self):
        a = expected_json(make_expected())
        b = expected_json(make_expected())
        self.assertEqual(a, b)
        self.assertIn('"event_id":"A"', a)


class DecideTest(unittest.TestCase):
    def test_applied_when_row_matches(self):
        outcome, actual = decide(make_expected(), source_event_row())
        self.assertEqual(outcome, "applied")
        self.assertEqual(actual, make_expected())

    def test_mismatch_when_row_differs(self):
        outcome, actual = decide(make_expected(), source_event_row(mag="5.0"))
        self.assertEqual(outcome, "rejected_mismatch")
        self.assertEqual(actual["magnitude"], "5.0")

    def test_deleted_when_row_missing(self):
        outcome, actual = decide(make_expected(), None)
        self.assertEqual(outcome, "rejected_deleted")
        self.assertIsNone(actual)


class ReceiptStateTest(unittest.TestCase):
    def test_states(self):
        self.assertEqual(receipt_state(True, ("x",)), "collected")
        self.assertEqual(receipt_state(True, None), "pending")
        # 镜像未就绪（含重建）：旧镜像内容即将被丢弃，采回状态待新镜像重新判定
        self.assertEqual(receipt_state(False, ("x",)), "rebuilding")
        self.assertEqual(receipt_state(False, None), "rebuilding")


# ---------------- 震级更正：提交流程（模拟源库） ----------------

class FakeIntegrityError(Exception):
    pass


class FakeOperationalError(Exception):
    pass


class FakeDB:
    """极简源库模拟：seismic_events / corrections 两表 + 事务（暂存-提交/回滚）。

    commit_mode:
      ok   正常提交；
      lost 提交生效但响应丢失（客户端收到连接错误）——对应“源库已提交但响应丢失”；
      fail 提交生效前断连，源库回滚。
    hide_selects: 前 N 次 corrections 查询返回空（模拟同号并发：查不到、插入才撞键）。
    """

    def __init__(self):
        self.events = {}          # event_id -> [eid, datetime, Decimal, Decimal, Decimal]
        self.corrections = {}     # request_id -> tuple
        self.update_count = 0     # UPDATE 执行次数（改写次数）
        self.commit_mode = "ok"
        self.hide_selects = 0

    def connect(self):
        return FakeConn(self)


class FakeConn:
    def __init__(self, db):
        self.db = db
        self._events = None
        self._corrections = None

    def cursor(self):
        return FakeCur(self)

    def txn(self):
        if self._events is None:
            self._events = {k: list(v) for k, v in self.db.events.items()}
            self._corrections = dict(self.db.corrections)
        return self._events, self._corrections

    def commit(self):
        self.txn()
        if self.db.commit_mode == "fail":
            self._events = self._corrections = None      # 源库回滚
            raise FakeOperationalError("connection lost before commit")
        self.db.events = {k: list(v) for k, v in self._events.items()}
        self.db.corrections = dict(self._corrections)
        self._events = self._corrections = None
        if self.db.commit_mode == "lost":
            raise FakeOperationalError("response lost after commit")

    def rollback(self):
        self._events = self._corrections = None

    def close(self):
        pass


class FakeCur:
    def __init__(self, conn):
        self.conn = conn
        self._result = []

    def execute(self, sql, params=()):
        db = self.conn.db
        events, corrections = self.conn.txn()
        if sql.startswith("SELECT request_id"):
            if db.hide_selects > 0:
                db.hide_selects -= 1
                row = None
            else:
                row = corrections.get(params[0])
            self._result = [row] if row else []
        elif sql.startswith("SELECT event_id"):
            row = events.get(params[0])
            self._result = [tuple(row)] if row else []
        elif sql.startswith("UPDATE seismic_events"):
            mag, eid = params
            assert eid in events, "更正前已锁定存在的行"
            events[eid][4] = Decimal(mag)
            db.update_count += 1
            self._result = []
        elif sql.startswith("INSERT INTO corrections"):
            rid, eid, mag, expected, outcome, source_row = params
            if rid in corrections:
                raise FakeIntegrityError("duplicate key value violates unique constraint")
            corrections[rid] = (
                rid, eid, Decimal(mag), expected, outcome, source_row,
                datetime(2026, 11, 16, 1, 2, 3, 456789, tzinfo=timezone.utc),
            )
            self._result = []
        else:
            raise AssertionError(f"未模拟的 SQL: {sql}")

    def fetchone(self):
        return self._result[0] if self._result else None


class SubmitFlowTest(unittest.TestCase):
    RID = "RQ20261116-abcdef01"

    def setUp(self):
        self.db = FakeDB()
        self.db.events["A"] = list(source_event_row())

    def submit(self, req):
        return submit_correction(self.db.connect, req, integrity_errors=(FakeIntegrityError,))

    def test_applied_writes_magnitude_and_receipt(self):
        rec = self.submit(make_req())
        self.assertEqual(rec["outcome"], "applied")
        self.assertFalse(rec["replay"])
        self.assertEqual(self.db.events["A"][4], Decimal("6.6"))
        self.assertEqual(self.db.update_count, 1)
        self.assertIn(self.RID, self.db.corrections)
        self.assertEqual(rec["submitted_at"], "2026-11-16T01:02:03.456789Z")

    def test_mismatch_rejected_no_rewrite(self):
        self.db.events["A"][4] = Decimal("5.0")     # 源库已被他人改动
        rec = self.submit(make_req())
        self.assertEqual(rec["outcome"], "rejected_mismatch")
        self.assertEqual(self.db.events["A"][4], Decimal("5.0"))
        self.assertEqual(self.db.update_count, 0)
        self.assertEqual(rec["source_row"]["magnitude"], "5.0")

    def test_deleted_rejected(self):
        del self.db.events["A"]
        rec = self.submit(make_req())
        self.assertEqual(rec["outcome"], "rejected_deleted")
        self.assertIsNone(rec["source_row"])
        self.assertEqual(self.db.update_count, 0)

    def test_same_id_same_content_returns_original(self):
        first = self.submit(make_req())
        again = self.submit(make_req())
        self.assertTrue(again["replay"])
        self.assertEqual(again["outcome"], first["outcome"])
        self.assertEqual(again["submitted_at"], first["submitted_at"])
        self.assertEqual(self.db.update_count, 1)   # 未重复改写
        self.assertEqual(len(self.db.corrections), 1)

    def test_same_id_different_content_rejected(self):
        self.submit(make_req())
        with self.assertRaises(ConflictContent) as cm:
            self.submit(make_req(new_mag="7.7"))
        self.assertEqual(cm.exception.stored["new_magnitude"], "6.6")
        self.assertEqual(self.db.update_count, 1)
        self.assertEqual(len(self.db.corrections), 1)

    def test_commit_lost_retry_returns_stored_without_rewrite(self):
        """源库已提交但响应丢失：重试返回原结果，不得重复改写。"""
        self.db.commit_mode = "lost"
        with self.assertRaises(UnknownOutcome):
            self.submit(make_req())
        self.assertIn(self.RID, self.db.corrections)          # 源库其实已提交
        self.assertEqual(self.db.events["A"][4], Decimal("6.6"))
        self.db.commit_mode = "ok"
        rec = self.submit(make_req())                          # 凭原号原内容重试
        self.assertTrue(rec["replay"])
        self.assertEqual(rec["outcome"], "applied")
        self.assertEqual(self.db.update_count, 1)              # 没有第二次改写
        self.assertEqual(len(self.db.corrections), 1)

    def test_commit_failed_retry_reexecutes(self):
        """提交前断连（源库已回滚）：重试重新执行，最终只生效一次。"""
        self.db.commit_mode = "fail"
        with self.assertRaises(UnknownOutcome):
            self.submit(make_req())
        self.assertEqual(len(self.db.corrections), 0)          # 未提交
        self.assertEqual(self.db.events["A"][4], Decimal("4.5"))
        self.db.commit_mode = "ok"
        rec = self.submit(make_req())
        self.assertEqual(rec["outcome"], "applied")
        self.assertEqual(self.db.events["A"][4], Decimal("6.6"))
        self.assertEqual(len(self.db.corrections), 1)

    def test_concurrent_same_id_insert_collision(self):
        """同号并发：插入撞唯一键 -> 回滚重读 -> 按同号同内容返回原结果。"""
        self.submit(make_req())
        self.db.hide_selects = 1                                 # 预查看不到，插入才撞键
        rec = self.submit(make_req())
        self.assertTrue(rec["replay"])
        self.assertEqual(self.db.update_count, 1)
        self.assertEqual(len(self.db.corrections), 1)


if __name__ == "__main__":
    unittest.main(verbosity=2)
