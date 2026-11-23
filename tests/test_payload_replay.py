"""载荷压缩与批量回放的回归测试：无损压缩、逐条解码、失败明细与幂等。

运行：backend/.venv/bin/python -m unittest discover -s tests -v
"""
from __future__ import annotations

import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "backend"))

from app.services import service_for  # noqa: E402
from app.store import store  # noqa: E402

LONG = "x" * 100 + "TAIL"
SHORT = "short-payload-短原文"
BATCH = "A"


def replay_id() -> int:
    rows = [r for r in store.rows("batch_replay") if r.get("目标Topic") == BATCH]
    return int(rows[0]["id"])


def reset_batch() -> None:
    """把批次 A 恢复成初始三态，避免用例之间互相污染。"""
    for row in store.rows("payload_store"):
        if row.get("采集批次") != BATCH:
            continue
        message_id = row.get("消息ID", "")
        row.pop("载荷编码", None)
        row.pop("可还原", None)
        row.pop("压缩结果", None)
        row.pop("源消息ID", None)
        row.pop("载荷ID", None)
        if message_id == "A-legacy-truncated":
            row["原始载荷"] = LONG[:100] + "...[compressed]"
            row["status"] = "已压缩"
            continue
        row["原始载荷"] = SHORT if message_id == "A-101-short" else LONG
        row["status"] = "已存储"
        row["压缩后大小"] = str(len(row["原始载荷"]))
    entry = store.find("batch_replay", replay_id())
    if entry is not None:
        entry.pop("交付明细", None)
        entry.pop("失败明细", None)
        entry.pop("交付数", None)
        entry.pop("失败数", None)
        entry.pop("版本分布", None)
        entry["status"] = "待回放"


class PayloadReplayTest(unittest.TestCase):
    def setUp(self) -> None:
        reset_batch()
        self.store_service = service_for("payload_store")
        self.replay_service = service_for("batch_replay")

    def test_interrupted_compress_then_replay_keeps_mixed_versions(self) -> None:
        first = self.store_service.batch_compress_payloads(BATCH, limit=40)
        self.assertEqual(first["compressed"], 40)
        self.assertEqual(first["failed_count"], 0)

        result = self.replay_service.start_replay(replay_id())
        self.assertEqual(result["交付数"], 101)
        self.assertEqual(result["failed_count"], 1)
        self.assertEqual(result["版本分布"]["zlib+b64"], 40)
        self.assertEqual(result["版本分布"]["原文"], 61)

        delivered = result["delivered"]
        long_rows = [
            d for d in delivered
            if d["源消息ID"].startswith("A-") and d["源消息ID"][2:].isdigit()
        ]
        self.assertEqual(len(long_rows), 100)
        self.assertTrue(all(d["原始载荷"] == LONG for d in long_rows))
        short_rows = [d for d in delivered if d["源消息ID"] == "A-101-short"]
        self.assertEqual(len(short_rows), 1)
        self.assertEqual(short_rows[0]["原始载荷"], SHORT)

        failures = result["failures"]
        self.assertEqual([f["源消息ID"] for f in failures], ["A-legacy-truncated"])
        self.assertIn("截断", failures[0]["失败原因"])

    def test_retry_compress_is_lossless_and_idempotent(self) -> None:
        self.store_service.batch_compress_payloads(BATCH, limit=40)
        second = self.store_service.batch_compress_payloads(BATCH)
        self.assertEqual(second["compressed"], 61)
        self.assertEqual(second["skipped"], 40)
        self.assertEqual(second["failed_count"], 1)

        third = self.store_service.batch_compress_payloads(BATCH)
        self.assertEqual(third["compressed"], 0)
        self.assertEqual(third["skipped"], 101)
        self.assertEqual(third["failed_count"], 1)

        result = self.replay_service.start_replay(replay_id())
        self.assertEqual(result["版本分布"].get("zlib+b64"), 101)
        self.assertEqual(result["failed_count"], 1)
        self.assertTrue(all(d["原始载荷"] in (LONG, SHORT) for d in result["delivered"]))
        self.assertTrue(
            all(d["载荷ID"] == d["源载荷ID"] and d["源消息ID"] for d in result["delivered"])
        )

    def test_legacy_truncation_is_never_a_success_payload(self) -> None:
        legacy = next(
            r for r in store.rows("payload_store") if r.get("消息ID") == "A-legacy-truncated")
        restored = self.store_service.restore_payload(legacy["id"])
        self.assertFalse(restored["ok"])
        self.assertEqual(restored["原始载荷"], "")

        first = self.replay_service.start_replay(replay_id())
        isolation_before = len(store.rows("failure_isolation"))
        second = self.replay_service.start_replay(replay_id())
        self.assertEqual(len(store.rows("failure_isolation")), isolation_before)
        self.assertEqual(first["failed_count"], second["failed_count"])

    def test_compressed_rows_keep_source_message_link(self) -> None:
        self.store_service.batch_compress_payloads(BATCH)
        row = next(
            r for r in store.rows("payload_store") if r.get("消息ID") == "A-001")
        self.assertEqual(row["载荷编码"], "zlib+b64")
        self.assertEqual(row["源消息ID"], "A-001")
        self.assertEqual(row["载荷ID"], row["id"])
        self.assertTrue(row["原始载荷"].startswith("zlib+b64:"))
        self.assertEqual(self.store_service.restore_payload(row["id"])["原始载荷"], LONG)


if __name__ == "__main__":
    unittest.main()
