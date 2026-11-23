"""批量消息回放领域服务：将历史消息按时间范围回放到指定Topic，用于数据修复和断点续传。

数据链路位置：数据修复层，在审计发现偏差时通过批量回放机制修复遗漏或失败的消息。

回放约定：
- 逐条读取载荷存储，按每条记录自身的表示版本解码（原文 / 无损压缩 / 旧截断）；
- 每次交付都记录真正读取到的载荷记录与源消息ID，避免压缩记录和原消息脱钩；
- 无法还原全部字节的旧截断消息不进入成功交付，单独列为失败并送入失败隔离区。
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from app.services.base import BaseModuleService
from app.services.payload_store import (
    decode_payload_row,
    infer_payload_encoding,
    select_batch_rows,
)
from app.store import store


class BatchReplayService(BaseModuleService):
    MODULE = "batch_replay"
    ENTITY = "回放任务"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["回放范围", "开始时间", "结束时间", "目标Topic", "回放速率"]
    REQUIRED_FIELDS = ["回放范围", "开始时间", "结束时间"]
    STATUS_ORDER = ["待回放", "回放中", "已完成", "回放中断"]
    ACTION_RULES = {"启动回放": "回放中", "中断回放": "回放中断"}
    NEGATIVE_ACTIONS = []

    # ---- 业务扩展：批量回放和断点续传 ----

    def start_replay(self, entry_id: int, limit: int | None = None) -> dict[str, Any]:
        """启动回放任务：从载荷存储逐条读取、解码并记录实际交付。

        同一批记录可能同时包含原文、无损压缩和旧截断三种表示，
        因此逐条按版本解码，不能假设整批只有一种编码。
        """
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {"ok": False, "message": "回放任务不存在"}

        target = str(entry.get("目标Topic", ""))
        payloads = store.rows("payload_store")
        matched = [
            p for p in (select_batch_rows(payloads, target) if target else [])
            if p.get("status") in ("已存储", "已压缩")
        ]
        matched.sort(key=lambda p: int(p.get("id", 0) or 0))
        if limit is not None:
            matched = matched[:max(int(limit), 0)]

        entry["status"] = "回放中"
        entry["pending"] = True
        entry["abnormal"] = False
        now = datetime.now(timezone.utc).isoformat()

        delivered: list[dict[str, Any]] = []
        failures: list[dict[str, Any]] = []
        version_counts: dict[str, int] = {}

        for payload_row in matched:
            encoding = infer_payload_encoding(payload_row)
            version_counts[encoding] = version_counts.get(encoding, 0) + 1
            ok, payload, reason = decode_payload_row(payload_row)
            source_message_id = str(
                payload_row.get("源消息ID") or payload_row.get("消息ID") or "")
            base = {
                "载荷ID": payload_row.get("id"),
                "源载荷ID": payload_row.get("id"),
                "消息ID": str(payload_row.get("消息ID") or ""),
                "源消息ID": source_message_id,
                "载荷编码": encoding,
                "回放批次": target,
                "回放时间": now,
            }
            if ok:
                delivered.append({
                    **base,
                    "原始载荷": payload,
                    "payload": payload,
                    "字节数": len(payload.encode("utf-8")),
                    "status": "已交付",
                    "可还原": True,
                })
            else:
                failures.append({
                    **base,
                    "失败原因": reason,
                    "reason": reason,
                    "status": "失败",
                    "可还原": False,
                })

        entry["交付明细"] = delivered
        entry["失败明细"] = failures
        entry["交付数"] = len(delivered)
        entry["失败数"] = len(failures)
        entry["最近回放时间"] = now
        entry["版本分布"] = version_counts
        if failures:
            entry["abnormal"] = True

        isolation_created = self._isolate_failures(entry, failures)

        message = (
            f"回放逐条完成：交付{len(delivered)}条，失败{len(failures)}条"
        )
        return {
            "ok": True,
            "message": message,
            "entry": entry,
            "replayed": len(delivered),
            "交付数": len(delivered),
            "delivered": delivered,
            "failures": failures,
            "失败明细": failures,
            "failed": len(failures),
            "failed_count": len(failures),
            "isolation_created": isolation_created,
            "版本分布": version_counts,
        }

    def _isolate_failures(
        self, entry: dict[str, Any], failures: list[dict[str, Any]]
    ) -> int:
        """把无法还原的消息送入失败隔离区；重复回放不重复登记同一条失败。"""
        if not failures:
            return 0
        isolation_rows = store.rows("failure_isolation")
        created = 0
        now = datetime.now(timezone.utc).isoformat()
        target = str(entry.get("目标Topic", ""))
        for failure in failures:
            message_id = str(failure.get("消息ID") or failure.get("源消息ID") or "")
            existing = next(
                (
                    row for row in isolation_rows
                    if str(row.get("失败消息ID", "")) == message_id
                    and str(row.get("原始Topic", "")) == target
                    and row.get("status") in ("待处理", "重试中")
                ),
                None,
            )
            if existing is not None:
                continue
            fid = max((int(r.get("id", 0)) for r in isolation_rows), default=0) + 1
            isolation_rows.append({
                "id": fid,
                "失败消息ID": message_id,
                "失败原因": str(failure.get("失败原因", "")),
                "原始Topic": target,
                "隔离时间": now,
                "重试次数": "0",
                "源载荷ID": failure.get("载荷ID"),
                "status": "待处理",
                "pending": True,
                "abnormal": False,
            })
            created += 1
        return created

    def get_last_delivery(self, entry_id: int) -> dict[str, Any]:
        """读取最近一次回放实际交付的明细。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {"ok": False, "message": "回放任务不存在"}
        delivered = list(entry.get("交付明细") or [])
        failures = list(entry.get("失败明细") or [])
        return {
            "ok": True,
            "message": f"最近回放交付{len(delivered)}条，失败{len(failures)}条",
            "entry": entry,
            "replayed": len(delivered),
            "交付数": len(delivered),
            "delivered": delivered,
            "failures": failures,
            "失败明细": failures,
            "failed": len(failures),
            "failed_count": len(failures),
        }

    def validate_replay_range(self, entry_id: int) -> dict[str, Any]:
        """校验回放时间范围是否有效且与载荷存储中的数据匹配。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {"ok": False, "message": "任务不存在"}
        start_str = entry.get("开始时间", "")
        end_str = entry.get("结束时间", "")
        if not start_str or not end_str:
            return {"ok": False, "message": "缺少开始或结束时间"}
        if start_str > end_str:
            return {"ok": False, "message": "开始时间不能晚于结束时间"}
        payloads = store.rows("payload_store")
        target = entry.get("目标Topic", "")
        in_range = [p for p in payloads if target in str(p.get("采集批次", ""))]
        return {"ok": True, "in_range_count": len(in_range), "range": f"{start_str}~{end_str}"}

    def handle_interrupt(self, entry_id: int, reason: str = "手动中断回放") -> dict[str, Any]:
        """处理回放中断：记录中断原因并清理上下文。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {"ok": False, "message": "任务不存在"}
        entry["status"] = "回放中断"
        entry["abnormal"] = True
        now = datetime.now(timezone.utc).isoformat()
        isolation_rows = store.rows("failure_isolation")
        fid = max((int(r.get("id", 0)) for r in isolation_rows), default=0) + 1
        isolation_rows.append({
            "id": fid, "失败消息ID": f"replay-err-{entry_id}",
            "失败原因": reason, "原始Topic": entry.get("目标Topic", ""),
            "隔离时间": now, "重试次数": "0",
            "status": "待处理", "pending": True, "abnormal": False,
        })
        return {"ok": True, "message": "回放中断已记录", "isolation_id": fid, "entry": entry}

    def complete_with_audit(self, entry_id: int) -> dict[str, Any]:
        """回放完成时联动审计和偏移量管理。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {"ok": False, "message": "任务不存在"}
        entry["status"] = "已完成"
        entry["pending"] = False
        entry["abnormal"] = bool(entry.get("失败数"))
        target = entry.get("目标Topic", "")
        now = datetime.now(timezone.utc).isoformat()
        offset_rows = store.rows("offset_manager")
        for row in offset_rows:
            if target in str(row.get("Topic/Queue", "")):
                try:
                    current = int(row.get("当前Offset", "0"))
                except ValueError:
                    continue
                row["当前Offset"] = str(current + 1)
                row["消费延迟"] = "0"
        audit_rows = store.rows("stream_audit")
        aid = max((int(r.get("id", 0)) for r in audit_rows), default=0) + 1
        delivered = int(entry.get("交付数", 0) or 0)
        failed = int(entry.get("失败数", 0) or 0)
        audit_rows.append({
            "id": aid, "处理时间": now, "数据源": target,
            "消息总数": str(delivered + failed), "成功数": str(delivered),
            "失败原因分布": "" if not failed else f"不可还原载荷{failed}条",
            "status": "正常" if not failed else "偏差超标",
            "pending": True, "abnormal": bool(failed),
        })
        return {"ok": True, "message": "回放完成已审计"}

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        """动作入口：启动回放真正逐条交付，中断回放记录失败原因。"""
        if action == "启动回放":
            result = self.start_replay(entry_id)
            entry = store.find(self.MODULE, entry_id)
            return (entry if result.get("ok") else None), str(result.get("message", ""))
        if action == "中断回放":
            result = self.handle_interrupt(entry_id)
            entry = store.find(self.MODULE, entry_id)
            return (entry if result.get("ok") else None), str(result.get("message", ""))
        return super().run_action(entry_id, action)
