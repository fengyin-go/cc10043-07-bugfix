"""消息载荷存储领域服务：存储消息的原始载荷副本，支持事后审计和回放，管理压缩和过期策略。

数据链路位置：数据归档层，保存原始消息载荷作为全链数据审计和回放的事实来源。

压缩约定：
- 新写入的压缩载荷使用 ``zlib+b64:`` 前缀 + zlib 压缩后的 base64 文本，可逐字节还原；
- 每条压缩记录通过 ``载荷ID`` / ``源消息ID`` 关联回放真正读取的原消息；
- 历史遗留的 ``...[compressed]`` 截断内容无法还原全部字节，只登记为失败，不再冒充成功载荷。
"""
from __future__ import annotations

import base64
import binascii
from datetime import datetime, timezone
from typing import Any

import zlib

from app.services.base import BaseModuleService
from app.store import store

COMPRESSED_PREFIX = "zlib+b64:"
LEGACY_TRUNCATED_MARKER = "...[compressed]"
ENCODING_PLAIN = "原文"
ENCODING_ZLIB = "zlib+b64"
ENCODING_LEGACY = "旧压缩（截断）"

LEGACY_TRUNCATED_REASON = "旧压缩格式已截断，尾字段丢失，无法还原全部字节"


def _raw_payload(row: dict[str, Any]) -> str:
    value = row.get("原始载荷", "")
    return "" if value is None else str(value)


def infer_payload_encoding(row: dict[str, Any]) -> str:
    """判断一条载荷记录当前使用的表示版本；兼容没有编码字段的历史记录。"""
    declared = str(row.get("载荷编码") or "").strip()
    if declared in (ENCODING_ZLIB, "zlib", "zlib+base64", "无损压缩"):
        return ENCODING_ZLIB
    if declared in (ENCODING_LEGACY, "truncated", "legacy"):
        return ENCODING_LEGACY

    payload = _raw_payload(row)
    if payload.startswith(COMPRESSED_PREFIX):
        return ENCODING_ZLIB
    if payload.endswith(LEGACY_TRUNCATED_MARKER):
        return ENCODING_LEGACY
    return declared or ENCODING_PLAIN


def encode_payload(payload: str) -> tuple[str, int]:
    """把原文无损压缩成可自描述的存储表示，返回 (存储文本, 压缩后字节数)。"""
    raw = payload.encode("utf-8")
    compressed = zlib.compress(raw, 9)
    stored = COMPRESSED_PREFIX + base64.b64encode(compressed).decode("ascii")
    return stored, len(compressed)


def decode_payload_row(row: dict[str, Any]) -> tuple[bool, str, str]:
    """按记录自身的版本解码载荷。

    返回 ``(是否可还原, 原文, 失败原因)``。只有能还原全部字节的记录才算成功。
    """
    encoding = infer_payload_encoding(row)
    payload = _raw_payload(row)

    if encoding == ENCODING_LEGACY:
        reason = str(row.get("不可还原原因") or LEGACY_TRUNCATED_REASON)
        return False, "", reason

    if encoding == ENCODING_ZLIB:
        if not payload.startswith(COMPRESSED_PREFIX):
            return False, "", "无损压缩数据缺少编码前缀，无法还原全部字节"
        blob = payload[len(COMPRESSED_PREFIX):]
        try:
            compressed = base64.b64decode(blob, validate=True)
            raw = zlib.decompress(compressed)
        except (binascii.Error, ValueError, zlib.error):
            return False, "", "无损压缩数据损坏，无法还原全部字节"
        try:
            return True, raw.decode("utf-8"), ""
        except UnicodeDecodeError:
            return False, "", "原始载荷不是合法UTF-8文本，无法按文本交付"

    if row.get("可还原") is False:
        return False, "", str(row.get("不可还原原因") or "载荷已被标记为不可还原")
    return True, payload, ""


def select_batch_rows(rows: list[dict[str, Any]], batch_name: str) -> list[dict[str, Any]]:
    """按批次挑记录：有同名批次时只取该批次，否则退回关键字包含匹配。"""
    exact = [r for r in rows if str(r.get("采集批次", "")) == batch_name]
    if exact:
        return exact
    return [r for r in rows if batch_name in str(r.get("采集批次", ""))]


class PayloadStoreService(BaseModuleService):
    MODULE = "payload_store"
    ENTITY = "原始载荷"
    OPERATION_TERM = "采集"
    LIST_FIELDS = ["采集批次", "消息ID", "原始载荷", "存储时间", "压缩后大小"]
    REQUIRED_FIELDS = ["采集批次", "消息ID", "原始载荷"]
    STATUS_ORDER = ["已存储", "已压缩", "已过期"]
    ACTION_RULES = {"批量压缩": "已存储", "标记过期": "已过期"}
    NEGATIVE_ACTIONS = []

    # ---- 业务扩展：载荷管理与归档 ----

    def restore_payload(self, row_or_id: dict[str, Any] | int) -> dict[str, Any]:
        """还原一条载荷记录的完整原文（逐字节），失败时给出原因。"""
        if isinstance(row_or_id, dict):
            row: dict[str, Any] | None = row_or_id
        else:
            row = store.find(self.MODULE, int(row_or_id))
        if row is None:
            return {"ok": False, "payload": "", "原始载荷": "", "reason": "载荷记录不存在"}

        encoding = infer_payload_encoding(row)
        ok, payload, reason = decode_payload_row(row)
        message_id = str(row.get("消息ID") or "")
        return {
            "ok": ok,
            "payload": payload,
            "原始载荷": payload,
            "reason": reason,
            "失败原因": reason,
            "载荷编码": encoding,
            "载荷ID": row.get("id"),
            "消息ID": message_id,
            "源消息ID": str(row.get("源消息ID") or message_id),
            "字节数": len(payload.encode("utf-8")) if ok else 0,
        }

    def batch_compress_payloads(
        self,
        batch_name: str | None = None,
        limit: int | None = None,
        *,
        stop_after: int | None = None,
    ) -> dict[str, Any]:
        """批量压缩指定批次的载荷，写入可逐字节还原的无损压缩数据。

        ``limit`` / ``stop_after`` 用于模拟批量写入中途中断：只处理前 N 条可压缩记录，
        已经处理过的记录保持独立的版本表示，后续回放需要逐条按版本解码。
        已经无损压缩过的记录不会再次登记，重复调用保持幂等。
        """
        rows = list(store.rows(self.MODULE))
        if batch_name:
            rows = select_batch_rows(rows, str(batch_name))

        max_writes = limit if limit is not None else stop_after
        writes = 0
        compressed = 0
        skipped = 0
        failed: list[dict[str, Any]] = []
        now = datetime.now(timezone.utc).isoformat()

        for row in rows:
            if row.get("status") == "已过期":
                continue

            encoding = infer_payload_encoding(row)
            message_id = str(row.get("消息ID") or "")
            source_message_id = str(row.get("源消息ID") or message_id)
            payload_id = row.get("id")

            if encoding == ENCODING_ZLIB:
                # 已经写入无损压缩数据：重复压缩不重复登记。
                ok, _, reason = decode_payload_row(row)
                if ok and row.get("可还原") is not False:
                    skipped += 1
                    continue
                row["可还原"] = False
                row["压缩结果"] = "失败"
                row["不可还原原因"] = reason
                failed.append({
                    "载荷ID": payload_id,
                    "消息ID": message_id,
                    "源消息ID": source_message_id,
                    "reason": reason,
                    "失败原因": reason,
                    "载荷编码": encoding,
                })
                continue

            if encoding == ENCODING_LEGACY:
                # 历史截断内容无论重试多少次都无法恢复全部字节，只登记失败。
                row["载荷编码"] = ENCODING_LEGACY
                row["源消息ID"] = source_message_id
                row["载荷ID"] = payload_id
                row["可还原"] = False
                row["压缩结果"] = "失败"
                row["不可还原原因"] = LEGACY_TRUNCATED_REASON
                failed.append({
                    "载荷ID": payload_id,
                    "消息ID": message_id,
                    "源消息ID": source_message_id,
                    "reason": LEGACY_TRUNCATED_REASON,
                    "失败原因": LEGACY_TRUNCATED_REASON,
                    "载荷编码": encoding,
                })
                continue

            if max_writes is not None and writes >= max_writes:
                break

            original = _raw_payload(row)
            stored_text, compressed_size = encode_payload(original)
            row["原始载荷"] = stored_text
            row["载荷编码"] = ENCODING_ZLIB
            row["载荷ID"] = payload_id
            row["源消息ID"] = source_message_id
            row["原消息ID"] = source_message_id
            row["原始大小"] = str(len(original.encode("utf-8")))
            row["压缩后大小"] = str(compressed_size)
            row["可还原"] = True
            row["压缩结果"] = "成功"
            row["压缩时间"] = now
            row["status"] = "已压缩"
            compressed += 1
            writes += 1

        message = (
            f"批量压缩完成：成功{compressed}条，跳过{skipped}条，失败{len(failed)}条"
        )
        return {
            "ok": True,
            "batch": batch_name,
            "message": message,
            "compressed": compressed,
            "已压缩": compressed,
            "skipped": skipped,
            "已跳过": skipped,
            "failed": failed,
            "failed_count": len(failed),
            "失败数": len(failed),
            "fully_restorable": len(failed) == 0,
        }

    def get_replay_source(self, topic: str, limit: int = 100) -> list[dict[str, Any]]:
        """从载荷存储提取回放数据源。"""
        rows = store.rows(self.MODULE)
        if not topic:
            return []
        matched = [
            r for r in select_batch_rows(rows, str(topic))
            if r.get("status") in ("已存储", "已压缩")
        ]
        matched.sort(key=lambda r: int(r.get("id", 0) or 0))
        return matched[:limit]

    def validate_batch_integrity(self, batch_name: str) -> dict[str, Any]:
        """校验采集批次的存储完整性，并区分可还原与不可还原的载荷。"""
        rows = store.rows(self.MODULE)
        batch_rows = select_batch_rows(rows, batch_name)
        total = len(batch_rows)
        stored = len([r for r in batch_rows if r.get("status") == "已存储"])
        compressed = len([r for r in batch_rows if r.get("status") == "已压缩"])
        expired = len([r for r in batch_rows if r.get("status") == "已过期"])
        restorable = len([
            r for r in batch_rows
            if r.get("status") in ("已存储", "已压缩") and decode_payload_row(r)[0]
        ])
        unrestorable = len([
            r for r in batch_rows
            if r.get("status") in ("已存储", "已压缩") and not decode_payload_row(r)[0]
        ])
        return {"batch": batch_name, "total": total, "stored": stored,
                "compressed": compressed, "expired": expired,
                "restorable": restorable, "unrestorable": unrestorable,
                "intact": total > 0 and expired == 0 and unrestorable == 0}

    def route_to_watermark(self, batch_name: str) -> str | None:
        """载荷批次路由到水位追踪。"""
        batch_rows = select_batch_rows(store.rows(self.MODULE), batch_name)
        if not batch_rows:
            return None
        now = datetime.now(timezone.utc).isoformat()
        watermark_rows = store.rows("watermark_tracker")
        wid = max((int(r.get("id", 0)) for r in watermark_rows), default=0) + 1
        watermark_rows.append({
            "id": wid, "数据源": batch_name,
            "水位字段": "载荷计数", "当前水位": str(len(batch_rows)),
            "上次推进时间": now, "延迟分钟数": "0",
            "status": "正常水位", "pending": True, "abnormal": False,
        })
        return now

    def export_snapshot(self, batch_name: str) -> dict[str, Any]:
        """导出载荷快照用于离线分析，快照中落原文而不是压缩文本。"""
        restored: list[dict[str, Any]] = []
        for row in self.get_replay_source(batch_name, limit=500):
            ok, payload, reason = decode_payload_row(row)
            restored.append({
                "载荷ID": row.get("id"),
                "消息ID": row.get("消息ID", ""),
                "源消息ID": row.get("源消息ID") or row.get("消息ID", ""),
                "原始载荷": payload if ok else None,
                "可还原": ok,
                "失败原因": reason,
            })
        export_rows = store.rows("stream_export")
        eid = max((int(r.get("id", 0)) for r in export_rows), default=0) + 1
        now = datetime.now(timezone.utc).isoformat()
        export_rows.append({
            "id": eid, "任务名称": f"载荷快照-{batch_name}",
            "数据源": batch_name, "导出格式": "json",
            "时间范围": now, "文件大小": str(len(str(restored))),
            "status": "排队中", "pending": True, "abnormal": False,
        })
        return {"ok": True, "export_id": eid, "payload_count": len(restored),
                "restored": sum(1 for item in restored if item["可还原"]),
                "failed": sum(1 for item in restored if not item["可还原"])}

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        """动作入口：批量压缩走无损压缩流程，不再只改状态。"""
        if action == "批量压缩":
            entry = store.find(self.MODULE, entry_id)
            if entry is None:
                return None, f"{self.ENTITY} {entry_id} 不存在或已归档"
            summary = self.batch_compress_payloads(str(entry.get("采集批次", "")))
            return store.find(self.MODULE, entry_id) or entry, str(summary["message"])
        return super().run_action(entry_id, action)
