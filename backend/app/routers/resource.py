"""统一资源接口：查询、导出、登记和动作按模块 key 分派到领域服务。"""
from __future__ import annotations

import csv
import io
from typing import Any

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import StreamingResponse

from app.schemas import (
    ActionPayload,
    ActionResult,
    CompressPayload,
    EntryPayload,
    PageResult,
    ReplayStartPayload,
)
from app.services import service_for

router = APIRouter(prefix="/api", tags=["业务资源"])


def require_service(module: str):
    service = service_for(module)
    if service is None:
        raise HTTPException(status_code=404, detail=f"未知业务模块：{module}")
    return service


# ---- 载荷压缩与回放交付：固定路径必须排在 /{module}/{entry_id} 之前 ----

@router.post("/payload_store/compress", response_model=dict)
def compress_payloads(payload: CompressPayload | None = None) -> dict[str, Any]:
    """批量写入无损压缩数据；limit 用于模拟中途中断，只处理前 N 条。"""
    service = require_service("payload_store")
    body = payload or CompressPayload()
    result = service.batch_compress_payloads(
        body.resolved_batch(), limit=body.limit, stop_after=body.stop_after)
    return result


@router.post("/payload_store/{entry_id}/restore", response_model=dict)
def restore_payload(entry_id: int) -> dict[str, Any]:
    """还原单条载荷的完整原文，无法还原时返回失败原因。"""
    service = require_service("payload_store")
    entry = service.get_entry(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"原始载荷 {entry_id} 不存在或已归档")
    return service.restore_payload(entry_id)


@router.post("/batch_replay/{entry_id}/start", response_model=dict)
def start_replay(
    entry_id: int,
    payload: ReplayStartPayload | None = None,
    limit: int | None = Query(default=None),
) -> dict[str, Any]:
    """启动回放：逐条解码载荷并返回实际交付与失败明细。"""
    service = require_service("batch_replay")
    body = payload or ReplayStartPayload()
    effective_limit = body.limit if body.limit is not None else limit
    result = service.start_replay(entry_id, limit=effective_limit)
    if not result.get("ok"):
        raise HTTPException(status_code=404, detail=result.get("message", "回放任务不存在"))
    return result


@router.get("/batch_replay/{entry_id}/delivery", response_model=dict)
def replay_delivery(entry_id: int) -> dict[str, Any]:
    """查看回放任务最近一次逐条交付的结果。"""
    service = require_service("batch_replay")
    result = service.get_last_delivery(entry_id)
    if not result.get("ok"):
        raise HTTPException(status_code=404, detail=result.get("message", "回放任务不存在"))
    return result


@router.get("/{module}", response_model=PageResult[dict])
def list_entries(
    module: str,
    request: Request,
    keyword: str | None = Query(default=None),
    status: str | None = Query(default=None),
    page: int = Query(default=1, ge=1),
    size: int = Query(default=20, ge=1, le=200),
) -> PageResult[dict]:
    service = require_service(module)
    filters = {
        key: value.strip()
        for key, value in request.query_params.items()
        if key in service.LIST_FIELDS and value.strip()
    }
    items, total = service.list_entries(
        filters=filters, keyword=keyword, status=status, page=page, size=size)
    return PageResult(items=items, total=total, page=page, size=size)


@router.get("/{module}/export")
def export_entries(
    module: str,
    request: Request,
    keyword: str | None = Query(default=None),
    status: str | None = Query(default=None),
) -> StreamingResponse:
    service = require_service(module)
    filters = {
        key: value.strip()
        for key, value in request.query_params.items()
        if key in service.LIST_FIELDS and value.strip()
    }
    items, _ = service.list_entries(
        filters=filters, keyword=keyword, status=status, page=1, size=1_000_000)
    output = io.StringIO()
    output.write("\ufeff")
    writer = csv.writer(output, lineterminator="\n")
    writer.writerow(["id", *service.LIST_FIELDS, "status"])
    for item in items:
        values = [item.get("id"), *(item.get(field, "") for field in service.LIST_FIELDS),
                  item.get("status", "")]
        writer.writerow([
            "'" + str(value) if str(value or "").startswith(("=", "+", "-", "@")) else value
            for value in values
        ])
    return StreamingResponse(
        iter([output.getvalue()]), media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{module}.csv"'},
    )


@router.get("/{module}/{entry_id}", response_model=dict)
def get_entry(module: str, entry_id: int) -> dict[str, Any]:
    service = require_service(module)
    entry = service.get_entry(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"{service.ENTITY} {entry_id} 不存在或已归档")
    return entry


@router.post("/{module}", response_model=ActionResult)
def create_entry(module: str, payload: EntryPayload) -> ActionResult:
    service = require_service(module)
    entry, missing = service.create_entry(payload.values)
    if missing:
        return ActionResult(ok=False, message=f"缺少必填字段：{'、'.join(missing)}")
    return ActionResult(ok=True, message=f"{service.ENTITY}已登记", entry=entry)


@router.post("/{module}/{entry_id}/actions", response_model=ActionResult)
def run_action(module: str, entry_id: int, payload: ActionPayload) -> ActionResult:
    service = require_service(module)
    entry, message = service.run_action(entry_id, payload.action.strip())
    return ActionResult(ok=entry is not None, message=message, entry=entry)
