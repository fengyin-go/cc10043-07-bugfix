"""通用接口契约；模块字段由领域服务持有，不再为每个模块复制一份模型类。"""
from __future__ import annotations

from typing import Any, Generic, TypeVar

from pydantic import BaseModel, Field

T = TypeVar("T")


class PageResult(BaseModel, Generic[T]):
    items: list[T]
    total: int
    page: int = 1
    size: int = 20


class ActionResult(BaseModel):
    ok: bool
    message: str
    entry: dict[str, Any] | None = None


class EntryPayload(BaseModel):
    values: dict[str, Any] = Field(default_factory=dict)
    remark: str | None = None


class ActionPayload(BaseModel):
    action: str
    remark: str | None = None


class CompressPayload(BaseModel):
    """批量压缩入参：可按批次筛选，并可用 limit/stop_after 模拟中途中断。"""
    batch: str | None = None
    batch_name: str | None = None
    采集批次: str | None = None
    limit: int | None = Field(default=None, ge=0)
    stop_after: int | None = Field(default=None, ge=0)

    def resolved_batch(self) -> str | None:
        for value in (self.batch, self.batch_name, self.采集批次):
            if value:
                return value
        return None


class ReplayStartPayload(BaseModel):
    """回放入参：limit 限制本次逐条交付的记录数。"""
    limit: int | None = Field(default=None, ge=0)
