"""HTTP 路由定义。

正常响应统一形如 {"ok": true, "request_id": ..., "data": {...}}；
失败由中间件归一化为 {"ok": false, "request_id": ..., "error": FailureView}，
正常与失败结论不会混淆。
"""

from __future__ import annotations

from fastapi import APIRouter

from app.context import current_request_id
from app.metrics import registry
from app.services import (
    call_internal_dependency,
    call_internal_dependency_async,
    spawn_background_work,
)

router = APIRouter()


def _ok(data: dict) -> dict:
    return {"ok": True, "request_id": current_request_id(), "data": data}


@router.get("/")
async def read_root() -> dict:
    """健康检查；回显当前关联标识，证明上下文已注入。"""
    return _ok({"service": "request-tracing-observability-local"})


@router.get("/echo/{item}")
async def echo_item(item: str, flag: str = "ok") -> dict:
    """走一次同步内部依赖调用，演示同步链路与失败分类。"""
    output = call_internal_dependency(flag)
    return _ok({"item": item, "output": output, "via": "sync"})


@router.get("/a-echo/{item}")
async def a_echo_item(item: str, flag: str = "ok") -> dict:
    """走异步内部调用，演示 await 链路中的标识延续。"""
    output = await call_internal_dependency_async(flag)
    return _ok({"item": item, "output": output, "via": "async"})


@router.get("/bg/{item}")
async def background_item(item: str, flag: str = "ok") -> dict:
    """派生后台任务并等待其结束，事件序列可证明标识在后台任务中延续。"""
    task = spawn_background_work(item, flag)
    result = await task
    return _ok({"item": item, "background": result, "via": "background"})


@router.get("/metrics")
async def get_metrics() -> dict:
    """查询当前指标快照（查询本身不计入业务指标）。"""
    return registry.snapshot()


@router.post("/metrics/reset")
async def reset_metrics() -> dict:
    """重置指标并返回重置前快照（历史随响应带走，重置不丢计数）。"""
    return {"reset": True, "previous": registry.reset()}
