"""HTTP 路由定义。

正常响应统一形如 {"ok": true, "request_id": ..., "data": {...}}；
失败由中间件归一化为 {"ok": false, "request_id": ..., "error": FailureView}，
正常与失败结论不会混淆。重试失败的 error 为安全重试视图（含逐次尝试结论，
不含底层异常原文）。
"""

from __future__ import annotations

from fastapi import APIRouter, Query

from app.context import current_request_id
from app.events import recorder
from app.metrics import registry
from app.services import (
    call_internal_dependency,
    call_internal_dependency_async,
    call_internal_dependency_in_thread,
    call_internal_dependency_thread_async,
    call_internal_dependency_with_retries,
    call_internal_dependency_with_retries_async,
    spawn_background_work,
    spawn_thread_background_work,
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


@router.get("/retry/{item}")
async def retry_item(
    item: str,
    attempts: list[str] | None = Query(default=None),  # noqa: B008 - FastAPI 声明式参数
) -> dict:
    """下游超时/连接失败的重试链路（同步）。

    attempts 为逐次尝试的 flag 序列（1..5 个，如 timeout,timeout,down）；
    缺省时单次尝试，行为与 /echo 一致。整次业务请求只计一次指标。
    """
    plan = attempts if attempts else ["ok"]
    output = call_internal_dependency_with_retries(plan)
    return _ok({"item": item, "output": output, "via": "retry", "attempts": plan})


@router.get("/a-retry/{item}")
async def a_retry_item(
    item: str,
    attempts: list[str] | None = Query(default=None),  # noqa: B008 - FastAPI 声明式参数
) -> dict:
    """异步重试链路（与 /retry 同口径）。"""
    plan = attempts if attempts else ["ok"]
    output = await call_internal_dependency_with_retries_async(plan)
    return _ok({"item": item, "output": output, "via": "async-retry", "attempts": plan})


@router.get("/thread/{item}")
async def thread_item(item: str, flag: str = "ok") -> dict:
    """把内部调用丢到共享线程池执行（同步等待），验证跨线程标识延续。"""
    output = call_internal_dependency_in_thread(flag)
    return _ok({"item": item, "output": output, "via": "thread"})


@router.get("/a-thread/{item}")
async def a_thread_item(item: str, flag: str = "ok") -> dict:
    """async/线程混合链路：await 线程池执行结果，标识同样延续。"""
    output = await call_internal_dependency_thread_async(flag)
    return _ok({"item": item, "output": output, "via": "thread-async"})


@router.get("/bg-thread/{item}")
async def bg_thread_item(item: str, flag: str = "ok") -> dict:
    """后台派生到线程池：工作在独立线程完成，仍沿用派生请求的标识。"""
    result = await spawn_thread_background_work(item, flag)
    return _ok({"item": item, "background": result, "via": "thread-background"})


@router.get("/metrics")
async def get_metrics() -> dict:
    """查询当前指标快照（查询本身不计入业务指标）。"""
    return registry.snapshot()


@router.post("/metrics/reset")
async def reset_metrics() -> dict:
    """重置指标并返回重置前快照（历史随响应带走，重置不丢计数）。"""
    return {"reset": True, "previous": registry.reset()}


@router.get("/observability/retention")
async def retention_status() -> dict:
    """查询事件与指标的容量/保留期限配置及累计回收统计（只读，不计业务指标）。"""
    return {
        "events": recorder.retention_stats(),
        "metrics": registry.retention_stats(),
    }
