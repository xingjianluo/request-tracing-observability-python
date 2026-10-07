"""HTTP 路由定义。

正常响应统一形如 {"ok": true, "request_id": ..., "data": {...}}；
失败由中间件归一化为 {"ok": false, "request_id": ..., "error": FailureView}，
正常与失败结论不会混淆。
"""

from __future__ import annotations

from fastapi import APIRouter

from app.context import current_request_id
from app.metrics import registry
from app.retry import run_with_retry
from app.services import (
    call_internal_dependency,
    call_internal_dependency_async,
    call_internal_dependency_in_thread,
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


@router.get("/thread/{item}")
async def thread_item(item: str, flag: str = "ok") -> dict:
    """把内部调用派生到独立线程池执行，演示跨线程的标识延续。"""
    result = await call_internal_dependency_in_thread(flag)
    return _ok({"item": item, **result, "via": "thread"})


@router.get("/mix/{item}")
async def mix_item(item: str, flag: str = "ok") -> dict:
    """同一请求内串行走完 同步/异步/线程 三条链路，事件标识应完全一致。"""
    sync_out = call_internal_dependency(flag)
    async_out = await call_internal_dependency_async(flag)
    thread_out = await call_internal_dependency_in_thread(flag)
    return _ok(
        {
            "item": item,
            "via": "mixed",
            "sync": sync_out,
            "async": async_out,
            "thread": thread_out["output"],
        }
    )


@router.get("/retry/{item}")
async def retry_item(item: str, flags: str = "ok", max_attempts: int = 3) -> dict:
    """带重试的下游调用：多次尝试归并为一个统一结论。

    flags 为逗号分隔的逐次尝试输入（如 flags=timeout,down,ok），
    用于本地确定性复现“前几次失败、最后一次成功”等场景。
    无论中间重试几次，本请求在指标中只计一次（由中间件统一计数）。
    """
    attempt_inputs = [f.strip() for f in flags.split(",") if f.strip()] or ["ok"]
    verdict = run_with_retry(
        call_internal_dependency,
        attempt_inputs=attempt_inputs,
        max_attempts=max_attempts,
    )
    if not verdict.ok:
        assert verdict.error is not None
        raise verdict.error
    return _ok(
        {
            "item": item,
            "output": verdict.result,
            "via": "retry",
            "attempts": verdict.attempts_summary(),
            "verdict_reason": verdict.reason,
        }
    )


@router.get("/metrics")
async def get_metrics() -> dict:
    """查询当前指标快照（查询本身不计入业务指标）。"""
    return registry.snapshot()


@router.post("/metrics/reset")
async def reset_metrics() -> dict:
    """重置指标并返回重置前快照（历史随响应带走，重置不丢计数）。"""
    return {"reset": True, "previous": registry.reset()}
