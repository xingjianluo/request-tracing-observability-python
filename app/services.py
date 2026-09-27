"""模拟内部服务调用与后台任务（本地可复现，不依赖外部服务/真实凭据）。

所有“故障”均由入参 flag 确定性触发，便于测试复现：
- flag=ok      正常返回
- flag=down    底层连接被拒 -> 适配层包装为 DependencyDownError（保留 cause）
- flag=timeout 底层超时（内置 TimeoutError）-> 归因层判为 timeout
- flag=bad     契约错误 ValueError -> client_error
- flag=boom    未预期 RuntimeError -> internal_error

同步调用、异步调用、asyncio 后台任务三条路径都打印结构化事件，
事件中的 request_id 直接来自 contextvars，用于证明标识延续。
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable
from typing import Any

from app.context import current_request_id
from app.errors import DependencyDownError
from app.events import recorder

VALID_FLAGS = {"ok", "down", "timeout", "bad", "boom"}


def _validate_flag(flag: str) -> None:
    if flag not in VALID_FLAGS:
        # 模拟下游契约校验失败
        raise ValueError(f"unknown dependency flag: {flag!r}; allowed={sorted(VALID_FLAGS)}")


def call_internal_dependency(flag: str = "ok") -> str:
    """同步内部调用，可能抛出各类已分类/未分类异常。"""
    rid = current_request_id()
    recorder.record("dependency.call.start", mode="sync", flag=flag, input={"flag": flag})
    try:
        result = _do_call(flag)
    except Exception as exc:
        recorder.record(
            "dependency.call.failure",
            level="error",
            mode="sync",
            flag=flag,
            raised_type=type(exc).__name__,
            request_id=rid,
        )
        raise
    recorder.record("dependency.call.success", mode="sync", flag=flag)
    return result


async def call_internal_dependency_async(flag: str = "ok") -> str:
    """异步内部调用（同口径），用于演示 await 链路中的标识延续。"""
    rid = current_request_id()
    recorder.record("dependency.call.start", mode="async", flag=flag, input={"flag": flag})
    try:
        await asyncio.sleep(0)  # 让出一次事件循环，制造真实交错
        result = _do_call(flag)
    except Exception as exc:
        recorder.record(
            "dependency.call.failure",
            level="error",
            mode="async",
            flag=flag,
            raised_type=type(exc).__name__,
            request_id=rid,
        )
        raise
    recorder.record("dependency.call.success", mode="async", flag=flag)
    return result


def _do_call(flag: str) -> str:
    _validate_flag(flag)
    if flag == "down":
        # 模拟底层网络异常，适配层翻译为已分类错误，但保留原始原因
        try:
            raise ConnectionRefusedError("connect failed: 127.0.0.1:6379 (fake)")
        except ConnectionRefusedError as raw:
            raise DependencyDownError(
                cause=raw,
                reason="adapter mapped ConnectionRefusedError to dependency_down",
            ) from raw
    if flag == "timeout":
        # 不经过适配层，直接由归因层按异常类型判定
        raise TimeoutError("fake deadline exceeded after 50ms")
    if flag == "bad":
        raise ValueError("fake payload validation failed: missing 'user_id'")
    if flag == "boom":
        raise RuntimeError("fake unexpected null dereference")
    return f"dependency-result[{flag}]"


async def _background_job(item: str, flag: str) -> dict[str, Any]:
    """后台任务体：作为独立 Task 调度，仍能读到派生时刻的 request_id。"""
    rid = current_request_id()
    recorder.record("background.start", item=item, flag=flag, request_id=rid)
    await asyncio.sleep(0)
    output = await call_internal_dependency_async(flag)
    recorder.record("background.done", item=item, output=output, request_id=rid)
    return {"item": item, "output": output, "request_id": rid}


def spawn_background_work(item: str, flag: str = "ok") -> Awaitable[dict[str, Any]]:
    """派生 asyncio 后台任务。

    asyncio.create_task 会拷贝当前 context，故任务内 request_id
    与派生出它的请求一致；任务之间、任务与其他请求之间互不串用。
    返回任务句柄，调用方可 await 以保证本地测试确定性。
    """
    parent_rid = current_request_id()
    recorder.record("background.spawn", item=item, flag=flag, request_id=parent_rid)
    return asyncio.create_task(_background_job(item, flag))
