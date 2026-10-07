"""模拟内部服务调用与后台任务（本地可复现，不依赖外部服务/真实凭据）。

所有“故障”均由入参 flag 确定性触发，便于测试复现：
- flag=ok      正常返回
- flag=down    底层连接被拒 -> 适配层包装为 DependencyDownError（保留 cause）
- flag=timeout 底层超时（内置 TimeoutError）-> 归因层判为 timeout
- flag=bad     契约错误 ValueError -> client_error
- flag=boom    未预期 RuntimeError -> internal_error

同步调用、异步调用、asyncio 后台任务、线程池执行、带重试的下游调用五条路径
都打印结构化事件，事件中的 request_id 直接来自 contextvars，用于证明标识延续。

新增路径：
- call_internal_dependency_with_retries(_async)：同一次业务请求对下游重试，
  由 app.retries 统一归因，对外只产出一个失败结论；
- call_internal_dependency_in_thread / call_internal_dependency_thread_async：
  把内部调用丢到 ThreadPoolExecutor 执行；线程不自动继承 contextvars，
  统一用 run_with_context 在 worker 内重新绑定为“派生时刻”的 request_id。
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Sequence
from concurrent.futures import ThreadPoolExecutor
from typing import Any

from app.context import current_request_id, run_with_context
from app.errors import DependencyDownError
from app.events import recorder
from app.retries import run_with_retries, run_with_retries_async

VALID_FLAGS = {"ok", "down", "timeout", "bad", "boom"}

#: 线程池执行路径共享的进程内执行器（worker 不继承提交时刻的 contextvars，
#: 故每次提交都用 run_with_context 显式重绑定派生请求的标识）。
_thread_pool = ThreadPoolExecutor(max_workers=8, thread_name_prefix="obs-dep")


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


# ---------------------------------------------------------------------------
# 新路径 1：下游调用带重试（同步 / 异步同口径）
# ---------------------------------------------------------------------------


def _validate_flags(flags: Sequence[str]) -> tuple[str, ...]:
    """校验逐次尝试的 flag 序列；非法立即作为客户端契约错误抛出。"""
    flags = tuple(flags)
    if not flags:
        raise ValueError("attempt flags must not be empty")
    if len(flags) > 5:
        raise ValueError("at most 5 attempts allowed")
    bad = [f for f in flags if f not in VALID_FLAGS]
    if bad:
        raise ValueError(f"unknown dependency flag(s): {bad!r}; allowed={sorted(VALID_FLAGS)}")
    return flags


def call_internal_dependency_with_retries(flags: Sequence[str]) -> str:
    """同步下游调用：按 flags 逐次尝试，失败由 retries 层重试并统一归因。

    同一次业务请求的多次尝试不产生业务指标；全部失败时抛 RetryExhaustedError。
    """
    plan = _validate_flags(flags)
    rid = current_request_id()
    recorder.record(
        "dependency.retry.start",
        mode="sync",
        flags=list(plan),
        max_attempts=len(plan),
        request_id=rid,
        input={"flags": list(plan), "mode": "sync"},
    )
    result = run_with_retries(
        lambda attempt: _do_call(plan[attempt - 1]),
        max_attempts=len(plan),
        name="internal-dependency",
    )
    recorder.record("dependency.retry.success", mode="sync", output=result, request_id=rid)
    return result


async def call_internal_dependency_with_retries_async(flags: Sequence[str]) -> str:
    """异步下游调用（与同步同口径）。"""
    plan = _validate_flags(flags)
    rid = current_request_id()
    recorder.record(
        "dependency.retry.start",
        mode="async",
        flags=list(plan),
        max_attempts=len(plan),
        request_id=rid,
        input={"flags": list(plan), "mode": "async"},
    )

    async def _attempt(attempt: int) -> str:
        await asyncio.sleep(0)
        return _do_call(plan[attempt - 1])

    result = await run_with_retries_async(
        _attempt, max_attempts=len(plan), name="internal-dependency"
    )
    recorder.record("dependency.retry.success", mode="async", output=result, request_id=rid)
    return result


# ---------------------------------------------------------------------------
# 新路径 2：丢到独立线程池执行（worker 重新绑定派生时刻的 request_id）
# ---------------------------------------------------------------------------


def _thread_call(flag: str, parent_rid: str) -> str:
    """线程 worker 体：在隔离上下文中重新绑定父请求标识后执行同步调用。

    run_with_context 用 copy_context() 隔离：worker 读到的始终是派生请求的
    request_id，线程池复用时也不会读到上一个任务或其他请求的标识。
    """
    def _body() -> str:
        rid = current_request_id()
        recorder.record("thread.worker.start", flag=flag, request_id=rid, bound_from=parent_rid)
        output = call_internal_dependency(flag)
        recorder.record("thread.worker.done", flag=flag, output=output, request_id=rid)
        return output

    return run_with_context(parent_rid, _body)


def call_internal_dependency_in_thread(flag: str) -> str:
    """把同步内部调用提交到共享线程池并阻塞等待结果。"""
    parent_rid = current_request_id()
    _validate_flag(flag)
    recorder.record("thread.submit", mode="thread", flag=flag, request_id=parent_rid)
    future = _thread_pool.submit(_thread_call, flag, parent_rid)
    return future.result()


async def call_internal_dependency_thread_async(flag: str) -> str:
    """在线程池中执行内部调用，await 其完成（混合 async/线程链路）。"""
    parent_rid = current_request_id()
    _validate_flag(flag)
    recorder.record("thread.submit", mode="thread-async", flag=flag, request_id=parent_rid)
    return await asyncio.wrap_future(_thread_pool.submit(_thread_call, flag, parent_rid))


async def spawn_thread_background_work(item: str, flag: str = "ok") -> dict[str, Any]:
    """“后台派生”场景：请求处理中派生一个线程池任务并等待其结束。

    与 asyncio 后台任务不同，这里工作发生在独立线程里；标识仍由
    run_with_context 绑定为派生时刻父请求的 request_id。
    """
    parent_rid = current_request_id()
    _validate_flag(flag)
    recorder.record(
        "thread.background.spawn", item=item, flag=flag, request_id=parent_rid
    )
    output = await call_internal_dependency_thread_async(flag)
    return {"item": item, "output": output, "request_id": parent_rid, "via": "thread"}
