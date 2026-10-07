"""跨线程执行的关联标识延续。

规则：
- ThreadPoolExecutor 的 worker 线程不会自动继承提交时刻的 contextvars，
  必须在提交时抓取当前 request_id，并在 worker 内用 run_with_context
  重新绑定（copy_context 隔离，worker 之间互不污染）；
- 派生时刻的标识即工作项全程使用的标识：同一请求在同步、异步、线程
  几条混合链路里产生的事件 request_id 完全一致；
- 多个请求并发派生时各自绑定各自标识，互不串用；
- 执行器有界（固定 worker 数、守护线程），进程退出不悬挂。
"""

from __future__ import annotations

import asyncio
import functools
import os
import threading
from collections.abc import Callable
from concurrent.futures import Future, ThreadPoolExecutor

from app.context import current_request_id, run_with_context, snapshot

_WORKERS = int(os.environ.get("OBS_THREAD_WORKERS", "8"))

#: 进程级共享执行器（有界）。worker 为守护线程，避免阻塞进程退出。
executor = ThreadPoolExecutor(max_workers=_WORKERS, thread_name_prefix="obs-worker")


def _propagating_call[T](request_id: str, func: Callable[..., T], *args, **kwargs) -> T:
    """在 worker 线程内重绑定派生时刻的标识后执行（copy_context 隔离）。"""
    return run_with_context(request_id, func, *args, **kwargs)


def submit_with_context[T](func: Callable[..., T], *args, **kwargs) -> Future[T]:
    """把同步函数派生到共享线程池，延续提交时刻的 request_id。

    提交时刻抓取标识；若调用方不在任何请求上下文内，则现场生成一个
    （与 context.snapshot 同口径），保证派生工作永远有可归属标识。
    """
    rid = current_request_id() or snapshot().request_id
    return executor.submit(functools.partial(_propagating_call, rid, func, *args, **kwargs))


async def run_in_thread[T](func: Callable[..., T], *args, **kwargs) -> T:
    """await 形态的派生：在共享线程池执行 func，延续当前 request_id。"""
    rid = current_request_id() or snapshot().request_id
    loop = asyncio.get_running_loop()
    call = functools.partial(_propagating_call, rid, func, *args, **kwargs)
    return await loop.run_in_executor(executor, call)


def current_thread_name() -> str:
    """测试辅助：返回当前线程名，用于证明工作确实跑在 worker 线程。"""
    return threading.current_thread().name
