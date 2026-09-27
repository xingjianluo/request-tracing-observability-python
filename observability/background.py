"""后台任务执行器：把请求关联标识延续到线程池。

实现要点：在**提交任务的线程内**调用 :func:`contextvars.copy_context`
捕获当前请求上下文，再在工作线程内通过 ``ctx.run`` 执行任务。
这样每个后台任务都固定绑定到提交它的那个请求，多个请求同时提交
后台任务也不会串号。

为保证本地测试的确定性，:func:`run_in_background` 提交后返回一个
:class:`BackgroundJob`，默认 ``wait`` 时（测试用法）同步等待结束并
回收异常；异常会被分类记录但不会吞没成“正常结果”。
"""
from __future__ import annotations

import logging
import threading
import contextvars
from dataclasses import dataclass
from typing import Any, Callable

from .context import current_request_id
from .errors import ClassifiedError, classify

log = logging.getLogger("observability.background")


@dataclass
class BackgroundJob:
    request_id: str
    _thread: threading.Thread
    _error: list[BaseException]

    def wait(self, timeout: float | None = None) -> None:
        """等待后台任务结束；若任务失败，重新抛出已分类异常。"""
        self._thread.join(timeout)
        if self._thread.is_alive():
            raise TimeoutError("background job did not finish in time")
        if self._error:
            raise self._error[0]

    @property
    def done(self) -> bool:
        return not self._thread.is_alive()

    @property
    def error(self) -> BaseException | None:
        return self._error[0] if self._error else None


def _make_worker(ctx: contextvars.Context, func: Callable, args: tuple,
                 kwargs: dict, sink: list, request_id: str) -> Callable[[], None]:
    def worker() -> None:
        def body() -> None:
            log.info("background job start",
                     extra={"request_id": request_id})
            try:
                func(*args, **kwargs)
            except BaseException as exc:  # 分类保留，不与成功混淆
                classified = classify(exc)
                log.error(
                    "background job failed",
                    extra={
                        "request_id": request_id,
                        "error_category": classified.category.value,
                        "reason_chain": classified.reason_chain(),
                    },
                )
                sink.append(classified)
            else:
                log.info("background job done",
                         extra={"request_id": request_id})
        ctx.run(body)

    return worker


def run_in_background(func: Callable, *args: Any, **kwargs: Any) -> BackgroundJob:
    """捕获当前请求上下文并在守护线程中执行 ``func``。"""
    ctx = contextvars.copy_context()
    request_id = current_request_id()
    sink: list[BaseException] = []
    thread = threading.Thread(
        target=_make_worker(ctx, func, args, kwargs, sink, request_id),
        name=f"bg-{request_id}",
        daemon=True,
    )
    thread.start()
    return BackgroundJob(request_id=request_id, _thread=thread, _error=sink)
