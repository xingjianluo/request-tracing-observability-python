"""请求级关联标识（request id）的生成、读取与透传。

规则：
- 每个入站请求生成或继承一个稳定标识；
- 通过 contextvars 在同步/异步内部调用之间自动延续；
- asyncio 每个 Task 在创建时拷贝当前 context，故并发请求互不串用；
- 后台线程不会自动继承 contextvars，统一在 worker 内重新绑定，保证延续。
"""

from __future__ import annotations

import uuid
from collections.abc import Callable
from contextvars import ContextVar, copy_context
from dataclasses import dataclass
from typing import Any, TypeVar

#: 当前请求的关联标识；未在请求上下文内时为 None。
request_id_var: ContextVar[str | None] = ContextVar("request_id", default=None)

#: 标识前缀，便于在日志中肉眼识别。
_REQUEST_ID_PREFIX = "req-"

T = TypeVar("T")


@dataclass(frozen=True, slots=True)
class RequestContext:
    """单次请求的关联上下文快照。"""

    request_id: str


def new_request_id() -> str:
    """生成一个新的请求关联标识：req-<32 位十六进制>。"""
    return f"{_REQUEST_ID_PREFIX}{uuid.uuid4().hex}"


def current_request_id() -> str | None:
    """读取当前上下文的关联标识；不在请求上下文内返回 None。"""
    return request_id_var.get()


def bind_request_id(request_id: str) -> Any:
    """把关联标识绑定到当前上下文，返回供 reset 使用的 token。"""
    return request_id_var.set(request_id)


def reset_request_id(token: Any) -> None:
    """根据 bind 返回的 token 恢复上下文。"""
    request_id_var.reset(token)


def snapshot() -> RequestContext:
    """抓取当前关联上下文快照（无标识时现场生成一个并绑定）。"""
    rid = request_id_var.get()
    if rid is None:
        rid = new_request_id()
        request_id_var.set(rid)
    return RequestContext(request_id=rid)


def run_with_context(request_id: str, func: Callable[..., T], *args: Any, **kwargs: Any) -> T:
    """在携带指定关联标识的隔离上下文中执行同步函数。

    使用 copy_context() 隔离，调用方上下文不被污染；适用于后台线程
    （ThreadPoolExecutor 的 worker 不会自动继承提交时刻的 contextvars）。
    """

    def _runner() -> T:
        bind_request_id(request_id)
        return func(*args, **kwargs)

    return copy_context().run(_runner)
