"""Request-level correlation ID context.

并发安全说明：使用 :mod:`contextvars` 保存当前请求上下文。
每个 asyncio 任务（以及经 ``copy_context()`` 派生出的后台线程）
都持有独立的上下文副本，因此并发请求之间不会串用或丢失标识。
"""
from __future__ import annotations

import contextvars
import re
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass

REQUEST_ID_HEADER = "X-Request-ID"
_REQUEST_ID_PREFIX = "req-"
# 入站透传标识仅接受保守字符集，避免日志注入与伪造结构。
_INBOUND_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]{0,126}$")

#: 未绑定上下文时日志中使用的占位符。
UNBOUND_REQUEST_ID = "-"


@dataclass(frozen=True)
class RequestContext:
    """单次请求的不可变关联上下文。"""

    request_id: str


_CONTEXT: contextvars.ContextVar[RequestContext | None] = contextvars.ContextVar(
    "observability_request_context", default=None
)


def new_request_id() -> str:
    """生成一个全局唯一、长度稳定的请求标识。"""
    return f"{_REQUEST_ID_PREFIX}{uuid.uuid4().hex}"


def is_valid_inbound_id(value: str | None) -> bool:
    """校验入站透传标识是否可被信任沿用。"""
    return bool(value) and bool(_INBOUND_ID_RE.match(value or ""))


def resolve_request_id(inbound: str | None) -> str:
    """决定本次请求使用的标识：合法的入站标识优先透传，否则新生成。"""
    if is_valid_inbound_id(inbound):
        return inbound  # type: ignore[return-value]
    return new_request_id()


def current_context() -> RequestContext | None:
    return _CONTEXT.get()


def current_request_id() -> str:
    """返回当前请求标识；未绑定时返回占位符而不是抛出异常。"""
    ctx = _CONTEXT.get()
    return ctx.request_id if ctx is not None else UNBOUND_REQUEST_ID


def bind_context(request_id: str) -> contextvars.Token[RequestContext | None]:
    """绑定标识到当前执行上下文，返回用于恢复的 token。"""
    return _CONTEXT.set(RequestContext(request_id))


def reset_context(token: contextvars.Token[RequestContext | None]) -> None:
    _CONTEXT.reset(token)


@contextmanager
def use_context(ctx: RequestContext) -> Iterator[RequestContext]:
    """在 ``with`` 块内临时绑定上下文，退出后恢复。"""
    token = _CONTEXT.set(ctx)
    try:
        yield ctx
    finally:
        _CONTEXT.reset(token)


def spawn_context() -> RequestContext:
    """派生子任务上下文：沿用当前请求标识；无绑定则新建。"""
    ctx = _CONTEXT.get()
    if ctx is not None:
        return ctx
    return RequestContext(new_request_id())
