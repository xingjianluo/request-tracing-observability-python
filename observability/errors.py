"""失败分类与异常链路处理。

四类失败必须可区分：

============================ ====================================
类别                          典型来源
============================ ====================================
``CLIENT_ERROR``             调用方参数非法（400）
``DEPENDENCY_UNAVAILABLE``   下游依赖拒绝/连接失败（503）
``TIMEOUT``                  下游依赖超时（504）
``INTERNAL_ERROR``           未预期异常兜底（500）
============================ ====================================

原始异常通过 ``__cause__`` 与 ``reason`` 属性保留，供日志归因；
对外响应只输出稳定的脱敏文案，绝不透出底层异常文本。
"""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

from .context import RequestContext, current_request_id

#: 各分类对应的 HTTP 状态码（判定依据之一，固定可复现）。
HTTP_STATUS: dict[str, int] = {
    "client_error": 400,
    "dependency_unavailable": 503,
    "timeout": 504,
    "internal_error": 500,
}

#: 对外稳定文案，不包含底层实现细节。
PUBLIC_MESSAGES: dict[str, str] = {
    "client_error": "request rejected: invalid parameter",
    "dependency_unavailable": "dependency unavailable",
    "timeout": "dependency timed out",
    "internal_error": "internal server error",
}


class ErrorCategory(str, Enum):
    CLIENT_ERROR = "client_error"
    DEPENDENCY_UNAVAILABLE = "dependency_unavailable"
    TIMEOUT = "timeout"
    INTERNAL_ERROR = "internal_error"


class ClientError(Exception):
    """调用方可修复的错误（如参数非法）。"""


class DependencyUnavailableError(Exception):
    """下游依赖不可用（拒绝连接、显式失败等）。"""


class DependencyTimeoutError(Exception):
    """下游依赖在截止时间内未返回。"""


@dataclass
class Classification:
    """一次失败判定的结论与依据。"""

    category: ErrorCategory
    public_message: str
    http_status: int
    reason_chain: list[str]


class ClassifiedError(Exception):
    """已完成分类的异常，原始原因经 ``raise ... from`` 链式保留。"""

    def __init__(
        self,
        category: ErrorCategory,
        *,
        reason: BaseException | None = None,
        request_id: str | None = None,
    ) -> None:
        super().__init__(PUBLIC_MESSAGES[category.value])
        self._category = category
        self._reason = reason
        self.request_id = request_id or current_request_id()
        if reason is not None:
            self.__cause__ = reason

    @property
    def category(self) -> ErrorCategory:
        return self._category

    @property
    def reason(self) -> BaseException | None:
        return self._reason

    @property
    def http_status(self) -> int:
        return HTTP_STATUS[self._category.value]

    def reason_chain(self) -> list[str]:
        """沿 ``__cause__`` / ``__context__`` 展开原始原因链，供日志归因。"""
        return _format_chain(self._reason if self._reason is not None else self)

    def classification(self) -> Classification:
        return Classification(
            category=self._category,
            public_message=PUBLIC_MESSAGES[self._category.value],
            http_status=self.http_status,
            reason_chain=self.reason_chain(),
        )


def _format_chain(exc: BaseException) -> list[str]:
    chain: list[str] = []
    seen: set[int] = set()
    cur: BaseException | None = exc
    while cur is not None and id(cur) not in seen:
        seen.add(id(cur))
        chain.append(f"{type(cur).__name__}: {cur}")
        cur = cur.__cause__ or cur.__context__
    return chain


#: 已知异常类型 -> 失败分类（判定依据表，未命中一律内部错误）。
_KNOWN: dict[type[BaseException], ErrorCategory] = {
    ClientError: ErrorCategory.CLIENT_ERROR,
    DependencyUnavailableError: ErrorCategory.DEPENDENCY_UNAVAILABLE,
    DependencyTimeoutError: ErrorCategory.TIMEOUT,
    TimeoutError: ErrorCategory.TIMEOUT,
}


def classify(exc: BaseException) -> ClassifiedError:
    """把任意异常归一为 :class:`ClassifiedError`，保留原始原因。

    判定依据：异常类型命中已知表则按表分类，否则兜底为内部错误。
    判定过程是纯函数，相同输入恒得相同分类。
    """
    if isinstance(exc, ClassifiedError):
        return exc
    category = _KNOWN.get(type(exc), ErrorCategory.INTERNAL_ERROR)
    for known_type, known_category in _KNOWN.items():
        if isinstance(exc, known_type):
            category = known_category
            break
    classified = ClassifiedError(category, reason=exc)
    # 完整复制原始异常链，避免中间异常被回收后原因丢失（保证可归因）。
    classified.__cause__ = exc.__cause__
    classified.__context__ = exc.__context__
    return classified


def error_response(exc: ClassifiedError, ctx: RequestContext | None = None) -> dict:
    """构造对外错误响应体：稳定文案 + 关联标识，绝不包含底层异常文本。"""
    request_id = ctx.request_id if ctx is not None else exc.request_id
    return {
        "error": {
            "category": exc.category.value,
            "message": PUBLIC_MESSAGES[exc.category.value],
            "http_status": exc.http_status,
        },
        "request_id": request_id,
    }
