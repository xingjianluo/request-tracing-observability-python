"""失败分类与异常链路处理。

四类必须可区分：
- client_error     客户端错误（参数非法等，4xx）
- dependency_down  依赖不可用（503）
- timeout          依赖超时（504）
- internal_error   内部错误（500）

归因原则（判定依据）：
- DomainError 子类：异常自带的 category 即结论；
- TimeoutError / asyncio.CancelledError（超时取消）：timeout；
- ConnectionError / OSError 等连接类：dependency_down；
- ValueError / KeyError 等入参契约类：client_error；
- 其余未预期异常：internal_error（保守处理，避免误判）。

对外只暴露安全摘要（异常类型名 + 固定/脱敏 message），不回显底层异常文本；
原始因果链完整保留在 FailureView.chain 与日志中。
"""

from __future__ import annotations

import asyncio
import enum
from dataclasses import dataclass, field
from typing import Any

from app.context import current_request_id


class ErrorCategory(str, enum.Enum):
    """失败类别。"""

    CLIENT_ERROR = "client_error"
    DEPENDENCY_DOWN = "dependency_down"
    TIMEOUT = "timeout"
    INTERNAL_ERROR = "internal_error"


#: 失败类别 -> HTTP 状态码
CATEGORY_STATUS: dict[ErrorCategory, int] = {
    ErrorCategory.CLIENT_ERROR: 400,
    ErrorCategory.DEPENDENCY_DOWN: 503,
    ErrorCategory.TIMEOUT: 504,
    ErrorCategory.INTERNAL_ERROR: 500,
}

#: 对外安全的固定文案，避免泄漏底层实现细节。
_PUBLIC_MESSAGE: dict[ErrorCategory, str] = {
    ErrorCategory.CLIENT_ERROR: "bad request",
    ErrorCategory.DEPENDENCY_DOWN: "dependency unavailable",
    ErrorCategory.TIMEOUT: "dependency timeout",
    ErrorCategory.INTERNAL_ERROR: "internal error",
}


class DomainError(Exception):
    """已分类的领域异常。

    - category：失败类别；
    - reason：判定依据（为什么归为这一类），写入日志与失败视图；
    - __cause__：被包装的原始异常（raise ... from ...）。
    """

    category: ErrorCategory = ErrorCategory.INTERNAL_ERROR

    def __init__(
        self,
        message: str = "",
        *,
        cause: BaseException | None = None,
        reason: str = "explicit domain error",
        auto_wrapped: bool = False,
        details: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message or self.category.value)
        self.message = message
        self.reason = reason
        # True 表示由归因层自动包装（包装节点本身无独立语义，链路展示时可折叠）
        self.auto_wrapped = auto_wrapped
        # 结构化的附加说明（如重试逐次结论），只允许安全字段，不含异常原文
        self.details = details
        if cause is not None:
            self.__cause__ = cause


class ClientError(DomainError):
    category = ErrorCategory.CLIENT_ERROR


class DependencyDownError(DomainError):
    category = ErrorCategory.DEPENDENCY_DOWN


class TimeoutError_(DomainError):
    category = ErrorCategory.TIMEOUT


class InternalError(DomainError):
    category = ErrorCategory.INTERNAL_ERROR


@dataclass(slots=True)
class FailureView:
    """失败归因结论：对外可暴露的结构化视图。

    不含原始堆栈，仅保留可解释的因果摘要；与正常结果字段互斥
    （响应体使用 {"ok": false, "error": FailureView} 结构）。
    """

    category: str
    http_status: int
    message: str
    reason: str
    request_id: str | None
    cause_type: str | None = None
    cause_summary: str | None = None
    chain: list[dict[str, Any]] = field(default_factory=list)
    details: dict[str, Any] | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "category": self.category,
            "http_status": self.http_status,
            "message": self.message,
            "reason": self.reason,
            "request_id": self.request_id,
            "cause_type": self.cause_type,
            "cause_summary": self.cause_summary,
            "chain": self.chain,
            "details": self.details,
        }


def _category_for_unexpected(exc: BaseException) -> tuple[ErrorCategory, str]:
    """对未分类异常给出类别与判定依据。顺序敏感：先具体后宽泛。"""
    if isinstance(exc, (TimeoutError, asyncio.TimeoutError)):
        return ErrorCategory.TIMEOUT, "raised type is TimeoutError"
    if isinstance(exc, ConnectionError):
        return ErrorCategory.DEPENDENCY_DOWN, "raised type is ConnectionError"
    if isinstance(exc, OSError):
        return ErrorCategory.DEPENDENCY_DOWN, "raised type is OSError (io/network)"
    if isinstance(exc, (ValueError, KeyError, TypeError)):
        return ErrorCategory.CLIENT_ERROR, "raised type is a contract/validation error"
    return ErrorCategory.INTERNAL_ERROR, "unexpected exception; conservative default"


def classify(exc: BaseException) -> DomainError:
    """把任意异常归一化为已分类的 DomainError，保留原始原因与判定依据。

    已是 DomainError 时原样返回（保留其自带 reason）；
    否则按类型规则包装为对应子类，并通过 __cause__ 保留原始异常。
    """
    if isinstance(exc, DomainError):
        return exc

    category, reason = _category_for_unexpected(exc)
    wrapper_cls = {
        ErrorCategory.CLIENT_ERROR: ClientError,
        ErrorCategory.DEPENDENCY_DOWN: DependencyDownError,
        ErrorCategory.TIMEOUT: TimeoutError_,
        ErrorCategory.INTERNAL_ERROR: InternalError,
    }[category]
    return wrapper_cls(cause=exc, reason=reason, auto_wrapped=True)


def cause_chain(exc: BaseException) -> list[dict[str, Any]]:
    """展开异常因果链（优先 __cause__，回退 __context__），返回判定依据列表。

    - 同一异常不会重复出现；最多保留 20 层防止环引用；
    - 归因层自动包装的占位节点（auto_wrapped 且仅有类别占位消息）折叠，
      链路直接从真正的根因类型开始呈现，避免“包装类与根因同名”的歧义。
    """
    chain: list[dict[str, Any]] = []
    seen: set[int] = set()
    cur: BaseException | None = exc
    if (
        isinstance(cur, DomainError)
        and cur.auto_wrapped
        and cur.__cause__ is not None
        and str(cur) == cur.category.value
    ):
        cur = cur.__cause__
    while cur is not None and id(cur) not in seen and len(chain) < 20:
        seen.add(id(cur))
        chain.append(
            {
                "index": len(chain),
                "type": type(cur).__name__,
                "summary": _safe_summary(cur),
            }
        )
        nxt = cur.__cause__ or cur.__context__
        cur = nxt
    return chain


def _safe_summary(exc: BaseException) -> str:
    """生成不泄漏敏感信息的异常摘要：类型名 + 定长截断文本。"""
    text = str(exc).strip()
    if len(text) > 200:
        text = text[:200] + "...(truncated)"
    return text


def to_failure_view(exc: BaseException, request_id: str | None = None) -> FailureView:
    """把异常转换为对外安全的失败视图。"""
    classified = classify(exc)
    category = classified.category
    rid = request_id if request_id is not None else current_request_id()
    chain = cause_chain(classified)
    # 被包装时原始异常即根因；未包装时分类异常自身即根因。
    root_cause: BaseException = classified.__cause__ or classified
    return FailureView(
        category=category.value,
        http_status=CATEGORY_STATUS[category],
        message=_PUBLIC_MESSAGE[category],
        reason=classified.reason,
        request_id=rid,
        cause_type=type(root_cause).__name__,
        cause_summary=_safe_summary(root_cause),
        chain=chain,
        details=getattr(classified, "details", None),
    )
