"""下游超时/连接失败的重试归因：同一次业务请求对外只给一个统一结论。

口径：
- 同一次业务请求内部无论重试多少次，对外只产出一个失败结论；
- 最终类别以“最后一次尝试”的归因类别为准（判定依据随结论给出），
  每次尝试的类别（timeout / dependency_down / ...）与异常类型逐一列出；
- 超时（TimeoutError 族）与连接失败（ConnectionError/OSError 族）
  在每次尝试上仍然分别归类、可区分；
- 重试不产生多次业务指标（指标只在请求边界由中间件记一次，
  本模块不触碰 MetricsRegistry）；
- 对外失败结论只含类型名/类别/固定安全文案，不含任何底层异常原文，
  原始细节仅保留在结构化日志中（本模块日志同样只记类型名与判定依据）。
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any, TypeVar

from app.context import current_request_id
from app.errors import CATEGORY_STATUS, DomainError, ErrorCategory, classify
from app.events import recorder

T = TypeVar("T")

#: 尝试结果标签
OUTCOME_OK = "ok"
OUTCOME_FAILED = "failed"

#: 最终类别判定方式：以最后一次尝试的归因类别为准
FINAL_CATEGORY_SOURCE = "last_attempt"


@dataclass(slots=True)
class AttemptResult:
    """单次尝试的安全结论（不含底层异常文本）。"""

    attempt: int
    outcome: str
    category: str | None = None
    raised_type: str | None = None
    reason: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "attempt": self.attempt,
            "outcome": self.outcome,
            "category": self.category,
            "raised_type": self.raised_type,
            "reason": self.reason,
        }


class RetryExhaustedError(DomainError):
    """全部尝试失败后抛出的统一业务失败。

    - category：最终归因类别（取自最后一次尝试）；
    - attempts：每次尝试的安全结论列表（类别/异常类型名/判定依据）；
    - 不持有原始异常对象，保证任何对外渲染路径都无法回显底层报错原文。
    """

    def __init__(self, *, max_attempts: int, attempts: list[AttemptResult]) -> None:
        last = attempts[-1]
        final_category = ErrorCategory(last.category)  # type: ignore[arg-type]
        # 实例级覆盖 DomainError 的类属性 category
        self.category = final_category
        super().__init__(
            reason=(
                f"all {len(attempts)} attempt(s) failed "
                f"({FINAL_CATEGORY_SOURCE} category={final_category.value})"
            ),
        )
        self.max_attempts = max_attempts
        self.attempts = attempts


def _run_attempt(call: Callable[[int], Any], index: int, name: str) -> tuple[bool, Any, AttemptResult]:
    """执行一次同步尝试，返回 (是否成功, 结果或None, 安全结论)。"""
    try:
        value = call(index)
    except Exception as exc:  # noqa: BLE001 - 重试层必须收口每次尝试
        classified = classify(exc)
        result = AttemptResult(
            attempt=index,
            outcome=OUTCOME_FAILED,
            category=classified.category.value,
            raised_type=type(exc).__name__,
            reason=classified.reason,
        )
        _record_attempt(result, name=name)
        return False, None, result
    result = AttemptResult(attempt=index, outcome=OUTCOME_OK)
    _record_attempt(result, name=name)
    return True, value, result


def _record_attempt(result: AttemptResult, *, name: str | None) -> None:
    fields: dict[str, Any] = {
        "name": name,
        "attempt": result.attempt,
        "outcome": result.outcome,
        "input": {"attempt": result.attempt, "name": name},
    }
    if result.outcome == OUTCOME_FAILED:
        fields["category"] = result.category
        fields["raised_type"] = result.raised_type
        fields["reason"] = result.reason
    recorder.record("retry.attempt", level="info" if result.outcome == OUTCOME_OK else "error",
                    **fields)


def _finish_or_raise(
    name: str,
    max_attempts: int,
    attempts: list[AttemptResult],
    value: Any,
    succeeded_at: int,
) -> Any:
    if succeeded_at > 0:
        recorder.record(
            "retry.success",
            name=name,
            max_attempts=max_attempts,
            attempts=[a.to_dict() for a in attempts],
            succeeded_at=succeeded_at,
            decision_basis={
                "attempts": [a.to_dict() for a in attempts],
                "final": "ok",
                "succeeded_at": succeeded_at,
            },
        )
        return value
    final = attempts[-1]
    recorder.record(
        "retry.exhausted",
        level="error",
        name=name,
        max_attempts=max_attempts,
        attempts=[a.to_dict() for a in attempts],
        final_category=final.category,
        decision_basis={
            "input": {"name": name, "max_attempts": max_attempts},
            "attempts": [a.to_dict() for a in attempts],
            "final_category": final.category,
            "final_category_source": FINAL_CATEGORY_SOURCE,
            "rule": "final failure category follows the last attempt; one business result",
        },
    )
    raise RetryExhaustedError(max_attempts=max_attempts, attempts=attempts)


def _validate_max_attempts(max_attempts: int) -> None:
    if not isinstance(max_attempts, int) or max_attempts < 1:
        raise ValueError("max_attempts must be a positive integer")


def run_with_retries(
    call: Callable[[int], Any],
    *,
    max_attempts: int = 3,
    name: str = "dependency",
) -> Any:
    """同步重试执行。

    call 接收从 1 开始的尝试序号；某次成功立即返回其结果；
    全部失败时抛 RetryExhaustedError（携带逐次安全结论与统一归因）。
    不记录任何业务指标，保证同一次业务请求只在请求边界计数一次。
    """
    _validate_max_attempts(max_attempts)
    attempts: list[AttemptResult] = []
    for index in range(1, max_attempts + 1):
        ok, value, result = _run_attempt(call, index, name)
        attempts.append(result)
        if ok:
            return _finish_or_raise(name, max_attempts, attempts, value, index)
    return _finish_or_raise(name, max_attempts, attempts, None, 0)


async def run_with_retries_async(
    call: Callable[[int], Awaitable[Any]],
    *,
    max_attempts: int = 3,
    name: str = "dependency",
) -> Any:
    """异步重试执行（口径同 run_with_retries），尝试之间让出事件循环。"""
    _validate_max_attempts(max_attempts)
    attempts: list[AttemptResult] = []
    for index in range(1, max_attempts + 1):
        try:
            value = await call(index)
        except Exception as exc:  # noqa: BLE001 - 重试层必须收口每次尝试
            classified = classify(exc)
            result = AttemptResult(
                attempt=index,
                outcome=OUTCOME_FAILED,
                category=classified.category.value,
                raised_type=type(exc).__name__,
                reason=classified.reason,
            )
        else:
            result = AttemptResult(attempt=index, outcome=OUTCOME_OK)
        _record_attempt(result, name=name)
        attempts.append(result)
        if result.outcome == OUTCOME_OK:
            return _finish_or_raise(name, max_attempts, attempts, value, index)
        await asyncio.sleep(0)  # 让出一次事件循环，制造真实交错
    return _finish_or_raise(name, max_attempts, attempts, None, 0)


def retry_failure_view(exc: RetryExhaustedError, request_id: str | None) -> dict[str, Any]:
    """把重试耗尽异常转换为对外安全的失败响应体。

    只有类别/固定安全文案/异常类型名/判定依据，绝无底层异常原文。
    """
    category = exc.category
    rid = request_id if request_id is not None else current_request_id()
    attempts = [a.to_dict() for a in exc.attempts]
    last = attempts[-1]
    return {
        "category": category.value,
        "http_status": CATEGORY_STATUS[category],
        "message": _SAFE_PUBLIC_MESSAGE[category],
        "reason": exc.reason,
        "request_id": rid,
        "attempts": attempts,
        "attempts_count": len(attempts),
        "retried": len(attempts) - 1,
        "final_category_source": FINAL_CATEGORY_SOURCE,
        "final_category_attempt": last["attempt"],
        "retry_policy": {"max_attempts": exc.max_attempts},
    }


#: 重试失败对外的固定安全文案（与 errors._PUBLIC_MESSAGE 口径一致，
#: 在本模块冗余声明以保证该视图不可能带出底层异常文本）。
_SAFE_PUBLIC_MESSAGE: dict[ErrorCategory, str] = {
    ErrorCategory.CLIENT_ERROR: "bad request",
    ErrorCategory.DEPENDENCY_DOWN: "dependency unavailable",
    ErrorCategory.TIMEOUT: "dependency timeout",
    ErrorCategory.INTERNAL_ERROR: "internal error",
}
