"""下游调用的重试归因：多次尝试归并为一个统一、可解释的结论。

判定规则（判定依据）：
- 每次尝试的异常先经 errors.classify 归因（timeout 与 dependency_down 严格区分）；
- 仅 timeout / dependency_down 可重试（瞬时故障）；
  client_error / internal_error 立即终止（契约或内部错误重试无意义）；
- 任一尝试成功 -> 整体成功；全部尝试耗尽 -> 整体失败，
  最终类别取**最后一次尝试**的归因类别，reason 写明尝试次数与逐次类别。

口径保证：
- 每次尝试与最终结论都写结构化事件（request_id 自动注入），
  事件里只有异常类型名与判定依据，不含底层报错原文；
- 本模块不触碰指标注册表：同一次业务请求无论重试几次，
  只由中间件在请求边界计一次数，重试不会造成重复计数。
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from app.errors import DomainError, ErrorCategory, classify
from app.events import recorder

#: 可重试的失败类别（瞬时故障）；其余类别 fail-fast
RETRYABLE_CATEGORIES = {ErrorCategory.TIMEOUT, ErrorCategory.DEPENDENCY_DOWN}


@dataclass(frozen=True, slots=True)
class AttemptOutcome:
    """单次尝试的结构化结论（只含类型名与判定依据，不含异常原文）。"""

    index: int  # 1 起始的尝试序号
    input: dict[str, Any]
    outcome: str  # "ok" | "failed"
    category: str  # 成功为 "none"，失败为归因类别
    reason: str  # 本次结论的判定依据
    error_type: str | None = None  # 失败时底层异常类型名

    def to_dict(self) -> dict[str, Any]:
        return {
            "index": self.index,
            "input": self.input,
            "outcome": self.outcome,
            "category": self.category,
            "reason": self.reason,
            "error_type": self.error_type,
        }


class RetryExhaustedError(DomainError):
    """重试耗尽后的统一失败结论。

    - category 取最终尝试的归因类别（实例级覆盖类属性）；
    - message 为固定安全文案，不携带任何底层异常原文；
    - 不挂 __cause__，FailureView 的 cause_summary 因此只会是本异常的
      安全文案；逐次尝试明细放在 details["attempts"]（类型名+判定依据）。
    """

    def __init__(self, final: DomainError, attempts: list[AttemptOutcome], reason: str) -> None:
        self.category = final.category
        super().__init__(
            f"retry exhausted after {len(attempts)} attempts",
            reason=reason,
            details={"attempts": [a.to_dict() for a in attempts]},
        )


@dataclass(slots=True)
class RetryVerdict:
    """一次业务调用的统一重试结论。"""

    ok: bool
    attempts: list[AttemptOutcome] = field(default_factory=list)
    final_category: str = "none"
    reason: str = ""
    result: Any = None
    error: RetryExhaustedError | None = None

    def attempts_summary(self) -> list[dict[str, Any]]:
        return [a.to_dict() for a in self.attempts]


def run_with_retry[T](
    func: Callable[[str], T],
    *,
    attempt_inputs: list[str],
    max_attempts: int = 3,
    operation: str = "dependency.call",
) -> RetryVerdict:
    """按重试策略执行 func(input) 并给出统一结论。

    attempt_inputs 提供逐次尝试的确定性输入（本地复现用）：第 i 次尝试
    取 attempt_inputs[min(i, len-1)]。func 抛出的任何异常都先归因再决策。
    """
    if not attempt_inputs:
        raise ValueError("attempt_inputs must be non-empty")
    if max_attempts < 1:
        raise ValueError("max_attempts must be >= 1")

    attempts: list[AttemptOutcome] = []
    for index in range(1, max_attempts + 1):
        flag = attempt_inputs[min(index - 1, len(attempt_inputs) - 1)]
        attempt_input = {"flag": flag}
        try:
            result = func(flag)
        except Exception as exc:  # noqa: BLE001 - 归因层统一接管
            classified = classify(exc)
            category = classified.category
            outcome = AttemptOutcome(
                index=index,
                input=attempt_input,
                outcome="failed",
                category=category.value,
                reason=classified.reason,
                error_type=type(classified.__cause__ or classified).__name__,
            )
            attempts.append(outcome)
            recorder.record(
                f"{operation}.retry.attempt",
                level="warning",
                attempt=outcome.to_dict(),
                retryable=category in RETRYABLE_CATEGORIES,
                will_retry=category in RETRYABLE_CATEGORIES and index < max_attempts,
            )
            if category not in RETRYABLE_CATEGORIES:
                reason = (
                    f"attempt {index} classified as {category.value} (not retryable); "
                    "stopped without further attempts"
                )
                return _fail(attempts, classified, reason, operation)
            if index == max_attempts:
                reason = (
                    f"all {index} attempts failed; "
                    f"final attempt classified as {category.value}"
                )
                return _fail(attempts, classified, reason, operation)
            continue

        outcome = AttemptOutcome(
            index=index,
            input=attempt_input,
            outcome="ok",
            category="none",
            reason="attempt succeeded",
        )
        attempts.append(outcome)
        recorder.record(
            f"{operation}.retry.attempt",
            attempt=outcome.to_dict(),
            retryable=False,
            will_retry=False,
        )
        failed_before = [a.category for a in attempts[:-1]]
        reason = (
            f"attempt {index} succeeded"
            + (f" after {len(failed_before)} failed attempt(s): {failed_before}" if failed_before else "")
        )
        verdict = RetryVerdict(
            ok=True, attempts=attempts, final_category="none", reason=reason, result=result
        )
        recorder.record(
            f"{operation}.retry.verdict",
            ok=True,
            attempts=len(attempts),
            final_category="none",
            reason=reason,
        )
        return verdict

    raise AssertionError("unreachable: retry loop always returns")


def _fail(
    attempts: list[AttemptOutcome], final: DomainError, reason: str, operation: str
) -> RetryVerdict:
    error = RetryExhaustedError(final, attempts, reason)
    recorder.record(
        f"{operation}.retry.verdict",
        level="error",
        ok=False,
        attempts=len(attempts),
        final_category=error.category.value,
        reason=reason,
        attempt_categories=[a.category for a in attempts],
    )
    return RetryVerdict(
        ok=False,
        attempts=attempts,
        final_category=error.category.value,
        reason=reason,
        error=error,
    )
