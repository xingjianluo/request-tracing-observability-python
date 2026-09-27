"""后台任务：标识延续 + 异常分类保留。"""
from __future__ import annotations

from observability import context
from observability.background import run_in_background
from observability.errors import ClassifiedError, DependencyUnavailableError, ErrorCategory


def test_background_success_has_no_error():
    def ok():
        context.current_request_id()

    token = context.bind_context("req-bg-ok")
    job = run_in_background(ok)
    context.reset_context(token)
    job.wait(timeout=2)
    print(f"[判定] 后台成功 | 输入=正常返回的任务 | 依据=done 且 error 为 None | "
          f"实际 done={job.done}, error={job.error}")
    assert job.done and job.error is None


def test_background_failure_is_classified_not_swallowed():
    def boom():
        raise DependencyUnavailableError("down is down")

    token = context.bind_context("req-bg-fail")
    job = run_in_background(boom)
    context.reset_context(token)

    raised = None
    try:
        job.wait(timeout=2)
    except ClassifiedError as exc:
        raised = exc

    print(f"[判定] 后台失败分类 | 输入=DependencyUnavailableError | "
          f"依据=wait 重新抛出 ClassifiedError/503 | "
          f"实际 category={raised.category.value if raised else None}, "
          f"reason_preserved={raised.reason is not None if raised else False}")
    assert raised is not None
    assert raised.category is ErrorCategory.DEPENDENCY_UNAVAILABLE
    assert raised.http_status == 503
    assert raised.reason is not None
    assert job.error is raised
