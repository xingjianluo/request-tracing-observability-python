"""失败分类与异常链路：四类可区分、原因链保留、响应脱敏。"""
from __future__ import annotations

import asyncio

from observability.errors import (
    ClassifiedError,
    ClientError,
    DependencyTimeoutError,
    DependencyUnavailableError,
    ErrorCategory,
    classify,
    error_response,
)


def _report(name, raw_exc, category, status):
    print(f"[判定] {name} | 输入={type(raw_exc).__name__}: {raw_exc} | "
          f"依据=分类表命中 => {category.value}/{status} | "
          f"实际=({category.value},{status})")


def test_four_categories_are_distinguishable():
    cases = [
        (ClientError("bad param"), ErrorCategory.CLIENT_ERROR, 400),
        (DependencyUnavailableError("down"), ErrorCategory.DEPENDENCY_UNAVAILABLE, 503),
        (DependencyTimeoutError("late"), ErrorCategory.TIMEOUT, 504),
        (ValueError("boom"), ErrorCategory.INTERNAL_ERROR, 500),
        (asyncio.TimeoutError(), ErrorCategory.TIMEOUT, 504),
    ]
    for raw, cat, status in cases:
        ce = classify(raw)
        _report("四分类", raw, cat, status)
        assert ce.category is cat
        assert ce.http_status == status


def test_original_reason_is_preserved_in_chain():
    root = ConnectionRefusedError("simulated downstream refused connection")
    mid = DependencyUnavailableError("downstream refused")
    mid.__cause__ = root
    ce = classify(mid)
    chain = ce.reason_chain()
    print(f"[判定] 原因链 | 输入=from ConnectionRefusedError | "
          f"依据=沿 __cause__/__context__ 展开 | 实际={chain}")
    assert ce.reason is mid
    assert ce.__cause__ is root
    assert any("ConnectionRefusedError" in c for c in chain)
    assert any("DependencyUnavailableError" in c for c in chain)


def test_public_response_never_leaks_raw_exception():
    secret = ValueError("db password=hunter2 connection failed")
    ce = classify(secret)
    resp = error_response(ce)
    text = str(resp)
    print(f"[判定] 脱敏 | 输入=ValueError({secret}) | "
          f"依据=响应体只含稳定文案 | 实际={resp}")
    assert "hunter2" not in text
    assert "db password" not in text
    assert resp["error"]["message"] == "internal server error"
    assert resp["error"]["category"] == "internal_error"


def test_classify_is_idempotent_and_pure():
    ce = classify(ClientError("x"))
    again = classify(ce)
    print(f"[判定] 幂等 | 输入=对 ClassifiedError 再 classify | "
          f"依据=同一对象返回 | 实际 same={again is ce}")
    assert again is ce
    assert isinstance(ce, ClassifiedError)
