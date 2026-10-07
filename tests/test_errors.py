"""失败分类：四类可区分、判定依据可见、原始原因保留、对外不泄漏。"""

from __future__ import annotations

from app.errors import (
    ClientError,
    DependencyDownError,
    DomainError,
    ErrorCategory,
    InternalError,
    TimeoutError_,
    cause_chain,
    classify,
    to_failure_view,
)


def test_explicit_domain_errors_pass_through():
    print("\n[输入] 四个 DomainError 子类；[判定依据] 显式分类原样返回，类别/状态码正确")
    cases = [
        (ClientError(reason="r1"), ErrorCategory.CLIENT_ERROR, 400),
        (DependencyDownError(reason="r2"), ErrorCategory.DEPENDENCY_DOWN, 503),
        (TimeoutError_(reason="r3"), ErrorCategory.TIMEOUT, 504),
        (InternalError(reason="r4"), ErrorCategory.INTERNAL_ERROR, 500),
    ]
    for exc, category, status in cases:
        assert classify(exc) is exc
        view = to_failure_view(exc, "req-1")
        assert view.category == category.value
        assert view.http_status == status
        assert view.reason == exc.reason
        assert view.request_id == "req-1"


def test_timeout_classification():
    print("\n[输入] TimeoutError / asyncio.TimeoutError；[判定依据] 类型匹配 timeout -> 504")
    for raw in (TimeoutError("deadline"), TimeoutError()):
        classified = classify(raw)
        assert isinstance(classified, TimeoutError_)
        assert classified.__cause__ is raw
        view = to_failure_view(raw)
        assert view.category == "timeout" and view.http_status == 504
        assert view.cause_type == type(raw).__name__


def test_connection_error_is_dependency_down():
    print("\n[输入] ConnectionRefusedError（直接归因 与 适配层显式包装两种路径）；"
          "[判定依据] ConnectionError 族 -> dependency_down -> 503")
    raw = ConnectionRefusedError("connect 127.0.0.1:6379 refused")
    classified = classify(raw)
    assert isinstance(classified, DependencyDownError)
    assert classified.auto_wrapped is True
    assert classified.__cause__ is raw
    view = to_failure_view(raw, "req-9")
    assert view.category == "dependency_down"
    assert view.http_status == 503
    assert view.cause_type == "ConnectionRefusedError"
    # 自动包装折叠占位节点：链路直接呈现真正根因
    chain_types = [node["type"] for node in view.chain]
    assert chain_types == ["ConnectionRefusedError"], chain_types
    assert "6379" in view.chain[-1]["summary"]  # 原始原因保留在链路

    # 业务适配层显式包装（auto_wrapped=False）：包装层的判定语义也保留
    try:
        raise DependencyDownError(cause=raw, reason="adapter mapped") from raw
    except DependencyDownError as explicit:
        assert explicit.auto_wrapped is False
        chain_types = [node["type"] for node in cause_chain(explicit)]
        assert chain_types == ["DependencyDownError", "ConnectionRefusedError"]


def test_value_error_is_client_error():
    print("\n[输入] ValueError；[判定依据] 契约/校验异常族 -> client_error -> 400")
    view = to_failure_view(ValueError("missing user_id"))
    assert view.category == "client_error" and view.http_status == 400
    assert view.cause_type == "ValueError"


def test_unexpected_is_internal_by_default():
    print("\n[输入] RuntimeError；[判定依据] 未匹配任何具体类型 -> 保守归 internal -> 500")
    view = to_failure_view(RuntimeError("null deref secret token ABC123"))
    assert view.category == "internal_error" and view.http_status == 500
    # 对外固定安全文案，不含底层异常文本
    assert view.message == "internal error"
    assert "secret" not in view.message and "ABC123" not in view.message


def test_nested_raise_from_preserves_full_chain():
    print("\n[输入] RuntimeError -> ConnectionError -> DomainError(raise from)；"
          "[判定依据] cause_chain 按 __cause__ 顺序完整展开且不重复")

    try:
        try:
            raise ConnectionRefusedError("socket reject")
        except ConnectionRefusedError as inner:
            raise DependencyDownError(cause=inner, reason="adapter mapping") from inner
    except DomainError as exc:
        chain = cause_chain(exc)
        types = [node["type"] for node in chain]
        assert types == ["DependencyDownError", "ConnectionRefusedError"], types
        assert chain[1]["summary"] == "socket reject"


def test_success_and_failure_views_are_distinguishable():
    print("\n[输入] 失败视图结构；[判定依据] 固定 category/http_status 枚举，不会与正常结果混淆")
    view = to_failure_view(TimeoutError())
    payload = view.to_dict()
    assert set(payload) >= {
        "category", "http_status", "message", "reason",
        "request_id", "cause_type", "cause_summary", "chain",
    }
    assert payload["category"] in {c.value for c in ErrorCategory}
