"""异常链路 HTTP 场景：四类失败状态码/归因/原始原因/不泄漏。"""

from __future__ import annotations

from app import events as events_mod

CASES = [
    ("down", "client request flag=down", "dependency_down", 503,
     "ConnectionRefusedError", "adapter mapped ConnectionRefusedError to dependency_down"),
    ("timeout", "client request flag=timeout", "timeout", 504,
     "TimeoutError", "raised type is TimeoutError"),
    ("bad", "client request flag=bad", "client_error", 400,
     "ValueError", "raised type is a contract/validation error"),
    ("boom", "client request flag=boom", "internal_error", 500,
     "RuntimeError", "unexpected exception; conservative default"),
]


def test_four_failure_categories_distinguishable(client):
    print("\n[输入] 同步链路 flag ∈ {down,timeout,bad,boom}；"
          "[判定依据] 状态码、category、reason、根因类型四者一一对应")
    for flag, _inp, category, status, cause_type, reason in CASES:
        rid = f"rid-err-{flag}-001"
        resp = client.get(f"/echo/x?flag={flag}", headers={"X-Request-Id": rid})
        assert resp.status_code == status, (flag, resp.status_code)
        body = resp.json()
        assert body["ok"] is False
        err = body["error"]
        assert err["category"] == category
        assert err["http_status"] == status
        assert err["reason"] == reason
        assert err["request_id"] == rid
        assert resp.headers["x-request-id"] == rid
        assert err["cause_type"] == cause_type
        print(f"  flag={flag:7s} -> {category:15s} {status} cause={cause_type}")


def test_async_failures_are_classified(client):
    print("\n[输入] 异步链路 flag=down/timeout/boom；[判定依据] 同样归一化且标识不丢")
    for flag, _inp, category, status, cause_type, _ in CASES:
        if flag == "bad":
            continue
        rid = f"rid-aerr-{flag}-01"
        resp = client.get(f"/a-echo/x?flag={flag}", headers={"X-Request-Id": rid})
        assert resp.status_code == status
        err = resp.json()["error"]
        assert err["category"] == category and err["cause_type"] == cause_type
        assert resp.headers["x-request-id"] == rid


def test_failure_does_not_leak_low_level_text(client):
    print("\n[输入] 底层异常含 'fake/secret' 等文本；"
          "[判定依据] 对外 message 是固定文案，仅 chain/cause_summary 保留原始原因供排查")
    resp = client.get("/echo/x?flag=boom", headers={"X-Request-Id": "rid-leak-0001"})
    err = resp.json()["error"]
    assert err["message"] == "internal error"
    assert "fake" not in err["message"]
    # 原始原因仍可解释（internal 为自动包装，链路折叠到真正根因 RuntimeError）
    assert err["cause_summary"] and "fake unexpected" in err["cause_summary"]
    assert [node["type"] for node in err["chain"]] == ["RuntimeError"]


def test_failure_and_success_shapes_never_confused(client):
    print("\n[输入] 同一资源分别 ok 与 boom；[判定依据] ok 信封 vs 失败信封互斥可区分")
    ok = client.get("/echo/x", headers={"X-Request-Id": "rid-shape-001"}).json()
    bad = client.get("/echo/x?flag=boom", headers={"X-Request-Id": "rid-shape-002"}).json()
    assert ok["ok"] is True and "error" not in ok
    assert bad["ok"] is False and "error" in bad and "data" not in bad


def test_failure_event_contains_decision_basis(client):
    print("\n[输入] flag=down 的请求；[判定依据] request.failure 日志含输入、归因、根因与完整链路")
    client.get("/echo/x?flag=down", headers={"X-Request-Id": "rid-log-00001"})
    failure = next(
        e for e in events_mod.recorder.snapshot() if e["event"] == "request.failure"
    )
    basis = failure["decision_basis"]
    assert basis["input"]["path"] == "/echo/{item}"
    assert basis["classified_as"] == "dependency_down"
    assert basis["root_cause_type"] == "ConnectionRefusedError"
    assert failure["chain"][-1]["type"] == "ConnectionRefusedError"
    assert failure["request_id"] == "rid-log-00001"


def test_unknown_flag_is_client_contract_error(client):
    print("\n[输入] flag=not-a-flag；[判定依据] 下游契约 ValueError -> client_error 400")
    resp = client.get("/echo/x?flag=not-a-flag", headers={"X-Request-Id": "rid-unk-00001"})
    assert resp.status_code == 400
    assert resp.json()["error"]["category"] == "client_error"
    assert resp.json()["error"]["cause_type"] == "ValueError"


def test_background_failure_preserves_cause(client):
    print("\n[输入] 后台任务 flag=timeout；[判定依据] 后台异常同样被归一化，链路保留 TimeoutError")
    resp = client.get("/bg/x?flag=timeout", headers={"X-Request-Id": "rid-bgf-00001"})
    assert resp.status_code == 504
    err = resp.json()["error"]
    assert err["category"] == "timeout" and err["cause_type"] == "TimeoutError"
    assert err["request_id"] == "rid-bgf-00001"
