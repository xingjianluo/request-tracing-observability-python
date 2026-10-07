"""重试归因：统一结论、逐次明细、超时与连接失败区分、指标去重、不泄漏原文。"""

from __future__ import annotations

from app import events as events_mod
from app import metrics as metrics_mod
from app.retry import run_with_retry
from app.services import call_internal_dependency


def test_retry_success_after_failures_gives_unified_verdict():
    print("\n[输入] flags=timeout,down,ok（前两次失败第三次成功）；"
          "[判定依据] 整体成功，结论写明第 3 次成功及前两次各自类别")
    v = run_with_retry(call_internal_dependency, attempt_inputs=["timeout", "down", "ok"])
    assert v.ok is True
    assert len(v.attempts) == 3
    assert [a.category for a in v.attempts] == ["timeout", "dependency_down", "none"]
    assert [a.outcome for a in v.attempts] == ["failed", "failed", "ok"]
    assert "attempt 3 succeeded" in v.reason
    print(f"  判定依据: {v.reason}")


def test_retry_timeout_and_connection_down_are_distinguished():
    print("\n[输入] flags=timeout,down,down 全部失败；"
          "[判定依据] 逐次类别区分 timeout 与 dependency_down，最终类别取最后一次")
    v = run_with_retry(call_internal_dependency, attempt_inputs=["timeout", "down", "down"])
    assert v.ok is False
    assert v.attempts[0].category == "timeout" and v.attempts[0].error_type == "TimeoutError"
    assert v.attempts[1].category == "dependency_down"
    assert v.attempts[1].error_type == "ConnectionRefusedError"
    assert v.final_category == "dependency_down"
    assert v.reason == "all 3 attempts failed; final attempt classified as dependency_down"
    print(f"  判定依据: {v.reason}")


def test_retry_exhausted_timeout_stays_timeout():
    print("\n[输入] flags=timeout,timeout,timeout；[判定依据] 最终结论为 timeout 而非其他类别")
    v = run_with_retry(call_internal_dependency, attempt_inputs=["timeout"] * 3)
    assert v.ok is False and v.final_category == "timeout"
    assert v.error is not None and v.error.category.value == "timeout"


def test_non_retryable_failure_stops_immediately():
    print("\n[输入] flags=bad,ok,ok（第一次即契约错误）；"
          "[判定依据] client_error 不可重试，只尝试 1 次即给出结论")
    v = run_with_retry(call_internal_dependency, attempt_inputs=["bad", "ok", "ok"])
    assert v.ok is False
    assert len(v.attempts) == 1
    assert v.final_category == "client_error"
    assert "not retryable" in v.reason
    print(f"  判定依据: {v.reason}")


def test_retry_events_carry_attempt_basis_and_request_id():
    print("\n[输入] 在请求上下文中重试 2 次；"
          "[判定依据] 每次尝试与最终结论都有结构化事件，含输入、判定依据与同一 request_id")
    from app.context import bind_request_id, reset_request_id

    token = bind_request_id("rid-retry-evt-1")
    try:
        run_with_retry(call_internal_dependency, attempt_inputs=["timeout", "ok"])
    finally:
        reset_request_id(token)
    events = [e for e in events_mod.recorder.snapshot()
              if e["event"].startswith("dependency.call.retry.")]
    attempts = [e for e in events if e["event"].endswith(".attempt")]
    verdicts = [e for e in events if e["event"].endswith(".verdict")]
    assert len(attempts) == 2 and len(verdicts) == 1
    assert all(e["request_id"] == "rid-retry-evt-1" for e in events)
    assert attempts[0]["attempt"]["input"] == {"flag": "timeout"}
    assert attempts[0]["attempt"]["reason"] == "raised type is TimeoutError"
    assert attempts[0]["will_retry"] is True
    assert verdicts[0]["ok"] is True and verdicts[0]["attempts"] == 2


def test_http_retry_success_counts_once_in_metrics(client):
    print("\n[输入] 一次 /retry 请求内部重试 3 次才成功；"
          "[判定依据] 指标只计 1 次 ok，重试次数不放大计数")
    before = metrics_mod.registry.snapshot()["total"]
    resp = client.get("/retry/x?flags=timeout,down,ok",
                      headers={"X-Request-Id": "rid-retry-http1"})
    assert resp.status_code == 200
    data = resp.json()["data"]
    assert len(data["attempts"]) == 3
    snap = metrics_mod.registry.snapshot()
    assert snap["total"] == before + 1
    row = next(r for r in snap["counters"] if r["route"] == "/retry/{item}")
    assert (row["result"], row["error_category"], row["count"]) == ("ok", "none", 1)


def test_http_retry_exhausted_counts_one_failure_with_final_category(client):
    print("\n[输入] 一次 /retry 请求 3 次全失败（timeout,timeout,down）；"
          "[判定依据] 指标只计 1 次 failed/dependency_down，响应含逐次明细且无原文")
    before = metrics_mod.registry.snapshot()["total"]
    resp = client.get("/retry/x?flags=timeout,timeout,down",
                      headers={"X-Request-Id": "rid-retry-http2"})
    assert resp.status_code == 503
    err = resp.json()["error"]
    assert err["category"] == "dependency_down"
    assert err["reason"] == "all 3 attempts failed; final attempt classified as dependency_down"
    attempts = err["details"]["attempts"]
    assert [a["category"] for a in attempts] == ["timeout", "timeout", "dependency_down"]
    # 失败响应任何字段都不得出现底层报错原文
    import json
    raw = json.dumps(err, ensure_ascii=False)
    for leaked in ("fake", "connect failed", "deadline exceeded", "127.0.0.1"):
        assert leaked not in raw, leaked
    snap = metrics_mod.registry.snapshot()
    assert snap["total"] == before + 1
    row = next(r for r in snap["counters"] if r["route"] == "/retry/{item}")
    assert (row["result"], row["error_category"]) == ("failed", "dependency_down")
    assert row["count"] == 1
    print(f"  判定依据: {err['reason']}; 逐次: {[a['category'] for a in attempts]}")


def test_http_retry_timeout_exhaustion_maps_to_504(client):
    print("\n[输入] flags=timeout,timeout,timeout；[判定依据] 统一结论 timeout -> 504，计 1 次")
    resp = client.get("/retry/x?flags=timeout,timeout,timeout",
                      headers={"X-Request-Id": "rid-retry-http3"})
    assert resp.status_code == 504
    err = resp.json()["error"]
    assert err["category"] == "timeout"
    assert err["message"] == "dependency timeout"  # 固定安全文案
    assert len(err["details"]["attempts"]) == 3
