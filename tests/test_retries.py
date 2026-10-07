"""下游重试：统一归因、超时/连接可区分、逐次尝试可见、指标去重、无底层报错泄漏。"""

from __future__ import annotations

import asyncio
import json

import pytest

from app import events as events_mod
from app import metrics as metrics_mod
from app.retries import (
    RetryExhaustedError,
    retry_failure_view,
    run_with_retries,
    run_with_retries_async,
)


def _raise(exc: BaseException) -> None:
    raise exc


def test_retry_succeeds_after_transient_failures():
    print("\n[输入] 第 1 次 timeout、第 2 次连接失败、第 3 次成功；"
          "[判定依据] 返回成功结果，成功事件含 3 次尝试结论与 succeeded_at=3")
    plan = [TimeoutError("t1"), ConnectionRefusedError("down 1.2.3.4"), "ok"]

    def call(attempt: int) -> str:
        item = plan[attempt - 1]
        if isinstance(item, BaseException):
            raise item
        return item

    assert run_with_retries(call, max_attempts=3, name="d") == "ok"
    success_evt = next(e for e in events_mod.recorder.snapshot() if e["event"] == "retry.success")
    assert success_evt["succeeded_at"] == 3
    outcomes = [(a["attempt"], a["outcome"], a["category"]) for a in success_evt["attempts"]]
    assert outcomes == [
        (1, "failed", "timeout"),
        (2, "failed", "dependency_down"),
        (3, "ok", None),
    ]


def test_retry_exhausted_final_category_follows_last_attempt():
    print("\n[输入] 三次尝试 timeout/timeout/down；"
          "[判定依据] 最终类别=dependency_down（末轮），逐次类别/类型/判定依据齐全，共尝试 3 次")
    def call(attempt: int):
        _raise([TimeoutError("deadline"), TimeoutError("deadline2"),
                ConnectionRefusedError("refused 9.9.9.9")][attempt - 1])

    with pytest.raises(RetryExhaustedError) as ei:
        run_with_retries(call, max_attempts=3, name="d")
    view = retry_failure_view(ei.value, "rid-retry-1")
    assert view["category"] == "dependency_down"
    assert view["http_status"] == 503
    assert view["message"] == "dependency unavailable"
    assert view["attempts_count"] == 3 and view["retried"] == 2
    assert view["final_category_source"] == "last_attempt"
    assert view["final_category_attempt"] == 3
    assert [a["category"] for a in view["attempts"]] == [
        "timeout", "timeout", "dependency_down"
    ]
    assert [a["raised_type"] for a in view["attempts"]] == [
        "TimeoutError", "TimeoutError", "ConnectionRefusedError"
    ]
    # 每次超时与连接失败的判定依据都清晰可辨
    assert view["attempts"][0]["reason"] == "raised type is TimeoutError"
    assert view["attempts"][2]["reason"] == "raised type is ConnectionError"
    # 任何底层报错原文（地址等）都不出现在对外结论中
    assert "9.9.9.9" not in json.dumps(view, ensure_ascii=False)


def test_retry_all_timeouts_is_timeout_504():
    print("\n[输入] 两次尝试均超时；[判定依据] 最终 timeout/504，可与连接失败明确区分")
    def call(attempt: int):
        _raise(TimeoutError("fake deadline secret"))

    with pytest.raises(RetryExhaustedError) as ei:
        run_with_retries(call, max_attempts=2)
    view = retry_failure_view(ei.value, None)
    assert view["category"] == "timeout" and view["http_status"] == 504
    assert len(view["attempts"]) == 2
    assert all(a["category"] == "timeout" for a in view["attempts"])
    assert all(a["raised_type"] == "TimeoutError" for a in view["attempts"])


def test_async_retry_exhausted_matches_sync_semantics():
    print("\n[输入] 异步重试：down 后 timeout；[判定依据] 最终 timeout/504，尝试 2 次")
    async def call(attempt: int):
        await asyncio.sleep(0)
        if attempt == 1:
            raise ConnectionRefusedError("down host secret")
        raise TimeoutError("deadline secret")

    async def scenario():
        with pytest.raises(RetryExhaustedError) as ei:
            await run_with_retries_async(call, max_attempts=2, name="ad")
        return retry_failure_view(ei.value, "rid-ar")

    view = asyncio.run(scenario())
    assert view["category"] == "timeout"
    assert [a["category"] for a in view["attempts"]] == ["dependency_down", "timeout"]
    assert "secret" not in json.dumps(view)


def test_http_retry_failure_counts_once_and_explains_decision(client):
    print("\n[输入] GET /retry?attempts=timeout&attempts=timeout&attempts=down；"
          "[判定依据] 503 统一结论、含 3 次尝试明细、该请求在指标中只出现 1 次，"
          "响应体不含任何底层报错原文")
    rid = "rid-http-retry-001"
    resp = client.get("/retry/x", params=[("attempts", "timeout"), ("attempts", "timeout"),
                                          ("attempts", "down")],
                      headers={"X-Request-Id": rid})
    assert resp.status_code == 503
    assert resp.headers["x-request-id"] == rid
    err = resp.json()["error"]
    assert err["category"] == "dependency_down"
    assert err["attempts_count"] == 3 and err["retried"] == 2
    assert [a["category"] for a in err["attempts"]] == [
        "timeout", "timeout", "dependency_down"
    ]
    body_text = resp.text
    assert "fake" not in body_text and "6379" not in body_text and "127.0.0.1" not in body_text

    # 指标去重：同一次业务请求只有 1 个 failed 计数
    snap = metrics_mod.registry.snapshot()
    route = snap["by_route"]["/retry/{item}"]
    assert route == {"ok": 0, "failed": 1, "total": 1}
    assert snap["total"] == 1

    # 日志：3 次尝试事件 + 1 个 retry.exhausted + 1 个 request.failure，id 全部一致
    evts = [e for e in events_mod.recorder.snapshot() if e["request_id"] == rid]
    assert [e["event"] for e in evts].count("retry.attempt") == 3
    exhausted = next(e for e in evts if e["event"] == "retry.exhausted")
    assert exhausted["final_category"] == "dependency_down"
    failure = next(e for e in evts if e["event"] == "request.failure")
    assert failure["decision_basis"]["retry"]["attempts_count"] == 3
    assert failure["decision_basis"]["retry"]["final_category_source"] == "last_attempt"


def test_http_async_retry_success_counts_once(client):
    print("\n[输入] GET /a-retry?attempts=down&attempts=ok；"
          "[判定依据] 200 成功、指标只计 1 次 ok，中途失败明细仍在日志中")
    resp = client.get("/a-retry/x", params=[("attempts", "down"), ("attempts", "ok")],
                      headers={"X-Request-Id": "rid-http-retry-002"})
    assert resp.status_code == 200
    assert resp.json()["data"]["output"] == "dependency-result[ok]"
    snap = metrics_mod.registry.snapshot()
    assert snap["by_route"]["/a-retry/{item}"] == {"ok": 1, "failed": 0, "total": 1}
    attempts = [e for e in events_mod.recorder.snapshot() if e["event"] == "retry.attempt"]
    assert [a["outcome"] for a in attempts] == ["failed", "ok"]


def test_http_mixed_retries_under_concurrency_count_exactly_once_each():
    print("\n[输入] 40 个并发请求，每个走 3 次尝试（2 失败 1 成功 或 3 次全失败）；"
          "[判定依据] 指标 total==40，重试不产生重复计数；逐请求响应 id 不串用")
    from concurrent.futures import ThreadPoolExecutor

    from starlette.testclient import TestClient

    from main import app

    def one(i: int):
        rid = f"rid-rc-{i:03d}"
        with TestClient(app, raise_server_exceptions=False) as local:
            if i % 2 == 0:
                r = local.get("/retry/x",
                              params=[("attempts", "timeout"), ("attempts", "down"),
                                      ("attempts", "ok")],
                              headers={"X-Request-Id": rid})
                return rid, r.status_code
            r = local.get("/retry/x",
                          params=[("attempts", "timeout"), ("attempts", "timeout"),
                                  ("attempts", "down")],
                          headers={"X-Request-Id": rid})
            return rid, r.status_code

    with ThreadPoolExecutor(max_workers=16) as pool:
        outcomes = list(pool.map(one, range(40)))
    statuses = {s for _rid, s in outcomes}
    assert statuses == {200, 503}
    snap = metrics_mod.registry.snapshot()
    assert snap["total"] == 40
    route = snap["by_route"]["/retry/{item}"]
    assert route["ok"] == 20 and route["failed"] == 20 and route["total"] == 40
