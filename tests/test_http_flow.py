"""端到端 HTTP：关联标识稳定透传、四类失败可区分、指标与事件一致。"""
from __future__ import annotations

from observability.registry import event_log, metrics


def _report(name, given, basis, actual):
    print(f"[判定] {name} | 输入={given} | 依据={basis} | 实际={actual}")


def test_request_id_stable_in_header_body_and_events(client):
    r = client.get("/ping", headers={"X-Request-ID": "stable-123"})
    header_id = r.headers["x-request-id"]
    body_id = r.json()["request_id"]
    events = event_log.query(request_id="stable-123")
    kinds = {e.kind for e in events}
    _report("标识稳定", "入站 X-Request-ID=stable-123",
            "响应头/响应体/事件三处一致且事件成对",
            f"header={header_id}, body={body_id}, kinds={kinds}")
    assert r.status_code == 200
    assert header_id == body_id == "stable-123"
    assert kinds == {"request.started", "request.completed"}


def test_generated_id_when_header_absent_or_invalid(client):
    r1 = client.get("/ping")
    r2 = client.get("/ping", headers={"X-Request-ID": "bad spaced id!"})
    id1, id2 = r1.headers["x-request-id"], r2.headers["x-request-id"]
    _report("缺省生成", "无标识 / 非法标识", "各自生成 req- 前缀且互不相同",
            f"{id1[:8]}... vs {id2[:8]}...")
    assert id1.startswith("req-") and id2.startswith("req-")
    assert id1 != id2


def test_four_error_classes_distinguishable_over_http(client):
    cases = [
        ("/downstream?mode=ok&value=-1", 400, "client_error"),
        ("/downstream?mode=unavailable", 503, "dependency_unavailable"),
        ("/downstream?mode=timeout", 504, "timeout"),
        ("/internal-fault", 500, "internal_error"),
    ]
    results = []
    for path, want_status, want_cat in cases:
        r = client.get(path)
        body = r.json()
        results.append((path, r.status_code, body["error"]["category"],
                        body["request_id"] == r.headers["x-request-id"]))
        assert r.status_code == want_status
        assert body["error"]["category"] == want_cat
        assert body["request_id"] == r.headers["x-request-id"]
        # 底层异常文本不得外泄
        assert "simulated" not in str(body) and "Traceback" not in str(body)
    _report("HTTP 四分类", [c[0] for c in cases],
            "状态码与 category 匹配、标识一致、无底层泄露", results)


def test_validation_error_is_client_error(client):
    r = client.post("/echo", json={})  # message 缺失
    body = r.json()
    _report("参数校验", "POST /echo 空体", "400 client_error",
            f"{r.status_code} {body['error']['category']}")
    assert r.status_code == 400
    assert body["error"]["category"] == "client_error"


def test_metrics_consistency_after_mixed_traffic(client):
    for _ in range(3):
        client.get("/ping")
    for _ in range(2):
        client.get("/downstream?mode=timeout")
    client.get("/downstream?mode=unavailable")
    snap = metrics.snapshot()
    t = snap.totals
    _report("指标口径", "3 成功 + 2 超时 + 1 不可用",
            "requests=6=success+error, timeout=2, unavailable=1", t)
    assert t["requests"] == 6
    assert t["requests"] == t["success"] + t["error"]
    assert t["error:timeout"] == 2
    assert t["error:dependency_unavailable"] == 1
    # 每个失败请求都带类别，未分类错误数为 0
    assert t["error"] == sum(v for k, v in t.items() if k.startswith("error:"))


def test_background_task_keeps_request_id_in_logs(client, caplog):
    import logging
    caplog.set_level(logging.INFO, logger="app.routes")
    r = client.post("/echo", json={"message": "hello"},
                    headers={"X-Request-ID": "bg-trace-1"})
    assert r.status_code == 200
    bg_records = [rec for rec in caplog.records if rec.getMessage() == "background side-effect"]
    _report("后台标识延续", "POST /echo 带 bg-trace-1",
            "后台日志的 request_id 等于 bg-trace-1",
            [getattr(rec, "request_id", None) for rec in bg_records])
    assert bg_records and all(
        getattr(rec, "request_id", None) == "bg-trace-1" for rec in bg_records)


def test_admin_metrics_and_events_query_and_reset(client):
    client.get("/ping")
    m = client.get("/_/metrics").json()
    assert m["metrics"]["totals"]["requests"] == 1
    ev = client.get("/_/events", params={"kind": "request.completed"}).json()
    assert ev["total_matched"] == 1
    reset = client.post("/_/metrics/reset").json()
    _report("管理端重置", "1 次请求后 reset",
            "返回重置前=1 且随后查询=0",
            f"reset={reset['reset']['totals']['requests']}, "
            f"after={metrics.snapshot().totals['requests']}")
    assert reset["reset"]["totals"]["requests"] == 1
    assert metrics.snapshot().totals["requests"] == 0
