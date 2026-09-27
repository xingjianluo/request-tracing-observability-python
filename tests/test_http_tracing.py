"""HTTP 端到端：关联标识在响应与日志中稳定、可入站透传、后台任务延续。"""

from __future__ import annotations

from app import events as events_mod


def test_generated_id_is_stable_across_body_header_and_logs(client):
    print("\n[输入] GET / 不带入站标识；"
          "[判定依据] 生成的 id 在响应头、响应体、request.start/complete 事件中完全一致")
    resp = client.get("/")
    assert resp.status_code == 200
    rid = resp.headers["x-request-id"]
    assert rid.startswith("req-") and len(rid) == 36
    body = resp.json()
    assert body["ok"] is True
    assert body["request_id"] == rid

    req_events = [e for e in events_mod.recorder.snapshot() if e.get("event", "").startswith("request.")]
    assert {e["request_id"] for e in req_events} == {rid}
    assert req_events[0]["id_basis"].endswith("generated new")


def test_inbound_id_is_inherited(client):
    print("\n[输入] GET / 携带 X-Request-Id: rid-inbound-001；"
          "[判定依据] 原样回显，判定依据为继承入站头")
    resp = client.get("/", headers={"X-Request-Id": "rid-inbound-001"})
    assert resp.headers["x-request-id"] == "rid-inbound-001"
    assert resp.json()["request_id"] == "rid-inbound-001"
    start_evt = next(e for e in events_mod.recorder.snapshot() if e["event"] == "request.start")
    assert start_evt["id_basis"] == "inherited inbound x-request-id"


def test_unsafe_inbound_id_rejected(client):
    print("\n[输入] X-Request-Id 含空格且超长风险字符 'bad id'；"
          "[判定依据] 安全规则拒绝并现场重新生成，判定依据写明拒绝原因")
    resp = client.get("/", headers={"X-Request-Id": "bad id"})
    rid = resp.headers["x-request-id"]
    assert rid.startswith("req-")
    assert rid != "bad id"
    start_evt = next(e for e in events_mod.recorder.snapshot() if e["event"] == "request.start")
    assert "rejected" in start_evt["id_basis"]


def test_sync_internal_call_propagates_id(client):
    print("\n[输入] GET /echo/widget 携带 rid-sync-0001；"
          "[判定依据] 内部依赖调用事件携带同一 id，正常响应信封结构")
    resp = client.get("/echo/widget", headers={"X-Request-Id": "rid-sync-0001"})
    assert resp.status_code == 200
    data = resp.json()["data"]
    assert data["item"] == "widget" and data["via"] == "sync"
    dep_events = [e for e in events_mod.recorder.snapshot() if e["event"].startswith("dependency.")]
    assert dep_events and all(e["request_id"] == "rid-sync-0001" for e in dep_events)


def test_async_internal_call_propagates_id(client):
    print("\n[输入] GET /a-echo/widget 携带 rid-async-001；"
          "[判定依据] await 链路中的依赖事件携带同一 id")
    resp = client.get("/a-echo/widget", headers={"X-Request-Id": "rid-async-001"})
    assert resp.status_code == 200
    dep_events = [e for e in events_mod.recorder.snapshot() if e["event"].startswith("dependency.")]
    assert dep_events and all(e["request_id"] == "rid-async-001" for e in dep_events)


def test_background_task_inherits_id(client):
    print("\n[输入] GET /bg/job1 携带 rid-bg-0000001；"
          "[判定依据] spawn/start/done 与后台内部调用事件全部沿用同一 id")
    resp = client.get("/bg/job1", headers={"X-Request-Id": "rid-bg-0000001"})
    assert resp.status_code == 200, resp.text
    assert resp.json()["data"]["background"]["request_id"] == "rid-bg-0000001"

    bg_events = [
        e
        for e in events_mod.recorder.snapshot()
        if e["event"].startswith("background.") or e["event"].startswith("dependency.")
    ]
    assert {e["event"] for e in bg_events if e["event"].startswith("background.")} == {
        "background.spawn",
        "background.start",
        "background.done",
    }
    assert all(e["request_id"] == "rid-bg-0000001" for e in bg_events)
