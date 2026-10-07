"""跨线程执行与后台派生：标识延续、并发隔离、混合链路一致。"""

from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor

from app import events as events_mod
from app.context import current_request_id
from app.threads import current_thread_name, submit_with_context


def test_submit_with_context_inherits_spawn_time_request_id():
    print("\n[输入] 在 rid-thr-001 上下文中向共享线程池派生工作；"
          "[判定依据] worker 线程内读到的标识与派生时刻一致，且确实跑在别的线程")
    from app.context import bind_request_id, reset_request_id

    token = bind_request_id("rid-thr-001")
    try:
        future = submit_with_context(lambda: (current_request_id(), current_thread_name()))
        rid, thread_name = future.result(timeout=5)
    finally:
        reset_request_id(token)
    assert rid == "rid-thr-001"
    assert thread_name.startswith("obs-worker")
    assert current_request_id() is None  # 提交方上下文不被污染
    print(f"  worker={thread_name}, rid={rid}")


def test_concurrent_thread_submissions_never_cross_talk():
    print("\n[输入] 32 个线程各自绑定不同标识并各派生 20 个任务；"
          "[判定依据] 每个任务读到的标识都等于它派生时刻的那个，无一串用")
    mismatches: list[tuple[str, str]] = []

    def owner(i: int) -> None:
        from app.context import bind_request_id, reset_request_id

        rid = f"rid-owner-{i:03d}"
        token = bind_request_id(rid)
        try:
            futures = [submit_with_context(current_request_id) for _ in range(20)]
            for f in futures:
                got = f.result(timeout=10)
                if got != rid:
                    mismatches.append((rid, got))
        finally:
            reset_request_id(token)

    threads = [threading.Thread(target=owner, args=(i,)) for i in range(32)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert not mismatches


def test_http_thread_path_events_carry_same_request_id(client):
    print("\n[输入] GET /thread/x 带 rid-thread-http1；"
          "[判定依据] 响应头/体与 worker 线程产生的事件标识三处一致")
    rid = "rid-thread-http1"
    resp = client.get("/thread/x", headers={"X-Request-Id": rid})
    assert resp.status_code == 200
    assert resp.headers["x-request-id"] == rid
    assert resp.json()["request_id"] == rid
    events = [e for e in events_mod.recorder.snapshot()
              if e["event"].startswith(("thread.call.", "dependency.call."))]
    assert events, "expected thread/dependency events"
    assert all(e["request_id"] == rid for e in events)
    kinds = {e["event"] for e in events}
    assert {"thread.call.start", "thread.call.done",
            "dependency.call.start", "dependency.call.success"} <= kinds


def test_http_mixed_chain_single_identity(client):
    print("\n[输入] GET /mix/x 一次请求串走 同步/异步/线程 三条链路；"
          "[判定依据] 该请求全部事件（含 worker 线程事件）标识完全一致")
    rid = "rid-mix-http-01"
    resp = client.get("/mix/x", headers={"X-Request-Id": rid})
    assert resp.status_code == 200
    body = resp.json()
    assert body["data"]["via"] == "mixed"
    events = [e for e in events_mod.recorder.snapshot() if e.get("request_id") == rid]
    modes = {e.get("mode") for e in events if e["event"] == "dependency.call.start"}
    assert modes == {"sync", "async"}  # 线程路径内部是 sync 模式事件
    assert any(e["event"] == "thread.call.done" for e in events)
    assert len(events) >= 10
    print(f"  混合链路事件数={len(events)}, 标识全部={rid}")


def test_http_concurrent_thread_requests_isolated(client):
    print("\n[输入] 24 个并发 /thread 请求各自带不同标识；"
          "[判定依据] 每个响应的标识与请求一致，事件按标识严格分组不串用")
    rids = [f"rid-thr-iso-{i:03d}" for i in range(24)]

    def call(rid: str) -> str:
        resp = client.get("/thread/x", headers={"X-Request-Id": rid})
        assert resp.status_code == 200
        return resp.json()["request_id"]

    with ThreadPoolExecutor(max_workers=8) as pool:
        got = list(pool.map(call, rids))
    assert got == rids

    events = events_mod.recorder.snapshot()
    for rid in rids:
        mine = [e for e in events if e.get("request_id") == rid]
        assert any(e["event"] == "thread.call.start" for e in mine), rid
        assert any(e["event"] == "dependency.call.success" for e in mine), rid
        assert any(e["event"] == "request.complete" for e in mine), rid
    # 事件标识集合不超出本次请求集合（无串用到别的标识）
    thread_events = [e for e in events if e["event"].startswith("thread.call.")]
    assert {e["request_id"] for e in thread_events} <= set(rids)


def test_http_thread_failure_keeps_identity_and_classification(client):
    print("\n[输入] /thread/x?flag=timeout；[判定依据] 跨线程异常同样归一化为 504 且标识不丢")
    rid = "rid-thr-fail-01"
    resp = client.get("/thread/x?flag=timeout", headers={"X-Request-Id": rid})
    assert resp.status_code == 504
    err = resp.json()["error"]
    assert err["category"] == "timeout" and err["request_id"] == rid
    failure = next(e for e in events_mod.recorder.snapshot()
                   if e["event"] == "request.failure")
    assert failure["request_id"] == rid
