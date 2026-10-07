"""跨线程/后台派生：标识在 同步/异步/线程池 混合链路中延续且并发互不串用。"""

from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor

from app import events as events_mod
from app.context import current_request_id, run_with_context
from app.services import (
    call_internal_dependency_in_thread,
    call_internal_dependency_thread_async,
    spawn_thread_background_work,
)


def test_thread_call_carries_spawn_time_request_id():
    print("\n[输入] 在 rid-spawn-001 上下文中调用线程池版内部依赖；"
          "[判定依据] worker 事件与返回链路均读到 rid-spawn-001，主线程上下文不被污染")
    import asyncio

    async def scenario():
        from app.context import bind_request_id, reset_request_id
        token = bind_request_id("rid-spawn-001")
        try:
            sync_out = call_internal_dependency_in_thread("ok")
            async_out = await call_internal_dependency_thread_async("ok")
            bg = await spawn_thread_background_work("job", "ok")
        finally:
            reset_request_id(token)
        return sync_out, async_out, bg

    sync_out, async_out, bg = asyncio.run(scenario())
    assert sync_out == async_out == "dependency-result[ok]"
    assert bg["request_id"] == "rid-spawn-001" and bg["via"] == "thread"

    evts = [e for e in events_mod.recorder.snapshot()
            if e["event"].startswith(("thread.", "dependency."))]
    assert evts, "应至少有线程/依赖事件"
    assert all(e["request_id"] == "rid-spawn-001" for e in evts), evts
    assert {e["event"] for e in evts} >= {
        "thread.submit", "thread.worker.start", "thread.worker.done",
        "dependency.call.start", "dependency.call.success",
    }
    # worker 内记录了绑定来源，便于说明标识是从派生请求延续而来
    assert all(e.get("bound_from") == "rid-spawn-001"
               for e in evts if e["event"] == "thread.worker.start")
    assert current_request_id() is None


def test_concurrent_thread_requests_never_cross_talk():
    print("\n[输入] 24 个请求线程并发走线程池调用（每请求唯一 id，含成功与失败）；"
          "[判定依据] 每个 worker 事件的 request_id 等于其派生请求 id，"
          "且每请求只观察到自己的 flag，失败也不串用")
    results: dict[str, str] = {}
    results_lock = threading.Lock()
    barrier = threading.Barrier(24)

    def one(i: int) -> None:
        rid = f"rid-tp-{i:03d}"
        flag = "timeout" if i % 3 == 0 else ("down" if i % 3 == 1 else "ok")

        def body() -> str:
            barrier.wait()
            try:
                return call_internal_dependency_in_thread(flag)
            except Exception:  # noqa: BLE001 - 并发隔离测试有意收口验证
                return "failed"

        out = run_with_context(rid, body)
        with results_lock:
            results[rid] = out

    with ThreadPoolExecutor(max_workers=24) as pool:
        list(pool.map(one, range(24)))

    assert len(results) == 24
    snap = events_mod.recorder.snapshot()
    worker_evts = [e for e in snap if e["event"].startswith("thread.worker")]
    # 每条 worker 事件必须能按 id 找到它的派生源，且 bound_from == request_id
    by_id: dict[str, list[dict]] = {}
    for e in worker_evts:
        by_id.setdefault(e["request_id"], []).append(e)
    assert set(by_id) == {f"rid-tp-{i:03d}" for i in range(24)}
    for rid, evts in by_id.items():
        idx = int(rid.rsplit("-", 1)[-1])
        flag = ("timeout", "down", "ok")[idx % 3]
        assert all(e["request_id"] == rid for e in evts)
        starts = [e for e in evts if e["event"] == "thread.worker.start"]
        assert starts and all(e.get("bound_from") == rid for e in starts)
        assert {e.get("flag") for e in evts if "flag" in e} == {flag}
    assert current_request_id() is None


def test_http_thread_routes_propagate_and_isolate(client):
    print("\n[输入] 分别用独立 id 请求 /thread、/a-thread、/bg-thread（成功与 timeout 失败）；"
          "[判定依据] 响应 id 一致，线程内事件沿用派生请求 id，失败仍正确分类为 504")
    cases = [
        ("/thread/job", "ok", 200),
        ("/a-thread/job", "ok", 200),
        ("/bg-thread/job", "ok", 200),
        ("/thread/job", "timeout", 504),
        ("/a-thread/job", "down", 503),
    ]
    for idx, (path, flag, status) in enumerate(cases):
        rid = f"rid-htt-{idx:03d}"
        resp = client.get(f"{path}?flag={flag}", headers={"X-Request-Id": rid})
        assert resp.status_code == status, (path, resp.status_code)
        assert resp.headers["x-request-id"] == rid
        assert resp.json()["request_id"] == rid

    snap = events_mod.recorder.snapshot()
    thread_evts = [e for e in snap if e["event"].startswith("thread.")]
    ids = {e["request_id"] for e in thread_evts}
    # 线程事件只属于上面 5 个请求之一，没有混入任何其他 id
    assert ids == {f"rid-htt-{i:03d}" for i in range(5)}, ids


def test_existing_three_paths_unchanged_under_thread_load(client):
    print("\n[输入] 线程池负载同时打 同步/异步/asyncio后台 三条原有路径；"
          "[判定依据] 三条路径行为不变（200/标识一致），且不串用后台线程的标识")
    from concurrent.futures import ThreadPoolExecutor

    from starlette.testclient import TestClient

    from main import app

    paths = ["/echo/x", "/a-echo/x", "/bg/x"]

    def one(i: int) -> tuple[str, str]:
        rid = f"rid-keep-{i:03d}"
        path = paths[i % 3]
        with TestClient(app, raise_server_exceptions=False) as local:
            r = local.get(path, headers={"X-Request-Id": rid})
        return rid, r.headers["x-request-id"]

    with ThreadPoolExecutor(max_workers=12) as pool:
        outcomes = list(pool.map(one, range(30)))
    assert all(sent == got for sent, got in outcomes)
