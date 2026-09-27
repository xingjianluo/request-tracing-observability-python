"""并发请求场景：关联标识不串用、指标计数精确、错误分类稳定。"""

from __future__ import annotations

from collections import Counter
from concurrent.futures import ThreadPoolExecutor

from starlette.testclient import TestClient

from app import events as events_mod
from app import metrics as metrics_mod

FLAGS = ["ok", "ok", "ok", "ok", "down", "timeout", "bad", "boom"]
FLAG_EXPECT = {
    "ok": (200, "ok", None),
    "down": (503, "failed", "dependency_down"),
    "timeout": (504, "failed", "timeout"),
    "bad": (400, "failed", "client_error"),
    "boom": (500, "failed", "internal_error"),
}


def test_mixed_concurrent_requests_are_consistent():
    """128 个客户端线程 x 8 种结果 = 1024 个并发请求。

    校验三件事：
    1. 每个请求的 响应头 id == 响应体 id，且等于该客户端发送的入站 id；
    2. 按 flag 的结果分类与状态码全部符合预期；
    3. 服务端日志中同一 id 的事件不携带任何其他 id 的字段（不串用）。
    """
    print("\n[输入] 128 线程并发，每线程依次发 8 个请求（flag=ok*4/down/timeout/bad/boom），"
          "每请求独立入站 id；")
    print("[判定依据] 头/体 id 一致、状态码与分类精确、事件按 id 归属无串用")

    from main import app

    outcomes: list[tuple[str, int, str, str | None, str]] = []

    def one_call(worker_idx: int, round_idx: int, flag: str) -> tuple[str, int, str, str | None, str]:
        rid = f"rid-c{worker_idx:03d}-r{round_idx:02d}-{flag}"
        with TestClient(app, raise_server_exceptions=False) as local:
            resp = local.get(f"/echo/item-{worker_idx}-{round_idx}?flag={flag}",
                             headers={"X-Request-Id": rid})
        body = resp.json()
        header_id = resp.headers["x-request-id"]
        if body.get("ok"):
            category = None
            body_id = body["request_id"]
        else:
            category = body["error"]["category"]
            body_id = body["request_id"]
        # 标识一致性：头 == 体 == 入站
        assert header_id == body_id == rid, (header_id, body_id, rid)
        return rid, resp.status_code, "ok" if body.get("ok") else "failed", category, flag

    with ThreadPoolExecutor(max_workers=32) as pool:
        futures = []
        for w in range(128):
            for r, flag in enumerate(FLAGS):
                futures.append(pool.submit(one_call, w, r, flag))
        outcomes = [f.result() for f in futures]

    # 1) 数量与分类精确
    assert len(outcomes) == 1024
    status_counter = Counter((flag, status, result, category)
                             for _rid, status, result, category, flag in outcomes)
    for flag, (status, result, category) in FLAG_EXPECT.items():
        expected_count = 128 * FLAGS.count(flag)
        assert status_counter[(flag, status, result, category)] == expected_count, (
            flag, status_counter
        )

    # 2) 每个入站 id 唯一（不存在串用/复用）
    rids = [o[0] for o in outcomes]
    assert len(set(rids)) == 1024

    # 3) 日志事件按 id 分组，每组只涉及自己的 flag（依据事件 flag 字段可反查）
    snap = events_mod.recorder.snapshot()
    by_id: dict[str, list[dict]] = {}
    for event in snap:
        rid = event.get("request_id")
        if rid and rid.startswith("rid-c"):
            by_id.setdefault(rid, []).append(event)
    assert len(by_id) == 1024
    for rid, evts in by_id.items():
        flag = rid.rsplit("-", 1)[-1]
        dep_flags = {e.get("flag") for e in evts if e["event"].startswith("dependency.")}
        assert dep_flags == {flag}, (rid, dep_flags)

    print(f"  共 {len(outcomes)} 请求；按 flag 计数："
          f"{dict(Counter(o[4] for o in outcomes))}；事件分组 {len(by_id)} 组全部归属正确")


def test_metrics_exact_under_concurrency():
    """并发结束后指标口径精确，并与逐响应观测完全对账。"""
    print("\n[输入] 同上 1024 并发请求（独立注册表视角由全局 registry 观测）；"
          "[判定依据] total=1024；ok=512；四类失败各 128")
    # 上一用例与本用例注册表不同（autouse 每用例重置），这里独立复跑一次以保持自包含
    from main import app

    def call(worker_idx: int, flag: str) -> tuple[int, str | None]:
        rid = f"rid-m{worker_idx:04d}-{flag}"
        with TestClient(app, raise_server_exceptions=False) as local:
            resp = local.get(f"/echo/m?flag={flag}", headers={"X-Request-Id": rid})
        body = resp.json()
        return resp.status_code, (None if body.get("ok") else body["error"]["category"])

    with ThreadPoolExecutor(max_workers=32) as pool:
        results = list(pool.map(lambda i: call(i, FLAGS[i % len(FLAGS)]), range(1024)))

    snap = metrics_mod.registry.snapshot()
    assert snap["total"] == 1024, snap["total"]
    route = snap["by_route"]["/echo/{item}"]
    assert route["total"] == 1024
    counts = Counter(status for status, _cat in results)
    assert counts[200] == 512
    by_cat = snap["by_error_category"]
    assert by_cat["none"] == 512
    assert by_cat["dependency_down"] == 128
    assert by_cat["timeout"] == 128
    assert by_cat["client_error"] == 128
    assert by_cat["internal_error"] == 128
    print(f"  指标对账一致：{by_cat}")
