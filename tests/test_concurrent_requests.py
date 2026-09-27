"""并发 HTTP 请求：关联标识、指标计数、错误分类三者一致且可复现。"""
from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor

from observability.registry import event_log, metrics

# (路径, 期望状态码, 期望结果, 期望类别)
PLAN = [
    ("/ping", 200, "success", ""),
    ("/ping", 200, "success", ""),
    ("/downstream?mode=ok", 200, "success", ""),
    ("/downstream?mode=timeout", 504, "error", "timeout"),
    ("/downstream?mode=unavailable", 503, "error", "dependency_unavailable"),
    ("/downstream?mode=ok&value=-1", 400, "error", "client_error"),
    ("/internal-fault", 500, "error", "internal_error"),
]


def test_concurrent_requests_consistent_tracing_and_metrics(client):
    repeats = 20
    jobs = PLAN * repeats
    findings: list[tuple] = []
    lock = threading.Lock()

    def one(idx_path):
        idx, (path, want_status, _result, _cat) = idx_path
        rid = f"req-cc-{idx:04d}"
        r = client.get(path, headers={"X-Request-ID": rid})
        with lock:
            findings.append((rid, path, r.status_code,
                             r.headers.get("x-request-id"),
                             r.json().get("error", {}).get("category", "")))

    indexed = list(enumerate(jobs))
    with ThreadPoolExecutor(max_workers=16) as pool:
        list(pool.map(one, indexed))

    # 1) 状态码与类别逐条符合预期，且响应头标识严格等于入站标识（无串号）
    by_path = {p: (s, c) for p, s, _, c in PLAN}
    for rid, path, status, header_id, cat in findings:
        want_status, want_cat = by_path[path]
        assert status == want_status, (path, status)
        assert header_id == rid, f"id leaked: {rid} -> {header_id}"
        assert cat == want_cat, (path, cat)
    print(f"[判定] 并发逐条一致 | 输入={len(jobs)} 个并发请求(7 类 x {repeats}) | "
          f"依据=状态码/类别符合, 响应头标识==入站标识 | 实际全部一致")

    # 2) 指标口径：总数、成功/失败、各错误类别
    snap = metrics.snapshot()
    t = snap.totals
    assert t["requests"] == len(jobs)
    success_n = sum(1 for j in jobs if j[2] == "success")
    error_n = len(jobs) - success_n
    print(f"[判定] 指标聚合 | 输入=成功 {success_n} / 失败 {error_n} | "
          f"依据=requests={len(jobs)} 且 success+error=requests | 实际={t}")
    assert t["success"] == success_n
    assert t["error"] == error_n
    assert t["requests"] == t["success"] + t["error"]
    assert t["error:timeout"] == repeats
    assert t["error:dependency_unavailable"] == repeats
    assert t["error:client_error"] == repeats
    assert t["error:internal_error"] == repeats

    # 3) 事件：每个请求恰好一对 started/completed，且标识不串
    completed = event_log.query(kind="request.completed")
    started = event_log.query(kind="request.started")
    assert len(completed) == len(jobs)
    assert len(started) == len(jobs)
    id_to_events = {}
    for e in completed:
        id_to_events.setdefault(e.request_id, []).append(e)
    assert all(len(v) == 1 for v in id_to_events.values()), "每个标识恰有一条完成事件"
    print(f"[判定] 事件成对 | 输入={len(jobs)} 请求 | "
          f"依据=started/completed 各 {len(jobs)} 条, 标识唯一 | 实际通过")


def test_repeated_runs_are_reproducible(client):
    """同一组请求跑两遍，重置后各自的聚合结论必须完全一致。"""
    def run_once():
        metrics.reset()
        event_log.reset()
        for path, status, _r, _c in PLAN:
            r = client.get(path)
            assert r.status_code == status
        return metrics.snapshot().totals

    first = run_once()
    second = run_once()
    print(f"[判定] 可复现 | 输入=同一组 {len(PLAN)} 请求跑两遍(每轮先重置) | "
          f"依据=两轮 totals 完全相等 | 实际={first == second}")
    assert first == second
