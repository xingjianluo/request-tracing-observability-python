"""指标聚合与事件日志：并发写入/查询/重置的一致性。"""
from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor

from observability.events import EventLog
from observability.metrics import MetricsRegistry


def test_metrics_concurrent_record_no_loss_no_dup():
    reg = MetricsRegistry()
    threads, per = 16, 1000

    def worker():
        for _ in range(per):
            reg.record("/a", "success")
        reg.record("/b", "error", "timeout")
        reg.record("/b", "error", "client_error")

    with ThreadPoolExecutor(max_workers=threads) as pool:
        list(pool.map(lambda _: worker(), range(threads)))

    snap = reg.snapshot()
    expected_total = threads * (per + 2)
    print(f"[判定] 指标并发 | 输入={threads} 线程, 各 {per} 成功 + 2 错误 | "
          f"依据=requests 应等于 {expected_total} | 实际={snap.totals['requests']}")
    assert snap.totals["requests"] == expected_total
    assert snap.totals["success"] == threads * per
    assert snap.totals["error"] == threads * 2
    # 口径恒等：requests == success + error
    assert snap.totals["requests"] == snap.totals["success"] + snap.totals["error"]
    assert snap.find("/b", "error:timeout") == threads
    assert snap.find("/b", "error:client_error") == threads


def test_metrics_snapshot_is_reproducible():
    reg = MetricsRegistry()
    reg.record("/a", "success")
    s1 = reg.snapshot()
    s2 = reg.snapshot()
    print(f"[判定] 快照可复现 | 输入=无写入连续两次 snapshot | "
          f"依据=计数视图相同 | 实际 same={s1.totals == s2.totals}")
    assert s1.totals == s2.totals
    assert s1.by_route == s2.by_route


def test_metrics_reset_returns_last_window_and_zeroes():
    reg = MetricsRegistry()
    reg.record("/a", "success")
    drained = reg.reset()
    after = reg.snapshot()
    print(f"[判定] 重置原子性 | 输入=1 次记录后 reset | "
          f"依据=重置前=1, 重置后=0 | 实际 before={drained.totals['requests']}, "
          f"after={after.totals['requests']}")
    assert drained.totals["requests"] == 1
    assert after.totals["requests"] == 0


def test_metrics_concurrent_record_query_reset():
    """写入、查询、重置同时发生：任取窗口，恒等式必须成立、计数不为负。"""
    reg = MetricsRegistry()
    stop = threading.Event()

    def writer():
        i = 0
        while not stop.is_set():
            cat = ("timeout", "client_error", "dependency_unavailable")[i % 3]
            reg.record("/c", "error", cat)
            i += 1

    def reader():
        while not stop.is_set():
            s = reg.snapshot()
            assert s.totals["requests"] == s.totals["success"] + s.totals["error"]
            for bucket in s.by_route.values():
                assert bucket["requests"] >= 0

    tw = [threading.Thread(target=writer) for _ in range(6)]
    tr = [threading.Thread(target=reader) for _ in range(2)]
    [t.start() for t in tw + tr]
    for _ in range(20):
        d = reg.reset()
        assert d.totals["requests"] == d.totals["success"] + d.totals["error"]
    stop.set()
    [t.join() for t in tw + tr]
    print("[判定] 读写重置并发 | 输入=6 写 + 2 读 + 20 次重置 | "
          "依据=全程恒等式成立、无负值 | 实际=通过")


def test_events_concurrent_append_seq_unique_and_contiguous():
    log = EventLog()
    threads, per = 12, 500

    def worker(idx):
        for _ in range(per):
            log.emit(request_id=f"req-{idx}", kind="k", route="/r")

    with ThreadPoolExecutor(max_workers=threads) as pool:
        list(pool.map(worker, range(threads)))

    seqs = sorted(e.seq for e in log.query())
    print(f"[判定] 事件序号 | 输入={threads}x{per}={threads*per} | "
          f"依据=序号为 1..N 连续不重 | 实际 len={len(seqs)}, "
          f"min={seqs[0]}, max={seqs[-1]}")
    assert seqs == list(range(1, threads * per + 1))
    assert len(log.query(request_id="req-3")) == per
    drained = log.reset()
    assert len(drained) == threads * per
    assert len(log) == 0
