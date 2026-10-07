"""容量上限与保留期限：事件与计数的自动回收、查询自洽、回收统计可见。"""

from __future__ import annotations

import io
import json
import threading

from app.events import EventRecorder
from app.metrics import NO_ERROR, RESULT_FAILED, RESULT_OK, MetricsRegistry


class FakeClock:
    """可手动推进的单调时钟，让 TTL 场景离线确定性复现。"""

    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


# ---------- 事件缓冲 ----------


def test_events_ttl_evicts_expired_and_stats_visible():
    print("\n[输入] TTL=10s 的记录器：t=0 写 3 条，推进 11s 后再写 2 条；"
          "[判定依据] 旧 3 条被按期限回收，snapshot 只剩新 2 条，统计可见 3 条 ttl 回收")
    clock = FakeClock()
    r = EventRecorder(capacity=100, ttl_seconds=10.0, stream=io.StringIO(), clock=clock)
    for i in range(3):
        r.record("old", i=i)
    clock.advance(11.0)
    r.record("new", i=3)
    r.record("new", i=4)

    snap = r.snapshot()
    assert [e["i"] for e in snap] == [3, 4]
    assert [e["seq"] for e in snap] == [4, 5]  # 全局序号连续，不受回收影响
    stats = r.eviction_stats()
    assert stats["evicted_by_ttl"] == 3
    assert stats["evicted_by_capacity"] == 0
    assert stats["evicted_total"] == 3 and stats["live"] == 2
    print(f"  回收统计: {stats}")


def test_events_capacity_and_ttl_eviction_counted_separately():
    print("\n[输入] 容量 4 + TTL 100s：t=0 写 4 条，推进 200s（全过期）再写 5 条；"
          "[判定依据] 4 条按 ttl 回收、1 条按容量回收，两种原因分别计数")
    clock = FakeClock()
    r = EventRecorder(capacity=4, ttl_seconds=100.0, stream=io.StringIO(), clock=clock)
    for i in range(4):
        r.record("e", i=i)
    clock.advance(200.0)
    for i in range(4, 9):
        r.record("e", i=i)
    snap = r.snapshot()
    assert [e["i"] for e in snap] == [5, 6, 7, 8]
    stats = r.eviction_stats()
    assert stats["evicted_by_ttl"] == 4
    assert stats["evicted_by_capacity"] == 1
    assert stats["evicted_total"] == 5
    print(f"  回收统计: {stats}")


def test_events_reset_after_eviction_keeps_valid_semantics():
    print("\n[输入] 部分事件被回收后 reset 再写；"
          "[判定依据] reset 只清存活缓冲，序号与回收统计不清零，之后写入正常")
    clock = FakeClock()
    r = EventRecorder(capacity=2, ttl_seconds=None, stream=io.StringIO(), clock=clock)
    for i in range(5):
        r.record("e", i=i)
    assert r.eviction_stats()["evicted_by_capacity"] == 3
    r.reset()
    assert r.snapshot() == []
    r.record("after-reset", i=99)
    snap = r.snapshot()
    assert [e["i"] for e in snap] == [99]
    assert snap[0]["seq"] == 6  # 序号延续，重置前后事件可区分
    assert r.eviction_stats()["evicted_total"] == 3  # 回收统计跨 reset 保留


def test_events_concurrent_write_with_ttl_eviction_stays_consistent():
    print("\n[输入] 8 线程 x 500 条写入，容量 200 + TTL 随写随过期；"
          "[判定依据] 序号无重复，live+evicted_total == 写入总数，snapshot 随时自洽")
    clock = FakeClock()
    r = EventRecorder(capacity=200, ttl_seconds=1.0, stream=io.StringIO(), clock=clock)
    violations: list[str] = []

    def writer() -> None:
        for _ in range(500):
            r.record("c")
            clock.advance(0.5)  # 推进时钟，让 TTL 回收在写入路径上持续发生

    def reader() -> None:
        for _ in range(200):
            snap = r.snapshot()
            seqs = [e["seq"] for e in snap]
            if len(seqs) != len(set(seqs)) or seqs != sorted(seqs):
                violations.append("snapshot not unique/sorted")

    threads = [threading.Thread(target=writer) for _ in range(8)]
    threads += [threading.Thread(target=reader) for _ in range(2)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert not violations
    stats = r.eviction_stats()
    total_written = 8 * 500
    assert stats["live"] + stats["evicted_total"] == total_written
    assert stats["evicted_total"] > 0  # 回收确实在并发下正常进行
    print(f"  写入 {total_written} 条, live={stats['live']}, evicted={stats['evicted_total']}")


# ---------- 指标计数 ----------


def test_metrics_capacity_evicts_lru_and_snapshot_stays_consistent():
    print("\n[输入] 容量 2 的注册表依次写 /a /b /c 三个维度键；"
          "[判定依据] 最久未更新的 /a 被回收，total 只算存活，recorded 含被回收部分")
    clock = FakeClock()
    m = MetricsRegistry(capacity=2, clock=clock)
    m.record("/a", RESULT_OK)
    clock.advance(1)
    m.record("/b", RESULT_OK)
    clock.advance(1)
    m.record("/c", RESULT_FAILED, "timeout")

    snap = m.snapshot()
    keys = {(r["route"], r["result"], r["error_category"]) for r in snap["counters"]}
    assert keys == {("/b", "ok", NO_ERROR), ("/c", "failed", "timeout")}
    assert snap["total"] == 2  # 不变式：存活维度计数之和 == total
    assert snap["recorded"] == 3  # 全量受理口径含被回收的 1 次
    assert snap["evicted"]["keys"] == 1 and snap["evicted"]["counts"] == 1
    assert snap["evicted"]["by_reason"]["capacity"] == {"keys": 1, "counts": 1}
    print(f"  evicted={snap['evicted']}, total={snap['total']}, recorded={snap['recorded']}")


def test_metrics_ttl_evicts_stale_keys_on_write():
    print("\n[输入] TTL=10s：t=0 写 /a，推进 11s 后写 /b；"
          "[判定依据] /a 按期限回收，/b 不受影响，evicted 按 ttl 原因计数")
    clock = FakeClock()
    m = MetricsRegistry(ttl_seconds=10.0, clock=clock)
    m.record("/a", RESULT_OK)
    m.record("/a", RESULT_OK)
    clock.advance(11.0)
    m.record("/b", RESULT_OK)

    snap = m.snapshot()
    assert snap["by_route"] == {"/b": {"ok": 1, "failed": 0, "total": 1}}
    assert snap["total"] == 1 and snap["recorded"] == 3
    assert snap["evicted"]["by_reason"]["ttl"] == {"keys": 1, "counts": 2}
    print(f"  evicted={snap['evicted']}")


def test_metrics_query_and_reset_unaffected_by_eviction():
    print("\n[输入] 回收发生后连续两次 snapshot、再 reset；"
          "[判定依据] 查询字节级可复现，reset 带走 recorded 全量历史后归零")
    clock = FakeClock()
    m = MetricsRegistry(capacity=1, ttl_seconds=5.0, clock=clock)
    m.record("/a", RESULT_OK)
    clock.advance(10.0)
    m.record("/b", RESULT_FAILED, "timeout")  # /a 先被 ttl 回收

    s1, s2 = m.snapshot(), m.snapshot()
    assert json.dumps(s1, sort_keys=True) == json.dumps(s2, sort_keys=True)
    taken = m.reset()
    assert taken["recorded"] == 2 and taken["evicted"]["counts"] == 1
    after = m.snapshot()
    assert after["total"] == 0 and after["recorded"] == 0
    assert after["evicted"]["counts"] == 0  # reset 后回收统计也归零重新累计


def test_metrics_concurrent_write_read_with_eviction_keeps_invariant():
    print("\n[输入] 12 写线程（64 个维度键）+ 4 读线程，容量 16 + TTL；"
          "[判定依据] 每次 snapshot 维度和==total，最终 recorded == 总写入次数")
    clock = FakeClock()
    m = MetricsRegistry(capacity=16, ttl_seconds=30.0, clock=clock)
    clock_lock = threading.Lock()
    stop = threading.Event()
    violations: list[tuple[int, int]] = []

    def writer(wid: int) -> None:
        for i in range(1000):
            m.record(f"/r{(wid + i) % 64}", RESULT_OK if i % 4 else RESULT_FAILED, "timeout")
            if i % 100 == 0:
                with clock_lock:
                    clock.advance(1.0)

    def reader() -> None:
        while not stop.is_set():
            snap = m.snapshot()
            subtotal = sum(row["count"] for row in snap["counters"])
            if subtotal != snap["total"]:
                violations.append((subtotal, snap["total"]))
            if snap["total"] + snap["evicted"]["counts"] != snap["recorded"]:
                violations.append((-1, snap["recorded"]))

    wt = [threading.Thread(target=writer, args=(w,)) for w in range(12)]
    rt = [threading.Thread(target=reader) for _ in range(4)]
    for t in wt + rt:
        t.start()
    for t in wt:
        t.join()
    stop.set()
    for t in rt:
        t.join()

    assert not violations
    snap = m.snapshot()
    assert snap["recorded"] == 12 * 1000
    assert snap["evicted"]["keys"] > 0  # 容量/期限回收确实发生
    assert len(snap["counters"]) <= 16  # 内存有界：存活键数不超过容量
    print(f"  recorded={snap['recorded']}, live_keys={len(snap['counters'])}, "
          f"evicted={snap['evicted']}")
