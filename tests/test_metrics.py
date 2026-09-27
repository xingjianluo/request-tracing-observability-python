"""指标聚合：维度口径、可复现查询、并发写/查/重置守恒。"""

from __future__ import annotations

import json
import threading

from app.metrics import NO_ERROR, RESULT_FAILED, RESULT_OK, MetricsRegistry


def test_dimensions_and_snapshot_shape():
    print("\n[输入] /a 2成功1超时失败、/b 1成功；[判定依据] 按 route x result x category 聚合")
    m = MetricsRegistry()
    m.record("/a", RESULT_OK)
    m.record("/a", RESULT_OK)
    m.record("/a", RESULT_FAILED, "timeout")
    m.record("/b", RESULT_OK)
    snap = m.snapshot()
    assert snap["total"] == 4
    assert snap["by_route"]["/a"] == {"ok": 2, "failed": 1, "total": 3}
    assert snap["by_route"]["/b"] == {"ok": 1, "failed": 0, "total": 1}
    assert snap["by_error_category"] == {NO_ERROR: 3, "timeout": 1}
    # 成功请求错误类别固定 none
    rows = {(r["route"], r["result"], r["error_category"]): r["count"] for r in snap["counters"]}
    assert rows[("/a", "ok", "none")] == 2


def test_snapshot_is_reproducible():
    print("\n[输入] 静止状态连续两次 snapshot；[判定依据] JSON 序列化字节级一致且独立深拷贝")
    m = MetricsRegistry()
    for _ in range(10):
        m.record("/r", RESULT_OK)
    s1 = m.snapshot()
    s2 = m.snapshot()
    assert json.dumps(s1, sort_keys=True) == json.dumps(s2, sort_keys=True)
    s1["counters"].clear()  # 改动返回对象不影响注册表
    assert m.snapshot()["total"] == 10


def test_reset_returns_history_then_zero():
    print("\n[输入] 3 次计数后 reset；[判定依据] 返回值带走全部历史，重置后为 0")
    m = MetricsRegistry()
    m.record("/r", RESULT_FAILED, "internal_error")
    m.record("/r", RESULT_OK)
    m.record("/r", RESULT_OK)
    taken = m.reset()
    assert taken["total"] == 3
    assert taken["by_error_category"]["internal_error"] == 1
    assert m.snapshot()["total"] == 0


def test_concurrent_writes_are_exact():
    print("\n[输入] 16 线程 x 500 次写入（每 5 次 1 个 timeout 失败）；"
          "[判定依据] total 精确为 8000，ok=6400 failed=1600")
    m = MetricsRegistry()

    def worker(route: str) -> None:
        for i in range(500):
            if i % 5 == 0:
                m.record(route, RESULT_FAILED, "timeout")
            else:
                m.record(route, RESULT_OK)

    threads = [threading.Thread(target=worker, args=(f"/r{i % 4}",)) for i in range(16)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    snap = m.snapshot()
    assert snap["total"] == 8000
    by_cat = snap["by_error_category"]
    assert by_cat["timeout"] == 1600 and by_cat[NO_ERROR] == 6400
    by_result = {"ok": 0, "failed": 0}
    for row in snap["counters"]:
        by_result[row["result"]] += row["count"]
    assert by_result == {"ok": 6400, "failed": 1600}


def test_concurrent_read_never_sees_broken_invariant():
    print("\n[输入] 写入同时 8 个读线程持续 snapshot；[判定依据] 每次 维度和==total")
    m = MetricsRegistry()
    stop = threading.Event()
    violations: list[tuple[int, int]] = []

    def writer() -> None:
        for i in range(2000):
            m.record("/w", RESULT_OK if i % 3 else RESULT_FAILED, "internal_error")

    def reader() -> None:
        while not stop.is_set():
            snap = m.snapshot()
            subtotal = sum(row["count"] for row in snap["counters"])
            if subtotal != snap["total"]:
                violations.append((subtotal, snap["total"]))

    wt = [threading.Thread(target=writer) for _ in range(4)]
    rt = [threading.Thread(target=reader) for _ in range(8)]
    for t in wt + rt:
        t.start()
    for t in wt:
        t.join()
    stop.set()
    for t in rt:
        t.join()
    assert not violations
    assert m.snapshot()["total"] == 8000


def test_concurrent_reset_conserves_every_count():
    print("\n[输入] 16 写线程持续计数，1 重置线程反复 reset；"
          "[判定依据] 所有 reset 快照之和 + 现存 == 8000，跨重置不丢不重")
    m = MetricsRegistry()
    stop = threading.Event()
    carried: list[int] = []
    carried_lock = threading.Lock()

    def writer(route: str) -> None:
        for i in range(500):
            if i % 5 == 0:
                m.record(route, RESULT_FAILED, "timeout")
            else:
                m.record(route, RESULT_OK)

    def resetter() -> None:
        while not stop.is_set():
            old = m.reset()
            with carried_lock:
                carried.append(old["total"])

    wt = [threading.Thread(target=writer, args=(f"/r{i % 4}",)) for i in range(16)]
    rt = threading.Thread(target=resetter)
    for t in wt + [rt]:
        t.start()
    for t in wt:
        t.join()
    stop.set()
    rt.join()

    remaining = m.snapshot()["total"]
    assert sum(carried) + remaining == 8000, (sum(carried), remaining)
