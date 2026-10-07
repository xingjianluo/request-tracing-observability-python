"""容量上限与保留期限：事件/计数的自动回收、查询自洽、重置语义与可见性。"""

from __future__ import annotations

import io
import threading
import time

from app.context import run_with_context
from app.events import EventRecorder
from app.metrics import NO_ERROR, RESULT_FAILED, RESULT_OK, MetricsRegistry

# ---------------------------------------------------------------------------
# 事件缓冲
# ---------------------------------------------------------------------------


def test_events_capacity_eviction_is_visible_and_seq_stays_gapless():
    print("\n[输入] 事件缓冲容量 5，连续写 12 条；"
          "[判定依据] 仅留最新 5 条、序号 8..12 连续、容量回收计数=7、回收原因=capacity")
    r = EventRecorder(capacity=5, stream=io.StringIO())
    for i in range(12):
        r.record("e", i=i)
    snap = r.snapshot()
    assert [e["i"] for e in snap] == [7, 8, 9, 10, 11]
    assert [e["seq"] for e in snap] == [8, 9, 10, 11, 12]
    stats = r.retention_stats()
    assert stats["capacity"] == 5
    assert stats["evicted_capacity_total"] == 7
    assert stats["evicted_expired_total"] == 0
    assert stats["evicted_total"] == 7
    assert stats["eviction_runs"] == 7
    assert stats["last_eviction_reason"] == "capacity"
    assert stats["buffered"] == 5


def test_events_retention_ttl_evicts_only_expired_and_query_is_self_consistent():
    print("\n[输入] retention=0.05s：写 3 条 -> 睡眠 -> 再写 2 条并查询；"
          "[判定依据] 前 3 条过期被回收，新 2 条仍可查，序号全局连续，期限计数=3")
    r = EventRecorder(capacity=10_000, retention_seconds=0.05, stream=io.StringIO())
    for i in range(3):
        r.record("old", i=i)
    time.sleep(0.08)
    for i in range(2):
        r.record("new", i=i)
    snap = r.snapshot()
    assert [e["event"] for e in snap] == ["new", "new"]
    assert [e["seq"] for e in snap] == [4, 5]  # 过期不影响序号
    stats = r.retention_stats()
    assert stats["evicted_expired_total"] == 3
    assert stats["evicted_capacity_total"] == 0
    assert stats["last_eviction_reason"] == "retention"
    assert stats["buffered"] == 2


def test_events_expired_data_can_be_reclaimed_by_query_alone():
    print("\n[输入] retention=0.02s，写后不写入只查询；"
          "[判定依据] snapshot/retention_stats 查询自身触发回收，返回结果只含有效数据")
    r = EventRecorder(capacity=10_000, retention_seconds=0.02, stream=io.StringIO())
    r.record("a")
    r.record("b")
    assert len(r.snapshot()) == 2
    time.sleep(0.04)
    assert r.snapshot() == []
    assert r.retention_stats()["evicted_expired_total"] == 2


def test_events_reset_keeps_seq_and_eviction_stats():
    print("\n[输入] 容量 2 写 5 条（回收 3）后 reset 再写 1 条；"
          "[判定依据] reset 只清缓冲，序号继续递增、回收统计跨 reset 保留")
    r = EventRecorder(capacity=2, stream=io.StringIO())
    for i in range(5):
        r.record("e", i=i)
    r.reset()
    payload = r.record("after-reset")
    assert payload["seq"] == 6
    assert len(r.snapshot()) == 1
    stats = r.retention_stats()
    assert stats["evicted_capacity_total"] == 3
    assert stats["buffered"] == 1


def test_events_eviction_notice_is_emitted_to_stderr():
    print("\n[输入] 容量 2 写 4 条（stderr 捕获）；"
          "[判定依据] stderr 出现 2 条 events.evicted，含 reason=capacity 与累计计数")
    stream = io.StringIO()
    r = EventRecorder(capacity=2, stream=stream)
    for i in range(4):
        r.record("e", i=i)
    notices = [
        line for line in stream.getvalue().splitlines() if '"events.evicted"' in line
    ]
    assert len(notices) == 2
    assert '"reason": "capacity"' in notices[-1]


# ---------------------------------------------------------------------------
# 指标注册表
# ---------------------------------------------------------------------------


def test_metrics_retention_ttl_drops_stale_series_only():
    print("\n[输入] retention=0.05s：/a 写 2 次后睡眠，/b 写 1 次并查询；"
          "[判定依据] /a 序列过期整组回收，/b 保留；total/维度和始终自洽，回收计数=2")
    m = MetricsRegistry(retention_seconds=0.05)
    m.record("/a", RESULT_OK)
    m.record("/a", RESULT_FAILED, "timeout")
    assert m.snapshot()["total"] == 2
    time.sleep(0.08)
    m.record("/b", RESULT_OK)
    snap = m.snapshot()
    assert snap["total"] == 1
    assert set(snap["by_route"]) == {"/b"}
    assert snap["by_error_category"] == {NO_ERROR: 1}
    stats = snap["retention"]
    # /a 下有 ok 与 failed/timeout 两个维度序列，各整组回收一次
    assert stats["evicted_series_total"] == 2
    assert stats["evicted_count_total"] == 2
    assert stats["last_eviction_reason"] == "retention"
    assert stats["series"] == 1


def test_metrics_capacity_evicts_least_recently_updated_series():
    print("\n[输入] capacity=2：按序写 /a /b /c（/a 因最旧被回收），再写 /b-failed"
          "与 /d（此时最久未更新是 /c）；[判定依据] 存活序列 /b-failed 与 /d，"
          "已回收计数=3（/a-ok、/b-ok、/c-ok 各 1），total 与维度和自洽")
    m = MetricsRegistry(capacity=2)
    m.record("/a", RESULT_OK)
    m.record("/b", RESULT_OK)
    m.record("/c", RESULT_OK)  # 超出：最久未更新的 /a-ok 被回收
    snap1 = m.snapshot()
    assert set(snap1["by_route"]) == {"/b", "/c"}
    m.record("/b", RESULT_FAILED, "timeout")  # 新维度序列，队尾
    m.record("/d", RESULT_OK)  # 超出：最久未更新的 /b-ok 被回收
    snap2 = m.snapshot()
    assert set(snap2["by_route"]) == {"/b", "/d"}, snap2["by_route"]
    assert snap2["by_route"]["/b"] == {"ok": 0, "failed": 1, "total": 1}
    assert snap2["total"] == 2
    stats = snap2["retention"]
    assert stats["evicted_count_total"] == 3
    assert stats["last_eviction_reason"] == "capacity"


def test_metrics_capacity_refresh_keeps_series_alive():
    print("\n[输入] capacity=2：写 /a /b 后反复刷新 /a，再写 /c；"
          "[判定依据] /a 因最近更新而存活，最久未更新的 /b 被回收，/a 计数完整保留")
    m = MetricsRegistry(capacity=2)
    m.record("/a", RESULT_OK)
    m.record("/b", RESULT_OK)
    for _ in range(3):
        m.record("/a", RESULT_OK)  # /a 始终是“最近更新”
    m.record("/c", RESULT_OK)
    snap = m.snapshot()
    assert set(snap["by_route"]) == {"/a", "/c"}
    assert snap["by_route"]["/a"]["ok"] == 4
    assert snap["total"] == 5
    assert snap["retention"]["evicted_count_total"] == 1


def test_metrics_refreshed_series_survives_ttl():
    print("\n[输入] retention=0.06s：/a 持续刷新、/b 写入后不再更新；"
          "[判定依据] 到期时 /a 存活（最近更新时间被刷新），仅 /b 整组回收")
    m = MetricsRegistry(retention_seconds=0.06)
    m.record("/a", RESULT_OK)
    m.record("/b", RESULT_OK)
    for _ in range(4):
        time.sleep(0.03)
        m.record("/a", RESULT_OK)  # /a 始终在期限内
    snap = m.snapshot()
    assert set(snap["by_route"]) == {"/a"}
    assert snap["by_route"]["/a"]["ok"] == 5
    assert snap["retention"]["evicted_count_total"] == 1


def test_metrics_reset_returns_history_and_keeps_eviction_stats():
    print("\n[输入] retention=0.02s：先写 /a 待其过期回收，再写 /b 后 reset；"
          "[判定依据] reset 带走 /b 历史，回收统计跨 reset 保留，之后计数从 0 起")
    m = MetricsRegistry(retention_seconds=0.02)
    m.record("/a", RESULT_FAILED, "timeout")
    time.sleep(0.04)
    m.record("/b", RESULT_OK)
    taken = m.reset()
    assert taken["total"] == 1 and set(taken["by_route"]) == {"/b"}
    assert taken["retention"]["evicted_count_total"] == 1
    after = m.snapshot()
    assert after["total"] == 0
    assert after["retention"]["evicted_count_total"] == 1  # 跨 reset 保留


def test_concurrent_write_query_evict_consistency():
    print("\n[输入] 12 写线程持续写随机路由（capacity=8, ttl=0.5s），8 读线程查询；"
          "[判定依据] 任一快照 total==维度和（无破损不变式），回收计数只增不减")
    m = MetricsRegistry(capacity=8, retention_seconds=0.5)
    stop = threading.Event()
    violations: list[str] = []

    def writer(idx: int) -> None:
        def body() -> None:
            import random
            for _ in range(800):
                route = f"/r{random.Random(idx).randrange(20)}"
                m.record(route, RESULT_OK if _ % 2 else RESULT_FAILED, "timeout")

        run_with_context(f"rid-ret-w-{idx}", body)

    def reader() -> None:
        last_evicted = -1
        while not stop.is_set():
            snap = m.snapshot()
            subtotal = sum(row["count"] for row in snap["counters"])
            if subtotal != snap["total"]:
                violations.append(f"{subtotal}!={snap['total']}")
            if len(snap["counters"]) > 8:
                violations.append("capacity exceeded")
            evicted = snap["retention"]["evicted_count_total"]
            if evicted < last_evicted:
                violations.append("eviction counter decreased")
            last_evicted = evicted

    wts = [threading.Thread(target=writer, args=(i,)) for i in range(12)]
    rts = [threading.Thread(target=reader) for _ in range(8)]
    for t in wts + rts:
        t.start()
    for t in wts:
        t.join()
    stop.set()
    for t in rts:
        t.join()
    assert not violations, violations
    # 最终快照自身守恒
    snap = m.snapshot()
    assert sum(row["count"] for row in snap["counters"]) == snap["total"]


def test_retention_endpoint_reports_stats_and_is_not_metric_counted(client):
    print("\n[输入] 先打 1 个业务请求，再查 GET /observability/retention；"
          "[判定依据] 返回 events/metrics 两组回收统计，且该观测端点自身不计入业务指标")
    client.get("/echo/x", headers={"X-Request-Id": "rid-ret-http-1"})
    resp = client.get("/observability/retention")
    assert resp.status_code == 200
    body = resp.json()
    assert set(body) == {"events", "metrics"}
    assert body["metrics"]["evicted_count_total"] == 0
    assert "capacity" in body["events"] and "retention_seconds" in body["events"]
    # 观测端点自身不计入业务指标：仍只有 /echo 的 1 次
    from app import metrics as mm
    assert mm.registry.snapshot()["total"] == 1
