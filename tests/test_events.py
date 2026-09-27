"""结构化事件：字段口径、request_id 自动注入、并发序号不丢不重。"""

from __future__ import annotations

import io
import json
import threading

from app.context import bind_request_id, reset_request_id, run_with_context
from app.events import EventRecorder


def test_event_payload_shape_and_context_injection():
    print("\n[输入] 上下文外记录 1 条，绑定 req-evt-001 后记录 1 条；"
          "[判定依据] request_id 自动注入，含 seq/timestamp/level/event")
    r = EventRecorder(capacity=100, stream=io.StringIO())
    r.record("outside")
    token = bind_request_id("req-evt-001")
    try:
        payload = r.record("inside", route="/x", result="ok")
    finally:
        reset_request_id(token)

    assert payload["request_id"] == "req-evt-001"
    assert payload["event"] == "inside" and payload["level"] == "info"
    assert payload["route"] == "/x"
    snap = r.snapshot()
    assert snap[0]["request_id"] is None
    assert snap[1]["request_id"] == "req-evt-001"
    assert [e["seq"] for e in snap] == [1, 2]


def test_stderr_json_lines_are_valid():
    print("\n[输入] 2 条事件写到内存流；[判定依据] 每行都是合法 JSON 且字段完整")
    stream = io.StringIO()
    r = EventRecorder(capacity=10, stream=stream)
    r.record("a", k="v")
    r.record("b", k=2)
    lines = [line for line in stream.getvalue().splitlines() if line]
    assert len(lines) == 2
    for line in lines:
        obj = json.loads(line)
        assert {"seq", "timestamp", "level", "event", "request_id"} <= set(obj)


def test_capacity_keeps_newest():
    print("\n[输入] 容量 3 的记录器写 10 条；[判定依据] 仅保留最新 3 条但全局序号连续")
    r = EventRecorder(capacity=3, stream=io.StringIO())
    for i in range(10):
        r.record("e", i=i)
    snap = r.snapshot()
    assert [e["i"] for e in snap] == [7, 8, 9]
    assert [e["seq"] for e in snap] == [8, 9, 10]


def test_concurrent_records_gapless_unique_seq():
    print("\n[输入] 20 线程 x 500 条并发记录（10 个在显式 request_id 上下文中）；"
          "[判定依据] 共 10000 条，序号 1..10000 无空洞无重复，归属正确")
    r = EventRecorder(capacity=100_000, stream=io.StringIO())

    def worker(rid: str) -> None:
        def body() -> None:
            for _ in range(500):
                r.record("c")

        if rid:
            run_with_context(rid, body)
        else:
            body()

    threads = [
        threading.Thread(target=worker, args=(f"rid-ev-{i:03d}" if i % 2 == 0 else "",))
        for i in range(20)
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    snap = r.snapshot()
    assert len(snap) == 10_000
    seqs = sorted(e["seq"] for e in snap)
    assert seqs == list(range(1, 10_001))
    with_id = [e for e in snap if e["request_id"] is not None]
    assert len(with_id) == 5_000
    assert all(e["request_id"].startswith("rid-ev-") for e in with_id)
