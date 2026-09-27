"""关联标识：生成、透传、并发隔离与跨线程延续。"""
from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor

from observability import context
from observability.background import run_in_background


def _report(name: str, given, expected, actual) -> None:
    print(f"[判定] {name} | 输入={given!r} | 依据=期望 {expected!r} | 实际={actual!r}")


def test_new_id_unique_and_stable_format():
    ids = {context.new_request_id() for _ in range(1000)}
    _report("生成唯一性", "1000 次 new_request_id", "1000 个不同且以 req- 开头",
            f"{len(ids)} 个唯一")
    assert len(ids) == 1000
    assert all(i.startswith("req-") for i in ids)


def test_inbound_id_passthrough_vs_regenerated():
    cases = [
        ("client-abc_1", "client-abc_1", "合法字符集 => 透传"),
        (None, "req-", "缺失 => 生成新标识"),
        ("bad id!", "req-", "含非法字符 => 拒绝并重新生成"),
        ("", "req-", "空串 => 生成新标识"),
    ]
    for inbound, expect_prefix, reason in cases:
        rid = context.resolve_request_id(inbound)
        _report(f"透传规则({reason})", inbound, f"前缀 {expect_prefix}", rid)
        assert rid.startswith(expect_prefix)


def test_bind_and_reset_scopes_context():
    assert context.current_request_id() == "-"
    token = context.bind_context("req-inside")
    assert context.current_request_id() == "req-inside"
    context.reset_context(token)
    _report("作用域解绑", "bind/reset", "解绑后回到占位 -", context.current_request_id())
    assert context.current_request_id() == "-"


def test_concurrent_threads_do_not_cross_ids():
    """并发请求：每个线程绑定自己的标识，读取结果必须严格等于自身标识。"""
    n_threads = 32
    barrier = threading.Barrier(n_threads)
    observed: dict[int, str] = {}
    errors: list[str] = []

    def worker(idx: int) -> None:
        rid = f"req-concurrent-{idx:03d}"
        token = context.bind_context(rid)
        barrier.wait()  # 让所有线程同时进入临界区，制造竞态
        for _ in range(200):
            seen = context.current_request_id()
            if seen != rid:
                errors.append(f"thread {idx} saw {seen}, want {rid}")
        observed[idx] = context.current_request_id()
        context.reset_context(token)

    with ThreadPoolExecutor(max_workers=n_threads) as pool:
        list(pool.map(worker, range(n_threads)))

    _report("并发隔离", f"{n_threads} 线程 x 200 次读取",
            "0 次串号", f"{len(errors)} 次串号")
    assert not errors
    assert observed == {i: f"req-concurrent-{i:03d}" for i in range(n_threads)}


def test_background_job_inherits_request_id():
    """后台任务必须延续提交者的请求标识，且不同请求互不串号。"""
    seen: dict[str, str] = {}

    def job(tag: str) -> None:
        seen[tag] = context.current_request_id()

    t1 = context.bind_context("req-parent-A")
    j1 = run_in_background(job, "A")
    t2 = context.bind_context("req-parent-B")
    j2 = run_in_background(job, "B")
    context.reset_context(t2)
    context.reset_context(t1)
    j1.wait(timeout=2)
    j2.wait(timeout=2)

    _report("后台延续", "A/B 两个请求各提交一个后台任务",
            "{'A':'req-parent-A','B':'req-parent-B'}", seen)
    assert seen == {"A": "req-parent-A", "B": "req-parent-B"}
