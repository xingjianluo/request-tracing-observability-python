"""关联标识：生成规则、绑定/恢复、跨线程与跨任务隔离。"""

from __future__ import annotations

import threading

from app.context import (
    bind_request_id,
    current_request_id,
    new_request_id,
    reset_request_id,
    run_with_context,
)


def test_new_request_id_shape_and_uniqueness():
    ids = {new_request_id() for _ in range(10_000)}
    print("\n[输入] 生成 10000 次；[判定依据] 形状 req-<32hex> 且全局唯一")
    sample = next(iter(ids))
    assert len(sample) == 36 and sample.startswith("req-")
    assert len(ids) == 10_000


def test_bind_and_reset_is_scoped():
    print("\n[输入] 无上下文 -> bind('req-x') -> reset；[判定依据] reset 后恢复为 None")
    assert current_request_id() is None
    token = bind_request_id("req-x")
    assert current_request_id() == "req-x"
    reset_request_id(token)
    assert current_request_id() is None


def test_run_with_context_isolates_caller():
    print("\n[输入] run_with_context('req-bg', worker)；[判定依据] worker 内可见、调用方不受影响")
    assert run_with_context("req-bg", current_request_id) == "req-bg"
    assert current_request_id() is None


def test_thread_pool_propagates_explicit_context():
    """线程不自动继承 contextvars；run_with_context 在 worker 内重新绑定。"""
    print("\n[输入] 20 线程各自显式携带不同 id；[判定依据] 每线程读到自己的 id，主线程为 None")
    results: dict[str, str] = {}
    barrier = threading.Barrier(20)

    def worker(rid: str) -> None:
        def body() -> str:
            barrier.wait()  # 强制并发交错
            return current_request_id()  # type: ignore[return-value]

        results[rid] = run_with_context(rid, body)

    threads = [threading.Thread(target=worker, args=(f"rid-thread-{i:03d}",)) for i in range(20)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert results == {f"rid-thread-{i:03d}": f"rid-thread-{i:03d}" for i in range(20)}
    assert current_request_id() is None


def test_asyncio_tasks_concurrent_no_cross_talk():
    import asyncio

    async def child(rid: str, seen: list[tuple[str, str | None]]) -> None:
        for _ in range(50):
            seen.append((rid, current_request_id()))
            await asyncio.sleep(0)  # 强制交错

    async def scenario() -> None:
        seen: list[tuple[str, str | None]] = []
        tasks = []
        for i in range(10):
            rid = f"rid-async-{i:03d}"
            token = bind_request_id(rid)
            try:
                tasks.append(asyncio.create_task(child(rid, seen)))
            finally:
                reset_request_id(token)
        await asyncio.gather(*tasks)
        return seen

    print("\n[输入] 10 个交错的 asyncio Task 各持不同 id；[判定依据] 500 次读取全部归属正确")
    seen = asyncio.run(scenario())
    assert len(seen) == 500
    assert all(got == rid for rid, got in seen)
