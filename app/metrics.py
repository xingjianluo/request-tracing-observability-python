"""指标聚合：按 路由 x 结果 x 错误类别 的计数矩阵。

口径：
- counters[route][result][error_category] = 次数；
- result ∈ {"ok", "failed"}；成功请求 error_category 固定为 "none"；
- total = 所有维度计数之和（冗余校验值，快照中断言一致）。

并发写入、查询与重置同时发生时不丢不重：
- 单一锁保护全部计数状态；
- 每个 record 对单键与 total 做一次原子 +1；
- snapshot 在锁内深拷贝并校验 total，任何时刻查询都自洽、可复现；
- reset 在锁内取出快照后清零，重置时刻之前的计数不会丢（随返回值给出），
  之后的写入从 0 重新累计。
"""

from __future__ import annotations

import threading
from typing import Any

#: 结果标签
RESULT_OK = "ok"
RESULT_FAILED = "failed"

#: 无错误类别时使用的占位标签
NO_ERROR = "none"


class MetricsRegistry:
    """并发安全的计数器注册表。"""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._counts: dict[tuple[str, str, str], int] = {}
        self._total = 0

    def record(self, route: str, result: str, error_category: str = NO_ERROR) -> None:
        """原子地累加一次（单键计数与 total 同锁更新，不丢不重）。"""
        key = (route, result, error_category)
        with self._lock:
            self._counts[key] = self._counts.get(key, 0) + 1
            self._total += 1

    def snapshot(self) -> dict[str, Any]:
        """返回可复现的查询结论。

        结构：
        {
          "total": int,
          "counters": [
            {"route": ..., "result": ..., "error_category": ..., "count": int}, ...
          ],
          "by_route": {route: {"ok": n, "failed": n, "total": n}},
          "by_error_category": {category: n},
        }
        列表按维度键排序，保证同一时刻两次查询字节级一致。
        """
        with self._lock:
            return self._snapshot_locked()

    def reset(self) -> dict[str, Any]:
        """原子地清零并返回清零前的快照（重置不丢历史，历史随返回值带走）。"""
        with self._lock:
            snap = self._snapshot_locked()
            self._counts = {}
            self._total = 0
            return snap

    def _snapshot_locked(self) -> dict[str, Any]:
        items = sorted(self._counts.items(), key=lambda kv: kv[0])
        counters = [
            {"route": route, "result": result, "error_category": category, "count": count}
            for (route, result, category), count in items
        ]
        by_route: dict[str, dict[str, int]] = {}
        by_error_category: dict[str, str] = {}
        subtotal = 0
        for (route, result, category), count in items:
            slot = by_route.setdefault(route, {"ok": 0, "failed": 0, "total": 0})
            slot[result] = slot.get(result, 0) + count
            slot["total"] += count
            by_error_category[category] = by_error_category.get(category, 0) + count
            subtotal += count
        # 不变式：维度计数之和必须等于 total，保证查询结论自洽。
        assert subtotal == self._total, f"metrics invariant broken: {subtotal} != {self._total}"
        return {
            "total": self._total,
            "counters": counters,
            "by_route": by_route,
            "by_error_category": dict(sorted(by_error_category.items())),
        }


#: 进程级默认指标注册表。
registry = MetricsRegistry()
