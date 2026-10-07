"""指标聚合：按 路由 x 结果 x 错误类别 的计数矩阵。

口径：
- counters[route][result][error_category] = 次数；
- result ∈ {"ok", "failed"}；成功请求 error_category 固定为 "none"；
- total = 当前仍有效（未被回收）维度计数之和（冗余校验值，快照中断言一致）；
- recorded = total + 已被回收的计数（全量受理口径，用于跨回收对账）。

并发写入、查询与重置同时发生时不丢不重：
- 单一锁保护全部计数状态；
- 每个 record 对单键与 total 做一次原子 +1；
- snapshot 在锁内深拷贝并校验 total，任何时刻查询都自洽、可复现；
- reset 在锁内取出快照后清零，重置时刻之前的计数不会丢（随返回值给出），
  之后的写入从 0 重新累计。

内存有界语义（容量上限 + 保留期限）：
- capacity：维度键（route x result x category 组合）的最大数量，
  满员且来了新键时回收最久未更新的键（LRU）；None 表示不限（默认）；
- ttl_seconds：超过该时长未被更新的维度键在写入路径上被惰性回收；
  None 表示不做时间回收（默认，保持既有行为）；
- 回收只发生在写入路径（record），snapshot 不随时间推移改变结论，
  保证同一状态连续两次查询字节级一致；
- 被回收的键与计数分别按 ttl / capacity 原因累计在 evicted 统计中，
  快照可见“一共清掉多少”；回收不影响仍有效数据的查询与重置语义：
  sum(存活维度计数) == total 的不变式始终成立。
"""

from __future__ import annotations

import os
import threading
import time
from collections.abc import Callable
from typing import Any

#: 结果标签
RESULT_OK = "ok"
RESULT_FAILED = "failed"

#: 无错误类别时使用的占位标签
NO_ERROR = "none"


class MetricsRegistry:
    """并发安全的计数器注册表（可选容量上限与保留期限）。"""

    def __init__(
        self,
        capacity: int | None = None,
        ttl_seconds: float | None = None,
        *,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        if capacity is not None and capacity < 1:
            raise ValueError("capacity must be >= 1 or None")
        if ttl_seconds is not None and ttl_seconds <= 0:
            raise ValueError("ttl_seconds must be positive or None")
        self._lock = threading.Lock()
        self._counts: dict[tuple[str, str, str], int] = {}
        self._touched: dict[tuple[str, str, str], float] = {}
        self._total = 0
        self._capacity = capacity
        self._ttl = ttl_seconds
        self._clock = clock
        # 回收统计：{reason: {"keys": 回收的维度键数, "counts": 随键清掉的计数}}
        self._evicted: dict[str, dict[str, int]] = {
            "ttl": {"keys": 0, "counts": 0},
            "capacity": {"keys": 0, "counts": 0},
        }

    def record(self, route: str, result: str, error_category: str = NO_ERROR) -> None:
        """原子地累加一次（单键计数与 total 同锁更新，不丢不重）。

        写入路径上先做惰性回收：先清掉超过保留期限的键，再在满员时
        为新键回收最久未更新的键；被回收计数计入 evicted 统计。
        """
        key = (route, result, error_category)
        with self._lock:
            now = self._clock()
            self._evict_expired_locked(now)
            if key not in self._counts and self._capacity is not None:
                while len(self._counts) >= self._capacity:
                    self._evict_lru_locked()
            self._counts[key] = self._counts.get(key, 0) + 1
            self._touched[key] = now
            self._total += 1

    def _evict_expired_locked(self, now: float) -> None:
        """回收超过保留期限未更新的维度键（调用方须持锁）。"""
        if self._ttl is None:
            return
        expired = [k for k, ts in self._touched.items() if now - ts > self._ttl]
        for k in expired:
            self._drop_locked(k, "ttl")

    def _evict_lru_locked(self) -> None:
        """回收最久未更新的维度键（调用方须持锁，且缓冲非空）。"""
        oldest = min(self._touched, key=lambda k: (self._touched[k], k))
        self._drop_locked(oldest, "capacity")

    def _drop_locked(self, key: tuple[str, str, str], reason: str) -> None:
        count = self._counts.pop(key, 0)
        self._touched.pop(key, None)
        self._total -= count
        slot = self._evicted[reason]
        slot["keys"] += 1
        slot["counts"] += count

    def snapshot(self) -> dict[str, Any]:
        """返回可复现的查询结论。

        结构：
        {
          "total": int,            # 仍有效计数之和
          "recorded": int,         # total + 已被回收计数（全量受理口径）
          "counters": [
            {"route": ..., "result": ..., "error_category": ..., "count": int}, ...
          ],
          "by_route": {route: {"ok": n, "failed": n, "total": n}},
          "by_error_category": {category: n},
          "evicted": {"keys": int, "counts": int,
                      "by_reason": {"ttl": {...}, "capacity": {...}}},
        }
        列表按维度键排序，保证同一时刻两次查询字节级一致。
        """
        with self._lock:
            return self._snapshot_locked()

    def reset(self) -> dict[str, Any]:
        """原子地清零并返回清零前的快照（重置不丢历史，历史随返回值带走）。

        清零范围含存活计数与回收统计，重置后 recorded 从 0 重新累计。
        """
        with self._lock:
            snap = self._snapshot_locked()
            self._counts = {}
            self._touched = {}
            self._total = 0
            self._evicted = {
                "ttl": {"keys": 0, "counts": 0},
                "capacity": {"keys": 0, "counts": 0},
            }
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
        # 不变式：存活维度计数之和必须等于 total，保证查询结论自洽。
        assert subtotal == self._total, f"metrics invariant broken: {subtotal} != {self._total}"
        evicted_keys = sum(slot["keys"] for slot in self._evicted.values())
        evicted_counts = sum(slot["counts"] for slot in self._evicted.values())
        return {
            "total": self._total,
            "recorded": self._total + evicted_counts,
            "counters": counters,
            "by_route": by_route,
            "by_error_category": dict(sorted(by_error_category.items())),
            "evicted": {
                "keys": evicted_keys,
                "counts": evicted_counts,
                "by_reason": {k: dict(v) for k, v in sorted(self._evicted.items())},
            },
        }


def _env_int_or_none(name: str) -> int | None:
    raw = os.environ.get(name)
    return int(raw) if raw else None


def _env_float_or_none(name: str) -> float | None:
    raw = os.environ.get(name)
    return float(raw) if raw else None


#: 进程级默认指标注册表。容量/保留期限可用环境变量调整：
#: OBS_METRICS_CAPACITY、OBS_METRICS_TTL_SECONDS（默认均不启用）。
registry = MetricsRegistry(
    capacity=_env_int_or_none("OBS_METRICS_CAPACITY"),
    ttl_seconds=_env_float_or_none("OBS_METRICS_TTL_SECONDS"),
)
