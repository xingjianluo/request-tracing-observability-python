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

内存有界（容量上限 + 保留期限，二者同时生效，均可配置）：
- capacity           最多保留的“维度序列”条数（route×result×category 去重后的
                     键数），超出后最久未更新的整组序列被回收；
- retention_seconds  序列保留秒数：一组序列超过该时长没有新计数，就在下次
                     写入/查询时被整体回收；
- 构造参数为 None 时分别读取环境变量 OBS_METRICS_CAPACITY /
  OBS_METRICS_RETENTION_SECONDS，仍缺省即 None（不限制，保持历史行为）；
- 回收只删除“已过期/最久未用”的整组序列，当前仍有效序列的计数与重置语义
  不受影响；累计回收情况通过快照的 retention 字段与 retention_stats() 查看，
  回收统计跨 reset 保留，反映注册表全生命周期。
守恒关系（任意时刻）：现存 total + Σ(reset 带走的 total) + 回收计数 == 总写入。
"""

from __future__ import annotations

import os
import threading
import time
from collections import OrderedDict
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

#: 结果标签
RESULT_OK = "ok"
RESULT_FAILED = "failed"

#: 无错误类别时使用的占位标签
NO_ERROR = "none"

#: 回收原因标签
EVICT_REASON_CAPACITY = "capacity"
EVICT_REASON_RETENTION = "retention"

#: 维度序列键
_SeriesKey = tuple[str, str, str]


@dataclass(slots=True)
class _Series:
    """单个维度序列的运行态：计数值、最近更新时刻。"""

    count: int
    ts: float


def _env_optional_int(name: str) -> int | None:
    raw = os.environ.get(name, "").strip()
    if not raw:
        return None
    value = int(raw)
    return value if value > 0 else None  # 0/负数视为“不限制”


class MetricsRegistry:
    """并发安全、内存有界的计数器注册表。"""

    def __init__(
        self,
        capacity: int | None = None,
        *,
        retention_seconds: float | None = None,
    ) -> None:
        self._lock = threading.Lock()
        # OrderedDict 作为标准 LRU：键序即“最近一次计数”从旧到新，
        # 每次计数 move_to_end；过期/超量统一从左端 pop。
        self._series: OrderedDict[_SeriesKey, _Series] = OrderedDict()
        self._total = 0
        self._capacity: int | None = (
            capacity if capacity is not None else _env_optional_int("OBS_METRICS_CAPACITY")
        )
        self._retention_seconds: float | None = (
            retention_seconds
            if retention_seconds is not None
            else _env_optional_int("OBS_METRICS_RETENTION_SECONDS")
        )
        # 回收统计（跨 reset 累计）
        self._evicted_series_total = 0
        self._evicted_count_total = 0
        self._eviction_runs = 0
        self._last_eviction_at: str | None = None
        self._last_eviction_reason: str | None = None
        self._last_eviction_series = 0
        self._last_eviction_count = 0

    def record(self, route: str, result: str, error_category: str = NO_ERROR) -> None:
        """原子地累加一次（单键计数与 total 同锁更新，不丢不重）。"""
        key = (route, result, error_category)
        now = time.monotonic()
        with self._lock:
            # 先回收过期序列，再完成本次计数
            self._evict_expired_locked(now)
            slot = self._series.get(key)
            if slot is None:
                slot = _Series(count=0, ts=now)
                self._series[key] = slot
            slot.count += 1
            slot.ts = now
            self._series.move_to_end(key)  # 最近更新，排到队尾
            self._total += 1
            # 计数后若超出容量，回收最久未更新的序列
            self._evict_overflow_locked()

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
          "retention": {容量/期限配置与累计回收统计},
        }
        列表按维度键排序，保证同一时刻两次查询字节级一致。
        """
        with self._lock:
            self._evict_expired_locked(time.monotonic())
            return self._snapshot_locked()

    def reset(self) -> dict[str, Any]:
        """原子地清零并返回清零前的快照（重置不丢历史，历史随返回值带走）。

        回收统计属于注册表全生命周期运维口径，不随 reset 清零。
        """
        with self._lock:
            snap = self._snapshot_locked()
            self._series = OrderedDict()
            self._total = 0
            return snap

    def retention_stats(self) -> dict[str, Any]:
        """返回容量/保留期限配置、当前序列数与累计回收统计（顺带完成一次回收）。"""
        with self._lock:
            self._evict_expired_locked(time.monotonic())
            return self._retention_stats_locked()

    def _evict_expired_locked(self, now: float) -> None:
        """回收超过保留期限未更新的整组序列（OrderedDict 左端即最久未更新）。"""
        if self._retention_seconds is None:
            return
        deadline = now - self._retention_seconds
        removed_series = 0
        removed_count = 0
        while self._series:
            _key, slot = next(iter(self._series.items()))
            if slot.ts > deadline:
                break  # 队首仍有效，后面的更新时刻更晚，不可能过期
            self._series.popitem(last=False)
            self._total -= slot.count
            removed_series += 1
            removed_count += slot.count
        if removed_series:
            self._note_eviction_locked(EVICT_REASON_RETENTION, removed_series, removed_count)

    def _evict_overflow_locked(self) -> None:
        """容量超限时回收最久未更新的整组序列，直到序列数不超过 capacity。"""
        if self._capacity is None:
            return
        removed_series = 0
        removed_count = 0
        while len(self._series) > self._capacity:
            _key, slot = self._series.popitem(last=False)
            self._total -= slot.count
            removed_series += 1
            removed_count += slot.count
        if removed_series:
            self._note_eviction_locked(EVICT_REASON_CAPACITY, removed_series, removed_count)

    def _note_eviction_locked(self, reason: str, series: int, count: int) -> None:
        self._eviction_runs += 1
        self._evicted_series_total += series
        self._evicted_count_total += count
        self._last_eviction_at = datetime.now(UTC).isoformat()
        self._last_eviction_reason = reason
        self._last_eviction_series = series
        self._last_eviction_count = count

    def _retention_stats_locked(self) -> dict[str, Any]:
        return {
            "capacity": self._capacity,
            "retention_seconds": self._retention_seconds,
            "series": len(self._series),
            "evicted_series_total": self._evicted_series_total,
            "evicted_count_total": self._evicted_count_total,
            "eviction_runs": self._eviction_runs,
            "last_eviction_at": self._last_eviction_at,
            "last_eviction_reason": self._last_eviction_reason,
            "last_eviction_series": self._last_eviction_series,
            "last_eviction_count": self._last_eviction_count,
        }

    def _snapshot_locked(self) -> dict[str, Any]:
        items = sorted(self._series.items(), key=lambda kv: kv[0])
        counters = [
            {"route": route, "result": result, "error_category": category, "count": slot.count}
            for (route, result, category), slot in items
        ]
        by_route: dict[str, dict[str, int]] = {}
        by_error_category: dict[str, str] = {}
        subtotal = 0
        for (route, result, category), slot in items:
            count = slot.count
            slot_view = by_route.setdefault(route, {"ok": 0, "failed": 0, "total": 0})
            slot_view[result] = slot_view.get(result, 0) + count
            slot_view["total"] += count
            by_error_category[category] = by_error_category.get(category, 0) + count
            subtotal += count
        # 不变式：维度计数之和必须等于 total，保证查询结论自洽。
        assert subtotal == self._total, f"metrics invariant broken: {subtotal} != {self._total}"
        return {
            "total": self._total,
            "counters": counters,
            "by_route": by_route,
            "by_error_category": dict(sorted(by_error_category.items())),
            "retention": self._retention_stats_locked(),
        }


#: 进程级默认指标注册表。
registry = MetricsRegistry()
