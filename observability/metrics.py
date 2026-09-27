"""指标聚合注册表。

口径（与文档一致）：

* ``requests``：该维度记录的请求总数；
* ``success``：结果为 ``success`` 的请求数；
* ``error``：结果为 ``error`` 的请求数；
* ``error:<category>``：按失败类别细分的错误数；
* 恒等校验：``requests == success + error``，
  ``error == sum(error:<category>)``。

线程安全：记录、快照查询与重置共用一把锁；``reset`` 与
``snapshot`` 返回锁外构造的不可变快照，读取结论不随后续写入变化，
因此同一快照重复查询可复现。
"""
from __future__ import annotations

import threading
from dataclasses import dataclass
from datetime import UTC, datetime
from time import time

SUCCESS = "success"
ERROR = "error"


@dataclass(frozen=True)
class MetricKey:
    route: str
    result: str
    error_category: str = ""


def _empty_bucket() -> dict[str, int]:
    return {"requests": 0, "success": 0, "error": 0}


@dataclass(frozen=True)
class MetricSnapshot:
    started_at: float
    ended_at: float
    started_at_iso: str
    ended_at_iso: str
    totals: dict[str, int]
    by_route: dict[str, dict[str, int]]

    def find(self, route: str, name: str) -> int:
        """便捷查询某路由某口径的值（缺失记 0）。"""
        return self.by_route.get(route, {}).get(name, 0)


class MetricsRegistry:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._counters: dict[MetricKey, int] = {}
        self._started_at = time()

    def record(self, route: str, result: str, error_category: str = "",
               count: int = 1) -> None:
        """累加一次观测；同一 (路由, 结果, 类别) 键合并计数。"""
        if result not in (SUCCESS, ERROR):
            raise ValueError(f"unknown result: {result!r}")
        key = MetricKey(route=route, result=result, error_category=error_category)
        with self._lock:
            self._counters[key] = self._counters.get(key, 0) + count

    def _build_snapshot_locked(self) -> MetricSnapshot:
        totals = _empty_bucket()
        by_route: dict[str, dict[str, int]] = {}
        for key, value in self._counters.items():
            bucket = by_route.setdefault(key.route, _empty_bucket())
            bucket["requests"] += value
            totals["requests"] += value
            bucket[key.result] += value
            totals[key.result] += value
            if key.result == ERROR and key.error_category:
                name = f"error:{key.error_category}"
                bucket[name] = bucket.get(name, 0) + value
                totals[name] = totals.get(name, 0) + value
        return MetricSnapshot(
            started_at=self._started_at,
            ended_at=time(),
            started_at_iso=datetime.fromtimestamp(self._started_at, UTC).isoformat(),
            ended_at_iso=datetime.now(UTC).isoformat(),
            totals=dict(totals),
            by_route={r: dict(b) for r, b in by_route.items()},
        )

    def snapshot(self) -> MetricSnapshot:
        with self._lock:
            return self._build_snapshot_locked()

    def reset(self) -> MetricSnapshot:
        """原子返回当前快照并清零计数（重置窗口起点随之刷新）。"""
        with self._lock:
            snap = self._build_snapshot_locked()
            self._counters = {}
            self._started_at = time()
        return snap
