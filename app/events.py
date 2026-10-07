"""结构化事件与 JSON 日志。

每条事件是一个结构化 dict，固定字段：
- seq       进程内单调递增序号（并发下也不重复，保证查询结论可复现）
- timestamp ISO-8601 UTC 时间
- level / event
- request_id 从 contextvars 自动注入（后台任务/内部调用中同样有效）
其余字段由调用方提供（route / result / error_category / 输入与判定依据等）。

输出去向：
- 内存环形缓冲（线程安全），供测试断言与本地复现；
- stderr 的 JSON 行（json.dumps 保证 key 稳定排序）。
可用环境变量 OBS_LOG_STDERR=0 关闭 stderr 输出。

内存有界（容量上限 + 保留期限，二者同时生效）：
- capacity           缓冲最多保留的事件条数，超出后最旧事件被回收（默认 10000，
                     环境变量 OBS_EVENT_CAPACITY 可覆盖；None 表示不限条数）；
- retention_seconds  事件保留秒数，超过保留期限的旧事件在下次写入/查询时回收
                     （默认 None=永久保留，环境变量 OBS_EVENT_RETENTION_SECONDS
                     可覆盖）；
- 回收统计通过 retention_stats() 暴露：累计按容量/按期限各清理多少、最近一次
  清理的时间/原因/条数；每次回收同时向 stderr 输出一条 events.evicted 日志
  （该日志只写出、不入缓冲，避免自递归）。
被回收的只是过期/超量历史，不影响仍有效数据：序号依然全局连续无空洞，
snapshot 永远只返回当前有效事件且按 seq 升序。
"""

from __future__ import annotations

import json
import os
import sys
import threading
import time
from collections import deque
from datetime import UTC, datetime
from typing import Any

from app.context import current_request_id

#: 默认容量上限（条）
DEFAULT_CAPACITY = 10_000

#: 回收原因标签
EVICT_REASON_CAPACITY = "capacity"
EVICT_REASON_RETENTION = "retention"


def _env_optional_int(name: str) -> int | None:
    raw = os.environ.get(name, "").strip()
    if not raw:
        return None
    value = int(raw)
    return value if value > 0 else None  # 0/负数视为“不限制”


class EventRecorder:
    """线程安全的结构化事件记录器（有界内存缓冲 + JSON 行输出）。"""

    def __init__(
        self,
        capacity: int | None = None,
        *,
        retention_seconds: float | None = None,
        stream: Any = None,
    ) -> None:
        self._lock = threading.Lock()
        # 构造参数为 None 时回退到环境变量，环境变量缺省再回退到内置默认值
        self._capacity: int | None = (
            capacity if capacity is not None else _env_optional_int("OBS_EVENT_CAPACITY")
        )
        if self._capacity is None:
            self._capacity = DEFAULT_CAPACITY
        env_ttl = _env_optional_int("OBS_EVENT_RETENTION_SECONDS")
        self._retention_seconds: float | None = (
            retention_seconds if retention_seconds is not None else env_ttl
        )
        # 元素为 (monotonic_ts, payload)；按写入时间升序
        self._buf: deque[tuple[float, dict[str, Any]]] = deque()
        self._seq = 0
        self._stream = stream if stream is not None else sys.stderr
        # 回收统计（累计值，reset 不清零，反映记录器全生命周期的回收情况）
        self._evicted_by_capacity = 0
        self._evicted_by_retention = 0
        self._eviction_runs = 0
        self._last_eviction_at: str | None = None
        self._last_eviction_reason: str | None = None
        self._last_eviction_count = 0

    def record(self, event: str, level: str = "info", **fields: Any) -> dict[str, Any]:
        """记录一条事件，自动带上当前 request_id 与单调序号。

        缓冲追加、序号自增、回收、写出在同一把锁内完成，保证并发写入不丢不重、
        序号无空洞（json.dumps 的 default=str 保证不可序列化字段不致整次丢失）。
        """
        # 上下文读取在锁外完成（contextvars 本身是任务/线程隔离的）
        base: dict[str, Any] = {
            "timestamp": datetime.now(UTC).isoformat(),
            "level": level,
            "event": event,
            "request_id": current_request_id(),
            **fields,
        }
        now = time.monotonic()
        with self._lock:
            self._seq += 1
            payload = {"seq": self._seq, **base}
            # 先按保留期限回收过期事件，再按容量回收超量事件
            self._evict_expired_locked(now)
            self._buf.append((now, payload))
            self._evict_overflow_locked(now)
            if self._stderr_enabled():
                line = json.dumps(payload, sort_keys=True, default=str, ensure_ascii=False)
                self._stream.write(line + "\n")
                self._stream.flush()
        return payload

    def snapshot(self) -> list[dict[str, Any]]:
        """返回当前有效事件的浅拷贝列表（按 seq 升序）。

        查询时同样先回收过期事件，保证任何时刻返回的都是仍在保留期限内、
        且未被容量挤出的自洽结果。
        """
        with self._lock:
            self._evict_expired_locked(time.monotonic())
            return [payload for _ts, payload in self._buf]

    def reset(self) -> None:
        """清空缓冲（序号与回收统计不清零）。

        序号保留可避免重置前后事件无法区分；回收统计是记录器全生命周期的
        运维口径，故同样跨 reset 累计。
        """
        with self._lock:
            self._buf.clear()

    def retention_stats(self) -> dict[str, Any]:
        """返回容量/保留期限配置与累计回收统计（查询时顺带完成一次过期回收）。"""
        with self._lock:
            self._evict_expired_locked(time.monotonic())
            return {
                "capacity": self._capacity,
                "retention_seconds": self._retention_seconds,
                "buffered": len(self._buf),
                "evicted_capacity_total": self._evicted_by_capacity,
                "evicted_expired_total": self._evicted_by_retention,
                "evicted_total": self._evicted_by_capacity + self._evicted_by_retention,
                "eviction_runs": self._eviction_runs,
                "last_eviction_at": self._last_eviction_at,
                "last_eviction_reason": self._last_eviction_reason,
                "last_eviction_count": self._last_eviction_count,
            }

    def _evict_expired_locked(self, now: float) -> None:
        """回收超过保留期限的旧事件（deque 按时间升序，从左端弹出）。"""
        if self._retention_seconds is None:
            return
        deadline = now - self._retention_seconds
        count = 0
        while self._buf and self._buf[0][0] <= deadline:
            self._buf.popleft()
            count += 1
        if count:
            self._note_eviction_locked(EVICT_REASON_RETENTION, count)

    def _evict_overflow_locked(self, now: float) -> None:
        """回收超出容量上限的最旧事件。"""
        if self._capacity is None:
            return
        count = 0
        while len(self._buf) > self._capacity:
            self._buf.popleft()
            count += 1
        if count:
            self._note_eviction_locked(EVICT_REASON_CAPACITY, count)

    def _note_eviction_locked(self, reason: str, count: int) -> None:
        """登记一次回收并向 stderr 输出 events.evicted（不入缓冲）。"""
        self._eviction_runs += 1
        if reason == EVICT_REASON_CAPACITY:
            self._evicted_by_capacity += count
        else:
            self._evicted_by_retention += count
        self._last_eviction_at = datetime.now(UTC).isoformat()
        self._last_eviction_reason = reason
        self._last_eviction_count = count
        if self._stderr_enabled():
            notice = {
                "timestamp": self._last_eviction_at,
                "level": "warning",
                "event": "events.evicted",
                "request_id": current_request_id(),
                "reason": reason,
                "evicted_count": count,
                "evicted_capacity_total": self._evicted_by_capacity,
                "evicted_expired_total": self._evicted_by_retention,
                "buffered": len(self._buf),
                "capacity": self._capacity,
                "retention_seconds": self._retention_seconds,
            }
            self._stream.write(
                json.dumps(notice, sort_keys=True, default=str, ensure_ascii=False) + "\n"
            )
            self._stream.flush()

    @staticmethod
    def _stderr_enabled() -> bool:
        return os.environ.get("OBS_LOG_STDERR", "1") != "0"


#: 进程级默认记录器单例。
recorder = EventRecorder()
