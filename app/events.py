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

内存有界语义（容量上限 + 保留期限）：
- 容量上限 capacity：缓冲满时丢弃最旧事件（deque maxlen 语义）；
- 保留期限 ttl_seconds：写入时先回收“停留时长已超过 TTL”的最旧事件；
  None 表示不做时间回收（默认，保持既有行为）；
- 两种回收分别计数（eviction_stats），可随时观测“一共清掉多少、
  回收是否在正常进行”；回收只影响已失效数据，snapshot/reset 对
  仍有效数据的语义不变（snapshot 按 seq 升序、reset 清空存活缓冲）。
"""

from __future__ import annotations

import json
import os
import sys
import threading
import time
from collections import deque
from collections.abc import Callable
from datetime import datetime, timezone
from typing import Any

from app.context import current_request_id


class EventRecorder:
    """线程安全的结构化事件记录器（内存缓冲 + JSON 行输出）。"""

    def __init__(
        self,
        capacity: int = 10_000,
        *,
        ttl_seconds: float | None = None,
        stream: Any = None,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        if capacity < 1:
            raise ValueError("capacity must be >= 1")
        if ttl_seconds is not None and ttl_seconds <= 0:
            raise ValueError("ttl_seconds must be positive or None")
        self._lock = threading.Lock()
        self._capacity = capacity
        self._ttl = ttl_seconds
        self._clock = clock
        # 缓冲元素为 (写入时刻的单调时钟, 事件载荷)，TTL 回收按时间戳判定
        self._buf: deque[tuple[float, dict[str, Any]]] = deque(maxlen=capacity)
        self._seq = 0
        self._evicted_capacity = 0
        self._evicted_ttl = 0
        self._stream = stream if stream is not None else sys.stderr

    def record(self, event: str, level: str = "info", **fields: Any) -> dict[str, Any]:
        """记录一条事件，自动带上当前 request_id 与单调序号。

        缓冲追加、序号自增、写出在同一把锁内完成，保证并发写入不丢不重、
        序号无空洞（json.dumps 的 default=str 保证不可序列化字段不致整次丢失）。
        """
        # 上下文读取在锁外完成（contextvars 本身是任务/线程隔离的）
        base: dict[str, Any] = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": level,
            "event": event,
            "request_id": current_request_id(),
            **fields,
        }
        with self._lock:
            now = self._clock()
            self._evict_expired_locked(now)
            self._seq += 1
            payload = {"seq": self._seq, **base}
            if len(self._buf) == self._capacity:
                # deque(maxlen) 追加时会静默丢弃最旧一条，这里显式计数
                self._evicted_capacity += 1
            self._buf.append((now, payload))
            if self._stderr_enabled():
                line = json.dumps(payload, sort_keys=True, default=str, ensure_ascii=False)
                self._stream.write(line + "\n")
                self._stream.flush()
        return payload

    def _evict_expired_locked(self, now: float) -> None:
        """回收停留时长超过 TTL 的最旧事件（调用方须持锁）。"""
        if self._ttl is None:
            return
        while self._buf and now - self._buf[0][0] > self._ttl:
            self._buf.popleft()
            self._evicted_ttl += 1

    def snapshot(self) -> list[dict[str, Any]]:
        """返回缓冲事件的浅拷贝列表（按 seq 升序，仅含仍有效事件）。"""
        with self._lock:
            return [payload for _, payload in self._buf]

    def eviction_stats(self) -> dict[str, Any]:
        """回收统计：一共清掉多少、按容量/按期限各多少、当前存活多少。

        计数单调递增、跨 reset 保留（描述回收过程本身的健康度），
        便于判断“清理是否在正常进行”。
        """
        with self._lock:
            return {
                "capacity": self._capacity,
                "ttl_seconds": self._ttl,
                "live": len(self._buf),
                "evicted_total": self._evicted_capacity + self._evicted_ttl,
                "evicted_by_capacity": self._evicted_capacity,
                "evicted_by_ttl": self._evicted_ttl,
            }

    def reset(self) -> None:
        """清空存活缓冲（序号与回收统计不清零，避免重置前后无法区分）。"""
        with self._lock:
            self._buf.clear()

    @staticmethod
    def _stderr_enabled() -> bool:
        return os.environ.get("OBS_LOG_STDERR", "1") != "0"


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    return int(raw) if raw else default


def _env_float_or_none(name: str) -> float | None:
    raw = os.environ.get(name)
    return float(raw) if raw else None


#: 进程级默认记录器单例。容量/保留期限可用环境变量调整：
#: OBS_EVENTS_CAPACITY（默认 10000）、OBS_EVENTS_TTL_SECONDS（默认不启用）。
recorder = EventRecorder(
    capacity=_env_int("OBS_EVENTS_CAPACITY", 10_000),
    ttl_seconds=_env_float_or_none("OBS_EVENTS_TTL_SECONDS"),
)
