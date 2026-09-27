"""结构化事件日志。

线程安全的 append-only 实现：所有写入、查询、重置都在同一把锁内
对列表做原子操作，保证并发写入/查询/重置同时发生时：

* 不丢：每次 ``emit`` 恰好追加一条；
* 不重：序号由锁内单调计数器分配，永不重复；
* 可复现：重置返回重置瞬间的不可变快照（元组），与后续写入无关。
"""
from __future__ import annotations

import threading
from dataclasses import dataclass, field
from datetime import UTC, datetime


@dataclass(frozen=True)
class Event:
    seq: int
    timestamp: str
    request_id: str
    kind: str
    route: str = ""
    result: str = ""
    error_category: str = ""
    detail: dict = field(default_factory=dict)


class EventLog:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._events: list[Event] = []
        self._seq = 0

    def emit(
        self,
        *,
        request_id: str,
        kind: str,
        route: str = "",
        result: str = "",
        error_category: str = "",
        detail: dict | None = None,
    ) -> Event:
        """原子追加一条事件，序号与时间戳在锁内确定。"""
        event = Event(
            seq=0,  # 占位，锁内替换
            timestamp=datetime.now(UTC).isoformat(),
            request_id=request_id,
            kind=kind,
            route=route,
            result=result,
            error_category=error_category,
            detail=dict(detail or {}),
        )
        with self._lock:
            self._seq += 1
            event = Event(
                seq=self._seq,
                timestamp=event.timestamp,
                request_id=event.request_id,
                kind=event.kind,
                route=event.route,
                result=event.result,
                error_category=event.error_category,
                detail=event.detail,
            )
            self._events.append(event)
        return event

    def query(
        self,
        *,
        request_id: str | None = None,
        route: str | None = None,
        kind: str | None = None,
    ) -> tuple[Event, ...]:
        """返回当前事件的不可变快照副本，可在锁外安全遍历。"""
        with self._lock:
            snapshot = tuple(self._events)
        return tuple(
            e
            for e in snapshot
            if (request_id is None or e.request_id == request_id)
            and (route is None or e.route == route)
            and (kind is None or e.kind == kind)
        )

    def reset(self) -> tuple[Event, ...]:
        """原子清空并返回被清掉的全部事件（不可变快照）。"""
        with self._lock:
            drained = tuple(self._events)
            self._events = []
            self._seq = 0
        return drained

    def __len__(self) -> int:
        with self._lock:
            return len(self._events)
