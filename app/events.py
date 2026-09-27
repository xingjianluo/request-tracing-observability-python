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
"""

from __future__ import annotations

import json
import os
import sys
import threading
from collections import deque
from datetime import datetime, timezone
from typing import Any

from app.context import current_request_id


class EventRecorder:
    """线程安全的结构化事件记录器（内存缓冲 + JSON 行输出）。"""

    def __init__(self, capacity: int = 10_000, *, stream: Any = None) -> None:
        self._lock = threading.Lock()
        self._buf: deque[dict[str, Any]] = deque(maxlen=capacity)
        self._seq = 0
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
            self._seq += 1
            payload = {"seq": self._seq, **base}
            self._buf.append(payload)
            if self._stderr_enabled():
                line = json.dumps(payload, sort_keys=True, default=str, ensure_ascii=False)
                self._stream.write(line + "\n")
                self._stream.flush()
        return payload

    def snapshot(self) -> list[dict[str, Any]]:
        """返回缓冲事件的浅拷贝列表（按 seq 升序）。"""
        with self._lock:
            return list(self._buf)

    def reset(self) -> None:
        """清空缓冲（序号不清零，避免重置前后事件无法区分）。"""
        with self._lock:
            self._buf.clear()

    @staticmethod
    def _stderr_enabled() -> bool:
        return os.environ.get("OBS_LOG_STDERR", "1") != "0"


#: 进程级默认记录器单例。
recorder = EventRecorder()
