"""结构化 JSON 日志配置。

每条日志输出单行 JSON，固定携带当前请求的 ``request_id`` 字段
（取自 :mod:`observability.context`，未绑定为 ``"-"``），
因此同一请求的日志可被精确过滤，并发请求的日志不会互相串号。
``extra`` 中的关键字段会并入 JSON 对象。
"""
from __future__ import annotations

import json
import logging
import sys
from datetime import UTC, datetime

from .context import current_request_id

_RESERVED = {
    "name", "msg", "args", "levelname", "levelno", "pathname", "filename",
    "module", "exc_info", "exc_text", "stack_info", "lineno", "funcName",
    "created", "msecs", "relativeCreated", "thread", "threadName",
    "processName", "process", "taskName", "request_id",
}


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, object] = {
            "timestamp": datetime.fromtimestamp(record.created, UTC).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "request_id": getattr(record, "request_id", None) or current_request_id(),
        }
        for key, value in record.__dict__.items():
            if key not in _RESERVED and not key.startswith("_"):
                payload[key] = value
        if record.exc_info is not None:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str)


def configure_logging(level: int = logging.INFO) -> None:
    """为根 logger 安装一次 JSON handler（幂等，测试可反复调用）。"""
    root = logging.getLogger()
    root.setLevel(level)
    for handler in root.handlers:
        if getattr(handler, "_observability_json", False):
            return
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(JsonFormatter())
    handler._observability_json = True  # type: ignore[attr-defined]
    root.addHandler(handler)
