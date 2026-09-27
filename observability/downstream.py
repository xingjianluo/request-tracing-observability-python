"""本地模拟的下游依赖。

不发起任何真实网络请求、不需要凭据；通过 ``mode`` 确定性地制造
成功 / 不可用 / 超时三类结果，便于本地复现失败归因。
"""
from __future__ import annotations

import logging
from enum import Enum

from .context import current_request_id
from .errors import (
    ClientError,
    DependencyTimeoutError,
    DependencyUnavailableError,
)

log = logging.getLogger("observability.downstream")


class DownstreamMode(str, Enum):
    OK = "ok"
    UNAVAILABLE = "unavailable"
    TIMEOUT = "timeout"


class DownstreamClient:
    def __init__(self, timeout_seconds: float = 0.05) -> None:
        self._timeout_seconds = timeout_seconds

    def call(self, mode: str = "ok", value: int = 1) -> dict:
        """调用下游。

        :param mode: ``ok`` / ``unavailable`` / ``timeout``。
        :param value: 演示用业务参数，负数视为客户端参数错误。
        """
        request_id = current_request_id()
        log.info("downstream call start",
                 extra={"request_id": request_id, "downstream_mode": mode})
        try:
            if mode not in (m.value for m in DownstreamMode):
                # 未知 mode 视为调用方参数问题。
                raise ClientError(f"unknown downstream mode: {mode!r}")
            if value < 0:
                raise ClientError(f"value must be >= 0, got {value}")
            if mode == DownstreamMode.UNAVAILABLE:
                raise ConnectionRefusedError(
                    "simulated downstream refused connection"
                )
            if mode == DownstreamMode.TIMEOUT:
                raise TimeoutError(
                    f"simulated downstream timeout after {self._timeout_seconds}s"
                )
        except ClientError:
            raise
        except ConnectionRefusedError as exc:
            raise DependencyUnavailableError("downstream refused") from exc
        except TimeoutError as exc:
            raise DependencyTimeoutError("downstream timeout") from exc

        result = {"service": "simulated-downstream", "mode": mode, "value": value}
        log.info("downstream call ok",
                 extra={"request_id": request_id, "result": "ok"})
        return result
