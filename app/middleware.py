"""入站请求处理：关联标识注入/透传、异常归一化、指标记录。"""

from __future__ import annotations

import json
import re
import time

from starlette.types import ASGIApp, Message, Receive, Scope, Send

from app.context import bind_request_id, new_request_id, reset_request_id
from app.errors import ErrorCategory, to_failure_view
from app.events import recorder
from app.metrics import NO_ERROR, RESULT_FAILED, RESULT_OK, registry
from app.retries import RetryExhaustedError, retry_failure_view

#: 优先采用的入站关联标识请求头/响应头
REQUEST_ID_HEADER = b"x-request-id"

#: 入站标识必须满足的形状（防头注入、限长）；不满足则现场生成并记录依据
_SAFE_ID = re.compile(r"^[A-Za-z0-9_-]{8,128}$")


def _resolve_request_id(scope: Scope) -> tuple[str, str]:
    """返回 (request_id, 判定依据)。"""
    for raw_name, raw_value in scope.get("headers", []):
        if raw_name.lower() == REQUEST_ID_HEADER:
            value = raw_value.decode("latin-1").strip()
            if _SAFE_ID.fullmatch(value):
                return value, "inherited inbound x-request-id"
            generated = new_request_id()
            return generated, "inbound x-request-id rejected by safety rule; generated new"
    generated = new_request_id()
    return generated, "inbound x-request-id absent; generated new"


def _category_from_status(status_code: int) -> str:
    """对“正常返回但状态码非 2xx”的响应（如框架 404/422）给出错误类别。"""
    if status_code == 503:
        return ErrorCategory.DEPENDENCY_DOWN.value
    if status_code == 504:
        return ErrorCategory.TIMEOUT.value
    if status_code >= 500:
        return ErrorCategory.INTERNAL_ERROR.value
    return ErrorCategory.CLIENT_ERROR.value


#: 可观测性端点自身不计入业务指标，避免查询/重置改变被观测结论
_EXCLUDED_PATHS = frozenset({"/metrics", "/metrics/reset", "/observability/retention"})


class _ErrorView:
    """统一的失败视图访问层：把 to_failure_view 的 FailureView 与重试安全
    视图（dict）归一为属性访问，中间件其余代码无需区分来源。"""

    def __init__(self, data: dict) -> None:
        self._data = data

    @property
    def http_status(self) -> int:
        return self._data["http_status"]  # type: ignore[no-any-return]

    @property
    def category(self) -> str:
        return self._data["category"]  # type: ignore[no-any-return]

    @property
    def reason(self) -> str:
        return self._data["reason"]  # type: ignore[no-any-return]

    @property
    def request_id(self) -> str | None:
        return self._data["request_id"]  # type: ignore[no-any-return]

    @property
    def cause_type(self) -> str | None:
        return self._data.get("cause_type")

    @property
    def chain(self) -> list:
        return self._data.get("chain", [])

    def to_dict(self) -> dict:
        return self._data


def _build_error_view(exc: Exception, request_id: str) -> _ErrorView:
    """构造对外失败视图：重试耗尽走安全重试视图（无任何底层异常原文），
    其余异常沿用统一归因视图。"""
    if isinstance(exc, RetryExhaustedError):
        return _ErrorView(retry_failure_view(exc, request_id))
    return _ErrorView(to_failure_view(exc, request_id).to_dict())


def _route_label(scope: Scope) -> str:
    """解析路由模板（/echo/{item}）；未匹配则回退为 unmatched:<path>。"""
    path = scope.get("path", "<unknown>")
    matched = scope.get("route")
    return getattr(matched, "path", None) or f"unmatched:{path}"


class ObservabilityMiddleware:
    """纯 ASGI 中间件。

    - 从入站头读取或生成 request_id，绑定到 contextvars；
    - 把 request_id 写回响应头，保证响应与日志标识一致；
    - 正常/失败都记录结构化事件与按路由/结果/类别区分的指标；
    - 未处理异常归一化为安全 JSON 失败响应，不透出底层异常文本；
      原始原因与判定依据保留在 FailureView.chain 与日志事件中。
    """

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        request_id, id_reason = _resolve_request_id(scope)
        token = bind_request_id(request_id)
        started_at = time.perf_counter()

        state = {"status": 200, "started": False}

        async def send_wrapper(message: Message) -> None:
            if message["type"] == "http.response.start":
                state["status"] = message["status"]
                state["started"] = True
                headers = list(message.get("headers", []))
                if not any(name.lower() == REQUEST_ID_HEADER for name, _ in headers):
                    headers.append((REQUEST_ID_HEADER, request_id.encode("latin-1")))
                message["headers"] = headers
            await send(message)

        query = scope.get("query_string", b"").decode("latin-1")
        path = scope.get("path", "<unknown>")
        # 可观测性端点只产生结构化日志，不计入业务指标
        metrics_excluded = path in _EXCLUDED_PATHS
        recorder.record(
            "request.start",
            method=scope.get("method"),
            path=path,
            query=query,
            input={"method": scope.get("method"), "path": path, "query": query},
            id_basis=id_reason,
            metrics_excluded=metrics_excluded,
        )

        try:
            await self.app(scope, receive, send_wrapper)
        except Exception as exc:
            route_label = _route_label(scope)
            view = _build_error_view(exc, request_id)
            elapsed_ms = round((time.perf_counter() - started_at) * 1000, 3)
            decision_basis = {
                "input": {"path": route_label, "query": query},
                "classified_as": view.category,
                "reason": view.reason,
                "root_cause_type": view.cause_type,
            }
            error_payload = view.to_dict()
            if "attempts" in error_payload:
                # 重试场景：补入逐次尝试与最终判定依据，可解释“怎么判定/试了几次”
                decision_basis["retry"] = {
                    "attempts": error_payload["attempts"],
                    "attempts_count": error_payload["attempts_count"],
                    "final_category_source": error_payload["final_category_source"],
                    "final_category_attempt": error_payload["final_category_attempt"],
                }
            recorder.record(
                "request.failure",
                level="error",
                route=route_label,
                status=view.http_status,
                error_category=view.category,
                reason=view.reason,
                cause_type=view.cause_type,
                chain=view.chain,
                attempts=error_payload.get("attempts"),
                elapsed_ms=elapsed_ms,
                decision_basis=decision_basis,
            )
            if not metrics_excluded:
                registry.record(route_label, RESULT_FAILED, view.category)
            if state["started"]:
                # 响应头已发出，无法改写为失败信封；交还上层处理避免半截响应
                raise
            await self._send_failure(send, view)
        else:
            elapsed_ms = round((time.perf_counter() - started_at) * 1000, 3)
            route_label = _route_label(scope)
            status = state["status"]
            if status < 400:
                result, category = RESULT_OK, NO_ERROR
            else:
                result, category = RESULT_FAILED, _category_from_status(status)
            recorder.record(
                "request.complete",
                route=route_label,
                status=status,
                result=result,
                error_category=category,
                elapsed_ms=elapsed_ms,
            )
            if not metrics_excluded:
                registry.record(route_label, result, category)
        finally:
            reset_request_id(token)

    async def _send_failure(self, send: Send, view) -> None:
        body = json.dumps(
            {"ok": False, "request_id": view.request_id, "error": view.to_dict()},
            ensure_ascii=False,
        ).encode("utf-8")
        await send(
            {
                "type": "http.response.start",
                "status": view.http_status,
                "headers": [
                    (b"content-type", b"application/json; charset=utf-8"),
                    (REQUEST_ID_HEADER, (view.request_id or "").encode("latin-1")),
                ],
            }
        )
        await send({"type": "http.response.body", "body": body})
