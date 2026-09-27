"""入站可观测性 ASGI 中间件与异常归一化。

职责：
1. 关联标识：读取并校验入站 ``X-Request-ID``（合法则透传，否则生成），
   绑定到 :mod:`contextvars`，并写回响应头——请求全程（含异常处理器
   执行期间）标识稳定一致；
2. 结构化事件：业务请求记录 ``request.started`` / ``request.completed``；
3. 指标聚合：按路由模板、结果（success/error）、错误类别计数；
4. 异常归一：所有逃逸异常经 :mod:`observability.errors` 分类后转换为
   脱敏错误响应，原始原因链只进日志、不进响应体。

架构要点：使用纯 ASGI 中间件并置于栈最外层。Starlette 的异常处理器
（ServerErrorMiddleware）位于普通 ``@app.middleware("http")`` 之外层，
异常会先穿过 http 中间件、再由处理器转换为响应。纯 ASGI 外层包装保证：

* 上下文在异常处理器运行期间仍然绑定；
* 无论成功还是异常，最终响应都经过同一收尾逻辑（状态码 +
  ``request.state.error_category``），指标不丢、事件成对。

``/_`` 前缀为本地观测管理端点，不参与业务指标统计。
"""
from __future__ import annotations

import logging

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException
from starlette.types import ASGIApp, Receive, Scope, Send

from .context import REQUEST_ID_HEADER, bind_context, reset_context, resolve_request_id
from .errors import (
    ClassifiedError,
    ErrorCategory,
    classify,
    error_response,
)
from .metrics import ERROR, SUCCESS
from .registry import event_log, metrics

log = logging.getLogger("observability.middleware")

ADMIN_PREFIX = "/_"


def _route_template(scope: Scope) -> str:
    route = scope.get("route")
    return getattr(route, "path", None) or scope.get("path", "")


def _is_admin(path: str) -> bool:
    return path.startswith(ADMIN_PREFIX)


def _json_error_response(exc: ClassifiedError) -> JSONResponse:
    return JSONResponse(
        status_code=exc.http_status,
        content=error_response(exc),
        headers={REQUEST_ID_HEADER: exc.request_id},
    )


class ObservabilityASGIMiddleware:
    """最外层 ASGI 中间件：关联标识 + 统一收尾（事件/指标/响应头）。"""

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        headers = scope.get("headers", [])
        inbound = ""
        for name, value in headers:
            if name.lower() == REQUEST_ID_HEADER.lower().encode():
                inbound = value.decode("latin-1")
                break
        request_id = resolve_request_id(inbound or None)
        token = bind_context(request_id)
        # 用一个可变载体让异常处理器把分类结果传回收尾逻辑。
        state: dict[str, str] = {"error_category": ""}
        scope["_obs_request_id"] = request_id
        scope["_obs_state"] = state

        path = scope.get("path", "")
        admin = _is_admin(path)
        if not admin:
            event_log.emit(
                request_id=request_id,
                kind="request.started",
                route=path,
                detail={"method": scope.get("method", ""), "inbound_id": inbound},
            )

        status_code = 0
        # 拦截响应头以读取状态码并注入关联标识。
        async def send_wrapper(message: dict) -> None:
            nonlocal status_code
            if message["type"] == "http.response.start":
                status_code = message["status"]
                raw = message.setdefault("headers", [])
                if not any(n.lower() == REQUEST_ID_HEADER.lower().encode()
                           for n, _ in raw):
                    raw.append((REQUEST_ID_HEADER.lower().encode(),
                                request_id.encode("latin-1")))
            await send(message)

        try:
            await self.app(scope, receive, send_wrapper)
        finally:
            reset_context(token)
            if not admin and status_code:
                route = _route_template(scope)
                if status_code < 400:
                    result, category = SUCCESS, ""
                else:
                    result = ERROR
                    category = state["error_category"] or (
                        "client_error" if status_code < 500
                        else "internal_error"
                    )
                metrics.record(route, result, category)
                event_log.emit(
                    request_id=request_id,
                    kind="request.completed",
                    route=route,
                    result=result,
                    error_category=category,
                    detail={"status_code": status_code},
                )


def install_observability(app: FastAPI) -> None:
    def _mark(request: Request, category: ErrorCategory) -> str:
        holder = request.scope.get("_obs_state")
        if holder is not None:
            holder["error_category"] = category.value
        return str(request.scope.get("_obs_request_id", ""))

    @app.exception_handler(ClassifiedError)
    async def handle_classified(request: Request, exc: ClassifiedError):  # type: ignore[no-untyped-def]
        request_id = _mark(request, exc.category) or exc.request_id
        log.error(
            "request failed (classified)",
            extra={
                "request_id": request_id,
                "route": request.url.path,
                "error_category": exc.category.value,
                "http_status": exc.http_status,
                "reason_chain": exc.reason_chain(),
            },
        )
        exc.request_id = request_id
        return _json_error_response(exc)

    @app.exception_handler(RequestValidationError)
    async def handle_validation(request: Request, exc: RequestValidationError):  # type: ignore[no-untyped-def]
        request_id = _mark(request, ErrorCategory.CLIENT_ERROR)
        log.error(
            "request rejected (validation)",
            extra={
                "request_id": request_id,
                "route": request.url.path,
                "error_category": ErrorCategory.CLIENT_ERROR.value,
                "reason_chain": [f"{type(exc).__name__}: validation failed"],
            },
        )
        classified = ClassifiedError(
            ErrorCategory.CLIENT_ERROR, reason=exc, request_id=request_id
        )
        return _json_error_response(classified)

    @app.exception_handler(StarletteHTTPException)
    async def handle_http_exception(request: Request, exc: StarletteHTTPException):  # type: ignore[no-untyped-def]
        category = (
            ErrorCategory.CLIENT_ERROR
            if exc.status_code < 500
            else ErrorCategory.INTERNAL_ERROR
        )
        request_id = _mark(request, category)
        log.log(
            logging.INFO if exc.status_code < 500 else logging.ERROR,
            "request ended with http status",
            extra={
                "request_id": request_id,
                "route": request.url.path,
                "error_category": category.value,
                "http_status": exc.status_code,
            },
        )
        classified = ClassifiedError(category, reason=exc, request_id=request_id)
        body = error_response(classified)
        body["error"]["http_status"] = exc.status_code
        return JSONResponse(
            status_code=exc.status_code,
            content=body,
            headers={REQUEST_ID_HEADER: request_id},
        )

    @app.exception_handler(Exception)
    async def handle_unknown(request: Request, exc: Exception):  # type: ignore[no-untyped-def]
        classified = classify(exc)
        request_id = _mark(request, classified.category)
        classified.request_id = request_id
        log.error(
            "request failed (unclassified exception)",
            extra={
                "request_id": request_id,
                "route": request.url.path,
                "error_category": classified.category.value,
                "http_status": classified.http_status,
                "reason_chain": classified.reason_chain(),
            },
        )
        return _json_error_response(classified)


class ObservabilityFastAPI(FastAPI):
    """把观测中间件置于异常处理器（ServerErrorMiddleware）外层的 FastAPI。

    这样异常被处理器转换为响应后，观测收尾逻辑仍能看到最终状态码，
    成功与失败两条路径都恰好记录一次指标与完成事件；lifespan 等
    非 HTTP scope 直接透传，不受影响。
    """

    def build_middleware_stack(self):  # type: ignore[no-untyped-def]
        inner = super().build_middleware_stack()
        return ObservabilityASGIMiddleware(inner)
