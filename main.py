"""服务入口：装配路由与可观测性中间件。

本地运行：uv run python main.py （端口 0 由系统分配）
"""

from __future__ import annotations

import uvicorn
from fastapi import FastAPI

from app.middleware import ObservabilityMiddleware
from app.routes import router


def create_app() -> FastAPI:
    application = FastAPI(title="request-tracing-observability-local")
    application.add_middleware(ObservabilityMiddleware)
    application.include_router(router)
    return application


app = create_app()


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=0)
