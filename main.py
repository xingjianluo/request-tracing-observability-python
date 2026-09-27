"""本地可观测性演示服务入口。

运行：``uv run python main.py``（或 ``uv run uvicorn main:app``），
全部能力本地可复现，不依赖外部服务或真实凭据。
"""
from __future__ import annotations

import uvicorn
from observability.middleware import ObservabilityFastAPI

from app_routes import router as app_router
from observability.admin import router as admin_router
from observability.logging_setup import configure_logging
from observability.middleware import install_observability


def create_app() -> ObservabilityFastAPI:
    app = ObservabilityFastAPI(title="request-tracing-observability (local)")
    configure_logging()
    install_observability(app)
    app.include_router(app_router)
    app.include_router(admin_router, prefix="/_")
    return app


app = create_app()


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000)
