"""pytest 公共夹具：每个用例隔离全局指标/事件状态。"""

from __future__ import annotations

import io

import pytest

from app import events as events_mod
from app import metrics as metrics_mod
from app.events import EventRecorder
from app.metrics import MetricsRegistry


@pytest.fixture(autouse=True)
def _isolate_global_state(monkeypatch):
    """每个用例使用全新的指标注册表与事件缓冲，避免用例间相互污染。

    中间件、路由与 app.* 的全局视图都指向同一批全新实例，
    测试可从 metrics_mod.registry / events_mod.recorder 直接观测。
    """
    fresh_registry = MetricsRegistry()
    monkeypatch.setattr(metrics_mod, "registry", fresh_registry)
    monkeypatch.setattr("app.middleware.registry", fresh_registry)
    monkeypatch.setattr("app.routes.registry", fresh_registry)

    fresh_recorder = EventRecorder(capacity=200_000, stream=io.StringIO())
    monkeypatch.setattr(events_mod, "recorder", fresh_recorder)
    monkeypatch.setattr("app.middleware.recorder", fresh_recorder)
    monkeypatch.setattr("app.services.recorder", fresh_recorder)
    monkeypatch.setattr("app.retries.recorder", fresh_recorder)
    monkeypatch.setattr("app.routes.recorder", fresh_recorder)
    yield fresh_registry, fresh_recorder


@pytest.fixture()
def client():
    """同步 ASGI 测试客户端，服务端异常以响应形式返回（不抛出）。"""
    from starlette.testclient import TestClient

    from main import app

    with TestClient(app, raise_server_exceptions=False) as c:
        yield c
