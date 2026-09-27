"""测试公共夹具。

每个测试开始前重置进程内指标/事件注册表，保证用例之间相互独立、
结论可复现。HTTP 用例统一以 ``raise_server_exceptions=False`` 访问，
让异常处理器返回真实错误信封（与 uvicorn 生产路径一致）。
"""
from __future__ import annotations

import logging

import pytest
from fastapi.testclient import TestClient

from main import create_app
from observability.logging_setup import configure_logging
from observability.registry import event_log, metrics


@pytest.fixture(autouse=True)
def _reset_registries():
    metrics.reset()
    event_log.reset()
    yield
    metrics.reset()
    event_log.reset()


@pytest.fixture
def client():
    configure_logging(logging.INFO)
    app = create_app()
    with TestClient(app, raise_server_exceptions=False) as c:
        yield c
