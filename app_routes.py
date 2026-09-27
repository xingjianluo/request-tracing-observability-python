"""业务 HTTP 路由（本地可复现，无外部依赖）。"""
from __future__ import annotations

import logging

from fastapi import APIRouter, Query
from pydantic import BaseModel, Field

from observability.background import run_in_background
from observability.context import current_request_id
from observability.downstream import DownstreamClient
from observability.errors import ClientError

log = logging.getLogger("app.routes")
router = APIRouter()

_client = DownstreamClient()


class EchoRequest(BaseModel):
    message: str = Field(min_length=1, max_length=200)
    fail: str | None = None


def _record_background(tag: str) -> None:
    """后台任务：仅用于证明关联标识能跨线程延续（查日志/事件）。"""
    log.info("background side-effect", extra={"background_tag": tag,
                                              "request_id": current_request_id()})


@router.get("/")
def read_root() -> dict:
    return {"Hello": "World", "request_id": current_request_id()}


@router.get("/ping")
def ping() -> dict:
    return {"pong": True, "request_id": current_request_id()}


@router.post("/echo")
def echo(body: EchoRequest) -> dict:
    request_id = current_request_id()
    if body.fail == "client_error":
        raise ClientError("echo requested a client failure")
    job = run_in_background(_record_background, "echo")
    job.wait(timeout=2.0)  # 本地确定性等待，避免测试竞态
    return {"echo": body.message, "request_id": request_id}


@router.get("/downstream")
def call_downstream(
    mode: str = Query(default="ok"),
    value: int = Query(default=1),
) -> dict:
    """调用模拟下游；成功返回结果，失败抛分类异常交由中间件归一。"""
    request_id = current_request_id()
    data = _client.call(mode=mode, value=value)
    job = run_in_background(_record_background, "downstream")
    job.wait(timeout=2.0)
    return {"downstream": data, "request_id": request_id}

@router.get("/internal-fault")
def internal_fault() -> dict:
    """模拟未预期的内部异常，验证兜底分类为 500 internal_error。"""
    raise RuntimeError("simulated unexpected internal failure")

