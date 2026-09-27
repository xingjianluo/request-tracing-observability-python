"""本地观测管理端点（不依赖外部服务，直接读取进程内单例）。"""
from __future__ import annotations

from fastapi import APIRouter, Query

from .context import current_request_id
from .registry import event_log, metrics

router = APIRouter()


def _snapshot_dict(snap) -> dict:
    return {
        "window": {
            "started_at": snap.started_at_iso,
            "ended_at": snap.ended_at_iso,
        },
        "totals": snap.totals,
        "by_route": snap.by_route,
    }


@router.get("/metrics")
def get_metrics() -> dict:
    """查询当前窗口指标（只读，不影响计数）。"""
    return {"request_id": current_request_id(), "metrics": _snapshot_dict(metrics.snapshot())}


@router.post("/metrics/reset")
def reset_metrics() -> dict:
    """原子返回重置前快照并清零（重置窗口从此刻重新累计）。"""
    return {"request_id": current_request_id(), "reset": _snapshot_dict(metrics.reset())}


@router.get("/events")
def get_events(
    request_id: str | None = Query(default=None),
    route: str | None = Query(default=None),
    kind: str | None = Query(default=None),
    limit: int = Query(default=500, ge=1, le=5000),
) -> dict:
    """按关联标识/路由/事件类型查询结构化事件（返回不可变快照）。"""
    events = event_log.query(request_id=request_id, route=route, kind=kind)
    items = [
        {
            "seq": e.seq,
            "timestamp": e.timestamp,
            "request_id": e.request_id,
            "kind": e.kind,
            "route": e.route,
            "result": e.result,
            "error_category": e.error_category,
            "detail": e.detail,
        }
        for e in events[-limit:]
    ]
    return {
        "request_id": current_request_id(),
        "count": len(items),
        "total_matched": len(events),
        "events": items,
    }


@router.post("/events/reset")
def reset_events() -> dict:
    drained = event_log.reset()
    return {"request_id": current_request_id(), "cleared": len(drained)}
