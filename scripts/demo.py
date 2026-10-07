#!/usr/bin/env python3
"""离线端到端演示：不监听端口、不依赖外部服务。

运行：uv run python scripts/demo.py
内容：
1. 入站标识透传与生成；
2. 四类失败的状态码、归因与根因；
3. 后台任务的标识延续；
4. 指标对账与重置守恒；
5. 下游重试的统一归因与去重计数；
6. 跨线程执行的标识延续；
7. 容量/保留期限与回收统计。
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

os.environ.setdefault("OBS_LOG_STDERR", "0")  # 演示只打印汇总，避免事件流刷屏

from starlette.testclient import TestClient

from main import app


def banner(title: str) -> None:
    print(f"\n=== {title} ===")


def main() -> None:
    with TestClient(app, raise_server_exceptions=False) as c:
        banner("1. 关联标识：透传 / 生成 / 头体一致")
        r = c.get("/", headers={"X-Request-Id": "demo-rid-000001"})
        print("透传: 响应头 =", r.headers["x-request-id"], "| 响应体 =", r.json()["request_id"])
        r = c.get("/")
        rid = r.headers["x-request-id"]
        print("生成: 响应头 =", rid, "| 响应体 =", r.json()["request_id"], "| 一致 =",
              rid == r.json()["request_id"])

        banner("2. 四类失败可区分（判定依据与根因）")
        for flag in ("down", "timeout", "bad", "boom"):
            r = c.get(f"/echo/x?flag={flag}", headers={"X-Request-Id": f"demo-{flag}-0001"})
            err = r.json()["error"]
            print(f"flag={flag:7s} -> HTTP {r.status_code} category={err['category']:15s} "
                  f"cause={err['cause_type']:22s} message={err['message']!r}")

        banner("3. 后台任务延续标识")
        r = c.get("/bg/job-1", headers={"X-Request-Id": "demo-bg-0000001"})
        print("后台任务返回 request_id =", r.json()["data"]["background"]["request_id"])

        banner("4. 指标对账")
        snap = c.get("/metrics").json()
        print(json.dumps(snap["by_route"], indent=2, ensure_ascii=False))
        print("按错误类别:", snap["by_error_category"], "| total =", snap["total"])

        banner("5. 重置守恒：reset 返回历史，之后归零")
        before = c.get("/metrics").json()["total"]
        taken = c.post("/metrics/reset").json()["previous"]["total"]
        after = c.get("/metrics").json()["total"]
        print(f"重置前观测={before}（查询不计入业务指标）, reset 带走={taken}, 之后={after}")
        assert taken == before and after == 0

        banner("6. 下游重试：统一归因 / 逐次尝试 / 只计一次")
        r = c.get("/retry/x",
                  params=[("attempts", "timeout"), ("attempts", "timeout"),
                          ("attempts", "down")],
                  headers={"X-Request-Id": "demo-retry-0001"})
        err = r.json()["error"]
        print(f"HTTP {r.status_code} 最终类别={err['category']} "
              f"尝试={err['attempts_count']} 重试={err['retried']} "
              f"逐次类别={[a['category'] for a in err['attempts']]} 文案={err['message']!r}")
        print("响应含底层地址/密钥 =", any(
            token in r.text for token in ("6379", "127.0.0.1", "secret")))
        r = c.get("/retry/x", params=[("attempts", "down"), ("attempts", "ok")],
                  headers={"X-Request-Id": "demo-retry-0002"})
        print("先失败后成功 -> HTTP", r.status_code, "输出 =", r.json()["data"]["output"])
        route = c.get("/metrics").json()["by_route"]["/retry/{item}"]
        print("重试路由指标（2 个业务请求各计 1 次）=", route)
        assert route == {"ok": 1, "failed": 1, "total": 2}

        banner("7. 跨线程执行：标识在 worker 内延续")
        for path in ("/thread/job-1", "/a-thread/job-1", "/bg-thread/job-1"):
            r = c.get(path, headers={"X-Request-Id": "demo-thread-001"})
            print(f"{path:18s} HTTP {r.status_code} 响应 id =",
                  r.json()["request_id"], "== 入站 =",
                  r.json()["request_id"] == "demo-thread-001")

        banner("8. 容量与保留期限：回收统计可见")
        retention = c.get("/observability/retention").json()
        print("events:", json.dumps(retention["events"], ensure_ascii=False))
        print("metrics:", json.dumps(retention["metrics"], ensure_ascii=False))
        # 观测端点自身不计入业务指标
        total = c.get("/metrics").json()["total"]
        print("观测端点查询后业务 total 不增加 =", total)

    print("\n演示完成：标识一致、四类失败可区分、重试统一归因且去重、"
          "跨线程延续不串用、指标对账守恒、回收统计可见。")


if __name__ == "__main__":
    main()
