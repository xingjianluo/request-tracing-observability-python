#!/usr/bin/env python3
"""离线端到端演示：不监听端口、不依赖外部服务。

运行：uv run python scripts/demo.py
内容：
1. 入站标识透传与生成；
2. 四类失败的状态码、归因与根因；
3. 后台任务的标识延续；
4. 指标对账与重置守恒。
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

    print("\n演示完成：标识一致、四类失败可区分、指标对账守恒。")


if __name__ == "__main__":
    main()
