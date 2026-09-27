# request-tracing-observability-python（本地可观测性）

在最小 FastAPI 服务上实现**本地可复现**的可观测性能力：请求关联标识透传、
结构化日志、并发安全的指标聚合、可解释的失败归因。不依赖外部服务或真实凭据。

## 快速开始

```bash
uv sync
uv run python main.py                 # 启动服务（127.0.0.1，系统分配端口）
uv run pytest                         # 38 个测试
uv run python scripts/demo.py         # 离线端到端演示（无需起服务）
```

## 能力一览

| 能力 | 入口 | 说明 |
|---|---|---|
| 关联标识 | `app/context.py` + 中间件 | 入站 `X-Request-Id` 透传/生成，头、体、日志三处一致；contextvars 在同步/异步/后台任务间延续，并发不串用 |
| 结构化日志 | `app/events.py` | JSON 行 + 内存缓冲；单调 `seq`，自动注入 `request_id`，失败事件含输入与判定依据 |
| 指标聚合 | `app/metrics.py` | 按 `route × result × error_category` 计数；单锁原子更新；并发写/查/重置不丢不重，重置返回历史 |
| 失败归因 | `app/errors.py` + 中间件 | client_error(400)/dependency_down(503)/timeout(504)/internal_error(500) 四类可区分；保留因果链与根因，对外只给安全文案 |

## 接口

| 方法/路径 | 说明 |
|---|---|
| `GET /` | 健康检查，回显 request id |
| `GET /echo/{item}?flag=` | 同步内部调用；`flag ∈ ok/down/timeout/bad/boom` 触发确定结果 |
| `GET /a-echo/{item}?flag=` | 异步内部调用（await 链路） |
| `GET /bg/{item}?flag=` | 派生后台任务，演示标识延续 |
| `GET /metrics` | 指标快照（查询本身不计入业务指标） |
| `POST /metrics/reset` | 原子重置，返回重置前快照 |

详细规则与验证方法见 [`docs/observability.md`](docs/observability.md)。
