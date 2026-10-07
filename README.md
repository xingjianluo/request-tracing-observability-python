# request-tracing-observability-python（本地可观测性）

在最小 FastAPI 服务上实现**本地可复现**的可观测性能力：请求关联标识透传、
结构化日志、并发安全的指标聚合、可解释的失败归因。不依赖外部服务或真实凭据。

## 快速开始

```bash
uv sync
uv run python main.py                 # 启动服务（127.0.0.1，系统分配端口）
uv run pytest                         # 60 个测试
uv run python scripts/demo.py         # 离线端到端演示（无需起服务）
```

## 能力一览

| 能力 | 入口 | 说明 |
|---|---|---|
| 关联标识 | `app/context.py` + 中间件 | 入站 `X-Request-Id` 透传/生成，头、体、日志三处一致；contextvars 在同步/异步/后台任务/线程池间延续，并发不串用 |
| 结构化日志 | `app/events.py` | JSON 行 + 内存缓冲；单调 `seq`，自动注入 `request_id`，失败事件含输入与判定依据 |
| 指标聚合 | `app/metrics.py` | 按 `route × result × error_category` 计数；单锁原子更新；并发写/查/重置不丢不重，重置返回历史 |
| 失败归因 | `app/errors.py` + 中间件 | client_error(400)/dependency_down(503)/timeout(504)/internal_error(500) 四类可区分；保留因果链与根因，对外只给安全文案 |
| 容量与保留期限 | `app/events.py` / `app/metrics.py` | 事件与计数均可配置容量上限与 TTL，超限自动回收、内存有界；回收量按原因统计可见，不影响仍有效数据的查询/重置语义 |
| 重试归因 | `app/retry.py` | 多次尝试归并为统一结论：逐次类别可区分（timeout vs dependency_down），最终类别取最后一次尝试；同一业务请求指标只计一次 |
| 跨线程延续 | `app/threads.py` | 派生到线程池的工作重绑定派生时刻的 request_id；同步/异步/线程混合链路标识一致，并发请求互不串用 |

## 接口

| 方法/路径 | 说明 |
|---|---|
| `GET /` | 健康检查，回显 request id |
| `GET /echo/{item}?flag=` | 同步内部调用；`flag ∈ ok/down/timeout/bad/boom` 触发确定结果 |
| `GET /a-echo/{item}?flag=` | 异步内部调用（await 链路） |
| `GET /bg/{item}?flag=` | 派生后台任务，演示标识延续 |
| `GET /thread/{item}?flag=` | 派生到共享线程池执行，演示跨线程标识延续 |
| `GET /mix/{item}?flag=` | 同一请求串走 同步/异步/线程 三条链路 |
| `GET /retry/{item}?flags=a,b,c&max_attempts=` | 带重试的下游调用，逐次输入逗号分隔，输出统一归因结论 |
| `GET /metrics` | 指标快照（查询本身不计入业务指标） |
| `POST /metrics/reset` | 原子重置，返回重置前快照 |

详细规则与验证方法见 [`docs/observability.md`](docs/observability.md)。
