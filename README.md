# Request Tracing & Observability（本地可复现）

一套面向**本地运行**的可观测性能力：请求级关联标识、结构化 JSON 日志、
指标聚合、结构化事件与失败归因。全部能力进程内实现，**不依赖任何外部
服务或真实凭据**（下游依赖为本地模拟）。

## 运行

```bash
uv sync
uv run python main.py            # http://127.0.0.1:8000
uv run pytest                    # 运行全部测试（含并发与异常链路）
```

## 目录

| 文件 | 职责 |
| --- | --- |
| `observability/context.py` | 基于 `contextvars` 的请求关联标识上下文 |
| `observability/background.py` | 后台线程执行器，跨线程延续标识 |
| `observability/errors.py` | 四类失败分类、异常原因链、脱敏错误信封 |
| `observability/metrics.py` | 线程安全指标聚合（记录/快照/重置） |
| `observability/events.py` | 线程安全结构化事件日志（追加/查询/重置） |
| `observability/logging_setup.py` | 单行 JSON 日志，自动注入 `request_id` |
| `observability/middleware.py` | 最外层 ASGI 中间件 + 异常归一化处理器 |
| `observability/downstream.py` | 本地模拟下游（成功/不可用/超时） |
| `observability/admin.py` | 本地观测端点 `/_/metrics`、`/_/events` |
| `app_routes.py` | 业务路由示例 |

详细规则见 [`docs/observability.md`](docs/observability.md)。
