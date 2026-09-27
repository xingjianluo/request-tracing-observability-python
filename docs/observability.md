# 可观测性设计说明（本地可复现）

本文档说明四件事：关联标识传播规则、指标口径、失败分类、验证方法。
所有结论均可在本地用 `uv run pytest` 或起服务后复现，无需外部服务。

---

## 1. 关联标识（Correlation ID）传播规则

### 1.1 生成与透传

- 入站请求读取请求头 `X-Request-ID`。
- 入站标识**通过校验**（正则 `^[A-Za-z0-9][A-Za-z0-9_-]{0,126}$`，
  防日志注入/伪造结构）时**原样透传**；缺失或非法时新生成
  `req-<uuid4 hex>`。
- 标识在三处保持稳定一致：
  1. 响应头 `X-Request-ID`；
  2. 响应体 `request_id`；
  3. 该请求的全部日志与结构化事件的 `request_id` 字段。

### 1.2 并发隔离（不串用、不丢失）

- 标识保存在 `contextvars.ContextVar` 中。每个 asyncio 任务拥有独立
  上下文副本，因此并发请求即使在同一事件循环/线程池内也互不串号。
- 最外层 ASGI 中间件在请求进入时 `bind_context`，在最终响应产出后
  `reset_context`。中间件被刻意置于 Starlette 异常处理器
  （`ServerErrorMiddleware`）**之外层**（通过重写
  `build_middleware_stack`），保证**异常处理器执行期间标识仍处于
  绑定状态**，错误日志/错误响应不会出现空标识。

### 1.3 内部调用与后台任务延续

- 业务代码、模拟下游调用、日志记录均通过
  `context.current_request_id()` 读取当前标识。
- 后台任务：`observability.background.run_in_background` 在**提交线程内**
  调用 `contextvars.copy_context()` 捕获请求上下文，并在工作线程中以
  `ctx.run(...)` 执行，因此每个后台任务固定继承提交它的请求标识，
  多请求并发提交后台任务也不串号。

---

## 2. 结构化事件与指标聚合

### 2.1 结构化事件（`EventLog`）

- append-only；字段含单调序号 `seq`、UTC 时间戳、`request_id`、
  `kind`（`request.started` / `request.completed`）、`route`、`result`、
  `error_category`、`detail`。
- 每次入站业务请求产生**一对**事件。
- 写入/查询/重置共用同一把锁：
  - **不丢**：每次 `emit` 恰好追加一条；
  - **不重**：序号在锁内单调分配，连续且唯一；
  - 查询 `query()` 与重置 `reset()` 返回**不可变元组快照**，拿到后
    不受后续写入影响，结论可复现。

### 2.2 指标口径（`MetricsRegistry`）

按**路由模板**（如 `/downstream`，而非带查询串的完整路径）与维度计数：

| 口径 | 含义 |
| --- | --- |
| `requests` | 该维度请求总数 |
| `success` | HTTP 状态码 `< 400` 的请求数 |
| `error` | HTTP 状态码 `>= 400` 的请求数 |
| `error:<category>` | 按失败类别细分的错误数（见第 3 节） |

**恒等校验**（任何快照、任何时刻都成立）：

```
requests == success + error
error    == Σ error:<category>
```

- 记录 `record()`、查询 `snapshot()`、重置 `reset()` 共用一把锁；
  `reset()` 原子返回重置前快照并清零、刷新窗口起点。
- 并发写入、查询与重置同时发生时，计数不丢不重、不为负。
- `/_` 前缀为本地观测管理端点，**不计入**业务指标。

### 2.3 本地观测端点

| 方法/路径 | 作用 |
| --- | --- |
| `GET /_/metrics` | 查询当前窗口指标（只读） |
| `POST /_/metrics/reset` | 原子返回重置前快照并清零 |
| `GET /_/events?request_id=&route=&kind=&limit=` | 过滤查询事件快照 |
| `POST /_/events/reset` | 清空事件 |

### 2.4 结构化日志

- 每行一条 JSON（`timestamp/level/logger/message/request_id`，外加
  通过 `extra` 传入的字段）。
- 失败日志额外带 `error_category`、`http_status`、`reason_chain`，
  用于失败归因。

---

## 3. 失败分类与异常链路

### 3.1 四类失败（可区分）

| 类别 `category` | HTTP | 判定依据 | 典型来源 |
| --- | --- | --- | --- |
| `client_error` | 400 | `ClientError` / 请求体校验失败 / 4xx HTTP 异常 | 参数非法 |
| `dependency_unavailable` | 503 | `DependencyUnavailableError`（包装连接被拒等） | 下游拒绝/不可达 |
| `timeout` | 504 | `DependencyTimeoutError` / `TimeoutError` / `asyncio.TimeoutError` | 下游超时 |
| `internal_error` | 500 | 未命中已知类型表的任何异常（兜底） | 未预期缺陷 |

判定是**纯函数** `classify(exc)`：同型输入恒得同型分类；对
`ClassifiedError` 再分类幂等。

### 3.2 异常原因链保留

- 业务异常用 `raise X from cause` 抛出；`classify()` 会把原始异常挂到
  `ClassifiedError.reason`，并复制 `__cause__` / `__context__`。
- `reason_chain()` 沿异常链展开为类型+消息的字符串列表，仅写入日志，
  作为归因判定依据。

### 3.3 不透出、不混淆

- 对外响应统一信封，只含**稳定脱敏文案**与关联标识：

  ```json
  {
    "error": {"category": "timeout", "message": "dependency timed out", "http_status": 504},
    "request_id": "req-..."
  }
  ```

- 底层异常文本（连接串、密码、内部细节）**只进日志，不进响应体**。
- 后台任务失败同样被 `classify()` 分类并记录，`wait()` 会重新抛出
  `ClassifiedError`，不会被吞掉或伪装成成功。

---

## 4. 验证方法

### 4.1 自动化测试

```bash
uv run pytest -q          # 25 个用例；单测日志打印 [判定] 输入/依据/实际
```

覆盖：

- `tests/test_context.py`：标识生成、透传校验、32 线程并发隔离、
  跨线程后台延续；
- `tests/test_errors.py`：四类失败可区分、原因链保留、响应脱敏、幂等；
- `tests/test_metrics_events.py`：并发记录不丢不重、快照可复现、
  记录/查询/重置并发时恒等式成立、事件序号连续；
- `tests/test_background.py`：后台成功无错误、失败被分类且重新抛出；
- `tests/test_http_flow.py`：响应头/体/事件标识一致、HTTP 四分类、
  指标口径、后台日志标识、管理端查询与重置；
- `tests/test_concurrent_requests.py`：7 类请求 × 20 并发，逐条校验
  状态码/类别/标识无串号，指标与事件聚合一致；同一组请求两轮结果可复现。

### 4.2 手动复现（真实服务器路径）

```bash
uv run python main.py
# 另一终端：
curl -i -H 'X-Request-ID: demo-1' http://127.0.0.1:8000/ping
curl -i 'http://127.0.0.1:8000/downstream?mode=timeout'      # 504
curl -i 'http://127.0.0.1:8000/downstream?mode=unavailable'  # 503
curl -i 'http://127.0.0.1:8000/internal-fault'               # 500
curl -s 'http://127.0.0.1:8000/_/metrics' | python -m json.tool
curl -s -X POST 'http://127.0.0.1:8000/_/metrics/reset'
```

观察点：错误响应的 `category`/`http_status` 与上表一致；
响应头 `X-Request-ID` 与响应体一致；`/_/metrics` 中恒等式成立；
stderr 为单行 JSON 日志且按 `request_id` 可过滤。
