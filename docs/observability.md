# 本地可观测性能力说明

面向本地运行，不依赖任何外部服务或真实凭据。所有“故障”均由请求参数 `flag`
确定性触发，所有验证均可离线复现。

- 运行环境：Python 3.14、FastAPI / Starlette、仅进程内数据结构
- 启动：`uv run python main.py`（监听 127.0.0.1，端口由系统分配）
- 测试：`uv run pytest`（加 `-s` 可在单测日志中查看每个用例打印的“输入”与“判定依据”）

---

## 1. 关联标识（request id）传播规则

### 1.1 标识形状与来源

| 场景 | 规则 |
|---|---|
| 入站带 `X-Request-Id` 且合法 | **原样继承**（透传），不重新生成 |
| 入带头缺失 | 现场生成 `req-<32 位十六进制 uuid4>` |
| 入带头不合法 | 拒绝并重新生成；事件 `id_basis` 写明拒绝原因 |

合法性规则（防头注入、限长、限字符）：`^[A-Za-z0-9_-]{8,128}$`。

### 1.2 稳定性保证

对一次入站请求，**同一个 request id 同时出现在三处且完全一致**：

1. 响应头 `X-Request-Id`；
2. 响应体 `request_id`；
3. 该请求产生的全部结构化日志事件的 `request_id` 字段
   （含内部依赖调用、后台任务事件）。

### 1.3 传播路径

| 传播路径 | 机制 | 保证 |
|---|---|---|
| 同步函数调用 | `contextvars.ContextVar` 随同一执行上下文可见 | 自动延续 |
| `await` 异步调用 | 同一 Task 内共享上下文 | 自动延续 |
| `asyncio.create_task` 后台任务 | Task 创建时**拷贝**当前 context | 自动延续，任务间隔离 |
| `ThreadPoolExecutor` 线程 | 线程不自动继承；`app/threads.py` 在提交时抓取标识、worker 内用 `run_with_context` 重绑定 | 显式重绑定后延续，worker 间隔离 |

并发隔离依据：`asyncio` 每个 Task 拥有创建时刻的 context 副本；线程之间本就
独立，且 worker 内通过 `copy_context()` 隔离执行。因此并发请求之间不会串用
标识；请求结束后中间件调用 `reset_request_id` 恢复上下文，标识也不会泄漏到
下一次复用。

相关代码：`app/context.py`、`app/middleware.py`、`app/services.py`、`app/threads.py`。

---

## 2. 结构化事件

每条事件是一行 JSON（stderr），同时保存在进程内环形缓冲（容量 10000，
可用 `EventRecorder` 自定义），字段固定包含：

```json
{"seq": 1, "timestamp": "2026-..T..Z", "level": "info",
 "event": "request.start", "request_id": "req-...",
 "method": "GET", "path": "/echo/x", "query": "flag=down",
 "input": {...}, "id_basis": "..."}
```

- `seq`：进程内单调递增、无空洞、不重复（锁内自增），并发写入后仍可按序复现；
- `request_id`：记录时刻自动从上下文注入；
- `input` / `decision_basis`：失败事件打印输入与归因判定依据；
- 事件类型：`request.start` / `request.complete` / `request.failure` /
  `dependency.call.start|success|failure` / `dependency.call.retry.attempt|verdict` /
  `background.spawn|start|done` / `thread.call.start|done`。
- 关闭 stderr JSON 输出：环境变量 `OBS_LOG_STDERR=0`。

### 2.1 事件缓冲的容量上限与保留期限

内存占用有界，两条独立规则可同时启用：

| 规则 | 口径 | 默认值 | 调整方式 |
|---|---|---|---|
| 容量上限 | 缓冲满时丢弃**最旧**事件（环形语义） | 10000 条 | 环境变量 `OBS_EVENTS_CAPACITY`，或构造 `EventRecorder(capacity=...)` |
| 保留期限 | 写入时惰性回收停留时长超过 TTL 的最旧事件 | 不启用 | 环境变量 `OBS_EVENTS_TTL_SECONDS`（秒），或构造参数 `ttl_seconds=` |

- 回收只影响已失效数据：`snapshot()` 仍按 `seq` 升序返回仍有效事件，
  `reset()` 只清存活缓冲；全局 `seq` 序号不受回收影响、保持连续；
- 回收量可见：`recorder.eviction_stats()` 返回
  `{live, evicted_total, evicted_by_capacity, evicted_by_ttl, capacity, ttl_seconds}`，
  计数单调递增、跨 `reset()` 保留，用于判断“清理是否在正常进行”；
- 并发写入与回收同锁进行，任何时刻 `live + evicted_total == 已写入条数`。

---

## 3. 指标口径

### 3.1 维度与计数

每次业务请求（成功或失败）原子累加一次计数，维度为：

```
route × result × error_category
```

- `route`：路由模板（如 `/echo/{item}`），未匹配路由记为 `unmatched:<path>`；
- `result`：`ok`（状态码 < 400）或 `failed`（状态码 ≥ 400 / 抛异常）；
- `error_category`：成功固定 `none`；失败为四选一（见第 4 节），
  对非异常的非 2xx 响应按状态码映射（4xx→client_error，503→dependency_down，
  504→timeout，其余 5xx→internal_error）。

`GET /metrics` 返回：

```json
{
  "total": 7,
  "recorded": 9,
  "counters": [{"route": "...", "result": "...", "error_category": "...", "count": n}],
  "by_route": {"/echo/{item}": {"ok": 4, "failed": 4, "total": 8}},
  "by_error_category": {"none": 4, "timeout": 2, "client_error": 1,
                        "dependency_down": 1, "internal_error": 1},
  "evicted": {"keys": 2, "counts": 2,
              "by_reason": {"ttl": {"keys": 1, "counts": 1},
                            "capacity": {"keys": 1, "counts": 1}}}
}
```

- `total`：当前**仍有效**（未被回收）维度计数之和；
- `recorded`：`total + 已被回收计数`，即全量受理口径，用于跨回收对账；
- `evicted`：回收统计，按 `ttl` / `capacity` 两种原因分别给出
  回收的维度键数与随键清掉的计数。

不变式：`sum(存活维度计数) == total` 且 `total + evicted.counts == recorded`。
快照在锁内生成、按维度键排序，因此同一时刻连续两次查询字节级一致、结论可复现。

### 3.2 可观测性端点的自观测口径

`/metrics` 与 `/metrics/reset` **只产生结构化日志，不计入业务指标**，
因此“查询”与“重置”动作本身不会改变被观测的业务结论。

### 3.3 重置语义与并发保证

- `POST /metrics/reset`：原子清零，**重置前的全部计数随响应体返回**
  （`previous`），历史不丢；
- 写入/查询/重置由同一把锁保护。并发下的守恒关系：

  **Σ(每次 reset 返回的 total) + reset 后当前 total == 期间总写入次数**

  测试 `test_concurrent_reset_conserves_every_count`（16 写线程 + 1 重置线程）
  对该等式做了精确断言（8000 次写入不丢不重）。

### 3.4 计数的容量上限与保留期限

压测或长时运行时，维度键（`route × result × error_category` 组合）可能持续
增多。两条独立规则可同时启用，使内存占用有界：

| 规则 | 口径 | 默认值 | 调整方式 |
|---|---|---|---|
| 容量上限 | 维度键数量上限；满员且来了新键时，回收**最久未更新**的键（LRU） | 不限 | 环境变量 `OBS_METRICS_CAPACITY`，或构造 `MetricsRegistry(capacity=...)` |
| 保留期限 | 超过 TTL 未被更新的键被回收 | 不启用 | 环境变量 `OBS_METRICS_TTL_SECONDS`（秒），或构造参数 `ttl_seconds=` |

- 回收是**写入路径上的惰性回收**（record 时执行），`snapshot()` 本身不随
  时间推移改变结论，保证查询可复现；
- 被回收键的计数从 `total` 转入 `evicted.counts`，不变式
  `sum(存活维度计数) == total` 始终成立，`recorded` 保留全量受理口径；
- `reset()` 清零存活计数与回收统计（重置前快照含两者，历史随响应带走）；
- 并发写/读/回收同锁进行：任意时刻 `total + evicted.counts == recorded`，
  存活键数不超过容量上限。

相关代码：`app/metrics.py`、`app/routes.py`。

---

## 4. 失败分类

四类失败严格可区分，各自对应独立 HTTP 状态码：

| category | 状态码 | 判定依据（reason） | 触发方式（本地） |
|---|---|---|---|
| `client_error` | 400 | `ValueError/KeyError/TypeError` 等契约/校验异常 | `?flag=bad`、非法 flag |
| `dependency_down` | 503 | `ConnectionError`/`OSError` 网络 IO 异常；或适配层显式包装 | `?flag=down` |
| `timeout` | 504 | `TimeoutError` / `asyncio.TimeoutError` | `?flag=timeout` |
| `internal_error` | 500 | 未匹配任何具体类型的未预期异常（保守默认） | `?flag=boom` |

判定顺序：先具体后宽泛（TimeoutError → ConnectionError → OSError →
契约异常族 → 兜底 internal）。`DomainError` 子类自带 category，原样采用。

### 4.1 原始原因保留

- 归一时通过 `raise ... from cause` 保留原始异常；
- 失败响应的 `error.chain` 是完整因果链（`__cause__` 优先，回退 `__context__`，
  去环、去重、限 20 层），每一项含异常类型名与定长截断摘要；
- `error.reason` 是判定依据，`error.cause_type / cause_summary` 指向根因。

### 4.2 不把底层异常透出

- 对外 `error.message` 是**固定安全文案**（如 `"dependency unavailable"`），
  不含底层异常文本、地址、密钥；
- 正常与失败响应结构互斥：成功 `{"ok": true, "data": ...}`，
  失败 `{"ok": false, "error": FailureView}`，两者不会混淆；
- 若响应头已发出后才发生异常（无法改写状态行），中间件重新抛出交由上层处理，
  避免产生半截响应。

相关代码：`app/errors.py`、`app/middleware.py`。

---

## 5. 重试归因（下游超时/连接失败重试）

入口：`GET /retry/{item}?flags=a,b,c&max_attempts=3`。`flags` 是逗号分隔的
逐次尝试输入（本地确定性复现“前几次失败、最后一次成功”等场景）。

### 5.1 判定规则

- 每次尝试的异常先按第 4 节规则归因，**timeout 与 dependency_down 严格区分**
  （`TimeoutError` → timeout；`ConnectionError`/适配层包装 → dependency_down）；
- 仅 `timeout` / `dependency_down` 可重试（瞬时故障）；
  `client_error` / `internal_error` 立即终止（契约或内部错误重试无意义）；
- 任一尝试成功 → 整体成功；尝试耗尽 → 整体失败，
  **最终类别取最后一次尝试的归因类别**。

### 5.2 统一结论与计数口径

- 无论中间重试几次、每次结果如何，对外只给出**一个**统一结论：
  成功响应的 `data.attempts` / 失败响应的 `error.details.attempts` 是逐次明细
  （序号、输入、结果、类别、判定依据、底层异常**类型名**），
  `reason` 写明最终判定依据（如
  `all 3 attempts failed; final attempt classified as dependency_down`）；
- 重试发生在请求边界之内，**同一业务请求在指标中只计一次**
  （按最终结论计 `ok` 或 `failed/<最终类别>`），重试次数不放大计数；
- 重试失败响应不挂原始异常因果链，`message` 为固定安全文案，
  逐次明细只含类型名与判定依据，**不出现任何底层报错原文**；
- 每次尝试与最终结论写 `dependency.call.retry.attempt|verdict` 事件，
  `request_id` 自动注入，可与该请求的其他事件对齐。

相关代码：`app/retry.py`、`app/routes.py`。

---

## 6. 跨线程执行与后台派生的标识延续

入口：`GET /thread/{item}?flag=`（派生到共享线程池）、
`GET /mix/{item}?flag=`（同一请求串走 同步/异步/线程 三条链路）。

规则：

- `app/threads.py` 提供进程级**有界**共享执行器
  （worker 数可用 `OBS_THREAD_WORKERS` 调整，默认 8，守护线程不阻塞退出）；
- `submit_with_context` / `run_in_thread` 在**提交时刻**抓取当前
  `request_id`，worker 线程内用 `run_with_context`（`copy_context` 隔离）
  重新绑定——派生时刻的标识即工作项全程使用的标识；
- 同一请求在同步、异步、线程混合链路里产生的全部事件 `request_id`
  完全一致；多个请求并发派生时各自绑定各自标识，互不串用；
- 提交方上下文不被污染（worker 内是 context 副本），
  已有的同步/异步/后台任务三条路径行为不变。

相关代码：`app/threads.py`、`app/context.py`、`app/services.py`。

---

## 7. 兼容范围

- 既有接口（`/`、`/echo`、`/a-echo`、`/bg`、`/metrics`、`/metrics/reset`）
  的响应结构与语义不变；`/metrics` 快照新增 `recorded` 与 `evicted` 两个
  **附加**字段（默认不启用回收时 `recorded == total`、`evicted` 全零）；
- 失败响应的 `error` 新增附加字段 `details`（一般为 `null`，
  仅重试耗尽时携带逐次明细）；既有字段含义不变；
- 事件缓冲与指标注册表默认配置（容量 10000 / 不限维度键、不启用 TTL）
  与既有行为一致；只有显式设置环境变量或构造参数后才启用回收；
- 新增接口 `/thread/{item}`、`/mix/{item}`、`/retry/{item}` 不影响既有路径。

---

## 8. 验证方法

```bash
# 全量测试（60 个用例）
uv run pytest

# 查看每个用例打印的“输入”与“判定依据”
uv run pytest -s

# 只跑并发/异常场景
uv run pytest tests/test_concurrent_http.py tests/test_http_errors.py -s

# 只跑新增能力
uv run pytest tests/test_retention.py tests/test_retry.py tests/test_thread_context.py -s
```

关键覆盖：

| 测试文件 | 覆盖点 |
|---|---|
| `tests/test_context.py` | 标识生成、绑定恢复、线程/asyncio 并发隔离（10 Task × 50 次交错读取） |
| `tests/test_metrics.py` | 维度口径、快照可复现、并发写 8000 次精确、读写并发不变式、跨重置守恒 |
| `tests/test_events.py` | 字段口径、上下文注入、JSON 行、并发 10000 条序号无空洞 |
| `tests/test_http_tracing.py` | 头/体/日志三处 id 一致、入站透传、非法头拒绝、同步/异步/后台延续 |
| `tests/test_http_errors.py` | 四类失败可区分、链路保留、不泄漏、信封互斥 |
| `tests/test_concurrent_http.py` | **1024 并发混合请求**：标识不串用、计数精确（ok 512/四类失败各 128） |
| `tests/test_retention.py` | 容量/TTL 触发回收后的查询与重置、两种回收原因分别计数、并发读写与回收同时发生时 `live+evicted==写入数`、`total+evicted==recorded` 不变式 |
| `tests/test_retry.py` | 重试统一结论与逐次明细、timeout/dependency_down 区分、不可重试类别 fail-fast、指标去重（1 请求 1 计数）、失败响应无底层原文 |
| `tests/test_thread_context.py` | 线程池派生的标识延续、32 线程 × 20 任务并发不串用、混合链路单一标识、24 并发 HTTP 请求隔离 |

### 手工复现

```bash
uv run python main.py            # 记录实际端口，或：
PORT=$(uv run python - <<'PY'
import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1]); s.close()
PY
)
curl -s -D- "http://127.0.0.1:$PORT/echo/x?flag=down" -H 'X-Request-Id: demo-rid-0001'
# 重试归因：前两次失败（timeout/down），第三次成功
curl -s "http://127.0.0.1:$PORT/retry/x?flags=timeout,down,ok" -H 'X-Request-Id: demo-rid-0002'
# 重试耗尽：统一结论取最后一次尝试的类别
curl -s "http://127.0.0.1:$PORT/retry/x?flags=timeout,timeout,down" -H 'X-Request-Id: demo-rid-0003'
# 跨线程标识延续
curl -s "http://127.0.0.1:$PORT/thread/x" -H 'X-Request-Id: demo-rid-0004'
curl -s "http://127.0.0.1:$PORT/mix/x" -H 'X-Request-Id: demo-rid-0005'
curl -s "http://127.0.0.1:$PORT/metrics" | python -m json.tool
curl -s -X POST "http://127.0.0.1:$PORT/metrics/reset" | python -m json.tool

# 启用容量与保留期限（事件 1000 条 / 60s，指标 100 键 / 300s）
OBS_EVENTS_CAPACITY=1000 OBS_EVENTS_TTL_SECONDS=60 \
OBS_METRICS_CAPACITY=100 OBS_METRICS_TTL_SECONDS=300 \
uv run python main.py
```

也可直接运行离线演示脚本：`python scripts/demo.py`（使用 ASGI 测试传输，
无需监听端口），它会打印四类失败、标识一致性与指标对账结论。
