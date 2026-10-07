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
| `ThreadPoolExecutor` 线程 | 线程不自动继承，须用 `run_with_context(rid, fn, ...)` | 显式重绑定后延续 |

并发隔离依据：`asyncio` 每个 Task 拥有创建时刻的 context 副本；线程之间本就
独立。因此并发请求之间不会串用标识；请求结束后中间件调用 `reset_request_id`
恢复上下文，标识也不会泄漏到下一次复用。

### 1.4 跨线程执行与后台派生（新增）

有些内部调用被丢到**独立线程池**（`ThreadPoolExecutor`）或在请求处理中被
“后台派生”到线程执行。线程 worker **不会**自动继承提交时刻的 `contextvars`，
因此线程池路径统一遵循：

1. 提交任务前，在派生请求的上下文里抓取 `current_request_id()`（父请求标识）；
2. worker 体内用 `run_with_context(parent_rid, fn, ...)` 在
   `copy_context()` 的隔离上下文中重新绑定该标识；
3. worker 内的全部结构化事件（`thread.submit` / `thread.worker.start` /
   `thread.worker.done` / `thread.background.spawn`）都带这个标识，
   `thread.worker.start` 额外记录 `bound_from` 字段表明标识来源。

因此同一次业务请求在**同步 → await 异步 → 线程执行 → 后台派生**的混合链路里
都能对上同一个 id；线程池复用时 worker 也读不到上一个任务或其他并发请求的
标识（24 请求并发隔离由 `test_concurrent_thread_requests_never_cross_talk`
覆盖）。对应 HTTP 路径：`GET /thread/{item}`、`GET /a-thread/{item}`
（async/线程混合）、`GET /bg-thread/{item}`（后台派生到线程）。

相关代码：`app/context.py`、`app/middleware.py`、`app/services.py`。

---

## 2. 结构化事件

每条事件是一行 JSON（stderr），同时保存在进程内**有界**内存缓冲，字段固定包含：

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
  `dependency.call.start|success|failure` / `background.spawn|start|done` /
  `thread.submit|worker.start|worker.done|background.spawn` /
  `retry.attempt|success|exhausted` / `events.evicted`。
- 关闭 stderr JSON 输出：环境变量 `OBS_LOG_STDERR=0`。

### 2.1 容量上限与保留期限（有界内存）

事件缓冲同时受**容量**与**保留期限**两条规则约束，构造参数（为 `None` 时
读取环境变量，环境变量缺省再用内置默认值）：

| 参数 | 环境变量 | 默认值 | 口径 |
|---|---|---|---|
| `capacity` | `OBS_EVENT_CAPACITY` | `10000` 条 | 缓冲最多保留的事件条数；超出后最旧事件立即回收 |
| `retention_seconds` | `OBS_EVENT_RETENTION_SECONDS` | `None`（永久保留） | 事件保留秒数；超时的旧事件在下次写入或查询时回收 |

- 两条规则独立计数：`evicted_capacity_total`（按容量）与
  `evicted_expired_total`（按期限）分别累计；
- 回收只删除过期/超量的历史事件，**不影响仍有效数据的查询**：
  `snapshot()` 永远返回当前有效事件且按 `seq` 升序，`seq` 全局连续无空洞；
- `reset()` 只清缓冲，序号与回收统计保留（统计反映记录器全生命周期）；
- 每次回收向 stderr 输出一条 `events.evicted` 告警（含原因/条数/累计值，
  该日志只写出、不入缓冲，避免自递归）；
- 运行时通过 `EventRecorder.retention_stats()` 或
  `GET /observability/retention` 查看：容量/期限配置、当前缓冲条数、
  两类累计回收数、回收次数、最近一次回收的时间/原因/条数。

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
  "counters": [{"route": "...", "result": "...", "error_category": "...", "count": n}],
  "by_route": {"/echo/{item}": {"ok": 4, "failed": 4, "total": 8}},
  "by_error_category": {"none": 4, "timeout": 2, "client_error": 1,
                        "dependency_down": 1, "internal_error": 1}
}
```

不变式：`sum(各维度计数) == total`。快照在锁内生成、按维度键排序，因此
同一时刻连续两次查询字节级一致、结论可复现。

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

### 3.4 容量上限与保留期限（有界内存）

指标注册表同样支持按**维度序列**（`route×result×category` 去重后的键）
设置容量与保留期限，构造参数（为 `None` 时读取环境变量，缺省即不限制，
保持历史行为）：

| 参数 | 环境变量 | 默认值 | 口径 |
|---|---|---|---|
| `capacity` | `OBS_METRICS_CAPACITY` | `None`（不限） | 最多保留的维度序列条数；超出后回收**最久未更新**的整组序列（LRU） |
| `retention_seconds` | `OBS_METRICS_RETENTION_SECONDS` | `None`（永久保留） | 一组序列超过该秒数没有新计数即整体回收（在下次写入/查询时执行） |

口径要点：

- 回收的最小单位是**整组维度序列**及其累计计数，绝不会只删序列里的一部分，
  因此任何时刻快照都满足 `sum(各维度计数) == total` 的不变式；
- 对已有序列再次计数会刷新它的“最近更新时间”并在 LRU 中排到队尾，热点序列
  不会被误回收；
- 被回收的只是过期/最久未用的历史，**当前仍有效序列的计数与 reset 语义不受
  影响**：reset 仍原子带走清零前全部现存计数，之后从 0 重新累计；
- 回收统计跨 reset 累计（`evicted_series_total` / `evicted_count_total` /
  `eviction_runs` / 最近一次回收信息），体现在每次快照的 `retention` 字段、
  `MetricsRegistry.retention_stats()` 与 `GET /observability/retention`；
- 完整守恒关系：**现存 total + Σ(reset 带走的 total) + 累计回收计数
  == 期间总写入次数**。

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

## 5. 下游重试的统一归因（新增）

内部调用允许对下游重试几次（`app/retries.py`，
`run_with_retries` / `run_with_retries_async`），HTTP 入口：

| 路径 | 说明 |
|---|---|
| `GET /retry/{item}?attempts=timeout&attempts=timeout&attempts=down` | 同步重试；`attempts` 为 1..5 个逐次尝试的 flag 序列，缺省视为单次 `ok` |
| `GET /a-retry/{item}?attempts=...` | 异步重试，口径相同 |

### 5.1 统一结论与判定依据

同一次业务请求无论内部重试多少次、每次结果如何，对外**只给出一个失败结论**：

- **最终类别以最后一次尝试的归因类别为准**
  （`final_category_source = "last_attempt"`，
  `final_category_attempt` 指明取自第几次）；
- 响应体 `error.attempts` 按顺序列出每次尝试：`attempt`（第几次）、
  `outcome`（ok/failed）、`category`、`raised_type`（异常类型名）、
  `reason`（该次的判定依据）；另含 `attempts_count`（总尝试次数）与
  `retried`（重试次数 = 总次数 − 1）、`retry_policy.max_attempts`；
- 例：`timeout,timeout,down` → 三次尝试的类别依次为
  timeout/timeout/dependency_down，最终结论 **503 dependency_down**，
  判定依据写作 `"all 3 attempt(s) failed (last_attempt category=dependency_down)"`。

### 5.2 超时与连接失败仍可区分

逐次尝试复用第 4 节的归因规则：`TimeoutError` 族 → `timeout`（504），
`ConnectionError`/`OSError` 族 → `dependency_down`（503）。因此“重试中先
超时、最后连接失败”这类过程在 `attempts` 中逐次可辨，最终类别与中间各次
类别互不混淆。

### 5.3 计数口径：一次业务请求只计一次

重试发生在**单次请求内部**，重试模块不触碰指标注册表；业务指标仍只在请求
边界由中间件记一次（成功/失败各 1 次）。中间各次尝试只产生
`retry.attempt` 结构化日志与最终的 `retry.success` / `retry.exhausted`
事件（含逐次尝试与判定依据），不会让同一次业务请求被多次计数。

### 5.4 不泄漏底层报错原文

重试失败响应是**独立的安全视图**，只含：类别、HTTP 状态码、**固定安全
文案**（如 `"dependency unavailable"`）、判定依据、异常**类型名**列表，
不包含任何底层异常文本/地址/密钥（`RetryExhaustedError` 甚至不持有原始
异常对象）。完整的原始细节不进响应，只在结构化日志中保留到“类型名 +
判定依据”粒度。

相关代码：`app/retries.py`、`app/services.py`、`app/middleware.py`。

---

## 6. 与现有接口行为的兼容范围

- 既有 `/`、`/echo`、`/a-echo`、`/bg`、`/metrics`、`/metrics/reset`
  路径的请求/响应结构、状态码、事件类型、指标口径**保持不变**（38 个原有用例
  全部不回归）；
- 指标快照仅**新增** `retention` 字段，原有 `total/counters/by_route/
  by_error_category` 字段与排序不变；
- 事件记录器默认容量仍为 10000；保留期限与指标容量/期限默认不限制，未显式
  配置时行为与历史一致；回收统计为新增只读信息；
- 新增 `/retry`、`/a-retry`、`/thread`、`/a-thread`、`/bg-thread`、
  `/observability/retention` 端点；`/observability/retention` 与
  `/metrics*` 一样只产生日志、不计入业务指标；
- 线程池为进程内共享执行器；线程路径必须经 `run_with_context` 显式绑定
  标识，直接向线程池裸提交（不绑定）不属于支持用法。

---

## 7. 验证方法

```bash
# 全量测试（61 个用例）
uv run pytest

# 查看每个用例打印的“输入”与“判定依据”
uv run pytest -s

# 只跑新增场景
uv run pytest tests/test_retention.py tests/test_retries.py \
               tests/test_thread_propagation.py -s

# 带容量/保留期限压测式本地验证（环境变量对新建实例生效）
OBS_EVENT_RETENTION_SECONDS=60 OBS_METRICS_CAPACITY=1000 \
OBS_METRICS_RETENTION_SECONDS=300 uv run pytest -q
```

关键覆盖：

| 测试文件 | 覆盖点 |
|---|---|
| `tests/test_context.py` | 标识生成、绑定恢复、线程/asyncio 并发隔离（10 Task × 50 次交错读取） |
| `tests/test_metrics.py` | 维度口径、快照可复现、并发写 8000 次精确、读写并发不变式、跨重置守恒 |
| `tests/test_events.py` | 字段口径、上下文注入、JSON 行、并发 10000 条序号无空洞 |
| `tests/test_retention.py` | **新增**：事件容量/TTL 回收后查询自洽、回收统计与告警可见、reset 语义；
|  | 指标 TTL 整组回收、LRU 容量回收、热点序列刷新存活、reset 保留统计、
|  | 12 写 + 8 读并发下不变式/容量上限/回收计数单调、回收统计 HTTP 端点 |
| `tests/test_retries.py` | **新增**：先失败后成功、耗尽后末轮归因、全超时 504、异步重试；
|  | HTTP 重试响应逐次明细/判定依据/无底层报错原文、指标每次请求只计 1 次、
|  | 40 并发请求重试去重计数精确 |
| `tests/test_thread_propagation.py` | **新增**：同步/异步/线程/后台派生混合链路标识延续、24 请求线程池并发
|  | 不串用、HTTP 三条线程路径成功/失败分类、原有三条路径在负载下行为不变 |
| `tests/test_http_tracing.py` | 头/体/日志三处 id 一致、入站透传、非法头拒绝、同步/异步/后台延续 |
| `tests/test_http_errors.py` | 四类失败可区分、链路保留、不泄漏、信封互斥 |
| `tests/test_concurrent_http.py` | **1024 并发混合请求**：标识不串用、计数精确（ok 512/四类失败各 128） |

### 手工复现

```bash
uv run python main.py            # 记录实际端口，或：
PORT=$(uv run python - <<'PY'
import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1]); s.close()
PY
)
curl -s -D- "http://127.0.0.1:$PORT/echo/x?flag=down" -H 'X-Request-Id: demo-rid-0001'
# 下游重试：两次超时后连接失败 -> 统一 503，逐次尝试明细在 error.attempts
curl -s "http://127.0.0.1:$PORT/retry/x?attempts=timeout&attempts=timeout&attempts=down" \
  -H 'X-Request-Id: demo-retry-001' | python -m json.tool
# 跨线程执行：线程内事件沿用同一标识
curl -s "http://127.0.0.1:$PORT/bg-thread/x?flag=ok" -H 'X-Request-Id: demo-thread-01'
# 容量/保留期限与累计回收统计
curl -s "http://127.0.0.1:$PORT/observability/retention" | python -m json.tool
curl -s "http://127.0.0.1:$PORT/metrics" | python -m json.tool
curl -s -X POST "http://127.0.0.1:$PORT/metrics/reset" | python -m json.tool
```

也可直接运行离线演示脚本：`python scripts/demo.py`（使用 ASGI 测试传输，
无需监听端口），它会打印四类失败、标识一致性与指标对账结论。
