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

相关代码：`app/context.py`、`app/middleware.py`、`app/services.py`。

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
  `dependency.call.start|success|failure` / `background.spawn|start|done`。
- 关闭 stderr JSON 输出：环境变量 `OBS_LOG_STDERR=0`。

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

## 5. 验证方法

```bash
# 全量测试（38 个用例）
uv run pytest

# 查看每个用例打印的“输入”与“判定依据”
uv run pytest -s

# 只跑并发/异常场景
uv run pytest tests/test_concurrent_http.py tests/test_http_errors.py -s
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

### 手工复现

```bash
uv run python main.py            # 记录实际端口，或：
PORT=$(uv run python - <<'PY'
import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1]); s.close()
PY
)
curl -s -D- "http://127.0.0.1:$PORT/echo/x?flag=down" -H 'X-Request-Id: demo-rid-0001'
curl -s "http://127.0.0.1:$PORT/metrics" | python -m json.tool
curl -s -X POST "http://127.0.0.1:$PORT/metrics/reset" | python -m json.tool
```

也可直接运行离线演示脚本：`python scripts/demo.py`（使用 ASGI 测试传输，
无需监听端口），它会打印四类失败、标识一致性与指标对账结论。
