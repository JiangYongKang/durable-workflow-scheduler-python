# Durable Workflow Scheduler（本地工作流调度与执行引擎）

在 FastAPI 服务之上实现的一套**本地持久化**工作流调度与执行引擎。状态保存在
SQLite 文件中，用于模拟任务依赖编排、失败重试、进程重启恢复与按时间窗补齐，
使以下三种情况下任务状态保持一致且可解释：

1. **并发触发同一流程**：实例按 `(workflow, window)` 幂等去重，不会被重复推进；
2. **进程重启**：未完成实例可继续推进，已成功步骤不重复执行、不重复产生副作用；
3. **按时间窗补齐**：补齐窗口与既有调度窗口重叠时复用已有实例，不产生重复执行。

## 目录结构

```
scheduler/
  models.py   # 状态机枚举、流程/任务定义、RunInstance/TaskRun
  errors.py   # 可重试错误 / 致命错误 / 提交期校验错误
  store.py    # SQLite 持久化（事务写入、唯一约束去重）
  engine.py   # 调度引擎：触发、退避重试、失败传播、超时、恢复、终态判定
main.py       # FastAPI 服务层
tests/        # 引擎与 API 单测（含触发来源/状态流转/判定依据日志）
```

## 状态机

### 任务状态 `TaskState`

```
PENDING ──(上游全部成功)──> READY ──> RUNNING ──成功──> SUCCEEDED   （终态）
                              ^          │
                              │          ├── 可重试失败且未达上限 ──┘ （退避后重试）
                              │          ├── 可重试失败且达到上限 ──> FAILED  （终态）
                              │          └── 致命错误(FatalTaskError) ──> FAILED
上游 FAILED/BLOCKED ─────────┴──────────────────────────────────────> BLOCKED （终态）
实例超过时限 ───────────────────────────────────────────────────────> TIMEOUT （终态）
```

- **BLOCKED**：上游业务失败的传播结果，表示“因依赖失败被阻止启动”，
  与自身执行失败 `FAILED` 区分，处理函数**不会被调用**，无副作用。
- **TIMEOUT**：实例整体超过 `run_timeout_seconds` 后由引擎标记，
  与业务失败可区分（`error_kind="timeout"`）。

### 实例状态 `RunState`

```
RUNNING ── 全部任务 SUCCEEDED ──────────────> SUCCEEDED （终态）
        ├── 存在 FAILED 且所有任务已终态 ────> FAILED    （终态）
        └── 当前时间超过 deadline ───────────> TIMEOUT   （终态）
```

终态判定稳定：只要还有任务处于非终态，实例保持 `RUNNING`（例如独立分支
仍在重试）；所有任务进入终态后才一次性给出实例终态，并记录判定依据
（`failure_reason` 及日志 `[终态]`）。

## 调度语义

### 流程编排与提交期校验

- 任务通过 `depends_on` 声明依赖，引擎注册流程时做 **DAG 校验**：
  - 依赖了不存在的任务 → `UnknownTaskError`；
  - 存在环形依赖 → `CycleDetectedError`（在提交/注册阶段拒绝，而非运行时死锁）。
- 每轮推进（`tick`）中：先做超时判定 → 晋升 PENDING 为 READY →
  执行就绪任务 → 传播失败 → 终态判定。

### 并发触发与去重

- 实例 ID 由 `workflow:window` 决定，`runs` 表对 `(workflow_name, window)`
  有唯一约束，插入采用 `INSERT OR IGNORE`；
- 重复触发（含多线程并发）只创建一个实例，后续触发返回既有实例并记录
  `[触发] ... 已存在实例 ... 去重不重复推进` 日志。

### 失败重试（有上限、有退避）

- 处理函数抛 `RetryableTaskError`：本次失败后任务回到 `READY`，
  `next_attempt_at = now + backoff * 2^(attempts-1)`（指数退避）；
- 尝试次数（含首次）达到 `max_attempts` 后置 `FAILED`，失败原因保留
  `error_kind="retryable"`；
- 抛 `FatalTaskError`：**不重试**，立即 `FAILED`（`error_kind="fatal"`）；
- 下游任务收到失败传播后置 `BLOCKED`，递归传播到所有传递依赖。

### 断点恢复（at-least-once + 终态不重复）

- 所有状态变更在每次流转时立即事务落盘；
- 进程重启后调用 `recover()`（或 `/recover`）：
  - 已 `SUCCEEDED` 的任务有完成记录，**不会重新执行**；
  - 崩溃前停留在 `RUNNING` 的任务没有完成记录，重置为 `READY` 重新执行
    （at-least-once；业务侧副作用需自行幂等）；
  - PENDING 任务根据已落盘的上游状态重新晋升 READY。

### 超时监控

- 实例创建时记录 `deadline = created_at + run_timeout_seconds`；
- 每次推进首先检查：超过 deadline 时，所有非终态任务置 `TIMEOUT`，
  实例置 `TIMEOUT`，`failure_reason` 以 `timeout:` 开头，
  与业务失败（`FAILED` + `fatal/retryable`）明确区分。

### 按时间窗补齐

- `backfill(windows)` 对每个窗口按 `BACKFILL` 来源触发；
- 与既有 `MANUAL/SCHEDULE` 窗口重叠时直接复用实例，不创建、不重复执行；
- 窗口标识由调用方给定（如日期字符串 `2026-09-25`）。

## HTTP 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/workflows` | 注册流程（环形/未知依赖返回 `accepted:false`） |
| POST | `/workflows/handlers` | 注册本地演示执行器（成功 / 注入可重试或致命失败） |
| POST | `/trigger` | 触发一次运行并推进一轮 |
| POST | `/backfill` | 按时间窗列表补齐 |
| POST | `/recover` | 重启后恢复所有未完成实例 |
| POST | `/tick` | 推进一轮（供定时器调用） |
| GET  | `/runs/{run_id}` | 查询实例与各任务状态（含 `terminal` 终态标记） |

## 本地验证

```bash
# 运行全部单测（日志中可见触发来源、状态流转、终态判定依据）
python3 -m pytest -s

# 启动服务（默认使用本地 scheduler.db，可用 SCHEDULER_DB 覆盖）
SCHEDULER_DB=scheduler.db python3 main.py
# 或
uv run fastapi dev main.py
```

手工验证示例：

```bash
# 1) 注册一个 a -> b 流程
curl -s localhost:8000/workflows -H 'content-type: application/json' -d '{
  "name":"wf","tasks":[{"name":"a"},{"name":"b","depends_on":["a"]}],
  "run_timeout_seconds":60}'

# 2) a 注入致命失败，b 正常
curl -s localhost:8000/workflows/handlers -H 'content-type: application/json' \
  -d '{"task":"a","succeed":false,"error":"boom"}'
curl -s localhost:8000/workflows/handlers -H 'content-type: application/json' \
  -d '{"task":"b","succeed":true}'

# 3) 触发（重复触发同一窗口终态不变）
curl -s localhost:8000/trigger -H 'content-type: application/json' \
  -d '{"workflow":"wf","window":"2026-09-25"}'
# -> a=FAILED(fatal), b=BLOCKED, run=FAILED

# 4) 时间窗补齐（重叠窗口不重复执行）
curl -s localhost:8000/backfill -H 'content-type: application/json' \
  -d '{"workflow":"wf","windows":["2026-09-25","2026-09-26"]}'

# 5) 重启进程后恢复（未完成实例继续推进）
curl -s localhost:8000/recover
```

测试覆盖的关键场景：

- 菱形依赖的正常并发推进（顺序约束）；
- 上游致命失败 / 重试耗尽失败的传播与 BLOCKED；
- 环形依赖、未知依赖在注册阶段被拒绝；
- 指数退避重试（成功 / 耗尽两条路径）；
- 8 线程并发触发同一窗口只执行一次；
- 重复提交（MANUAL/SCHEDULE/BACKFILL）终态不变；
- 基于临时 DB 文件模拟进程重启的恢复（成功步骤不重复、未完成继续推进）；
- 补齐窗口与既有调度窗口重叠不产生新实例；
- 超时标记与业务失败可区分。
