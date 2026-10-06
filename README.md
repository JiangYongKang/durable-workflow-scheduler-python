# Durable Workflow Scheduler（本地工作流调度与执行引擎）

在 FastAPI 服务之上实现的一套**本地持久化**工作流调度与执行引擎。状态保存在
SQLite 文件中，用于模拟任务依赖编排、失败重试、进程重启恢复与按时间窗补齐，
使以下三种情况下任务状态保持一致且可解释：

1. **并发触发同一流程**：实例按 `(workflow, window)` 幂等去重，不会被重复推进；
2. **进程重启**：未完成实例可继续推进，已成功步骤不重复执行、不重复产生副作用；
3. **按时间窗补齐**：补齐窗口与既有调度窗口重叠时复用已有实例，不产生重复执行。

在此之上还提供一层**人工运维操作**（不推翻原有语义）：

4. **作废（cancel）**：让还在跑的实例立即停止推进，已完成步骤保留；
5. **暂停/恢复（pause/resume）**：冻结实例进度，恢复后从暂停处继续；
6. **重跑（rerun）**：对已收尾的窗口发起一次新的尝试，历史结果保留可查。

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
实例被作废 ─────────────────────────────────────────────────────────> CANCELLED（终态）
```

- **BLOCKED**：上游业务失败的传播结果，表示“因依赖失败被阻止启动”，
  与自身执行失败 `FAILED` 区分，处理函数**不会被调用**，无副作用。
- **TIMEOUT**：实例整体超过 `run_timeout_seconds` 后由引擎标记，
  与业务失败可区分（`error_kind="timeout"`）。
- **CANCELLED**：实例被人工作废时，所有非终态任务被标记为该状态
  （`error_kind="cancelled"`），与业务失败、超时明确区分。

### 实例状态 `RunState`

```
RUNNING ── 全部任务 SUCCEEDED ──────────────> SUCCEEDED （终态）
   │    ├── 存在 FAILED 且所有任务已终态 ────> FAILED    （终态）
   │    ├── 当前时间超过 deadline ───────────> TIMEOUT   （终态）
   │    └── 人工作废 ────────────────────────> CANCELLED （终态）
   └──(人工暂停)──> PAUSED ──(人工恢复)──> RUNNING       （非终态，冻结推进）
```

- **CANCELLED** 是明确的收尾结果：`failure_reason` 以 `cancelled:` 开头，
  查状态一眼可区分“被人作废”与“自己挂掉（FAILED/TIMEOUT）”。
- **PAUSED** 不是终态：实例停止推进，但进度、重试次数与退避节奏全部保留；
  恢复（resume）后回到 RUNNING，从暂停处继续。

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
  - PENDING 任务根据已落盘的上游状态重新晋升 READY；
  - **PAUSED 实例是人为停下的，恢复动作不会顺手把它跑起来**，
    只会记录日志 `处于 PAUSED，保持暂停不推进`。

### 超时监控

- 实例创建时记录 `deadline = created_at + run_timeout_seconds`；
- 每次推进首先检查：超过 deadline 时，所有非终态任务置 `TIMEOUT`，
  实例置 `TIMEOUT`，`failure_reason` 以 `timeout:` 开头，
  与业务失败（`FAILED` + `fatal/retryable`）明确区分。

### 按时间窗补齐

- `backfill(windows)` 对每个窗口按 `BACKFILL` 来源触发；
- 与既有 `MANUAL/SCHEDULE` 窗口重叠时直接复用实例，不创建、不重复执行；
- 窗口标识由调用方给定（如日期字符串 `2026-09-25`）。

## 运维操作（人工介入）

以下操作不推翻原有调度语义：正常调度、去重、恢复逻辑完全照旧，
运维操作只是在其上增加人工可控的状态出口。

### 作废 cancel

- 对未收尾（RUNNING/PAUSED）的实例生效：所有非终态任务置 `CANCELLED`，
  实例置 `CANCELLED`（终态），`failure_reason` 以 `cancelled:` 开头；
- **已完成步骤保持原样不回滚**（如已 SUCCEEDED 的任务不受影响）；
- **立即生效**：作废直接走存储层原子写入，不占用引擎主锁——即使另一个
  实例正卡在慢任务上，作废请求也能立刻落盘；推进循环在每个任务执行前
  复查实例状态，看到 CANCELLED 即停止；执行中的任务返回后若发现实例
  已被作废，结果丢弃不回写；
- 已终态的实例再作废返回 409（`InvalidTransitionError`）。

### 暂停 / 恢复 pause / resume

- `pause` 仅接受 RUNNING 实例：置为 PAUSED 后 `tick`/`recover` 都跳过它，
  进度、重试次数（attempts）与退避时间（next_attempt_at）原样保留；
- `resume` 仅接受 PAUSED 实例：回到 RUNNING，下一轮 tick 从暂停处继续，
  退避节奏不受影响；
- **进程重启后，PAUSED 实例不会被 recover 当成“还没跑完”顺手推进**，
  必须显式 resume；
- 状态不匹配（如暂停已终态实例、恢复未暂停实例）返回 409。

### 重跑 rerun

- 已收尾的窗口（SUCCEEDED/FAILED/TIMEOUT/CANCELLED 均可）支持重跑：
  产生一次**新的尝试**，`attempt` 递增，run_id 为 `workflow:window#N`
  （首次尝试仍是 `workflow:window`，保持兼容），来源标记为 `RERUN`；
- **历史结果保留**：旧尝试的实例与任务记录不被覆盖，可通过
  `GET /runs/{run_id}` 或 `GET /workflows/{wf}/windows/{window}/runs`
  查询对比两次运行的差异；
- **并发不松**：同一窗口已有进行中的尝试时，重跑请求去重返回该尝试；
  同一波并发重跑在存储层串行，只有第一个能创建新尝试；
- 普通 `trigger`/`backfill` 仍按窗口去重到**最新一次尝试**，不会新建实例。

> 存储说明：`runs` 表唯一约束为 `(workflow_name, window, attempt)`；
> 旧版库文件打开时会自动迁移（旧数据 attempt 记为 1）。

## HTTP 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/workflows` | 注册流程（环形/未知依赖返回 `accepted:false`） |
| POST | `/workflows/handlers` | 注册本地演示执行器（成功 / 注入可重试或致命失败） |
| POST | `/trigger` | 触发一次运行并推进一轮 |
| POST | `/backfill` | 按时间窗列表补齐 |
| POST | `/recover` | 重启后恢复所有未完成实例（PAUSED 除外） |
| POST | `/tick` | 推进一轮（供定时器调用） |
| GET  | `/runs/{run_id}` | 查询实例与各任务状态（含 `terminal` 终态标记、`attempt`） |
| POST | `/runs/{run_id}/cancel` | 作废未收尾实例（立即生效，已完成步骤保留） |
| POST | `/runs/{run_id}/pause` | 暂停运行中的实例（进度与重试节奏保留） |
| POST | `/runs/{run_id}/resume` | 恢复暂停中的实例并推进一轮 |
| POST | `/rerun` | 重跑已收尾窗口：新一次尝试，历史保留，并发去重 |
| GET  | `/workflows/{wf}/windows/{window}/runs` | 列出该窗口全部尝试（含历史重跑结果） |

运维操作的状态约定：实例不存在返回 404；操作与当前状态不匹配
（如作废已终态实例、恢复未暂停实例）返回 409。

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

# 5) 重启进程后恢复（未完成实例继续推进，PAUSED 除外）
curl -s localhost:8000/recover

# 6) 暂停 / 恢复（暂停期间 tick 不推进，恢复后接着走）
curl -s -X POST localhost:8000/runs/wf:2026-09-26/pause
curl -s -X POST localhost:8000/runs/wf:2026-09-26/resume

# 7) 作废（已完成步骤保留，状态为 CANCELLED 而非 FAILED/TIMEOUT）
curl -s -X POST localhost:8000/runs/wf:2026-09-26/cancel \
  -H 'content-type: application/json' -d '{"reason":"人工介入"}'

# 8) 重跑已收尾窗口（新尝试 wf:2026-09-25#2，历史保留）
curl -s localhost:8000/rerun -H 'content-type: application/json' \
  -d '{"workflow":"wf","window":"2026-09-25"}'
curl -s localhost:8000/workflows/wf/windows/2026-09-25/runs   # 对比两次尝试
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
- 超时标记与业务失败可区分；
- 作废：未完成步骤置 CANCELLED、已完成步骤保留、与 FAILED/TIMEOUT 可区分；
- 作废立即生效：慢任务卡住引擎主锁时仍能立刻作废本实例及其他实例，
  执行中的任务结果在作废后丢弃不回写；
- 暂停/恢复：暂停期间 tick 不推进、重试次数与退避节奏保留、恢复后接着走；
- 进程重启后 recover 不推进 PAUSED 实例，显式 resume 才继续；
- 非法状态转换（暂停终态实例、恢复未暂停实例等）被拒绝；
- 重跑：新尝试 attempt 递增、历史结果保留可查、普通触发仍去重到最新尝试；
- 8 线程并发重跑只创建一个新尝试，进行中的窗口重跑去重。
