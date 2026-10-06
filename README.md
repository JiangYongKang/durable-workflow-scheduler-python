# Durable Workflow Scheduler（本地工作流调度与执行引擎）

在 FastAPI 服务之上实现的一套**本地持久化**工作流调度与执行引擎。状态保存在
SQLite 文件中，用于模拟任务依赖编排、失败重试、进程重启恢复、按时间窗补齐，
以及运维侧的**人工作废 / 暂停恢复 / 终态窗口重跑**，使以下情况下任务状态
保持一致且可解释：

1. **并发触发同一流程**：实例按 `(workflow, window)` 幂等去重，不会被重复推进；
2. **进程重启**：未完成实例可继续推进，已成功步骤不重复执行、不重复产生副作用；
   人工暂停（PAUSED）的实例重启后保持暂停，不会被恢复流程自动跑起来；
3. **按时间窗补齐**：补齐窗口与既有调度窗口重叠时复用已有实例，不产生重复执行；
4. **人工作废**：立即收尾为独立终态 CANCELLED，已完成步骤不回滚，与业务失败、
   超时明确区分；操作即时生效，不被其他流程的慢步骤阻塞；
5. **暂停 / 恢复**：暂停期间原地停车，进度、重试次数与退避节奏全部保留；恢复后
   从暂停处继续；
6. **终态窗口重跑**：成功/失败/超时/作废的窗口都可重新跑一次，作为一轮**新的
   尝试**，历史尝试保留可查；同一窗口同时只允许一个活跃尝试，并发/同波重跑请求
   合并为一次。

## 目录结构

```
scheduler/
  models.py   # 状态机枚举（含 CANCELLED/PAUSED）、流程/任务定义、RunInstance/TaskRun
  errors.py   # 可重试/致命错误、提交期校验错误、人工操作错误（NotFound/Control）
  store.py    # SQLite 持久化（事务写入、活跃窗口唯一索引、条件写守卫、旧库迁移）
  engine.py   # 调度引擎：触发、退避重试、失败传播、超时、恢复、作废/暂停/重跑
main.py       # FastAPI 服务层
tests/        # 引擎/API/人工操作单测（日志逐条打印覆盖的业务场景）
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
实例被作废（含 PENDING/READY/退避中/执行中的未完成任务）──────────────> CANCELLED（终态）
```

- **BLOCKED**：上游业务失败的传播结果，处理函数**不会被调用**，无副作用。
- **TIMEOUT**：实例整体超过 `run_timeout_seconds` 后由引擎标记
  （`error_kind="timeout"`）。
- **CANCELLED**：人工作废的收尾结果（`error_kind="cancelled"`）。已完成
  （SUCCEEDED/FAILED/BLOCKED/TIMEOUT）的步骤**保持原样不回滚**；只有尚未
  收尾的任务统一置 CANCELLED。

### 实例状态 `RunState`

```
RUNNING ── 全部任务 SUCCEEDED ──────────────> SUCCEEDED （终态）
   │      ├── 存在 FAILED 且所有任务已终态 ──> FAILED    （终态）
   │      ├── 当前时间超过 deadline ────────> TIMEOUT   （终态）
   │      └── 人工作废 cancel ──────────────> CANCELLED （终态）
   └──(人工暂停 pause)──> PAUSED ──(人工恢复 resume)──> RUNNING
```

- 终态判定稳定：只要还有任务处于非终态，实例保持 `RUNNING`（例如独立分支
  仍在重试）；所有任务进入终态后才一次性给出实例终态，并记录判定依据。
- `PAUSED` 是**持久化状态**：暂停期间 `tick()` 与重启后的 `recover()` 都不会
  推进它；必须显式 `resume` 才继续。
- `CANCELLED` 与 `FAILED`/`TIMEOUT` 平级独立：查状态时一眼可辨「是人工作废，
  不是自己挂的」，`failure_reason` 以 `cancelled:` 开头。

## 调度语义

### 流程编排与提交期校验（原有，不变）

- 任务通过 `depends_on` 声明依赖，注册流程时做 **DAG 校验**（未知依赖 →
  `UnknownTaskError`，环形依赖 → `CycleDetectedError`）。
- 每轮推进（`tick`）：超时判定 → 晋升 PENDING 为 READY → 执行就绪任务 →
  传播失败 → 终态判定。

### 并发触发与去重（原有，不变）

- 首次触发的实例 ID 为 `workflow:window`；
- 重复触发（含多线程并发）只创建一个实例，后续触发返回**最新**既有实例。

### 失败重试（原有，不变）

- `RetryableTaskError`：任务回 READY，`next_attempt_at = now +
  backoff * 2^(attempts-1)`（指数退避）；达到 `max_attempts` 置 FAILED；
- `FatalTaskError`：不重试，立即 FAILED；下游递归传播为 BLOCKED。

### 断点恢复（原有语义 + 暂停例外）

- 状态变更在每次流转时立即事务落盘；
- 重启后 `recover()`：崩溃前停留在 RUNNING 的任务（包括 PAUSED 实例中的）
  重置为 READY（at-least-once），但**只有 RUNNING 实例被推进**；PAUSED 实例
  只重置不执行，等待显式恢复；SUCCEEDED 不重复执行。

### 超时监控（原有，不变）

- 超过 deadline 时未收尾任务置 TIMEOUT、实例置 TIMEOUT，
  `failure_reason` 以 `timeout:` 开头。暂停/作废后不再判超时。

### 按时间窗补齐（原有，不变）

- `backfill(windows)` 按 `BACKFILL` 来源触发，与既有窗口重叠时复用实例。

## 人工运维操作（新增）

### 作废 `cancel`

- 入口：`engine.cancel_run(run_id)` / `cancel_window(workflow, window)`，
  HTTP `POST /runs/{run_id}/cancel` 或 `POST /ops/cancel`；
- 仅对未收尾实例有效（RUNNING/PAUSED）；对其他终态作废返回 409
  （`RunControlError`）；重复作废幂等；
- **立即生效**：作废只做一次短事务落盘（实例 + 所有未收尾任务一起置
  CANCELLED），不等待任何正在执行的慢步骤。引擎采用「注册表短锁 + 每实例锁」，
  因此别的流程卡在慢步骤上时，本实例的作废也不被阻塞；
- 慢步骤执行完返回时，其回写会被**条件写守卫**拒绝（库中已是 CANCELLED），
  不会把作废结果覆盖回去，引擎随即停止推进该实例；
- 已完成步骤不回滚，attempts、退避时间点等历史信息原样保留。

### 暂停 / 恢复 `pause` / `resume`

- 入口：`pause_run/resume_run` 或 HTTP `POST /ops/pause`、`POST /ops/resume`
  （`/runs/{run_id}/pause|resume` 同样可用）；
- 暂停只允许 RUNNING → PAUSED（重复暂停幂等，对终态暂停返回 409）；恢复只允许
  PAUSED → RUNNING（对运行中实例恢复幂等，对终态恢复返回 409）；
- 暂停期间不晋升、不执行、不判超时；任务进度、`attempts`、`next_attempt_at`
  全部保留。注意退避时间点是**绝对时间戳**，暂停期间时间继续流逝，恢复后若已
  过退避点会立即获得重试机会（重试次数不重置）；
- 正在执行的慢步骤不受强制打断：它完成后正常落盘（若成功则保留成功进度），
  但引擎检测到 PAUSED 后立即停车，不再启动任何后续任务；
- 重启后 PAUSED 不会被 `recover()` 自动推进（见上）。

### 终态窗口重跑 `rerun`

- 入口：`engine.rerun(workflow, window)` / HTTP `POST /rerun`；
- 仅当窗口**最新**尝试已收尾（SUCCESS/FAILED/TIMEOUT/CANCELLED）时创建新一轮
  尝试：`attempt` 递增（首次为 1），第 N 次实例 ID 为 `workflow:window#N`，
  来源标记为 `RERUN`，任务从全 PENDING/READY 重新开始；
- **历史不覆盖**：`list_attempts(workflow, window)`
  （HTTP `GET /workflows/{wf}/windows/{window}/attempts`）按序号升序返回每一轮
  的实例与任务明细，可直接对比两次差异；`GET /runs/{run_id}` 用带 `#N` 的
  run_id 可精确查询某一轮；
- **并发不松**：同一窗口同时只允许一个活跃尝试（RUNNING/PAUSED），由库层
  部分唯一索引 `(workflow_name, window) WHERE state IN ('RUNNING','PAUSED')`
  兜底。活跃尝试存在时，所有重跑请求（含同一波并发请求）合并返回该活跃实例，
  绝不跑两遍；
- 与原触发语义兼容：重跑之后再次 `trigger` 同一窗口仍去重，返回最新尝试，
  不会再新建实例。

### 旧库迁移

首次打开旧版数据库（`runs` 表带全量 `UNIQUE(workflow_name, window)`、无
`attempt` 列）时会自动原地迁移：历史记录标记为 `attempt=1`，唯一约束替换为
「仅活跃尝试唯一」索引，随后即可对终态历史窗口重跑。

## HTTP 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/workflows` | 注册流程（环形/未知依赖返回 `accepted:false`） |
| POST | `/workflows/handlers` | 注册本地演示执行器（成功 / 注入可重试或致命失败） |
| POST | `/trigger` | 触发一次运行并推进一轮 |
| POST | `/backfill` | 按时间窗列表补齐 |
| POST | `/recover` | 重启后恢复所有 RUNNING 实例（PAUSED 不自动推进） |
| POST | `/tick` | 推进一轮（供定时器调用） |
| POST | `/ops/cancel` | **新增** 按窗口作废最新尝试（body: workflow/window/reason） |
| POST | `/ops/pause` | **新增** 按窗口暂停最新尝试 |
| POST | `/ops/resume` | **新增** 按窗口恢复暂停尝试并推进一轮 |
| POST | `/runs/{run_id}/cancel` | **新增** 按 run_id 作废（首次尝试 run_id 无 `#` 后缀） |
| POST | `/runs/{run_id}/pause` `/resume` | **新增** 按 run_id 暂停/恢复 |
| POST | `/rerun` | **新增** 终态窗口重跑一次（活跃时合并） |
| GET  | `/workflows/{wf}/windows/{window}/attempts` | **新增** 窗口全部尝试（历史可对比） |
| GET  | `/runs/{run_id}` | 查询某轮实例与任务状态（含 `terminal`、`attempt`） |

人工操作的状态码：目标不存在 → `404`；状态前提不满足（如对终态作废）→
`409`；成功 → `200`，作废/暂停/恢复重复调用幂等。

## 本地验证

```bash
# 运行全部单测（-s 打印日志；每条用例开始时会打印其覆盖的业务场景，
# 运行过程中可见触发来源、作废/暂停/重跑、状态流转与终态判定依据）
python3 -m pytest -s
# 仅看新增人工操作行为
python3 -m pytest tests/test_control.py tests/test_control_api.py -s

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
curl -s localhost:8000/trigger -H 'content-type:application/json' \
  -d '{"workflow":"wf","window":"2026-10-07"}'
# -> a=FAILED(fatal), b=BLOCKED, run=FAILED

# 4) 作废一个还在跑的实例（立即生效，已完成步骤不回滚，run=CANCELLED）
curl -s localhost:8000/ops/cancel -H 'content-type: application/json' \
  -d '{"workflow":"wf","window":"2026-10-08","reason":"先停掉"}'

# 5) 暂停 / 恢复（暂停后 tick、recover 都不推进，恢复后从暂停处继续）
curl -s localhost:8000/ops/pause  -H 'content-type: application/json' \
  -d '{"workflow":"wf","window":"2026-10-08"}'
curl -s localhost:8000/ops/resume -H 'content-type: application/json' \
  -d '{"workflow":"wf","window":"2026-10-08"}'

# 6) 失败窗口重跑：修复处理器后重跑，历史尝试保留可对比
curl -s localhost:8000/workflows/handlers -H 'content-type: application/json' \
  -d '{"task":"a","succeed":true}'
curl -s localhost:8000/rerun -H 'content-type: application/json' \
  -d '{"workflow":"wf","window":"2026-10-07"}'
# -> 新尝试 run_id=wf:2026-10-07#2, attempt=2, state=SUCCEEDED
curl -s localhost:8000/workflows/wf/windows/2026-10-07/attempts
# -> attempts: [attempt=1 FAILED, attempt=2 SUCCEEDED]
```

测试覆盖的关键场景：

*原有行为（回归）*

- 菱形依赖的正常并发推进（顺序约束）；
- 上游致命失败 / 重试耗尽失败的传播与 BLOCKED；
- 环形依赖、未知依赖在注册阶段被拒绝；
- 指数退避重试（成功 / 耗尽两条路径）；
- 8 线程并发触发同一窗口只执行一次；
- 重复提交（MANUAL/SCHEDULE/BACKFILL）终态不变；
- 基于临时 DB 文件模拟进程重启的恢复（成功步骤不重复、未完成继续推进）；
- 补齐窗口与既有调度窗口重叠不产生新实例；
- 超时标记与业务失败可区分。

*新增人工操作（tests/test_control.py、tests/test_control_api.py）*

- 慢步骤执行期间作废立即返回；慢步骤结束后的回写被守卫拒绝，作废结果不被
  覆盖；已成功步骤保持原样、attempts/退避信息保留；
- 其他流程卡慢步骤时，作废本实例仍然立即生效（每实例锁 + 短事务）；
- CANCELLED 与 FAILED/TIMEOUT/SUCCEEDED 可区分；重复作废幂等、对其他终态
  作废报 409；
- 暂停期间 tick 不推进、进度与退避节奏保留；恢复后从暂停处继续且计数不重头；
- 慢步骤中途暂停：慢步骤完成后不启动后续任务，恢复后沿 DAG 继续、已执行步骤
  不重复；
- 重启后 PAUSED 实例不被 recover 自动跑起来，停滞 RUNNING 任务只重置不执行，
  显式恢复后才推进；暂停中的实例可直接作废；
- 四种终态窗口（成功/失败/超时/作废）都可重跑；重跑产生新 attempt 且历史尝试
  与任务明细保留可查；重跑后普通 trigger 仍去重返回最新尝试；
- 8 线程并发重跑：活跃尝试存在时合并为同一个，作废收尾后同波并发重跑也只产生
  一个新尝试；暂停尝试同样占用窗口、重跑被合并；
- 旧版全量唯一约束数据库原地迁移成功，迁移后可插入第 2 次尝试；
- API 层：作废/暂停/恢复/重跑/尝试列表的正常路径、404/409 校验与幂等。
