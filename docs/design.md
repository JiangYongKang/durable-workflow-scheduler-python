# 本地持久化工作流调度与执行引擎 — 设计与调度语义

## 1. 目标与范围

在单台机器、单个 Python 进程内，用 SQLite 持久化全部状态，提供：

1. 带依赖（DAG）的流程编排与并发执行；上游失败按规则阻断下游；
2. 环形依赖等非法定义在**提交阶段**拒绝，而不是运行时死锁；
3. 可重试/不可重试错误分流，重试次数与退避有上限；
4. 进程重启后自动恢复未完成实例，已成功步骤不重复执行、不重复产生副作用；
5. 按时间窗的调度与补齐（backfill），重叠窗口天然去重；
6. 实例级/节点级超时监控，超时与业务失败可区分；终态稳定、可解释（审计日志）。

不包含：分布式多活调度、Cron 表达式解析（窗口长度固定 minute/hour/day/week）、
跨机器任务派发。所有时间戳为挂钟时间（`time.time()`），保证重启后退避与超时
仍然有效；测试可注入时钟。

## 2. 模块结构

| 文件 | 职责 |
| --- | --- |
| `scheduler/models.py` | 状态枚举、`WorkflowSpec`/`NodeSpec` 数据类型 |
| `scheduler/errors.py` | 领域异常：可重试、不可重试、超时、校验失败 |
| `scheduler/store.py` | SQLite 持久化：实例/节点/审计/锁，CAS 状态流转 |
| `scheduler/executor.py` | 任务注册表与本地同步执行器（异常分类） |
| `scheduler/engine.py` | 编排引擎：校验、触发、单驱动推进、重试、恢复、超时扫描 |
| `scheduler/windows.py` | 对齐到 epoch 的时间窗与窗口键计算 |
| `scheduler/api.py` | FastAPI HTTP 接口 |
| `main.py` | 本地服务入口（文件型 SQLite，启动自动恢复） |

## 3. 状态机

### 3.1 节点（Node）

```
PENDING ──所有依赖成功──▶ READY ──CAS claim──▶ RUNNING ──成功──▶ SUCCEEDED（终态）
                            ▲                      │
                            └────可重试错误────────┤ 回到 READY，带 not_before 退避门
                                                   │
                              不可重试/重试耗尽/超时─▶ FAILED（终态，fail_kind=BUSINESS|TIMEOUT）
PENDING / READY ──上游失败传递──▶ SKIPPED（终态，从不执行）
```

关键规则：

* `READY -> RUNNING` 是带条件的 CAS（`state='READY' AND not_before<=now`），
  并发工作线程中只有一个能赢，因此同一 attempt 的节点函数体至多执行一次；
* `RUNNING -> SUCCEEDED/FAILED/READY` 全部带 `attempt` 守卫，恢复后迟到的
  旧结果会因 attempt 不匹配被丢弃，不可能把新 attempt 的结果写错；
* `SUCCEEDED` 不可变，恢复时不触碰，因此**成功步骤及其副作用绝不重复**；
* 节点级超时把 RUNNING 节点置为 `FAILED/fail_kind=TIMEOUT`（见第 6 节）。

### 3.2 实例（Instance）

```
PENDING ──驱动获取锁──▶ RUNNING ──全部节点 SUCCEEDED──────────▶ SUCCEEDED（终态）
                                  ├─业务失败（含节点超时）────▶ FAILED（终态，fail_kind=BUSINESS|TIMEOUT）
                                  └─超过实例时限──────────────▶ TIMED_OUT（终态，fail_kind=TIMEOUT）
```

* `FAILED` 与 `TIMED_OUT` 是两种不同终态；即使节点超时导致实例 `FAILED`，
  其 `fail_kind=TIMEOUT`，依然可与 `BUSINESS` 业务失败区分；
* 终态判定稳定：所有终态写库都带
  `WHERE state IN ('PENDING','RUNNING')`，重复触发、重复恢复、重复扫描都
  无法改变终态（幂等）；
* `get_status().terminal == state ∈ {SUCCEEDED, FAILED, TIMED_OUT}`。

## 4. 调度与并发语义

### 4.1 触发来源

`MANUAL`（手动）、`SCHEDULE`（定时）、`BACKFILL`（补齐）、`RECOVERY`（恢复）
四种来源记录在实例行与审计日志中。

* **手动触发**：每次创建新实例。并发 4 次手动触发 → 4 个独立实例，各自执行。
* **调度/补齐触发**：以 `(workflow_name, window_key)` 为幂等键
  （SQLite 部分唯一索引 `WHERE window_key IS NOT NULL`）。同一窗口的定时与
  补齐调用解析到同一实例：先到者创建，后到者写 `DEDUP` 审计并返回
  `created=false`，窗口只执行一次。
* 进程内用一把存储写锁串行化 SQLite 写入；跨进程并发由唯一索引兜底。

### 4.2 单驱动（single-driver）推进

`instance_locks` 是持久化的排他锁。推进实例前必须抢到锁：

* 并发触发同一流程的同一窗口实例时，只有一个线程启动驱动，其余直接记录
  “lock held”并返回，实例不会被重复推进；
* 每个实例的驱动是一个守护线程；同一批次中所有无依赖冲突的 READY 节点
  被并发 claim，提交到有界 `ThreadPoolExecutor` 执行；
* 驱动循环每轮：检查时限 → 失败传播与跳过 → 激活满足依赖的 PENDING →
  claim 可运行 READY → 并发执行一批 → 处理结果/退避 → 直到终态。

### 4.3 失败传播

某节点终态 FAILED 时，计算其全部**传递下游**（`_transitive_dependents`）：

* 处于 PENDING/READY 的下游节点全部置 `SKIPPED`，永不启动，审计写明
  `reason=upstream '<x>' failed (...)`；
* 与失败节点无依赖关系的分支照常执行；
* 实例终态 `FAILED`，`fail_kind` 取根因失败节点的类别
  （BUSINESS 或 TIMEOUT），`error` 保留根因错误信息。

## 5. 重试与退避规则

* 任务抛出 `RetryableTaskError`（或任何非 `TaskError` 的未知异常，按瞬时
  错误约定处理）→ 可重试；
* 任务抛出 `NonRetryableTaskError` → 立即失败，**不消耗重试次数**；
* 每个节点 `max_retries` 为重试次数上限；总执行次数 = `1 + max_retries`；
* 第 `k` 次失败后的退避：
  `delay = min(retry_backoff_seconds * 2**(k-1), retry_backoff_max_seconds)`，
  指数退避有上限；退避为 0 表示立即重试；
* 重试通过 `READY + not_before_ts` 退避门实现：退避窗口内节点不可被 claim；
* 重试预算耗尽时节点 FAILED，错误为
  `retries exhausted after N attempt(s): <原始错误>`，实例按失败传播收尾。

## 6. 超时规则

* **实例级超时**：`instance_timeout_seconds`，在触发/创建实例时换算成
  `deadline_ts`（绝对挂钟时间，重启后依然有效）。超过时限：
  * 实例 → `TIMED_OUT / fail_kind=TIMEOUT`；
  * 仍在 RUNNING 的节点 → `FAILED / fail_kind=TIMEOUT`（成功节点保持成功）；
  * PENDING/READY 节点 → SKIPPED；
  * 迟到的任务结果由 CAS 丢弃，不会“复活”实例。
* **节点级超时**：`timeout_seconds` 是单节点单次 attempt 的相对时限
  （`started_at + timeout_seconds`）。超时后节点 `FAILED/fail_kind=TIMEOUT`，
  实例 `FAILED/fail_kind=TIMEOUT`（注意实例态是 FAILED 而非 TIMED_OUT，
  但 fail_kind 仍可与业务失败区分）。
* 当实例时限与节点时限同时到达时，**实例时限优先**（等待批次结果后先复检
  实例 deadline）。
* 一个后台守护线程周期性 `sweep_timeouts()`：对没有活驱动（例如上一进程
  遗留）的过期实例抢锁并按实例超时收尾；有活驱动的实例由驱动自己收尾。
  已终态实例不会被扫描重复处理。

## 7. 恢复语义（进程重启）

`Scheduler(auto_recover=True)`（`main.py` 默认）启动时执行 `recover()`：

1. `force_clear_all_locks()`：上一进程已死，持有的全部实例锁视为脏锁清除；
2. 找出所有 `PENDING/RUNNING` 实例；
3. `reset_interrupted_nodes()`：崩溃时停留在 RUNNING 的节点（执行到一半、
   结果未提交）回置 READY 并可再次执行；**SUCCEEDED 节点一行都不改**；
4. 为每个未完成实例写 `RECOVERY` 审计（`source=RECOVERY`，记录被重置节点），
   重新启动驱动继续推进。

副作用语义：节点函数在“结果成功提交”之前崩溃，属于 at-most-once 语义下
允许的重新执行；一旦节点 SUCCEEDED（结果已落库），任何恢复路径都不会再
调用它。任务自身应保证输入级幂等（以 `ctx.results` 中已落库的上游结果为
准，而非外部可变状态）。

## 8. 时间窗与补齐

窗口按 epoch 对齐：`start = floor(t / W) * W`，窗口键
`"w{W}:{start_iso_utc}"`。`windows_between(start, end, length)` 枚举覆盖
区间的所有窗口。补齐即对每个窗口调用 `trigger_scheduled(..., BACKFILL)`：

* 定时已经跑过的窗口 → 命中唯一索引 → `created=false`，不重复执行；
* 未跑过的窗口 → 创建并执行；
* 因此“补齐与既有调度窗口重叠时不产生重复执行”是存储层保证的，不依赖
  调用方记忆。

## 9. 可解释性：审计日志

所有关键判定都追加到 `audit_log`（只增不改），字段包含
`trigger_source / node / event / detail / ts`。事件序列例如：

```
TRIGGER(source, window, reason=new window|manual trigger)
DEDUP(source, window, reason=window already exists, state=...)
INSTANCE_RUNNING(reason=driver acquired lock)
READY_BATCH(nodes, reason=all deps SUCCEEDED)
NODE_RUNNING(attempt, trigger)
NODE_RETRY(attempt, next_attempt, backoff_seconds, reason)
NODE_SUCCEEDED(attempt) | NODE_FAILED(attempt, fail_kind, reason)
NODE_SKIPPED(reason=upstream ... failed)
RECOVERY(source=RECOVERY, reset_running=..., state=...)
INSTANCE_SUCCEEDED | INSTANCE_FAILED(fail_kind, failed_node, skipped)
INSTANCE_TIMED_OUT(fail_kind=TIMEOUT, reason)
```

`GET /instances/{id}` 返回终态标志、每个节点的状态/尝试次数/失败类别与
完整审计，可直接回答“谁触发的、何时流转、依据什么判定”。

## 10. HTTP 接口摘要

| 方法/路径 | 说明 |
| --- | --- |
| `POST /workflows` | 注册流程；环形依赖/未知节点/未知任务返回 400 |
| `POST /workflows/{name}/trigger` | 手动触发，返回新实例 id（202） |
| `POST /workflows/{name}/scheduled` | 给定时点所属窗口的幂等触发 |
| `POST /workflows/{name}/backfill` | 区间内逐窗口补齐，返回 created/deduplicated 计数 |
| `GET /instances?workflow=` | 列出实例 |
| `GET /instances/{id}` | 稳定终态 + 节点明细 + 审计 |
| `POST /recover` | 显式恢复未完成实例 |
| `GET /health` | 健康检查 |
