# Durable Workflow Scheduler (本地持久化工作流调度引擎)

纯本地、SQLite 持久化的工作流调度与执行引擎。用本地数据库模拟任务依赖、
失败重试与运行恢复，保证**并发触发、进程重启、按时间窗补齐**三种场景下
任务状态一致、终态稳定且可解释。

- DAG 依赖编排 + 同批次节点并发；上游失败向下游传播并阻断下游；
- 环形依赖在**提交阶段**返回 400，不会运行时死锁；
- 可重试 / 不可重试错误分流，重试次数与指数退避均有上限；
- 进程重启自动恢复：崩溃时在执行的节点重跑，已成功节点绝不重复；
- 时间窗调度与补齐共享 `(workflow, window_key)` 幂等键，重叠不重跑；
- 实例级/节点级超时分别标记，`fail_kind` 可区分超时与业务失败；
- 每个实例带只增的审计日志：触发来源、状态流转、判定依据。

## 目录

```
scheduler/   引擎、存储、执行器、窗口工具、HTTP API
tests/       34 个单测：校验/依赖传播/重试/并发/恢复/超时/补齐/API
docs/        design.md（调度语义、状态机、重试与超时规则的完整说明）
main.py      本地服务入口（文件型 SQLite: data/scheduler.db）
```

## 快速开始

```bash
# 使用仓库已有 venv（或 uv sync）
python3 -m uvicorn main:app --host 127.0.0.1 --port 8000
# 数据文件默认 data/scheduler.db；可用 SCHEDULER_DB 覆盖：
# SCHEDULER_DB=/tmp/wf.db PORT=8000 python3 main.py
```

服务启动即对未完成实例执行恢复（`auto_recover=True`）。

### 注册一个带依赖的流程

```bash
curl -s -X POST localhost:8000/workflows -H 'Content-Type: application/json' -d '{
  "name": "etl",
  "instance_timeout_seconds": 60,
  "nodes": [
    {"name": "extract", "task": "extract", "max_retries": 3,
     "retry_backoff_seconds": 0.2, "retry_backoff_max_seconds": 2,
     "timeout_seconds": 10},
    {"name": "load", "task": "load", "deps": ["extract"]}
  ]
}'
```

> 任务通过代码侧 `TaskRegistry.register(name, fn)` 注册（见下）。HTTP 只
> 负责流程定义与触发；注册一个引用了不存在任务/节点，或含环形依赖的流程
> 会在提交时得到 400，例如：
> `{"detail":"cyclic dependency rejected at submit time: a -> b -> a"}`。

### 触发 / 查询

```bash
# 手动触发（每次一个新实例）
IID=$(curl -s -X POST localhost:8000/workflows/etl/trigger \
  -H 'Content-Type: application/json' -d '{"source":"MANUAL"}' | python3 -c 'import sys,json;print(json.load(sys.stdin)["instance_id"])')

# 窗口调度（同一窗口重复调用幂等）
curl -s -X POST localhost:8000/workflows/etl/scheduled \
  -H 'Content-Type: application/json' \
  -d '{"ts":"2026-09-25T10:45:00+00:00","length":"hour","source":"SCHEDULE"}'

# 区间补齐（已跑过的窗口自动去重，返回 created / deduplicated 计数）
curl -s -X POST localhost:8000/workflows/etl/backfill \
  -H 'Content-Type: application/json' \
  -d '{"start":"2026-09-25T08:00:00+00:00","end":"2026-09-25T11:00:00+00:00","length":"hour"}'

# 状态 + 完整审计（终态标志 terminal 稳定）
curl -s localhost:8000/instances/$IID | python3 -m json.tool
```

状态响应中的关键判定字段：

* `state`：`SUCCEEDED` / `FAILED` / `TIMED_OUT`（外加运行中 PENDING/RUNNING）；
* `terminal`：稳定终态标志，重复提交/恢复/超时扫描都不会翻转；
* `fail_kind`：`BUSINESS`（业务失败/重试耗尽）或 `TIMEOUT`（超时）；
* `nodes[*].state/attempt/fail_kind/error`：逐节点状态、尝试次数与原因；
* `audit[*]`：`trigger_source + event + detail` 的完整判定链。

## 以代码方式使用

```python
from scheduler.store import Store
from scheduler.executor import TaskRegistry
from scheduler.engine import Scheduler
from scheduler.errors import RetryableTaskError, NonRetryableTaskError
from scheduler.models import NodeSpec, WorkflowSpec

registry = TaskRegistry()
registry.register("extract", lambda ctx: {"rows": 7})
registry.register("load", lambda ctx: print("load with", ctx["results"]))

sched = Scheduler(Store("data/scheduler.db"), registry, auto_recover=True)
sched.register_workflow(WorkflowSpec(name="etl", nodes=(
    NodeSpec(name="extract", task="extract", max_retries=3,
             retry_backoff_seconds=0.2, retry_backoff_max_seconds=2),
    NodeSpec(name="load", task="load", deps=("extract",)),
)))
iid = sched.trigger("etl")                       # 手动
# iid, created = sched.trigger_scheduled("etl", "w3600:2026-09-25T10:00:00+00:00")
print(sched.wait_until_terminal(iid))
```

任务函数签名：`fn(ctx) -> json-serializable`，`ctx` 包含
`instance_id / workflow / node / attempt / trigger_source / window_key / results`
（`results` 为已成功上游节点的落库结果）。

* `raise RetryableTaskError(...)`：按退避重试，直到 `max_retries`；
* `raise NonRetryableTaskError(...)`：立即失败，不重试；
* 其他异常按瞬时错误处理（视为可重试）。

## 本地验证（测试）

```bash
python3 -m pytest tests                      # 全部 34 个用例
python3 -m pytest tests -o log_cli=true      # 同时打印触发来源/状态流转/判定依据
python3 -m pytest tests/test_recovery.py -o log_cli=true   # 只看重启恢复
```

测试覆盖：

| 文件 | 场景 |
| --- | --- |
| `test_validation.py` | 自环/环依赖/未知依赖/重名节点/未知任务/非法重试配置在提交期拒绝 |
| `test_dependencies.py` | 钻石 DAG 顺序、独立节点并发、上游失败阻断全部传递下游、独立分支继续、不可重试不重试 |
| `test_retry.py` | 可重试至成功、重试上限（1+max_retries）、退避指数且封顶、0 退避立即重试 |
| `test_concurrency.py` | 8 线程并发同窗口（SCHEDULE/BACKFILL 混合）只建一例只执行一次、终态后重复提交稳定、手动并发互不干扰、重复 advance 单驱动 |
| `test_recovery.py` | 文件 DB 跨“进程”恢复：成功节点不重跑、PENDING 续跑、崩溃中 RUNNING 节点重试、恢复幂等、脏锁清理 |
| `test_timeout.py` | 实例超时 TIMED_OUT 并跳过下游、业务失败 FAILED/BUSINESS、节点超时 FAILED/TIMEOUT、终态稳定、扫描器处理无主过期实例 |
| `test_backfill_api.py` | 定时后补齐重叠窗口去重、HTTP 全生命周期、环依赖 400、补齐计数、状态查询稳定性 |

单测日志中每个关键用例都会调用 `dump_audit()`，输出形如：

```
INFO tests.audit:   source=SCHEDULE node=-    event=TRIGGER  detail=source=SCHEDULE window_key=... reason=new window
INFO tests.audit:   source=BACKFILL node=-    event=DEDUP    detail=... reason=window already exists state=SUCCEEDED
INFO tests.audit:   source=-        node=load event=NODE_SKIPPED detail=reason=upstream 'extract' failed (BUSINESS): ...
INFO tests.audit:   node-state node=extract state=SUCCEEDED attempt=1 fail_kind=NONE error=None
```

## 设计文档

完整调度语义、状态机图、重试/退避公式、超时优先级、恢复与副作用语义见
[`docs/design.md`](docs/design.md)。
