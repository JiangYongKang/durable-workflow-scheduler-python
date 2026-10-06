"""核心类型定义：工作流定义、任务定义、运行实例与状态枚举。"""
from __future__ import annotations

import enum
from dataclasses import dataclass, field


class TaskState(str, enum.Enum):
    """单个任务运行的状态机。"""

    PENDING = "PENDING"        # 等待上游完成
    READY = "READY"            # 上游已全部成功，等待调度
    RUNNING = "RUNNING"        # 正在执行
    SUCCEEDED = "SUCCEEDED"    # 成功（终态）
    FAILED = "FAILED"          # 业务失败且重试耗尽 / 不可重试（终态）
    BLOCKED = "BLOCKED"        # 上游失败传播，被阻止启动（终态）
    TIMEOUT = "TIMEOUT"        # 超过实例时限被标记（终态）
    CANCELLED = "CANCELLED"    # 人为作废实例时被标记（终态）


class RunState(str, enum.Enum):
    """流程实例的状态机。"""

    RUNNING = "RUNNING"
    PAUSED = "PAUSED"          # 人为暂停：停止推进但保留进度（非终态）
    SUCCEEDED = "SUCCEEDED"    # 全部任务成功（终态）
    FAILED = "FAILED"          # 存在业务失败（终态）
    TIMEOUT = "TIMEOUT"        # 超时导致终止（终态）
    CANCELLED = "CANCELLED"    # 人为作废（终态，与业务失败/超时区分）


TERMINAL_TASK_STATES = frozenset(
    {TaskState.SUCCEEDED, TaskState.FAILED, TaskState.BLOCKED, TaskState.TIMEOUT,
     TaskState.CANCELLED}
)
TERMINAL_RUN_STATES = frozenset(
    {RunState.SUCCEEDED, RunState.FAILED, RunState.TIMEOUT, RunState.CANCELLED})


class TriggerSource(str, enum.Enum):
    """触发来源，用于日志与去重判定。"""

    MANUAL = "MANUAL"          # 手动触发
    SCHEDULE = "SCHEDULE"      # 调度窗口触发
    BACKFILL = "BACKFILL"      # 时间窗补齐
    RECOVERY = "RECOVERY"      # 进程重启后的恢复推进
    RERUN = "RERUN"            # 已收尾窗口的重跑（新一次尝试）


@dataclass(frozen=True)
class RetryPolicy:
    max_attempts: int = 3          # 最大尝试次数（含首次）
    backoff_seconds: float = 1.0   # 基础退避秒数，指数增长


@dataclass(frozen=True)
class TaskDef:
    name: str
    depends_on: tuple[str, ...] = ()
    retry_policy: RetryPolicy = field(default_factory=RetryPolicy)


@dataclass(frozen=True)
class WorkflowDef:
    name: str
    tasks: tuple[TaskDef, ...]
    run_timeout_seconds: float = 3600.0   # 实例整体时限


@dataclass
class TaskRun:
    run_id: str
    task_name: str
    state: TaskState = TaskState.PENDING
    attempts: int = 0
    next_attempt_at: float | None = None   # 退避后的下次可执行时间戳
    error: str | None = None
    error_kind: str | None = None          # "retryable" | "fatal" | "timeout"
    started_at: float | None = None
    finished_at: float | None = None


@dataclass
class RunInstance:
    run_id: str
    workflow_name: str
    window: str                 # 调度窗口标识（如 "2026-09-25"），用于补齐去重
    source: TriggerSource
    attempt: int = 1            # 第几次尝试；重跑产生 >1 的新尝试，旧结果保留
    state: RunState = RunState.RUNNING
    created_at: float = 0.0
    deadline: float = 0.0
    finished_at: float | None = None
    failure_reason: str | None = None
