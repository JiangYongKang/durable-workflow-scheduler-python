"""工作流调度与执行引擎。

语义概览：
- 提交（trigger/backfill）按 (workflow, window) 幂等去重；
- 依赖失败沿 DAG 传播为 BLOCKED，阻止下游启动；
- 可重试错误按指数退避重试，致命错误立即失败；
- 进程重启后通过 recover() 继续推进未完成实例；
- 超过 run_timeout 的实例标记为 TIMEOUT，与业务失败区分。
"""
from __future__ import annotations

import logging
import threading
import time
from collections.abc import Callable

from .errors import (
    CycleDetectedError,
    FatalTaskError,
    RetryableTaskError,
    UnknownTaskError,
)
from .models import (
    RunInstance,
    RunState,
    TaskRun,
    TaskState,
    TERMINAL_TASK_STATES,
    TriggerSource,
    WorkflowDef,
)
from .store import StateStore

logger = logging.getLogger("scheduler.engine")

TaskHandler = Callable[[], None]


class Engine:
    """调度与执行引擎：所有状态推进经由本类并落盘。"""

    def __init__(self, store: StateStore, now: Callable[[], float] = time.time) -> None:
        self._store = store
        self._now = now
        self._lock = threading.RLock()
        self._workflows: dict[str, WorkflowDef] = {}
        self._handlers: dict[str, TaskHandler] = {}

    # ---- 注册 ----
    def register_workflow(self, wf: WorkflowDef) -> None:
        """注册流程定义；环形依赖或未知依赖在提交阶段被拒绝。"""
        names = {t.name for t in wf.tasks}
        for t in wf.tasks:
            for dep in t.depends_on:
                if dep not in names:
                    raise UnknownTaskError(
                        f"workflow {wf.name}: task {t.name} 依赖未知任务 {dep}")
        self._check_cycle(wf)
        with self._lock:
            self._workflows[wf.name] = wf
        logger.info("注册流程 %s，任务数=%d", wf.name, len(wf.tasks))

    @staticmethod
    def _check_cycle(wf: WorkflowDef) -> None:
        deps = {t.name: t.depends_on for t in wf.tasks}
        WHITE, GRAY, BLACK = 0, 1, 2
        color = dict.fromkeys(deps, WHITE)

        def visit(node: str, path: list[str]) -> None:
            color[node] = GRAY
            for dep in deps[node]:
                if color[dep] == GRAY:
                    cycle = " -> ".join(path + [node, dep])
                    raise CycleDetectedError(
                        f"workflow {wf.name} 存在环形依赖: {cycle}")
                if color[dep] == WHITE:
                    visit(dep, path + [node])
            color[node] = BLACK

        for name in deps:
            if color[name] == WHITE:
                visit(name, [])

    def register_handler(self, task_name: str, handler: TaskHandler) -> None:
        with self._lock:
            self._handlers[task_name] = handler

    # ---- 触发 ----
    def trigger(self, workflow_name: str, window: str,
                source: TriggerSource = TriggerSource.MANUAL) -> RunInstance:
        """触发一次运行；同窗口重复触发返回已有实例，不重复推进。"""
        with self._lock:
            wf = self._workflows[workflow_name]
            now = self._now()
            run_id = f"{workflow_name}:{window}"
            run = RunInstance(
                run_id=run_id, workflow_name=workflow_name, window=window,
                source=source, created_at=now,
                deadline=now + wf.run_timeout_seconds,
            )
            tasks = [
                TaskRun(
                    run_id=run_id, task_name=t.name,
                    state=TaskState.READY if not t.depends_on else TaskState.PENDING,
                )
                for t in wf.tasks
            ]
            inserted = self._store.insert_run_if_absent(run, tasks)
            if inserted:
                logger.info("[触发] source=%s workflow=%s window=%s -> 新实例 %s",
                            source.value, workflow_name, window, run_id)
                return run
            existing = self._store.get_run_by_window(workflow_name, window)
            assert existing is not None
            logger.info("[触发] source=%s workflow=%s window=%s -> 已存在实例 %s"
                        "（state=%s），去重不重复推进",
                        source.value, workflow_name, window,
                        existing.run_id, existing.state.value)
            return existing

    def backfill(self, workflow_name: str, windows: list[str]) -> list[RunInstance]:
        """按时间窗补齐；与既有窗口重叠时复用已有实例，不产生重复执行。"""
        logger.info("[补齐] workflow=%s windows=%s", workflow_name, windows)
        return [self.trigger(workflow_name, w, TriggerSource.BACKFILL) for w in windows]

    # ---- 推进 ----
    def tick(self) -> None:
        """推进一轮：执行就绪任务、传播失败、判定终态、标记超时。"""
        with self._lock:
            for run in self._store.list_unfinished_runs():
                self._advance(run)

    def _advance(self, run: RunInstance) -> None:
        wf = self._workflows[run.workflow_name]
        tasks = {t.task_name: t for t in self._store.list_tasks(run.run_id)}
        now = self._now()

        # 1. 超时判定：与业务失败区分
        if now > run.deadline:
            logger.info("[超时] run=%s 超过时限 %.1fs，标记 TIMEOUT",
                        run.run_id, wf.run_timeout_seconds)
            for t in tasks.values():
                if t.state not in TERMINAL_TASK_STATES:
                    self._transition(t, TaskState.TIMEOUT, error_kind="timeout",
                                     error="实例超过时限")
            run.state = RunState.TIMEOUT
            run.finished_at = now
            run.failure_reason = "timeout: 实例超过时限"
            self._store.save_run(run)
            return

        # 2. 晋升：上游已全部成功的 PENDING 任务转为 READY
        #    （重启恢复时上游成功发生在重启前，需要在此补一次晋升）
        self._promote_ready(wf, tasks)

        # 3. 执行就绪任务（退避时间未到则跳过）
        for t in tasks.values():
            if t.state != TaskState.READY:
                continue
            if t.next_attempt_at is not None and now < t.next_attempt_at:
                continue
            self._execute(wf, run, t, tasks)

        # 4. 终态判定
        self._finalize(run, tasks)

    def _execute(self, wf: WorkflowDef, run: RunInstance,
                 task: TaskRun, tasks: dict[str, TaskRun]) -> None:
        tdef = next(t for t in wf.tasks if t.name == task.task_name)
        handler = self._handlers.get(task.task_name)
        if handler is None:
            raise FatalTaskError(f"任务 {task.task_name} 未注册执行器")
        now = self._now()
        task.attempts += 1
        task.started_at = now
        self._transition(task, TaskState.RUNNING)
        try:
            handler()
        except RetryableTaskError as exc:
            if task.attempts >= tdef.retry_policy.max_attempts:
                self._transition(task, TaskState.FAILED, error_kind="retryable",
                                 error=f"重试耗尽({task.attempts}次): {exc}")
                self._propagate_blocked(tasks, wf, task.task_name)
            else:
                delay = tdef.retry_policy.backoff_seconds * (2 ** (task.attempts - 1))
                task.next_attempt_at = self._now() + delay
                self._transition(task, TaskState.READY, error_kind="retryable",
                                 error=str(exc))
                logger.info("[重试] run=%s task=%s 第%d次失败，%.1fs 后重试",
                            run.run_id, task.task_name, task.attempts, delay)
        except FatalTaskError as exc:
            self._transition(task, TaskState.FAILED, error_kind="fatal",
                             error=f"不可重试: {exc}")
            self._propagate_blocked(tasks, wf, task.task_name)
        else:
            task.finished_at = self._now()
            self._transition(task, TaskState.SUCCEEDED)
            self._promote_ready(wf, tasks)

    def _promote_ready(self, wf: WorkflowDef, tasks: dict[str, TaskRun]) -> None:
        """上游全部成功的下游任务由 PENDING 转为 READY。"""
        for tdef in wf.tasks:
            t = tasks[tdef.name]
            if t.state != TaskState.PENDING:
                continue
            if all(tasks[d].state == TaskState.SUCCEEDED for d in tdef.depends_on):
                self._transition(t, TaskState.READY)

    def _propagate_blocked(self, tasks: dict[str, TaskRun],
                           wf: WorkflowDef, failed: str) -> None:
        """失败沿 DAG 向下游传播为 BLOCKED，阻止其启动。"""
        changed = True
        while changed:
            changed = False
            for tdef in wf.tasks:
                t = tasks[tdef.name]
                if t.state in TERMINAL_TASK_STATES or t.state == TaskState.RUNNING:
                    continue
                if any(tasks[d].state in (TaskState.FAILED, TaskState.BLOCKED)
                       for d in tdef.depends_on):
                    self._transition(t, TaskState.BLOCKED, error_kind="fatal",
                                     error=f"上游 {failed} 失败，阻止启动")
                    changed = True

    def _finalize(self, run: RunInstance, tasks: dict[str, TaskRun]) -> None:
        """全部任务进入终态后，给出实例的稳定终态判定。"""
        if any(t.state not in TERMINAL_TASK_STATES for t in tasks.values()):
            return
        now = self._now()
        if all(t.state == TaskState.SUCCEEDED for t in tasks.values()):
            run.state = RunState.SUCCEEDED
            run.failure_reason = None
            logger.info("[终态] run=%s 判定 SUCCEEDED，依据：全部任务成功", run.run_id)
        else:
            failed = [t for t in tasks.values() if t.state == TaskState.FAILED]
            run.state = RunState.FAILED
            run.failure_reason = "; ".join(
                f"{t.task_name}: {t.error}" for t in failed) or "blocked"
            logger.info("[终态] run=%s 判定 FAILED，依据：%s",
                        run.run_id, run.failure_reason)
        run.finished_at = now
        self._store.save_run(run)

    def _transition(self, task: TaskRun, new_state: TaskState,
                    error: str | None = None, error_kind: str | None = None) -> None:
        old = task.state
        task.state = new_state
        if error is not None:
            task.error = error
        if error_kind is not None:
            task.error_kind = error_kind
        if new_state in TERMINAL_TASK_STATES and task.finished_at is None:
            task.finished_at = self._now()
        self._store.save_task(task)
        logger.info("[状态] run=%s task=%s %s -> %s%s",
                    task.run_id, task.task_name, old.value, new_state.value,
                    f"（{error}）" if error else "")

    # ---- 恢复 ----
    def recover(self) -> list[str]:
        """进程重启后恢复：继续推进所有未完成实例，已成功步骤不重复执行。

        崩溃前停留在 RUNNING 的任务无完成记录，按 at-least-once 语义重置为
        READY 重新执行；SUCCEEDED 是在副作用完成后落盘的，不会重复执行。
        """
        with self._lock:
            runs = self._store.list_unfinished_runs()
            for run in runs:
                for task in self._store.list_tasks(run.run_id):
                    if task.state == TaskState.RUNNING:
                        self._transition(task, TaskState.READY,
                                         error="进程重启，重新执行未完成任务")
                        task.next_attempt_at = None
                        self._store.save_task(task)
                logger.info("[恢复] source=%s run=%s 继续推进（state=%s）",
                            TriggerSource.RECOVERY.value, run.run_id, run.state.value)
                self._advance(run)
            return [r.run_id for r in runs]

    # ---- 查询 ----
    def get_status(self, run_id: str) -> tuple[RunInstance, list[TaskRun]] | None:
        run = self._store.get_run(run_id)
        if run is None:
            return None
        return run, self._store.list_tasks(run_id)
