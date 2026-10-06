"""工作流调度与执行引擎。

语义概览：
- 提交（trigger/backfill）按 (workflow, window) 幂等去重；
- 依赖失败沿 DAG 传播为 BLOCKED，阻止下游启动；
- 可重试错误按指数退避重试，致命错误立即失败；
- 进程重启后通过 recover() 继续推进未完成实例，PAUSED 实例不会被自动推进；
- 超过 run_timeout 的实例标记为 TIMEOUT，与业务失败区分；
- 人工操作不改变原有调度/去重/恢复语义：
  - cancel_run 作废：立即落盘生效，已完成步骤不回滚，未完成任务 CANCELLED，
    实例终态 CANCELLED（与 FAILED/TIMEOUT 可区分）；
  - pause_run/resume_run 暂停恢复：进度、attempts 与退避节奏原样保留；
  - rerun 重跑：终态窗口创建新一轮尝试，历史尝试保留可查；同一窗口同时只允许
    一个活跃尝试，并发/同波重跑合并为一次。
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
    RunControlError,
    RunNotFoundError,
    UnknownTaskError,
)
from .models import (
    TERMINAL_RUN_STATES,
    TERMINAL_TASK_STATES,
    RunInstance,
    RunState,
    TaskRun,
    TaskState,
    TriggerSource,
    WorkflowDef,
)
from .store import StateStore

logger = logging.getLogger("scheduler.engine")

TaskHandler = Callable[[], None]


class _ControlInterrupted(Exception):
    """推进期间检测到人工操作（如作废）已改变库中状态，立即停止本实例推进。"""


class Engine:
    """调度与执行引擎：所有状态推进经由本类并落盘。"""

    def __init__(self, store: StateStore, now: Callable[[], float] = time.time) -> None:
        self._store = store
        self._now = now
        self._registry_lock = threading.RLock()
        self._run_locks: dict[str, threading.RLock] = {}
        self._workflows: dict[str, WorkflowDef] = {}
        self._handlers: dict[str, TaskHandler] = {}

    # ---- 锁 ----
    def _run_lock(self, run_id: str) -> threading.RLock:
        with self._registry_lock:
            lock = self._run_locks.get(run_id)
            if lock is None:
                lock = threading.RLock()
                self._run_locks[run_id] = lock
            return lock

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
        with self._registry_lock:
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
        with self._registry_lock:
            self._handlers[task_name] = handler

    # ---- 触发 ----
    def trigger(self, workflow_name: str, window: str,
                source: TriggerSource = TriggerSource.MANUAL) -> RunInstance:
        """触发一次运行；同窗口重复触发返回最新既有实例，不重复推进。"""
        with self._registry_lock:
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
                        "（state=%s, attempt=%d），去重不重复推进",
                        source.value, workflow_name, window,
                        existing.run_id, existing.state.value, existing.attempt)
            return existing

    def backfill(self, workflow_name: str, windows: list[str]) -> list[RunInstance]:
        """按时间窗补齐；与既有窗口重叠时复用已有实例，不产生重复执行。"""
        logger.info("[补齐] workflow=%s windows=%s", workflow_name, windows)
        return [self.trigger(workflow_name, w, TriggerSource.BACKFILL) for w in windows]

    # ---- 人工操作：作废 / 暂停 / 恢复 / 重跑 ----
    def cancel_run(self, run_id: str, reason: str | None = None) -> RunInstance:
        """作废一个未收尾的实例，立即生效。

        - 已完成的任务保持原样不回滚；未完成的任务统一置 CANCELLED；
        - 实例终态为 CANCELLED（failure_reason 以 cancelled: 开头），与业务
          失败/超时可区分；
        - 只在短临界区内落盘，不等待任何慢任务执行结束：即使同一实例有正在
          执行的慢步骤，作废也立即返回；慢步骤结束后的回写被条件守卫拒绝；
        - 对已作废实例重复作废是幂等的；对其他终态作废报错。
        """
        note = reason or "人工作废"
        with self._registry_lock:
            run = self._store.get_run(run_id)
            if run is None:
                raise RunNotFoundError(f"实例不存在: {run_id}")
            if run.state in TERMINAL_RUN_STATES:
                if run.state == RunState.CANCELLED:
                    logger.info("[作废] run=%s 已处于 CANCELLED，幂等返回", run_id)
                    return run
                raise RunControlError(
                    f"实例 {run_id} 已终态（{run.state.value}），不能作废")
            changed = self._store.mark_cancelled(
                run_id, self._now(), f"cancelled: {note}")
            fresh = self._store.get_run(run_id)
            assert fresh is not None
            if changed:
                logger.warning("[作废] run=%s 立即收尾为 CANCELLED（%s），"
                               "已完成步骤不回滚，未完成任务置 CANCELLED",
                               run_id, note)
            return fresh

    def pause_run(self, run_id: str) -> RunInstance:
        """暂停运行中实例：RUNNING -> PAUSED，立即生效且不等待慢步骤。

        任务进度、attempts 与 next_attempt_at（退避节奏）原样保留；暂停期间
        tick/recover 都不会推进它。重复暂停幂等；对终态实例暂停报错。
        """
        with self._registry_lock:
            run = self._store.get_run(run_id)
            if run is None:
                raise RunNotFoundError(f"实例不存在: {run_id}")
            if run.state in TERMINAL_RUN_STATES:
                raise RunControlError(
                    f"实例 {run_id} 已终态（{run.state.value}），不能暂停")
            if self._store.mark_paused(run_id):
                logger.warning("[暂停] run=%s RUNNING -> PAUSED，保留进度/重试节奏，"
                               "暂停期间不推进", run_id)
            else:
                logger.info("[暂停] run=%s 已处于 PAUSED，幂等返回", run_id)
            fresh = self._store.get_run(run_id)
            assert fresh is not None
            return fresh

    def resume_run(self, run_id: str) -> RunInstance:
        """恢复暂停实例：PAUSED -> RUNNING。

        仅显式恢复会改变暂停状态；进程重启不会自动恢复暂停实例。
        对运行中实例恢复幂等返回；对终态实例恢复报错。
        """
        with self._registry_lock:
            run = self._store.get_run(run_id)
            if run is None:
                raise RunNotFoundError(f"实例不存在: {run_id}")
            if run.state in TERMINAL_RUN_STATES:
                raise RunControlError(
                    f"实例 {run_id} 已终态（{run.state.value}），不能恢复")
            if self._store.mark_resumed(run_id):
                logger.info("[恢复] run=%s PAUSED -> RUNNING，从暂停处继续推进",
                            run_id)
            else:
                logger.info("[恢复] run=%s 本就 RUNNING，幂等返回", run_id)
            fresh = self._store.get_run(run_id)
            assert fresh is not None
            return fresh

    def rerun(self, workflow_name: str, window: str) -> RunInstance:
        """对已收尾窗口重新跑一次，返回新的尝试实例。

        - 仅窗口最新尝试为终态（SUCCESS/FAILED/TIMEOUT/CANCELLED）时才创建
          新尝试；历史尝试（含任务明细）原样保留，可通过 list_attempts 对比；
        - 窗口下已有活跃尝试（RUNNING/PAUSED）时，并发/同波重跑请求合并，
          统一返回该活跃尝试，保证同一窗口不会同时跑两遍。
        """
        with self._registry_lock:
            wf = self._workflows[workflow_name]
            latest = self._store.get_run_by_window(workflow_name, window)
            if latest is None:
                raise RunNotFoundError(
                    f"窗口不存在尝试: workflow={workflow_name} window={window}")
            active = self._store.get_active_run_by_window(workflow_name, window)
            if active is not None:
                logger.info("[重跑] workflow=%s window=%s 已有活跃尝试 %s"
                            "（state=%s, attempt=%d），重跑请求合并，不重复执行",
                            workflow_name, window, active.run_id,
                            active.state.value, active.attempt)
                return active
            if latest.state not in TERMINAL_RUN_STATES:
                # 理论不可达（非终态即活跃，已在上面返回），防御性报错
                raise RunControlError(
                    f"窗口 {workflow_name}:{window} 当前不可重跑"
                    f"（state={latest.state.value}）")
            attempt = latest.attempt + 1
            run_id = (f"{workflow_name}:{window}" if attempt == 1
                      else f"{workflow_name}:{window}#{attempt}")
            now = self._now()
            run = RunInstance(
                run_id=run_id, workflow_name=workflow_name, window=window,
                source=TriggerSource.RERUN, attempt=attempt, created_at=now,
                deadline=now + wf.run_timeout_seconds,
            )
            tasks = [
                TaskRun(
                    run_id=run_id, task_name=t.name,
                    state=TaskState.READY if not t.depends_on else TaskState.PENDING,
                )
                for t in wf.tasks
            ]
            inserted = self._store.insert_rerun_if_no_active(run, tasks)
            if inserted:
                logger.info("[重跑] workflow=%s window=%s 上一次尝试 %s"
                            "（state=%s）-> 新尝试 %s（attempt=%d）",
                            workflow_name, window, latest.run_id,
                            latest.state.value, run_id, attempt)
                return run
            # 竞态兜底：插入瞬间另一进程/线程创建了活跃尝试
            active = self._store.get_active_run_by_window(workflow_name, window)
            assert active is not None
            logger.info("[重跑] workflow=%s window=%s 竞态合并到活跃尝试 %s",
                        workflow_name, window, active.run_id)
            return active

    def list_attempts(self, workflow_name: str,
                      window: str) -> list[tuple[RunInstance, list[TaskRun]]]:
        """列出窗口下全部尝试（按序号升序），历史结果保留可对比。"""
        return [
            (r, self._store.list_tasks(r.run_id))
            for r in self._store.list_runs_by_window(workflow_name, window)
        ]

    # 按窗口定位最新尝试的人工操作入口（运维通常按窗口点操作）
    def cancel_window(self, workflow_name: str, window: str,
                      reason: str | None = None) -> RunInstance:
        run = self._require_latest(workflow_name, window)
        return self.cancel_run(run.run_id, reason)

    def pause_window(self, workflow_name: str, window: str) -> RunInstance:
        run = self._require_latest(workflow_name, window)
        return self.pause_run(run.run_id)

    def resume_window(self, workflow_name: str, window: str) -> RunInstance:
        run = self._require_latest(workflow_name, window)
        return self.resume_run(run.run_id)

    def _require_latest(self, workflow_name: str, window: str) -> RunInstance:
        run = self._store.get_run_by_window(workflow_name, window)
        if run is None:
            raise RunNotFoundError(
                f"窗口不存在尝试: workflow={workflow_name} window={window}")
        return run

    # ---- 推进 ----
    def tick(self) -> None:
        """推进一轮：执行就绪任务、传播失败、判定终态、标记超时。

        每个实例使用独立的实例锁：别的实例正卡在慢步骤上，不会阻塞本实例
        （也不会阻塞作废/暂停等人工操作落盘）。锁被占用时本轮跳过，等下一轮。
        """
        for run in self._store.list_running_runs():
            lock = self._run_lock(run.run_id)
            if not lock.acquire(blocking=False):
                logger.info("[推进] run=%s 正在执行中，本轮跳过", run.run_id)
                continue
            try:
                self._advance(run)
            finally:
                lock.release()

    def _refresh_run_or_skip(self, run: RunInstance) -> bool:
        """从库中重读实例状态；仅 RUNNING 继续推进。"""
        fresh = self._store.get_run(run.run_id)
        if fresh is None or fresh.state != RunState.RUNNING:
            return False
        run.state = fresh.state
        return True

    def _advance(self, run: RunInstance) -> None:
        if not self._refresh_run_or_skip(run):
            return
        wf = self._workflows[run.workflow_name]
        tasks = {t.task_name: t for t in self._store.list_tasks(run.run_id)}
        now = self._now()

        # 1. 超时判定：与业务失败区分（暂停/作废后不再判超时）
        if now > run.deadline:
            logger.info("[超时] run=%s 超过时限 %.1fs，标记 TIMEOUT",
                        run.run_id, wf.run_timeout_seconds)
            for t in tasks.values():
                if t.state not in TERMINAL_TASK_STATES:
                    try:
                        self._transition(t, TaskState.TIMEOUT, error_kind="timeout",
                                         error="实例超过时限")
                    except _ControlInterrupted:
                        return
            run.state = RunState.TIMEOUT
            run.finished_at = now
            run.failure_reason = "timeout: 实例超过时限"
            if not self._store.save_run_if_state(run, frozenset({RunState.RUNNING})):
                return
            return

        # 2. 晋升：上游已全部成功的 PENDING 任务转为 READY
        #    （重启恢复时上游成功发生在重启前，需要在此补一次晋升）
        self._promote_ready(wf, tasks, run)

        # 3. 执行就绪任务（退避时间未到则跳过）；任务间检查暂停/作废
        for t in tasks.values():
            if t.state != TaskState.READY:
                continue
            if t.next_attempt_at is not None and now < t.next_attempt_at:
                continue
            try:
                self._execute(wf, run, t, tasks)
            except _ControlInterrupted:
                logger.info("[推进] run=%s 检测到人工操作，立即停止推进", run.run_id)
                return
            if not self._refresh_run_or_skip(run):
                return

        # 4. 终态判定（守卫：暂停/作废后不覆盖人工状态）
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
            self._promote_ready(wf, tasks, run)

    def _promote_ready(self, wf: WorkflowDef,
                       tasks: dict[str, TaskRun],
                       run: RunInstance | None = None) -> None:
        """上游全部成功的下游任务由 PENDING 转为 READY。

        暂停/作废后不再晋升：暂停实例停在原地（下游保持 PENDING），
        作废的收尾由 mark_cancelled 统一完成。
        """
        if run is not None and not self._refresh_run_or_skip(run):
            return
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
        elif any(t.state == TaskState.FAILED for t in tasks.values()):
            failed = [t for t in tasks.values() if t.state == TaskState.FAILED]
            run.state = RunState.FAILED
            run.failure_reason = "; ".join(
                f"{t.task_name}: {t.error}" for t in failed)
            logger.info("[终态] run=%s 判定 FAILED，依据：%s",
                        run.run_id, run.failure_reason)
        else:
            # 没有 FAILED 却未全部成功：存在 TIMEOUT/CANCELLED/BLOCKED。
            # 正常暂停/作废路径不会走到这里（守卫拦截），这里做稳定兜底判定。
            run.state = RunState.FAILED
            run.failure_reason = "blocked"
            logger.info("[终态] run=%s 判定 FAILED（兜底），依据：存在非成功终态",
                        run.run_id)
        run.finished_at = now
        if not self._store.save_run_if_state(run, frozenset({RunState.RUNNING})):
            logger.info("[终态] run=%s 落盘被人工状态守卫拦截，保持库中状态",
                        run.run_id)

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
        # 条件守卫：作废可能已在任务执行期间把库中状态改为 CANCELLED，
        # 此时慢任务的回写必须被拒绝，并立即中断本实例推进。
        if self._store.save_task_if_state(task, frozenset({old})):
            logger.info("[状态] run=%s task=%s %s -> %s%s",
                        task.run_id, task.task_name, old.value, new_state.value,
                        f"（{error}）" if error else "")
            return
        db_task = next(
            (t for t in self._store.list_tasks(task.run_id)
             if t.task_name == task.task_name), None)
        if db_task is not None:
            logger.warning("[状态] run=%s task=%s 落盘被拒：库中状态已被人工操作"
                           "改变为 %s，放弃 %s 回写并停止推进",
                           task.run_id, task.task_name,
                           db_task.state.value, new_state.value)
            task.state = db_task.state
        raise _ControlInterrupted(task.task_name)

    # ---- 恢复 ----
    def recover(self) -> list[str]:
        """进程重启后恢复：继续推进 RUNNING 实例，已成功步骤不重复执行。

        - 崩溃前停留在 RUNNING 的任务无完成记录，RUNNING/PAUSED 实例中的这类
          任务都重置为 READY（at-least-once）；
        - PAUSED 实例只做上述重置，**不会被推进**——暂停是持久化状态，重启
          后必须等待显式 resume；
        - SUCCEEDED 在副作用完成后落盘，不会重复执行。
        """
        advanced: list[str] = []
        running = self._store.list_running_runs()
        paused = self._store.list_paused_runs()
        for run in running + paused:
            reset = self._store.reset_running_tasks(
                run.run_id, "进程重启，重新执行未完成任务")
            if reset:
                logger.info("[恢复] run=%s 重置 %d 个停滞 RUNNING 任务为 READY",
                            run.run_id, reset)
        for run in running:
            lock = self._run_lock(run.run_id)
            if not lock.acquire(blocking=False):
                continue
            try:
                logger.info("[恢复] source=%s run=%s 继续推进（state=%s）",
                            TriggerSource.RECOVERY.value, run.run_id,
                            run.state.value)
                self._advance(run)
                advanced.append(run.run_id)
            finally:
                lock.release()
        for run in paused:
            logger.info("[恢复] run=%s 处于 PAUSED，重启后保持暂停不自动推进",
                        run.run_id)
        return advanced

    # ---- 查询 ----
    def get_status(self, run_id: str) -> tuple[RunInstance, list[TaskRun]] | None:
        run = self._store.get_run(run_id)
        if run is None:
            return None
        return run, self._store.list_tasks(run_id)
