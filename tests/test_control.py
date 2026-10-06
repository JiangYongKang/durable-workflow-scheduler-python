"""人工运维操作单测：作废 / 暂停恢复 / 终态窗口重跑。

覆盖业务（每条用例开始时会在日志中再次打印）：
- 作废立即生效，不受同实例/其他实例慢步骤阻塞；
- 已完成步骤不回滚、未完成任务 CANCELLED、终态与 FAILED/TIMEOUT 可区分；
- 暂停期间不推进，进度/attempts/退避时间点保留，恢复后原地继续；
- 重启后 PAUSED 实例不被 recover 自动跑起来，需显式恢复；
- 重跑创建新尝试且历史尝试保留可查，活跃尝试存在时并发重跑合并为一次；
- 旧版全量唯一约束库可原地迁移。
"""
from __future__ import annotations

import sqlite3
import threading
import time

import pytest

from scheduler.engine import Engine
from scheduler.errors import (
    FatalTaskError,
    RetryableTaskError,
    RunControlError,
    RunNotFoundError,
)
from scheduler.models import (
    RetryPolicy,
    RunState,
    TaskDef,
    TaskRun,
    TaskState,
    TriggerSource,
    WorkflowDef,
)
from scheduler.store import StateStore


class FakeClock:
    def __init__(self) -> None:
        self.t = 1000.0

    def __call__(self) -> float:
        return self.t

    def advance(self, seconds: float) -> None:
        self.t += seconds


@pytest.fixture
def env():
    """返回 (engine, store, clock)。"""
    clock = FakeClock()
    store = StateStore()
    eng = Engine(store, now=clock)
    return eng, store, clock


DIAMOND = WorkflowDef("diamond", (
    TaskDef("a"),
    TaskDef("b", ("a",)),
    TaskDef("c", ("a",)),
    TaskDef("d", ("b", "c")),
))


def _states(eng, run_id) -> dict[str, TaskState]:
    return {t.task_name: t.state for t in eng.get_status(run_id)[1]}


# ---------------- 作废 ----------------

def test_cancel_mid_flight_takes_effect_immediately_and_guards_rollback(env):
    """作废：慢步骤执行期间调用也立即返回；已成功步骤不回滚，慢步骤完成后的
    回写被守卫拒绝，实例终态稳定为 CANCELLED。"""
    eng, _, _ = env
    eng.register_workflow(DIAMOND)
    eng.register_handler("a", lambda: None)
    eng.register_handler("c", lambda: None)
    eng.register_handler("d", lambda: None)
    started = threading.Event()
    gate = threading.Event()

    def slow_b():
        started.set()
        gate.wait(5.0)  # 模拟很慢的步骤

    eng.register_handler("b", slow_b)
    run = eng.trigger("diamond", "w")

    t = threading.Thread(target=eng.tick)
    t.start()
    assert started.wait(2.0)

    start = time.perf_counter()
    cancelled = eng.cancel_run(run.run_id, reason="人工介入")
    elapsed = time.perf_counter() - start
    assert elapsed < 1.0  # 不等慢步骤结束即生效

    assert cancelled.state == RunState.CANCELLED
    assert cancelled.failure_reason == "cancelled: 人工介入"
    states = _states(eng, run.run_id)
    assert states == {"a": TaskState.SUCCEEDED, "b": TaskState.CANCELLED,
                      "c": TaskState.CANCELLED, "d": TaskState.CANCELLED}

    gate.set()
    t.join(2.0)
    # 慢步骤返回后不得覆盖作废结果，后续 tick 也不再推进
    eng.tick()
    run1, tasks = eng.get_status(run.run_id)
    assert run1.state == RunState.CANCELLED
    assert {x.task_name: x.state for x in tasks} == states


def test_cancel_keeps_finished_steps_attempts_and_backoff(env):
    """作废：已成功步骤原样保留；退避中的任务 attempts/next_attempt_at 保留，
    仅状态收尾为 CANCELLED，不回滚。"""
    eng, _, clock = env
    wf = WorkflowDef("ab", (
        TaskDef("a"),
        TaskDef("b", retry_policy=RetryPolicy(max_attempts=3, backoff_seconds=10)),
    ))
    eng.register_workflow(wf)
    eng.register_handler("a", lambda: None)
    eng.register_handler("b", lambda: (_ for _ in ()).throw(
        RetryableTaskError("transient")))
    run = eng.trigger("ab", "w")
    eng.tick()  # a 成功；b 第 1 次失败，10s 后才到重试点
    _, tasks0 = eng.get_status(run.run_id)
    b0 = next(x for x in tasks0 if x.task_name == "b")
    assert b0.state == TaskState.READY and b0.attempts == 1
    backoff_at = b0.next_attempt_at
    assert backoff_at == pytest.approx(clock.t + 10)

    eng.cancel_run(run.run_id)
    run1, tasks1 = eng.get_status(run.run_id)
    assert run1.state == RunState.CANCELLED
    a1 = next(x for x in tasks1 if x.task_name == "a")
    b1 = next(x for x in tasks1 if x.task_name == "b")
    assert a1.state == TaskState.SUCCEEDED          # 已完成不回滚
    assert b1.state == TaskState.CANCELLED
    assert b1.attempts == 1                          # 重试次数保留
    assert b1.next_attempt_at == pytest.approx(backoff_at)  # 节奏信息保留
    assert b1.error_kind == "cancelled"


def test_cancel_state_distinguishable_from_failure_and_timeout(env):
    """作废：CANCELLED 与 FAILED/TIMEOUT/SUCCEEDED 并列、一眼可区分，且为终态。"""
    from scheduler.models import TERMINAL_RUN_STATES

    eng, _, clock = env
    # 窗口 1：作废 -> CANCELLED
    eng.register_workflow(WorkflowDef("solo", (TaskDef("x"),)))
    eng.register_handler("x", lambda: None)
    run = eng.trigger("solo", "wc")
    out = eng.cancel_run(run.run_id)
    assert out.state == RunState.CANCELLED
    assert out.state in TERMINAL_RUN_STATES
    assert out.failure_reason.startswith("cancelled:")

    # 窗口 2：致命业务失败 -> FAILED，与作废并列可区分
    eng.register_workflow(WorkflowDef("boom", (TaskDef("z"),)))
    eng.register_handler("z", lambda: (_ for _ in ()).throw(
        FatalTaskError("biz boom")))
    r3 = eng.trigger("boom", "wf")
    eng.tick()
    assert eng.get_status(r3.run_id)[0].state == RunState.FAILED

    # 窗口 3：超时 -> TIMEOUT
    eng.register_workflow(WorkflowDef(
        "slow", (TaskDef("s", retry_policy=RetryPolicy(
            max_attempts=5, backoff_seconds=100)),),
        run_timeout_seconds=10))
    eng.register_handler("s", lambda: (_ for _ in ()).throw(
        RetryableTaskError("retry later")))
    r4 = eng.trigger("slow", "wt")
    eng.tick()
    clock.advance(11)
    eng.tick()
    assert eng.get_status(r4.run_id)[0].state == RunState.TIMEOUT
    assert eng.get_status(run.run_id)[0].state == RunState.CANCELLED


def test_cancel_idempotent_and_rejects_other_terminal(env):
    """作废：重复作废幂等；对其他终态（如成功）作废报错，不改变终态。"""
    eng, _, _ = env
    eng.register_workflow(DIAMOND)
    for n in "abcd":
        eng.register_handler(n, lambda: None)
    run = eng.trigger("diamond", "done")
    eng.tick()
    assert eng.get_status(run.run_id)[0].state == RunState.SUCCEEDED
    with pytest.raises(RunControlError):
        eng.cancel_run(run.run_id)

    run2 = eng.trigger("diamond", "cancelme")
    first = eng.cancel_run(run2.run_id)
    second = eng.cancel_run(run2.run_id)
    assert first.state == second.state == RunState.CANCELLED
    assert eng.get_status(run2.run_id)[0].state == RunState.CANCELLED


def test_cancel_not_blocked_by_slow_step_in_another_run(env):
    """作废：别的流程正卡在很慢的步骤上时，作废另一个实例仍然立即生效。"""
    eng, _, _ = env
    eng.register_workflow(WorkflowDef("slow", (TaskDef("s"),)))
    gate = threading.Event()
    started = threading.Event()
    eng.register_handler("s", lambda: (started.set(), gate.wait(5.0)))
    eng.register_workflow(WorkflowDef("fast", (TaskDef("x"),)))
    eng.register_handler("x", lambda: None)

    slow_run = eng.trigger("slow", "W1")
    tick_thread = threading.Thread(target=eng.tick)
    tick_thread.start()
    assert started.wait(2.0)  # slow.s 在执行中，持有 W1 的实例锁

    victim = eng.trigger("fast", "W2")
    start = time.perf_counter()
    out = eng.cancel_run(victim.run_id)
    assert time.perf_counter() - start < 1.0
    assert out.state == RunState.CANCELLED

    # 作废慢流程自身同样立即生效
    out1 = eng.cancel_run(slow_run.run_id)
    assert out1.state == RunState.CANCELLED
    gate.set()
    tick_thread.join(2.0)
    assert eng.get_status(slow_run.run_id)[0].state == RunState.CANCELLED
    assert eng.get_status(victim.run_id)[0].state == RunState.CANCELLED


# ---------------- 暂停 / 恢复 ----------------

def test_pause_stops_progress_resume_continues_with_progress_kept(env):
    """暂停恢复：暂停期间不推进，attempts 与退避时间点保留；恢复后从暂停处继续。"""
    eng, _, clock = env
    wf = WorkflowDef("ab", (
        TaskDef("a"),
        TaskDef("b", retry_policy=RetryPolicy(max_attempts=3, backoff_seconds=10)),
    ))
    eng.register_workflow(wf)
    eng.register_handler("a", lambda: None)
    flaky = {"n": 0}

    def flaky_b():
        flaky["n"] += 1
        if flaky["n"] < 2:
            raise RetryableTaskError("transient")

    eng.register_handler("b", flaky_b)
    run = eng.trigger("ab", "w")
    eng.tick()  # a 成功，b 第 1 次失败进入退避
    _, tasks0 = eng.get_status(run.run_id)
    b0 = next(x for x in tasks0 if x.task_name == "b")
    assert b0.attempts == 1
    backoff_at = b0.next_attempt_at

    paused = eng.pause_run(run.run_id)
    assert paused.state == RunState.PAUSED
    clock.advance(100)  # 即使越过退避点
    eng.tick()
    eng.tick()
    run1, tasks1 = eng.get_status(run.run_id)
    b1 = next(x for x in tasks1 if x.task_name == "b")
    assert run1.state == RunState.PAUSED
    assert b1.state == TaskState.READY and b1.attempts == 1
    assert b1.next_attempt_at == pytest.approx(backoff_at)  # 节奏原样保留

    resumed = eng.resume_run(run.run_id)
    assert resumed.state == RunState.RUNNING
    eng.tick()  # 恢复后第 2 次尝试成功，从暂停处继续
    run2, tasks2 = eng.get_status(run.run_id)
    assert run2.state == RunState.SUCCEEDED
    b2 = next(x for x in tasks2 if x.task_name == "b")
    assert b2.state == TaskState.SUCCEEDED and b2.attempts == 2  # 不重头计数
    assert flaky["n"] == 2


def test_pause_mid_flight_then_resume_continues_dag(env):
    """暂停恢复：暂停发生在慢步骤执行期间时，慢步骤完成后不再启动下游任务；
    恢复后沿 DAG 继续，已执行步骤不重复。与慢步骤无依赖、暂停前已就绪的
    兄弟任务会在同一轮正常执行（保持既有 DAG 并发语义）。"""
    eng, _, _ = env
    eng.register_workflow(DIAMOND)
    effects = []
    eng.register_handler("a", lambda: effects.append("a"))
    eng.register_handler("c", lambda: effects.append("c"))
    eng.register_handler("d", lambda: effects.append("d"))
    started = threading.Event()
    gate = threading.Event()

    def slow_b():
        effects.append("b:start")
        started.set()
        gate.wait(5.0)

    eng.register_handler("b", slow_b)
    run = eng.trigger("diamond", "w")
    t = threading.Thread(target=eng.tick)
    t.start()
    assert started.wait(2.0)
    assert eng.pause_run(run.run_id).state == RunState.PAUSED
    gate.set()
    t.join(2.0)

    run1, tasks1 = eng.get_status(run.run_id)
    assert run1.state == RunState.PAUSED
    st = {x.task_name: x.state for x in tasks1}
    # a 已成功；b 慢步骤跑完正常落盘成功；同轮循环中排在 b 之后的 c 未被
    # 启动，停在 READY；下游 d 保持 PENDING——暂停后原地停车
    assert st == {"a": TaskState.SUCCEEDED, "b": TaskState.SUCCEEDED,
                  "c": TaskState.READY, "d": TaskState.PENDING}

    eng.resume_run(run.run_id)
    eng.tick()
    run2, tasks2 = eng.get_status(run.run_id)
    assert run2.state == RunState.SUCCEEDED
    assert all(x.state == TaskState.SUCCEEDED for x in tasks2)
    assert effects.count("a") == 1 and effects.count("b:start") == 1
    assert effects.count("c") == 1 and effects.count("d") == 1


def test_paused_run_not_auto_resumed_by_recover_after_restart(tmp_path):
    """重启恢复：PAUSED 实例不被 recover 顺手跑起来；停滞在 RUNNING 的任务只做
    at-least-once 重置但不执行；显式恢复后才继续。"""
    clock = FakeClock()
    db = str(tmp_path / "state.db")

    # 进程 A：创建实例，并制造「暂停时任务 a 停留在 RUNNING 后进程被杀」的现场
    store = StateStore(db)
    eng = Engine(store, now=clock)
    eng.register_workflow(DIAMOND)
    run = eng.trigger("diamond", "W")
    eng.pause_run(run.run_id)
    t_a = next(x for x in eng.get_status(run.run_id)[1] if x.task_name == "a")
    t_a.state = TaskState.RUNNING
    t_a.started_at = clock()
    store.save_task(t_a)
    store.close()

    # 进程 B：重启后 recover
    effects = []
    store2 = StateStore(db)
    eng2 = Engine(store2, now=clock)
    eng2.register_workflow(DIAMOND)
    eng2.register_handler("a", lambda: effects.append("a"))
    advanced = eng2.recover()
    assert advanced == []  # 暂停实例不被推进
    run2, tasks2 = eng2.get_status(run.run_id)
    assert run2.state == RunState.PAUSED
    a2 = next(x for x in tasks2 if x.task_name == "a")
    assert a2.state == TaskState.READY  # stale RUNNING 仅重置，不执行
    assert effects == []

    # 显式恢复后正常推进
    for n in "bcd":
        eng2.register_handler(n, (lambda n=n: effects.append(n)))
    eng2.resume_run(run.run_id)
    eng2.tick()
    run3, tasks3 = eng2.get_status(run.run_id)
    assert run3.state == RunState.SUCCEEDED
    assert all(x.state == TaskState.SUCCEEDED for x in tasks3)
    assert effects.count("a") == 1
    store2.close()


def test_cancel_paused_run_allowed(env):
    """作废：暂停中的实例可以直接作废，作废后终态为 CANCELLED。"""
    eng, _, _ = env
    eng.register_workflow(WorkflowDef("solo", (TaskDef("x"),)))
    eng.register_handler("x", lambda: None)
    run = eng.trigger("solo", "w")
    assert eng.pause_run(run.run_id).state == RunState.PAUSED
    out = eng.cancel_run(run.run_id, reason="不要了")
    assert out.state == RunState.CANCELLED
    assert _states(eng, run.run_id) == {"x": TaskState.CANCELLED}


def test_pause_and_resume_reject_terminal_runs(env):
    """暂停/恢复：对终态实例操作报 RunControlError，终态不被改变。"""
    eng, _, _ = env
    eng.register_workflow(DIAMOND)
    for n in "abcd":
        eng.register_handler(n, lambda: None)
    run = eng.trigger("diamond", "w")
    eng.tick()
    assert eng.get_status(run.run_id)[0].state == RunState.SUCCEEDED
    with pytest.raises(RunControlError):
        eng.pause_run(run.run_id)
    with pytest.raises(RunControlError):
        eng.resume_run(run.run_id)


def test_pause_resume_idempotent(env):
    """暂停/恢复：重复暂停、重复运行中恢复均幂等。"""
    eng, _, _ = env
    eng.register_workflow(WorkflowDef("solo", (TaskDef("x"),)))
    eng.register_handler("x", lambda: None)
    run = eng.trigger("solo", "w")
    assert eng.pause_run(run.run_id).state == RunState.PAUSED
    assert eng.pause_run(run.run_id).state == RunState.PAUSED
    assert eng.resume_run(run.run_id).state == RunState.RUNNING
    assert eng.resume_run(run.run_id).state == RunState.RUNNING


# ---------------- 重跑 ----------------

def test_rerun_creates_new_attempt_and_keeps_history(env):
    """重跑：失败窗口重跑产生第 2 次尝试；第 1 次结果仍可查、不被覆盖，
    且两次任务明细独立，可对比差异。"""
    eng, _, _ = env
    eng.register_workflow(WorkflowDef("solo", (TaskDef("f"),)))
    eng.register_handler("f", lambda: (_ for _ in ()).throw(
        FatalTaskError("biz bug")))
    run1 = eng.trigger("solo", "w")
    eng.tick()
    assert eng.get_status(run1.run_id)[0].state == RunState.FAILED

    # 修复业务后重跑
    eng.register_handler("f", lambda: None)
    run2 = eng.rerun("solo", "w")
    assert run2.run_id == "solo:w#2" and run2.attempt == 2
    assert run2.source == TriggerSource.RERUN
    eng.tick()
    assert eng.get_status(run2.run_id)[0].state == RunState.SUCCEEDED

    # 历史尝试保留
    old_run, old_tasks = eng.get_status("solo:w")
    assert old_run.state == RunState.FAILED and old_run.attempt == 1
    assert old_tasks[0].state == TaskState.FAILED and "biz bug" in old_tasks[0].error
    assert eng._store.get_run_by_window("solo", "w").run_id == "solo:w#2"

    attempts = eng.list_attempts("solo", "w")
    assert [r.attempt for r, _ in attempts] == [1, 2]
    assert [r.state for r, _ in attempts] == [RunState.FAILED, RunState.SUCCEEDED]


def test_rerun_succeeded_window_also_supported(env):
    """重跑：成功窗口同样可以重跑，新尝试独立计数。"""
    eng, _, _ = env
    eng.register_workflow(WorkflowDef("solo", (TaskDef("x"),)))
    eng.register_handler("x", lambda: None)
    eng.trigger("solo", "w")
    eng.tick()
    run2 = eng.rerun("solo", "w")
    assert run2.attempt == 2 and run2.state == RunState.RUNNING
    eng.tick()
    assert eng.get_status(run2.run_id)[0].state == RunState.SUCCEEDED
    assert len(eng.list_attempts("solo", "w")) == 2


def test_rerun_coalesced_while_active_attempt_running(env):
    """重跑并发：窗口已有活跃尝试时，8 个并发重跑请求合并为同一个实例，
    绝不跑两遍；作废收尾后再并发重跑，仍然只产生一个新尝试。"""
    eng, _, _ = env
    eng.register_workflow(WorkflowDef(
        "slow",
        (TaskDef("f", retry_policy=RetryPolicy(max_attempts=5, backoff_seconds=100)),),
    ))
    eng.register_handler("f", lambda: (_ for _ in ()).throw(
        RetryableTaskError("retrying")))
    run1 = eng.trigger("slow", "w")
    eng.tick()  # 第 1 次失败进入长退避，实例活跃
    results: list = []

    def fire():
        results.append(eng.rerun("slow", "w"))

    threads = [threading.Thread(target=fire) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert {r.run_id for r in results} == {run1.run_id}
    assert len(eng.list_attempts("slow", "w")) == 1

    eng.cancel_run(run1.run_id)
    results2: list = []

    def fire2():
        results2.append(eng.rerun("slow", "w"))

    threads = [threading.Thread(target=fire2) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert {r.run_id for r in results2} == {"slow:w#2"}
    attempts = eng.list_attempts("slow", "w")
    assert [r.attempt for r, _ in attempts] == [1, 2]
    assert attempts[0][0].state == RunState.CANCELLED
    assert attempts[1][0].state == RunState.RUNNING


def test_rerun_paused_attempt_blocks_new_attempt(env):
    """重跑并发：暂停中的尝试仍占用窗口，重跑请求合并到该暂停尝试而非新开。"""
    eng, _, _ = env
    eng.register_workflow(WorkflowDef("solo", (TaskDef("x"),)))
    eng.register_handler("x", lambda: None)
    run1 = eng.trigger("solo", "w")
    eng.pause_run(run1.run_id)
    merged = eng.rerun("solo", "w")
    assert merged.run_id == run1.run_id and merged.state == RunState.PAUSED
    assert len(eng.list_attempts("solo", "w")) == 1


@pytest.mark.parametrize("terminal", ["SUCCEEDED", "FAILED", "TIMEOUT", "CANCELLED"])
def test_rerun_supported_for_every_terminal_state(env, terminal):
    """重跑：成功/失败/超时/被作废四类收尾窗口都允许重跑。"""
    eng, _, clock = env
    if terminal == "TIMEOUT":
        # 长退避、大重试上限：实例先被整体超时标记，而非业务重试耗尽
        eng.register_workflow(WorkflowDef(
            "solo",
            (TaskDef("x", retry_policy=RetryPolicy(
                max_attempts=5, backoff_seconds=100)),),
            run_timeout_seconds=10,
        ))
        eng.register_handler("x", lambda: (_ for _ in ()).throw(
            RetryableTaskError("late")))
        run = eng.trigger("solo", terminal)
        eng.tick()
        clock.advance(11)
        eng.tick()
    else:
        eng.register_workflow(WorkflowDef("solo", (TaskDef("x"),)))
        run = eng.trigger("solo", terminal)
        if terminal == "SUCCEEDED":
            eng.register_handler("x", lambda: None)
            eng.tick()
        elif terminal == "FAILED":
            eng.register_handler("x", lambda: (_ for _ in ()).throw(
                FatalTaskError("boom")))
            eng.tick()
        elif terminal == "CANCELLED":
            eng.register_handler("x", lambda: None)
            eng.cancel_run(run.run_id)
    assert eng.get_status(run.run_id)[0].state == RunState(terminal)

    run2 = eng.rerun("solo", terminal)
    assert run2.attempt == 2 and run2.source == TriggerSource.RERUN
    assert run2.state == RunState.RUNNING
    assert len(eng.list_attempts("solo", terminal)) == 2


def test_rerun_unknown_window_raises(env):
    """重跑：窗口从未有过尝试时报 RunNotFoundError。"""
    eng, _, _ = env
    eng.register_workflow(WorkflowDef("solo", (TaskDef("x"),)))
    with pytest.raises(RunNotFoundError):
        eng.rerun("solo", "never")


def test_trigger_after_rerun_still_dedups_to_latest_attempt(env):
    """兼容：重跑后再次 trigger 同一窗口仍去重，返回最新既有尝试不重复执行。"""
    eng, _, _ = env
    eng.register_workflow(WorkflowDef("solo", (TaskDef("x"),)))
    eng.register_handler("x", lambda: None)
    run1 = eng.trigger("solo", "w")
    eng.tick()
    run2 = eng.rerun("solo", "w")
    eng.tick()
    assert run1.attempt == 1
    again = eng.trigger("solo", "w")
    assert again.run_id == run2.run_id and again.attempt == 2
    assert len(eng.list_attempts("solo", "w")) == 2


# ---------------- 存储迁移 ----------------

def test_old_schema_db_migrates_and_rerun_allowed(tmp_path):
    """存储：旧版全量 UNIQUE(workflow,window) 库可原地迁移，历史记录标记为
    第 1 次尝试，迁移后允许重跑插入第 2 次尝试。"""
    db = str(tmp_path / "old.db")
    raw = sqlite3.connect(db)
    raw.execute(
        "CREATE TABLE runs (run_id TEXT PRIMARY KEY, workflow_name TEXT NOT NULL,"
        " window TEXT NOT NULL, source TEXT NOT NULL, state TEXT NOT NULL,"
        " created_at REAL NOT NULL, deadline REAL NOT NULL, finished_at REAL,"
        " failure_reason TEXT, UNIQUE (workflow_name, window))")
    raw.execute(
        "CREATE TABLE task_runs (run_id TEXT NOT NULL, task_name TEXT NOT NULL,"
        " state TEXT NOT NULL, attempts INTEGER NOT NULL, next_attempt_at REAL,"
        " error TEXT, error_kind TEXT, started_at REAL, finished_at REAL,"
        " PRIMARY KEY (run_id, task_name))")
    raw.execute("INSERT INTO runs VALUES (?,?,?,?,?,?,?,?,?)",
                ("solo:old", "solo", "old", "MANUAL", "FAILED", 1.0, 11.0,
                 2.0, "fatal: boom"))
    raw.execute("INSERT INTO task_runs VALUES (?,?,?,?,?,?,?,?,?)",
                ("solo:old", "x", "FAILED", 1, None, "boom", "fatal", 1.0, 2.0))
    raw.commit()
    raw.close()

    store = StateStore(db)
    run = store.get_run("solo:old")
    assert run is not None and run.attempt == 1 and run.state == RunState.FAILED
    # 迁移后全量唯一约束不再阻止第 2 次尝试
    new_run = type(run)(
        run_id="solo:old#2", workflow_name="solo", window="old",
        source=TriggerSource.RERUN, state=RunState.RUNNING, attempt=2,
        created_at=3.0, deadline=103.0)
    assert store.insert_rerun_if_no_active(new_run, [
        TaskRun(run_id="solo:old#2", task_name="x", state=TaskState.READY)]) is True
    attempts = store.list_runs_by_window("solo", "old")
    assert [r.attempt for r in attempts] == [1, 2]
    store.close()
