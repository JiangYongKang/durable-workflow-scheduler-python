"""运维操作单测：作废 / 暂停恢复 / 重跑。

日志中会打印操作类型、触发来源、状态流转与终态判定依据，
从日志可看出本轮覆盖了哪些业务路径。
"""
from __future__ import annotations

import logging
import threading

import pytest

from scheduler.engine import Engine
from scheduler.errors import (
    FatalTaskError,
    InvalidTransitionError,
    RetryableTaskError,
    UnknownRunError,
)
from scheduler.models import (
    RunState,
    RetryPolicy,
    TaskDef,
    TaskState,
    TriggerSource,
    WorkflowDef,
)
from scheduler.store import StateStore

logging.basicConfig(level=logging.INFO,
                    format="TEST %(asctime)s %(message)s", force=True)


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


CHAIN = WorkflowDef("chain", (
    TaskDef("a"),
    TaskDef("b", ("a",)),
    TaskDef("c", ("b",)),
))


def _states(eng, run_id):
    return {t.task_name: t.state for t in eng.get_status(run_id)[1]}


# ---------- 作废 ----------

def test_cancel_running_instance_stops_and_is_distinguishable(env):
    """作废：未完成任务置 CANCELLED，已成功步骤保留，状态与失败/超时区分。"""
    eng, _, _ = env
    eng.register_workflow(CHAIN)
    eng.register_handler("a", lambda: None)
    eng.register_handler("b", lambda: (_ for _ in ()).throw(
        RetryableTaskError("boom")))
    eng.register_handler("c", lambda: None)

    run = eng.trigger("chain", "w")
    eng.tick()  # a 成功，b 失败进入退避
    assert _states(eng, run.run_id)["a"] == TaskState.SUCCEEDED

    cancelled = eng.cancel(run.run_id, reason="人工介入")
    assert cancelled.state == RunState.CANCELLED
    run0, tasks = eng.get_status(run.run_id)
    assert run0.state == RunState.CANCELLED
    assert run0.failure_reason.startswith("cancelled:")
    states = {t.task_name: t.state for t in tasks}
    # 已完成的 a 保持 SUCCEEDED 不回滚；未完成的 b/c 置 CANCELLED
    assert states["a"] == TaskState.SUCCEEDED
    assert states["b"] == TaskState.CANCELLED
    assert states["c"] == TaskState.CANCELLED
    assert next(t for t in tasks if t.task_name == "b").error_kind == "cancelled"

    # 作废后 tick 不再推进
    eng.tick()
    assert eng.get_status(run.run_id)[0].state == RunState.CANCELLED
    # 与业务失败/超时明确区分
    assert run0.state not in (RunState.FAILED, RunState.TIMEOUT)


def test_cancel_takes_effect_immediately_while_slow_step_blocks(env):
    """作废立即生效：另一个流程卡在慢任务上时，作废不等引擎锁。"""
    eng, _, _ = env
    eng.register_workflow(CHAIN)
    gate = threading.Event()
    started = threading.Event()

    def slow():
        started.set()
        gate.wait(5)

    eng.register_handler("a", slow)
    for n in "bc":
        eng.register_handler(n, lambda: None)
    slow_run = eng.trigger("chain", "slow")

    other = WorkflowDef("other", (TaskDef("x"), TaskDef("y", ("x",))))
    eng.register_workflow(other)
    eng.register_handler("x", lambda: None)
    eng.register_handler("y", lambda: None)
    victim = eng.trigger("other", "victim")

    # 后台线程推进：卡在 slow_run 的 a 上
    ticker = threading.Thread(target=eng.tick)
    ticker.start()
    assert started.wait(2)

    # 引擎主锁被慢任务占用，但作废走存储层，立即生效
    eng.cancel(victim.run_id)
    run0, tasks = eng.get_status(victim.run_id)
    assert run0.state == RunState.CANCELLED
    assert all(t.state == TaskState.CANCELLED for t in tasks)

    # 卡住的实例本身也可被作废；慢任务结束后结果不回写
    eng.cancel(slow_run.run_id)
    gate.set()
    ticker.join(5)
    assert not ticker.is_alive()
    run1, tasks1 = eng.get_status(slow_run.run_id)
    assert run1.state == RunState.CANCELLED
    assert _states(eng, slow_run.run_id)["a"] == TaskState.CANCELLED
    # 作废后不再推进
    eng.tick()
    assert eng.get_status(slow_run.run_id)[0].state == RunState.CANCELLED


def test_cancel_terminal_or_unknown_run_rejected(env):
    eng, _, _ = env
    eng.register_workflow(CHAIN)
    for n in "abc":
        eng.register_handler(n, lambda: None)
    run = eng.trigger("chain", "w")
    eng.tick()
    assert eng.get_status(run.run_id)[0].state == RunState.SUCCEEDED
    with pytest.raises(InvalidTransitionError):
        eng.cancel(run.run_id)
    with pytest.raises(UnknownRunError):
        eng.cancel("chain:ghost")


# ---------- 暂停 / 恢复 ----------

def test_pause_freezes_progress_and_resume_continues_with_retry_rhythm(env):
    """暂停期间不推进；重试次数与退避节奏保留；恢复后接着走。"""
    eng, _, clock = env
    wf = WorkflowDef("flaky", (
        TaskDef("f", retry_policy=RetryPolicy(max_attempts=3, backoff_seconds=10)),
    ))
    eng.register_workflow(wf)
    calls = {"n": 0}

    def handler():
        calls["n"] += 1
        if calls["n"] < 2:
            raise RetryableTaskError("transient")

    eng.register_handler("f", handler)
    run = eng.trigger("flaky", "w")
    eng.tick()  # 第 1 次失败，10s 退避
    task = eng.get_status(run.run_id)[1][0]
    assert task.attempts == 1 and task.state == TaskState.READY
    backoff_at = task.next_attempt_at

    eng.pause(run.run_id)
    assert eng.get_status(run.run_id)[0].state == RunState.PAUSED
    clock.advance(30)  # 退避早已到期
    eng.tick()         # 暂停中不推进
    task = eng.get_status(run.run_id)[1][0]
    assert task.attempts == 1 and calls["n"] == 1
    assert task.next_attempt_at == backoff_at  # 重试节奏原样保留

    eng.resume(run.run_id)
    assert eng.get_status(run.run_id)[0].state == RunState.RUNNING
    eng.tick()  # 恢复后从暂停处接着走：第 2 次尝试成功
    task = eng.get_status(run.run_id)[1][0]
    assert task.attempts == 2 and task.state == TaskState.SUCCEEDED
    assert eng.get_status(run.run_id)[0].state == RunState.SUCCEEDED


def test_paused_instance_not_resumed_by_recovery_after_restart(env, tmp_path):
    """进程重启后，recover 不得把暂停中的实例顺手跑起来。"""
    clock = FakeClock()
    db = str(tmp_path / "state.db")
    effects = []
    store = StateStore(db)
    eng = Engine(store, now=clock)
    eng.register_workflow(CHAIN)
    for n in "abc":
        eng.register_handler(n, (lambda n=n: effects.append(n)))
    run = eng.trigger("chain", "w")
    eng.pause(run.run_id)
    store.close()

    # 模拟进程重启：同库恢复
    store2 = StateStore(db)
    eng2 = Engine(store2, now=clock)
    eng2.register_workflow(CHAIN)
    for n in "abc":
        eng2.register_handler(n, (lambda n=n: effects.append(n)))
    recovered = eng2.recover()
    assert recovered == []  # 暂停实例不被恢复推进
    run0 = eng2.get_status(run.run_id)[0]
    assert run0.state == RunState.PAUSED
    assert effects == []  # 没有任何任务被执行

    # 显式恢复后才继续推进
    eng2.resume(run.run_id)
    eng2.tick()
    assert eng2.get_status(run.run_id)[0].state == RunState.SUCCEEDED
    assert sorted(effects) == ["a", "b", "c"]
    store2.close()


def test_pause_resume_invalid_transitions_rejected(env):
    eng, _, _ = env
    eng.register_workflow(CHAIN)
    for n in "abc":
        eng.register_handler(n, lambda: None)
    run = eng.trigger("chain", "w")
    with pytest.raises(InvalidTransitionError):
        eng.resume(run.run_id)  # 未暂停不能恢复
    eng.pause(run.run_id)
    with pytest.raises(InvalidTransitionError):
        eng.pause(run.run_id)  # 重复暂停
    eng.tick()  # 暂停中不推进
    eng.resume(run.run_id)
    eng.tick()
    assert eng.get_status(run.run_id)[0].state == RunState.SUCCEEDED
    with pytest.raises(InvalidTransitionError):
        eng.pause(run.run_id)  # 终态不能暂停
    with pytest.raises(UnknownRunError):
        eng.pause("chain:ghost")


def test_cancel_paused_instance(env):
    """暂停中的实例也可被作废。"""
    eng, _, _ = env
    eng.register_workflow(CHAIN)
    for n in "abc":
        eng.register_handler(n, lambda: None)
    run = eng.trigger("chain", "w")
    eng.pause(run.run_id)
    eng.cancel(run.run_id)
    assert eng.get_status(run.run_id)[0].state == RunState.CANCELLED


# ---------- 重跑 ----------

def test_rerun_terminal_window_creates_new_attempt_and_keeps_history(env):
    """重跑产生新尝试；上一次结果保留可查，便于对比。"""
    eng, store, _ = env
    eng.register_workflow(CHAIN)
    fail = {"on": True}

    def maybe_fail():
        if fail["on"]:
            raise FatalTaskError("biz broken")

    eng.register_handler("a", maybe_fail)
    for n in "bc":
        eng.register_handler(n, lambda: None)

    first = eng.trigger("chain", "2026-10-01")
    eng.tick()
    assert eng.get_status(first.run_id)[0].state == RunState.FAILED

    # 修复后重跑：新尝试成功
    fail["on"] = False
    second = eng.rerun("chain", "2026-10-01")
    assert second.attempt == 2
    assert second.run_id == "chain:2026-10-01#2"
    assert second.source == TriggerSource.RERUN
    eng.tick()
    assert eng.get_status(second.run_id)[0].state == RunState.SUCCEEDED

    # 历史结果保留：第一次失败记录仍可查
    old, old_tasks = eng.get_status(first.run_id)
    assert old.state == RunState.FAILED
    assert "biz broken" in old.failure_reason
    assert _states(eng, first.run_id)["a"] == TaskState.FAILED

    attempts = store.list_attempts("chain", "2026-10-01")
    assert [r.attempt for r in attempts] == [1, 2]
    assert [r.state for r in attempts] == [RunState.FAILED, RunState.SUCCEEDED]


def test_rerun_while_running_dedups_and_trigger_still_dedups_latest(env):
    """窗口已有进行中的尝试时，重跑去重；普通触发仍按窗口去重到最新尝试。"""
    eng, store, _ = env
    wf = WorkflowDef("slow", (
        TaskDef("s", retry_policy=RetryPolicy(max_attempts=3, backoff_seconds=100)),
    ))
    eng.register_workflow(wf)
    eng.register_handler("s", lambda: (_ for _ in ()).throw(
        RetryableTaskError("later")))
    first = eng.trigger("slow", "w")
    eng.tick()
    assert eng.get_status(first.run_id)[0].state == RunState.RUNNING

    # 进行中重跑：不创建新尝试
    again = eng.rerun("slow", "w")
    assert again.run_id == first.run_id and again.attempt == 1
    assert len(store.list_attempts("slow", "w")) == 1

    # 作废后重跑出新尝试；普通触发去重到最新尝试，不再新建
    eng.cancel(first.run_id)
    second = eng.rerun("slow", "w")
    assert second.attempt == 2
    plain = eng.trigger("slow", "w", TriggerSource.SCHEDULE)
    assert plain.run_id == second.run_id
    assert len(store.list_attempts("slow", "w")) == 2


def test_concurrent_reruns_create_single_new_attempt(env):
    """同一波并发重跑只算一次；且不会与进行中的尝试并存两遍。"""
    eng, store, _ = env
    eng.register_workflow(CHAIN)
    for n in "abc":
        eng.register_handler(n, lambda: None)
    first = eng.trigger("chain", "w")
    eng.tick()
    assert eng.get_status(first.run_id)[0].state == RunState.SUCCEEDED

    results: list = []
    barrier = threading.Barrier(8)

    def fire():
        barrier.wait()
        results.append(eng.rerun("chain", "w"))

    threads = [threading.Thread(target=fire) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert len({r.run_id for r in results}) == 1
    assert results[0].attempt == 2
    attempts = store.list_attempts("chain", "w")
    assert len(attempts) == 2  # 只新增了一次尝试
    eng.tick()
    assert eng.get_status(results[0].run_id)[0].state == RunState.SUCCEEDED


def test_rerun_of_cancelled_and_timed_out_windows(env):
    """被作废与超时的窗口都算已收尾，可重跑。"""
    eng, _, clock = env
    eng.register_workflow(CHAIN)
    for n in "abc":
        eng.register_handler(n, lambda: None)
    r1 = eng.trigger("chain", "w1")
    eng.cancel(r1.run_id)
    again = eng.rerun("chain", "w1")
    assert again.attempt == 2
    eng.tick()
    assert eng.get_status(again.run_id)[0].state == RunState.SUCCEEDED

    wf = WorkflowDef("t", (TaskDef("x",),), run_timeout_seconds=5)
    eng.register_workflow(wf)
    eng.register_handler("x", lambda: None)
    r2 = eng.trigger("t", "w2")
    clock.advance(10)
    eng.tick()
    assert eng.get_status(r2.run_id)[0].state == RunState.TIMEOUT
    again2 = eng.rerun("t", "w2")
    assert again2.attempt == 2
    eng.tick()
    assert eng.get_status(again2.run_id)[0].state == RunState.SUCCEEDED
