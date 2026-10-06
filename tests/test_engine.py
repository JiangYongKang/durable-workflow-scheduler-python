"""引擎单测：依赖失败传播 / 并发触发 / 重试退避 / 重启恢复 / 补齐去重 / 超时。

日志中会打印触发来源、状态流转与终态判定依据。
"""
from __future__ import annotations

import threading

import pytest

from scheduler.engine import Engine
from scheduler.errors import (
    CycleDetectedError,
    FatalTaskError,
    RetryableTaskError,
    UnknownTaskError,
)
from scheduler.models import (
    RetryPolicy,
    RunState,
    TaskDef,
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


def test_happy_path_diamond(env):
    eng, _, _ = env
    order = []
    eng.register_workflow(DIAMOND)
    for n in "abcd":
        eng.register_handler(n, (lambda n=n: order.append(n)))
    run = eng.trigger("diamond", "w")
    eng.tick()
    status = eng.get_status(run.run_id)
    assert status[0].state == RunState.SUCCEEDED
    assert order.index("a") < order.index("b") < order.index("d")
    assert order.index("a") < order.index("c") < order.index("d")


def test_cycle_rejected_at_submit(env):
    eng, _, _ = env
    wf = WorkflowDef("cyc", (TaskDef("a", ("b",)), TaskDef("b", ("a",))))
    with pytest.raises(CycleDetectedError):
        eng.register_workflow(wf)


def test_unknown_dependency_rejected(env):
    eng, _, _ = env
    wf = WorkflowDef("x", (TaskDef("a", ("ghost",)),))
    with pytest.raises(UnknownTaskError):
        eng.register_workflow(wf)


def test_upstream_failure_propagates_and_blocks(env):
    eng, _, _ = env
    eng.register_workflow(DIAMOND)
    eng.register_handler("a", lambda: None)

    def fail_b():
        raise FatalTaskError("b exploded")

    eng.register_handler("b", fail_b)
    eng.register_handler("c", lambda: None)
    d_calls = []
    eng.register_handler("d", lambda: d_calls.append(1))

    run = eng.trigger("diamond", "w")
    eng.tick()

    _, tasks = eng.get_status(run.run_id)
    states = {t.task_name: t.state for t in tasks}
    assert states == {"a": TaskState.SUCCEEDED, "b": TaskState.FAILED,
                      "c": TaskState.SUCCEEDED, "d": TaskState.BLOCKED}
    assert d_calls == []  # 下游被阻止启动，无副作用
    run0, _ = eng.get_status(run.run_id)
    assert run0.state == RunState.FAILED
    assert "b exploded" in run0.failure_reason
    blocked = next(t for t in tasks if t.task_name == "d")
    assert blocked.error_kind == "fatal"


def test_retryable_error_retries_with_backoff_then_succeeds(env):
    eng, _, clock = env
    wf = WorkflowDef("flaky", (
        TaskDef("f", retry_policy=RetryPolicy(max_attempts=3, backoff_seconds=2)),
    ))
    eng.register_workflow(wf)
    flaky = {"n": 0}

    def handler():
        flaky["n"] += 1
        if flaky["n"] < 3:
            raise RetryableTaskError("transient")

    eng.register_handler("f", handler)
    run = eng.trigger("flaky", "w")

    def task():
        return eng.get_status(run.run_id)[1][0]

    eng.tick()  # 第1次失败，2s 后重试
    t = task()
    assert t.state == TaskState.READY and t.attempts == 1
    assert t.next_attempt_at == pytest.approx(clock.t + 2)
    eng.tick()  # 退避未到，不执行
    assert task().attempts == 1
    clock.advance(2)
    eng.tick()  # 第2次失败，4s 后重试
    assert task().attempts == 2 and task().state == TaskState.READY
    clock.advance(4)
    eng.tick()  # 第3次成功
    assert task().state == TaskState.SUCCEEDED and flaky["n"] == 3
    assert eng.get_status(run.run_id)[0].state == RunState.SUCCEEDED


def test_retryable_error_exhausted_marks_failed(env):
    eng, _, clock = env
    wf = WorkflowDef("always", (
        TaskDef("f", retry_policy=RetryPolicy(max_attempts=2, backoff_seconds=1)),
        TaskDef("g", ("f",)),
    ))
    eng.register_workflow(wf)
    eng.register_handler("f", lambda: (_ for _ in ()).throw(
        RetryableTaskError("always")))
    g_calls = []
    eng.register_handler("g", lambda: g_calls.append(1))

    run = eng.trigger("always", "w")
    eng.tick()
    clock.advance(1)
    eng.tick()  # 重试耗尽
    _, tasks = eng.get_status(run.run_id)
    f = next(t for t in tasks if t.task_name == "f")
    g = next(t for t in tasks if t.task_name == "g")
    assert f.state == TaskState.FAILED and f.error_kind == "retryable"
    assert f.attempts == 2
    assert g.state == TaskState.BLOCKED and g_calls == []
    assert eng.get_status(run.run_id)[0].state == RunState.FAILED


def test_concurrent_trigger_same_window_runs_once(env):
    eng, store, _ = env
    eng.register_workflow(DIAMOND)
    gate = threading.Event()
    counter = {"n": 0}

    def slow_a():
        counter["n"] += 1
        gate.wait(0.5)

    eng.register_handler("a", slow_a)
    for n in "bcd":
        eng.register_handler(n, lambda: None)

    results: list = []

    def fire():
        results.append(eng.trigger("diamond", "W", TriggerSource.MANUAL))

    threads = [threading.Thread(target=fire) for _ in range(8)]
    for t in threads:
        t.start()
    gate.set()
    for t in threads:
        t.join()
    eng.tick()

    assert len({r.run_id for r in results}) == 1
    assert counter["n"] == 1  # 同一实例未被重复推进
    assert store.get_run_by_window("diamond", "W").state == RunState.SUCCEEDED


def test_repeated_submit_does_not_change_terminal_state(env):
    eng, _, _ = env
    eng.register_workflow(DIAMOND)
    for n in "abcd":
        eng.register_handler(n, lambda: None)
    run = eng.trigger("diamond", "W")
    eng.tick()
    snapshot = [(t.task_name, t.state.value, t.attempts)
                for t in eng.get_status(run.run_id)[1]]
    for src in (TriggerSource.MANUAL, TriggerSource.SCHEDULE,
                TriggerSource.BACKFILL):
        again = eng.trigger("diamond", "W", src)
        assert again.run_id == run.run_id
        eng.tick()
    assert [(t.task_name, t.state.value, t.attempts)
            for t in eng.get_status(run.run_id)[1]] == snapshot


def test_recovery_after_restart_continues_and_no_dup_side_effects(env, tmp_path):
    clock = FakeClock()
    db = str(tmp_path / "state.db")
    store = StateStore(db)
    eng = Engine(store, now=clock)
    eng.register_workflow(DIAMOND)
    effects = []
    eng.register_handler("a", lambda: effects.append("a"))
    eng.register_handler("b", lambda: effects.append("b"))
    eng.register_handler("c", lambda: effects.append("c"))
    eng.register_handler("d", lambda: effects.append("d"))

    run = eng.trigger("diamond", "W")
    eng.tick()
    run0, tasks0 = eng.get_status(run.run_id)
    assert run0.state == RunState.SUCCEEDED
    finished = {t.task_name: t.finished_at for t in tasks0}
    store.close()

    # 模拟进程重启：同 DB 文件重新打开并恢复
    store2 = StateStore(db)
    eng2 = Engine(store2, now=clock)
    eng2.register_workflow(DIAMOND)
    recovered = eng2.recover()
    assert recovered == []  # 终态实例不再推进
    _, tasks2 = eng2.get_status(run.run_id)
    assert all(t.state == TaskState.SUCCEEDED for t in tasks2)
    assert effects == ["a", "b", "c", "d"] or set(effects) == set("abcd")
    assert effects.count("a") == 1 and effects.count("d") == 1
    assert {t.task_name: t.finished_at for t in tasks2} == finished
    store2.close()


def test_recovery_continues_unfinished_after_restart(env, tmp_path):
    clock = FakeClock()
    db = str(tmp_path / "state.db")
    effects = []

    # 进程 A：只让 a 成功；b 在 READY 时进程被杀掉（直接关闭存储模拟 kill）
    store = StateStore(db)
    eng = Engine(store, now=clock)
    eng.register_workflow(DIAMOND)
    eng.register_handler("a", lambda: effects.append("a"))
    run = eng.trigger("diamond", "W")
    # 受控地只执行一次 a（模拟 a 刚成功进程即被杀掉）
    eng._handlers["a"]()
    t_a = next(t for t in eng.get_status(run.run_id)[1] if t.task_name == "a")
    t_a.attempts = 1
    t_a.state = TaskState.SUCCEEDED
    t_a.started_at = t_a.finished_at = clock()
    eng._store.save_task(t_a)
    _, tasks = eng.get_status(run.run_id)
    assert next(t for t in tasks if t.task_name == "a").state == TaskState.SUCCEEDED
    assert all(next(t for t in tasks if t.task_name == n).state == TaskState.PENDING
               for n in "bcd")
    store.close()

    # 进程 B：重启后注册剩余处理器并恢复
    store2 = StateStore(db)
    eng2 = Engine(store2, now=clock)
    eng2.register_workflow(DIAMOND)
    for n in "bcd":
        eng2.register_handler(n, (lambda n=n: effects.append(n)))
    recovered = eng2.recover()
    assert recovered == [run.run_id]
    run2, tasks2 = eng2.get_status(run.run_id)
    assert run2.state == RunState.SUCCEEDED
    assert all(t.state == TaskState.SUCCEEDED for t in tasks2)
    assert effects.count("a") == 1  # 已成功步骤不重复执行
    store2.close()


def test_backfill_overlapping_windows_no_duplicate(env):
    eng, _, _ = env
    eng.register_workflow(DIAMOND)
    for n in "abcd":
        eng.register_handler(n, lambda: None)

    r1 = eng.trigger("diamond", "2026-09-23", TriggerSource.SCHEDULE)
    eng.tick()
    # 补齐窗口与既有调度窗口重叠
    runs = eng.backfill("diamond", ["2026-09-23", "2026-09-24", "2026-09-25"])
    eng.tick()
    assert runs[0].run_id == r1.run_id
    assert {r.run_id for r in runs} == {
        "diamond:2026-09-23", "diamond:2026-09-24", "diamond:2026-09-25"}
    # 再次补齐仍然不产生新实例
    again = eng.backfill("diamond", ["2026-09-23", "2026-09-24"])
    assert [r.run_id for r in again] == [
        "diamond:2026-09-23", "diamond:2026-09-24"]


def test_timeout_marked_distinguishable_from_failure(env):
    eng, _, clock = env
    wf = WorkflowDef("slow", (
        TaskDef("s", retry_policy=RetryPolicy(max_attempts=3, backoff_seconds=30)),
    ), run_timeout_seconds=10)
    eng.register_workflow(wf)
    eng.register_handler("s", lambda: (_ for _ in ()).throw(
        RetryableTaskError("will-retry-later")))
    run = eng.trigger("slow", "w")
    eng.tick()  # 尝试 1 次失败，30s 后才重试，但实例 10s 即超时
    assert eng.get_status(run.run_id)[1][0].state == TaskState.READY
    clock.advance(11)  # 超过实例时限
    eng.tick()
    run0, tasks = eng.get_status(run.run_id)
    assert run0.state == RunState.TIMEOUT
    assert run0.failure_reason.startswith("timeout")
    assert tasks[0].state == TaskState.TIMEOUT
    assert tasks[0].error_kind == "timeout"

    # 业务失败与超时可区分
    eng.trigger("slow", "w2")
    clock.advance(20)
    eng.tick()
    # 超时优先；另用致命失败验证 FAILED 分支
    wf2 = WorkflowDef("boom", (TaskDef("z"),), run_timeout_seconds=1000)
    eng.register_workflow(wf2)
    eng.register_handler("z", lambda: (_ for _ in ()).throw(
        FatalTaskError("biz error")))
    run3 = eng.trigger("boom", "w3")
    eng.tick()
    r3, _ = eng.get_status(run3.run_id)
    assert r3.state == RunState.FAILED and "biz error" in r3.failure_reason
