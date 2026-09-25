"""Concurrent triggers must never double-advance one instance.

* schedule/backfill triggers for the overlapping window collapse to one
  instance (created exactly once, executed exactly once);
* repeated schedule calls once the window is terminal change nothing;
* manual triggers are independent runs;
* even a re-entrant advance attempt while a driver holds the lock is a no-op.
"""
from __future__ import annotations

import threading

from scheduler.models import NodeSpec, WorkflowSpec
from scheduler.models import TriggerSource

from .conftest import dump_audit


WINDOW = "w3600:2026-09-25T10:00:00+00:00"


def test_concurrent_scheduled_triggers_share_one_instance(make_scheduler) -> None:
    started = threading.Barrier(8)
    results: list[tuple[str, bool]] = []
    lock = threading.Lock()
    runs = {"task": 0}
    run_lock = threading.Lock()

    def task(ctx):
        with run_lock:
            runs["task"] += 1
        return None

    sched = make_scheduler(max_workers=8)
    sched.registry.register("task", task)
    sched.register_workflow(WorkflowSpec(name="wf", nodes=(
        NodeSpec(name="task", task="task"),
    )))

    def fire(idx: int) -> None:
        started.wait()
        source = TriggerSource.SCHEDULE.value if idx % 2 else TriggerSource.BACKFILL.value
        iid, created = sched.trigger_scheduled("wf", WINDOW, source=source)
        with lock:
            results.append((iid, created))

    threads = [threading.Thread(target=fire, args=(i,)) for i in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    instance_ids = {iid for iid, _ in results}
    assert len(instance_ids) == 1, results
    iid = instance_ids.pop()
    created_flags = [c for _, c in results]
    assert sum(1 for c in created_flags if c) == 1, created_flags

    st = sched.wait_until_terminal(iid, 5)
    dump_audit(sched, iid)
    assert st["state"] == "SUCCEEDED"
    assert runs["task"] == 1, runs  # body ran exactly once
    sources = {a["trigger_source"] for a in st["audit"] if a["event"] == "TRIGGER"}
    assert sources <= {"SCHEDULE", "BACKFILL"}
    dedups = [a for a in st["audit"] if a["event"] == "DEDUP"]
    assert len(dedups) == 7
    assert all("window already exists" in a["detail"] for a in dedups)


def test_repeated_scheduled_trigger_after_terminal_is_stable(make_scheduler) -> None:
    sched = make_scheduler()
    sched.registry.register("task", lambda ctx: None)
    sched.register_workflow(WorkflowSpec(name="wf", nodes=(
        NodeSpec(name="task", task="task"),
    )))
    iid1, created1 = sched.trigger_scheduled("wf", WINDOW)
    st1 = sched.wait_until_terminal(iid1, 5)
    assert created1 and st1["state"] == "SUCCEEDED"

    # duplicate submissions must not change final state
    for _ in range(3):
        iid2, created2 = sched.trigger_scheduled("wf", WINDOW)
        assert iid2 == iid1 and created2 is False
        st2 = sched.wait_until_terminal(iid2, 5)
        assert st2["state"] == "SUCCEEDED"
        assert st2 == st1 or st2["state"] == st1["state"]
    dump_audit(sched, iid1)


def test_manual_triggers_are_separate_instances(make_scheduler) -> None:
    runs = {"n": 0}
    lock = threading.Lock()

    def task(ctx):
        with lock:
            runs["n"] += 1
        return None

    sched = make_scheduler(max_workers=4)
    sched.registry.register("task", task)
    sched.register_workflow(WorkflowSpec(name="wf", nodes=(
        NodeSpec(name="task", task="task"),
    )))
    ids = set()
    barrier = threading.Barrier(4)

    def fire() -> None:
        barrier.wait()
        ids.add(sched.trigger("wf"))

    threads = [threading.Thread(target=fire) for _ in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert len(ids) == 4  # manual concurrency -> 4 distinct instances
    for iid in ids:
        st = sched.wait_until_terminal(iid, 5)
        assert st["state"] == "SUCCEEDED"
    assert runs["n"] == 4


def test_concurrent_advance_same_instance_single_driver(make_scheduler) -> None:
    gate = threading.Event()
    entered = threading.Event()

    def task(ctx):
        entered.set()
        gate.wait(timeout=2)
        return None

    sched = make_scheduler()
    sched.registry.register("task", task)
    sched.register_workflow(WorkflowSpec(name="wf", nodes=(
        NodeSpec(name="task", task="task"),
    )))
    iid = sched.trigger("wf")
    assert entered.wait(2)
    # while the task is in flight, attempt more advancement: lock must refuse
    sched.advance_instance(iid)
    sched.advance_instance(iid)
    gate.set()
    st = sched.wait_until_terminal(iid, 5)
    dump_audit(sched, iid)
    assert st["state"] == "SUCCEEDED"
    running_events = [a for a in st["audit"] if a["event"] == "NODE_RUNNING"]
    assert len(running_events) == 1
