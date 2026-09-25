"""Timeout monitoring: over-deadline instances are marked TIMED_OUT and stay
terminal; timeout is distinguishable from a business failure."""
from __future__ import annotations

import threading
import time

from scheduler.errors import NonRetryableTaskError
from scheduler.models import NodeSpec, WorkflowSpec

from .conftest import dump_audit


def test_instance_timeout_marks_timed_out_and_skips_downstream(make_scheduler) -> None:
    gate = threading.Event()
    entered = threading.Event()

    def slow(ctx):
        entered.set()
        gate.wait(5)
        return "done"  # never reached before deadline

    def down(ctx):
        return None

    sched = make_scheduler(max_workers=2)
    sched.registry.register("slow", slow)
    sched.registry.register("down", down)
    sched.register_workflow(WorkflowSpec(
        name="wf",
        nodes=(
            NodeSpec(name="slow", task="slow"),
            NodeSpec(name="down", task="down", deps=("slow",)),
        ),
        instance_timeout_seconds=0.3,
    ))
    iid = sched.trigger("wf")
    assert entered.wait(2)
    st = sched.wait_until_terminal(iid, 5)
    dump_audit(sched, iid)
    assert st["state"] == "TIMED_OUT"
    assert st["fail_kind"] == "TIMEOUT"  # distinguishable from BUSINESS
    assert "deadline" in (st["error"] or "")
    states = {n["node"]: n["state"] for n in st["nodes"]}
    assert states["down"] == "SKIPPED"
    assert states["slow"] == "FAILED"
    slow_node = next(n for n in st["nodes"] if n["node"] == "slow")
    assert slow_node["fail_kind"] == "TIMEOUT"
    assert any(a["event"] == "INSTANCE_TIMED_OUT" for a in st["audit"])
    gate.set()


def test_business_failure_is_not_timeout(make_scheduler) -> None:
    def boom(ctx):
        raise NonRetryableTaskError("invalid payload")

    sched = make_scheduler()
    sched.registry.register("boom", boom)
    sched.register_workflow(WorkflowSpec(
        name="wf",
        nodes=(NodeSpec(name="boom", task="boom"),),
        instance_timeout_seconds=30.0,
    ))
    iid = sched.trigger("wf")
    st = sched.wait_until_terminal(iid, 5)
    dump_audit(sched, iid)
    assert st["state"] == "FAILED"
    assert st["fail_kind"] == "BUSINESS"
    assert st["error"] == "invalid payload"


def test_node_timeout_is_distinguishable_from_instance_timeout(make_scheduler) -> None:
    gate = threading.Event()
    entered = threading.Event()

    def slow(ctx):
        entered.set()
        gate.wait(5)
        return None

    sched = make_scheduler(max_workers=2)
    sched.registry.register("slow", slow)
    sched.register_workflow(WorkflowSpec(name="wf", nodes=(
        NodeSpec(name="slow", task="slow", timeout_seconds=0.3, max_retries=0),
    )))
    iid = sched.trigger("wf")
    st = sched.wait_until_terminal(iid, 5)
    dump_audit(sched, iid)
    # node deadline exceeded -> instance FAILED with TIMEOUT fail_kind
    assert st["state"] == "FAILED"
    assert st["fail_kind"] == "TIMEOUT"
    node = next(n for n in st["nodes"] if n["node"] == "slow")
    assert node["state"] == "FAILED" and node["fail_kind"] == "TIMEOUT"
    assert "exceeded timeout" in (node["error"] or "")
    gate.set()


def test_terminal_state_is_stable_after_timeout(make_scheduler) -> None:
    gate = threading.Event()
    entered = threading.Event()

    def slow(ctx):
        entered.set()
        gate.wait(5)
        return None

    sched = make_scheduler()
    sched.registry.register("slow", slow)
    sched.register_workflow(WorkflowSpec(
        name="wf",
        nodes=(NodeSpec(name="slow", task="slow"),),
        instance_timeout_seconds=0.3,
    ))
    iid = sched.trigger("wf")
    assert entered.wait(2)
    st1 = sched.wait_until_terminal(iid, 5)
    assert st1["state"] == "TIMED_OUT"
    # repeated sweeps / triggers / recovery cannot reopen it
    time.sleep(0.3)
    assert sched.sweep_timeouts() == []
    assert sched.recover() == []
    st2 = sched.get_status(iid)
    assert st2["state"] == "TIMED_OUT" and st2["terminal"] is True
    assert st2["fail_kind"] == "TIMEOUT"
    dump_audit(sched, iid)
    gate.set()


def test_sweep_marks_unclaimed_expired_instance(tmp_path) -> None:
    """An instance created in a previous process (no live driver) is marked
    by the timeout sweep monitor."""
    from scheduler.store import Store
    from scheduler.executor import TaskRegistry
    from scheduler.engine import Scheduler

    db = str(tmp_path / "sweep.db")
    store = Store(db)
    s0 = Scheduler(store, TaskRegistry(), auto_recover=False)
    s0.registry.register("t", lambda ctx: None)
    s0.register_workflow(WorkflowSpec(
        name="wf", nodes=(NodeSpec(name="t", task="t"),),
        instance_timeout_seconds=0.2,
    ))
    deadline = time.time() + 0.2
    iid = store.create_instance("wf", "MANUAL", None, deadline)
    s0.shutdown(wait=False)
    store.close()

    store2 = Store(db)
    s2 = Scheduler(store2, TaskRegistry(), auto_recover=False)
    st = s2.wait_until_terminal(iid, 5)
    dump_audit(s2, iid)
    assert st["state"] == "TIMED_OUT"
    assert st["fail_kind"] == "TIMEOUT"
    assert "swept by monitor" in (st["error"] or "")
    s2.shutdown()
    store2.close()
