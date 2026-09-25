"""Restart recovery: resume unfinished instances without re-running success."""
from __future__ import annotations

import threading
import time

from scheduler.engine import Scheduler
from scheduler.executor import TaskRegistry
from scheduler.models import NodeSpec, WorkflowSpec
from scheduler.store import Store

from .conftest import dump_audit


def test_recovery_resumes_pending_without_repeating_success(make_scheduler, tmp_path) -> None:
    db = str(tmp_path / "recovery.db")
    runs = {"a": 0, "b": 0}
    lock = threading.Lock()

    def make(node: str):
        def fn(ctx):
            with lock:
                runs[node] += 1
            return None
        return fn

    # --- process 1: task 'a' executes and its success is persisted, then the
    # process dies with 'b' still PENDING (no driver ever advanced it).
    store1 = Store(db)
    reg1 = TaskRegistry()
    reg1.register("a", make("a"))
    reg1.register("b", make("b"))
    s1 = Scheduler(store1, reg1, auto_recover=False)
    s1.register_workflow(WorkflowSpec(name="wf", nodes=(
        NodeSpec(name="a", task="a"),
        NodeSpec(name="b", task="b", deps=("a",)),
    )))
    iid = store1.create_instance("wf", "MANUAL", None, None)
    store1.mark_instance_running(iid)
    store1.activate_ready_nodes(iid, ["a"])
    now = time.time()
    assert store1.claim_node(iid, "a", now)
    result = reg1.get("a")({"instance_id": iid, "node": "a", "results": {}})
    import json
    store1.complete_node(iid, "a", json.dumps(result))
    s1.shutdown()
    store1.close()
    assert runs == {"a": 1, "b": 0}, runs

    # --- process 2: new Scheduler on the same durable store, auto recovery
    store2 = Store(db)
    reg2 = TaskRegistry()
    reg2.register("a", make("a"))
    reg2.register("b", make("b"))
    s2 = Scheduler(store2, reg2, auto_recover=True)
    st = s2.wait_until_terminal(iid, 5)
    dump_audit(s2, iid)
    assert st["state"] == "SUCCEEDED"
    assert runs == {"a": 1, "b": 1}, runs  # a NOT re-executed, b ran once
    recovery_events = [a for a in st["audit"] if a["event"] == "RECOVERY"]
    assert len(recovery_events) == 1
    assert recovery_events[0]["trigger_source"] == "RECOVERY"
    s2.shutdown()
    store2.close()


def test_interrupted_running_node_is_retried_but_succeeded_node_not(tmp_path) -> None:
    db = str(tmp_path / "interrupted.db")
    gate = threading.Event()
    entered = threading.Event()
    runs = {"a": 0, "slow": 0}
    lock = threading.Lock()

    def a(ctx):
        with lock:
            runs["a"] += 1
        return None

    def slow(ctx):
        with lock:
            runs["slow"] += 1
            n = runs["slow"]
        if n == 1:
            entered.set()
            gate.wait(30)  # first attempt is killed mid-flight in process 1
        return None

    store1 = Store(db)
    reg1 = TaskRegistry()
    reg1.register("a", a)
    reg1.register("slow", slow)
    s1 = Scheduler(store1, reg1, auto_recover=False, max_workers=2)
    s1.register_workflow(WorkflowSpec(name="wf", nodes=(
        NodeSpec(name="a", task="a"),
        NodeSpec(name="slow", task="slow", deps=("a",)),
    )))
    iid = s1.trigger("wf")
    assert entered.wait(3)
    # slow is RUNNING when the process dies
    node = store1.get_node(iid, "slow")
    assert node["state"] == "RUNNING"
    # Simulate a hard crash: do not wait for driver threads / in-flight task.
    s1.shutdown(wait=False)
    store1.close()

    # process 2: slow resets RUNNING -> READY and is retried; a stays done
    store2 = Store(db)
    reg2 = TaskRegistry()
    reg2.register("a", a)
    reg2.register("slow", slow)
    s2 = Scheduler(store2, reg2, auto_recover=True, max_workers=2)
    st = s2.wait_until_terminal(iid, 8)
    dump_audit(s2, iid)
    assert st["state"] == "SUCCEEDED"
    assert runs["a"] == 1, runs  # succeeded step never repeated
    assert runs["slow"] == 2, runs  # interrupted step retried once
    recovery_events = [a for a in st["audit"] if a["event"] == "RECOVERY"]
    assert any("reset_running=slow" in a["detail"] for a in recovery_events)
    gate.set()  # release the orphaned task thread from the "crashed" process
    s2.shutdown()
    store2.close()


def test_recover_is_idempotent_on_terminal_instance(make_scheduler) -> None:
    sched = make_scheduler()
    sched.registry.register("t", lambda ctx: None)
    sched.register_workflow(WorkflowSpec(name="wf", nodes=(
        NodeSpec(name="t", task="t"),
    )))
    iid = sched.trigger("wf")
    st = sched.wait_until_terminal(iid, 5)
    assert st["state"] == "SUCCEEDED"
    assert sched.recover() == []
    st2 = sched.get_status(iid)
    assert st2["state"] == "SUCCEEDED" and st2["terminal"] is True
    dump_audit(sched, iid)


def test_stale_locks_cleared_on_recovery(tmp_path) -> None:
    db = str(tmp_path / "locks.db")
    store1 = Store(db)
    s1 = Scheduler(store1, TaskRegistry(), auto_recover=False)
    s1.registry.register("t", lambda ctx: None)
    s1.register_workflow(WorkflowSpec(name="wf", nodes=(NodeSpec(name="t", task="t"),)))
    iid = s1.trigger("wf")
    s1.wait_until_terminal(iid, 5)
    # simulate an abandoned lock left behind by a crashed process
    assert store1.try_lock_instance("phantom-instance") is True
    s1.shutdown()
    store1.close()

    store2 = Store(db)
    s2 = Scheduler(store2, TaskRegistry(), auto_recover=True)
    # lock table usable: phantom cleared, and no unfinished instances listed
    assert s2.recover() == []
    assert store2.try_lock_instance("phantom-instance") is True
    s2.shutdown()
    store2.close()
