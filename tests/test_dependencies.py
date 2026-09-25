"""Dependency orchestration: ordering, concurrency, and failure propagation."""
from __future__ import annotations

import threading
import time

from scheduler.errors import NonRetryableTaskError, RetryableTaskError
from scheduler.models import NodeSpec, WorkflowSpec

from .conftest import dump_audit


def test_diamond_runs_in_dependency_order(make_scheduler) -> None:
    order: list[str] = []
    lock = threading.Lock()

    def make(name: str):
        def fn(ctx):
            with lock:
                order.append(name)
            time.sleep(0.01)
            return {"from": name, "upstream": ctx["results"]}
        return fn

    sched = make_scheduler()
    for n in ("root", "l", "r", "leaf"):
        sched.registry.register(n, make(n))
    sched.register_workflow(WorkflowSpec(name="diamond", nodes=(
        NodeSpec(name="root", task="root"),
        NodeSpec(name="l", task="l", deps=("root",)),
        NodeSpec(name="r", task="r", deps=("root",)),
        NodeSpec(name="leaf", task="leaf", deps=("l", "r")),
    )))
    iid = sched.trigger("diamond")
    st = sched.wait_until_terminal(iid, 5)
    dump_audit(sched, iid)

    assert st["state"] == "SUCCEEDED"
    assert order.index("root") < order.index("l")
    assert order.index("root") < order.index("r")
    assert order.index("l") < order.index("leaf")
    assert order.index("r") < order.index("leaf")


def test_independent_nodes_run_concurrently(make_scheduler) -> None:
    max_active = {"v": 0}
    active = {"v": 0}
    lock = threading.Lock()

    def slow(ctx):
        with lock:
            active["v"] += 1
            max_active["v"] = max(max_active["v"], active["v"])
        time.sleep(0.15)
        with lock:
            active["v"] -= 1
        return None

    sched = make_scheduler(max_workers=4)
    for n in ("a", "b", "c"):
        sched.registry.register(n, slow)
    sched.register_workflow(WorkflowSpec(name="fan", nodes=tuple(
        NodeSpec(name=n, task=n) for n in ("a", "b", "c")
    )))
    iid = sched.trigger("fan")
    st = sched.wait_until_terminal(iid, 5)
    dump_audit(sched, iid)
    assert st["state"] == "SUCCEEDED"
    assert max_active["v"] >= 2, max_active  # observed parallel execution


def test_upstream_failure_blocks_all_downstream(make_scheduler) -> None:
    executed: list[str] = []
    lock = threading.Lock()

    def record(name: str, fail: bool = False):
        def fn(ctx):
            with lock:
                executed.append(name)
            if fail:
                raise NonRetryableTaskError("bad data at bad")
            return None
        return fn

    sched = make_scheduler()
    sched.registry.register("a", record("a"))
    sched.registry.register("bad", record("bad", fail=True))
    sched.registry.register("down", record("down"))
    sched.registry.register("down2", record("down2"))
    sched.registry.register("other", record("other"))
    sched.register_workflow(WorkflowSpec(name="wf", nodes=(
        NodeSpec(name="a", task="a"),
        NodeSpec(name="bad", task="bad", deps=("a",)),
        NodeSpec(name="down", task="down", deps=("bad",)),
        NodeSpec(name="down2", task="down2", deps=("down",)),
        NodeSpec(name="other", task="other"),  # independent branch still runs
    )))
    iid = sched.trigger("wf")
    st = sched.wait_until_terminal(iid, 5)
    dump_audit(sched, iid)

    assert st["state"] == "FAILED"
    assert st["fail_kind"] == "BUSINESS"  # distinguishable from TIMEOUT
    states = {n["node"]: n["state"] for n in st["nodes"]}
    assert states == {
        "a": "SUCCEEDED",
        "bad": "FAILED",
        "down": "SKIPPED",
        "down2": "SKIPPED",
        "other": "SUCCEEDED",
    }
    assert "down" not in executed and "down2" not in executed
    assert set(executed) == {"a", "bad", "other"}
    # audit explains the decision basis
    skipped_events = [a for a in st["audit"] if a["event"] == "NODE_SKIPPED"]
    assert len(skipped_events) == 2
    assert all("upstream 'bad' failed" in a["detail"] for a in skipped_events)


def test_non_retryable_fails_immediately_without_retries(make_scheduler) -> None:
    calls = {"bad": 0}

    def bad(ctx):
        calls["bad"] += 1
        raise NonRetryableTaskError("config error")

    sched = make_scheduler()
    sched.registry.register("bad", bad)
    sched.register_workflow(WorkflowSpec(name="nr", nodes=(
        NodeSpec(name="bad", task="bad", max_retries=5, retry_backoff_seconds=0.0),
    )))
    iid = sched.trigger("nr")
    st = sched.wait_until_terminal(iid, 5)
    dump_audit(sched, iid)
    assert st["state"] == "FAILED"
    assert st["fail_kind"] == "BUSINESS"
    assert calls["bad"] == 1  # never retried


def test_failure_settles_sibling_waiting_in_backoff(make_scheduler) -> None:
    flaky_calls = {"x": 0}
    import threading as _t
    lock = _t.Lock()

    def boom(ctx):
        raise NonRetryableTaskError("immediate business failure")

    def flaky(ctx):
        with lock:
            flaky_calls["x"] += 1
        raise RetryableTaskError("still retrying when sibling fails")

    sched = make_scheduler(max_workers=2)
    sched.registry.register("boom", boom)
    sched.registry.register("flaky", flaky)
    sched.register_workflow(WorkflowSpec(name="wf", nodes=(
        NodeSpec(name="boom", task="boom", max_retries=0),
        NodeSpec(name="flaky", task="flaky", max_retries=5,
                 retry_backoff_seconds=30.0, retry_backoff_max_seconds=30.0),
    )))
    iid = sched.trigger("wf")
    st = sched.wait_until_terminal(iid, 5)
    dump_audit(sched, iid)
    assert st["state"] == "FAILED"
    assert st["fail_kind"] == "BUSINESS"
    states = {n["node"]: n["state"] for n in st["nodes"]}
    assert states["boom"] == "FAILED"
    assert states["flaky"] == "SKIPPED", states  # not left READY/RUNNING
    # no node is ever left in a non-terminal state
    assert all(n["state"] in {"SUCCEEDED", "FAILED", "SKIPPED"} for n in st["nodes"])
