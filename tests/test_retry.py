"""Retry classification, attempt caps and bounded exponential backoff."""
from __future__ import annotations

import time

from scheduler.engine import _backoff_delay
from scheduler.errors import RetryableTaskError
from scheduler.models import NodeSpec, WorkflowSpec

from .conftest import dump_audit


def test_retryable_error_retried_until_success(make_scheduler) -> None:
    calls = {"n": 0}

    def flaky(ctx):
        calls["n"] += 1
        if calls["n"] < 3:
            raise RetryableTaskError("temporary glitch")
        return {"ok": True}

    sched = make_scheduler()
    sched.registry.register("flaky", flaky)
    sched.register_workflow(WorkflowSpec(name="wf", nodes=(
        NodeSpec(name="n", task="flaky", max_retries=3,
                 retry_backoff_seconds=0.01),
    )))
    iid = sched.trigger("wf")
    st = sched.wait_until_terminal(iid, 5)
    dump_audit(sched, iid)
    assert st["state"] == "SUCCEEDED"
    assert calls["n"] == 3
    retries = [a for a in st["audit"] if a["event"] == "NODE_RETRY"]
    assert len(retries) == 2
    assert all("retryable error" in a["detail"] for a in retries)


def test_retry_budget_is_bounded(make_scheduler) -> None:
    calls = {"n": 0}

    def always(ctx):
        calls["n"] += 1
        raise RetryableTaskError("persistent")

    sched = make_scheduler()
    sched.registry.register("always", always)
    sched.register_workflow(WorkflowSpec(name="wf", nodes=(
        NodeSpec(name="n", task="always", max_retries=2,
                 retry_backoff_seconds=0.0),
    )))
    iid = sched.trigger("wf")
    st = sched.wait_until_terminal(iid, 5)
    dump_audit(sched, iid)
    assert st["state"] == "FAILED"
    assert st["fail_kind"] == "BUSINESS"
    assert calls["n"] == 3  # 1 initial attempt + 2 retries
    assert "retries exhausted after 3 attempt(s)" in (st["error"] or "")
    failed_nodes = [n for n in st["nodes"] if n["state"] == "FAILED"]
    assert failed_nodes[0]["attempt"] == 3


def test_exponential_backoff_is_capped() -> None:
    node = NodeSpec(
        name="n", task="t",
        retry_backoff_seconds=1.0, retry_backoff_max_seconds=5.0,
    )
    assert _backoff_delay(node, 1) == 1.0
    assert _backoff_delay(node, 2) == 2.0
    assert _backoff_delay(node, 3) == 4.0
    assert _backoff_delay(node, 4) == 5.0  # capped, not 8
    assert _backoff_delay(node, 10) == 5.0


def test_zero_backoff_means_immediate_retry(make_scheduler) -> None:
    calls = {"n": 0}

    def twice(ctx):
        calls["n"] += 1
        if calls["n"] == 1:
            raise RetryableTaskError("once")
        return None

    sched = make_scheduler()
    sched.registry.register("twice", twice)
    sched.register_workflow(WorkflowSpec(name="wf", nodes=(
        NodeSpec(name="n", task="twice", max_retries=1,
                 retry_backoff_seconds=0.0),
    )))
    t0 = time.monotonic()
    iid = sched.trigger("wf")
    st = sched.wait_until_terminal(iid, 5)
    assert time.monotonic() - t0 < 1.0
    dump_audit(sched, iid)
    assert st["state"] == "SUCCEEDED"
    assert calls["n"] == 2
