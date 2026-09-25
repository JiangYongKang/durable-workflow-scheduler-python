"""Submit-time validation: cyclic dependencies must be rejected, never deadlock."""
from __future__ import annotations

import pytest

from scheduler.engine import validate_workflow
from scheduler.errors import SchedulerError
from scheduler.models import NodeSpec, WorkflowSpec


def _wf(name: str, nodes: tuple[NodeSpec, ...]) -> WorkflowSpec:
    return WorkflowSpec(name=name, nodes=nodes)


def test_self_dependency_rejected() -> None:
    spec = _wf("self", (NodeSpec(name="a", task="t", deps=("a",)),))
    with pytest.raises(SchedulerError, match="itself"):
        validate_workflow(spec, None)


def test_cycle_rejected_at_submit_time() -> None:
    spec = _wf("cyc", (
        NodeSpec(name="a", task="t", deps=("c",)),
        NodeSpec(name="b", task="t", deps=("a",)),
        NodeSpec(name="c", task="t", deps=("b",)),
    ))
    with pytest.raises(SchedulerError, match="cyclic dependency rejected") as exc:
        validate_workflow(spec, None)
    # decision basis names the cycle explicitly
    assert "a -> c -> b -> a" in str(exc.value)


def test_diamond_is_accepted() -> None:
    spec = _wf("diamond", (
        NodeSpec(name="root", task="t"),
        NodeSpec(name="l", task="t", deps=("root",)),
        NodeSpec(name="r", task="t", deps=("root",)),
        NodeSpec(name="leaf", task="t", deps=("l", "r")),
    ))
    validate_workflow(spec, None)  # no raise


def test_unknown_dependency_rejected() -> None:
    spec = _wf("u", (NodeSpec(name="a", task="t", deps=("ghost",)),))
    with pytest.raises(SchedulerError, match="unknown node"):
        validate_workflow(spec, None)


def test_duplicate_node_name_rejected() -> None:
    spec = _wf("dup", (NodeSpec(name="a", task="t"), NodeSpec(name="a", task="t2")))
    with pytest.raises(SchedulerError, match="duplicate node"):
        validate_workflow(spec, None)


def test_unregistered_task_rejected() -> None:
    spec = _wf("t", (NodeSpec(name="a", task="missing"),))
    with pytest.raises(SchedulerError, match="unregistered task"):
        validate_workflow(spec, set())


def test_negative_retry_config_rejected() -> None:
    spec = _wf("neg", (NodeSpec(name="a", task="t", max_retries=-1),))
    with pytest.raises(SchedulerError, match="max_retries"):
        validate_workflow(spec, None)
