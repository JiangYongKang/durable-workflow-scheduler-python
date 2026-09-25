"""Shared test fixtures and helpers.

Every test prints the audit trail of the instances it touches: trigger source,
state transition and the recorded decision basis (``reason=...``).
"""
from __future__ import annotations

import logging
import threading
import time
from collections.abc import Callable
from typing import Any

import pytest

from scheduler.engine import Scheduler
from scheduler.executor import TaskRegistry
from scheduler.store import Store


def configure_logging() -> None:
    root = logging.getLogger()
    if not getattr(root, "_pytest_configured", False):
        handler = logging.StreamHandler()
        handler.setFormatter(
            logging.Formatter(
                "%(asctime)s %(levelname)s [%(threadName)s] %(name)s: %(message)s",
                datefmt="%H:%M:%S",
            )
        )
        root.addHandler(handler)
        root.setLevel(logging.INFO)
        root._pytest_configured = True  # type: ignore[attr-defined]


configure_logging()
log = logging.getLogger("tests.audit")


@pytest.fixture
def make_scheduler(tmp_path: Any) -> Callable[..., Scheduler]:
    schedulers: list[Scheduler] = []

    def _make(path: str | None = None, **kwargs: Any) -> Scheduler:
        db = path or str(tmp_path / f"sched-{len(schedulers)}.db")
        store = Store(db)
        registry = TaskRegistry()
        sched = Scheduler(store, registry, auto_recover=kwargs.pop("auto_recover", True), **kwargs)
        schedulers.append(sched)
        return sched

    yield _make

    for sched in schedulers:
        sched.shutdown()


def dump_audit(sched: Scheduler, *instance_ids: str) -> None:
    """Print trigger source / transition / decision basis for each instance."""
    for instance_id in instance_ids:
        status = sched.get_status(instance_id)
        log.info("===== audit for instance %s (final=%s state=%s fail_kind=%s) =====",
                 instance_id[:8], status["terminal"], status["state"],
                 status["fail_kind"])
        for event in status["audit"]:
            log.info(
                "  source=%-8s node=%-6s event=%-18s detail=%s",
                event["trigger_source"] or "-",
                event["node"] or "-",
                event["event"],
                event["detail"],
            )
        for node in status["nodes"]:
            log.info(
                "  node-state node=%-6s state=%-9s attempt=%d fail_kind=%s error=%s",
                node["node"], node["state"], node["attempt"],
                node["fail_kind"], node["error"],
            )


def wait_state(sched: Scheduler, instance_id: str, timeout: float = 5.0) -> dict[str, Any]:
    return sched.wait_until_terminal(instance_id, timeout=timeout)


@pytest.fixture
def gate() -> threading.Event:
    return threading.Event()


__all__ = ["dump_audit", "wait_state", "log", "configure_logging"]
