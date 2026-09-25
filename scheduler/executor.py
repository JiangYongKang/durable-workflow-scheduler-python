"""Task registry and local in-process task executor.

A task is any callable ``fn(ctx: dict) -> JSON-serializable``. Tasks signal
business errors by raising:

* :class:`~scheduler.errors.RetryableTaskError` — may be retried up to the
  node's ``max_retries`` with backoff;
* :class:`~scheduler.errors.NonRetryableTaskError` — fails the node at once.

Any *other* exception is treated as a transient/retryable error (wrapped in
``RetryableTaskError``), because the common cause is a temporary local
dependency problem.  Callers that need fail-fast semantics must raise
``NonRetryableTaskError`` explicitly.

Because node execution is guarded by the store CAS (RUNNING -> SUCCEEDED),
the engine executes a node's body at most once per attempt; completed nodes
are never re-run (see ``Store.reset_interrupted_nodes``).
"""
from __future__ import annotations

import json
from collections.abc import Callable
from typing import Any

from .errors import (
    NonRetryableTaskError,
    RetryableTaskError,
    TaskError,
    WorkflowNotFound,
)

# Task callable: receives a context dict, returns a JSON-serializable result.
TaskFn = Callable[[dict[str, Any]], Any]


class TaskRegistry:
    def __init__(self) -> None:
        self._tasks: dict[str, TaskFn] = {}

    def register(self, name: str, fn: TaskFn) -> None:
        if not name:
            raise ValueError("task name must be non-empty")
        self._tasks[name] = fn

    def get(self, name: str) -> TaskFn:
        try:
            return self._tasks[name]
        except KeyError:
            raise WorkflowNotFound(f"task {name!r} is not registered") from None

    def names(self) -> list[str]:
        return sorted(self._tasks)


class Executor:
    """Runs task callables locally, synchronously, inside a worker thread."""

    def __init__(self, registry: TaskRegistry) -> None:
        self.registry = registry

    def run_once(self, task_name: str, ctx: dict[str, Any]) -> str:
        """Execute the task exactly once for this attempt.

        Returns the JSON-encoded result. Retryable/non-retryable task errors
        propagate as :class:`TaskError` so the engine can decide.
        """
        fn = self.registry.get(task_name)
        try:
            result = fn(ctx)
        except TaskError:
            raise
        except Exception as exc:  # unexpected -> transient by convention
            raise RetryableTaskError(
                f"task {task_name!r} raised {type(exc).__name__}: {exc}"
            ) from exc
        return json.dumps(result, sort_keys=True, default=str)
