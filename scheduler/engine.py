"""Workflow orchestration engine.

Concurrency / durability model
------------------------------
* Every instance is driven by **at most one driver thread** at a time. The
  durable ``instance_locks`` row is claimed before advancement; concurrent
  triggers of the same workflow therefore never advance the same instance
  twice (manual triggers each create a fresh instance; schedule/backfill
  triggers share one instance per window key).
* Node bodies are executed after a CAS claim (READY -> RUNNING) on a bounded
  thread pool, so independent nodes run concurrently while a node body runs
  at most once per attempt. Results land through CAS (attempt-guarded), and
  SUCCEEDED nodes are immutable: they are never re-executed after restart.
* All state changes are written to SQLite before effects are considered
  done, and every decision is appended to the audit log (trigger source,
  transition, reason), making the final state explainable.

State machine
-------------
Node:    PENDING -> READY -> RUNNING -> SUCCEEDED (final)
                       ^         |
                       |---------+  (retryable failure, backoff-gated)
                                 |-> FAILED   (final; fail_kind BUSINESS/TIMEOUT)
PENDING/READY -> SKIPPED (final) when an upstream failed.

Instance: PENDING -> RUNNING -> SUCCEEDED | FAILED | TIMED_OUT (all final).
``FAILED`` carries fail_kind BUSINESS or TIMEOUT so timeout and business
failures are distinguishable; ``TIMED_OUT`` means the instance deadline was
exceeded. Terminal states are stable: no later trigger/recovery changes them.
"""
from __future__ import annotations

import logging
import threading
import time
from collections.abc import Callable
from concurrent.futures import Future, ThreadPoolExecutor, wait
from typing import Any

from .errors import (
    NonRetryableTaskError,
    RetryableTaskError,
    SchedulerError,
    TaskError,
    WorkflowNotFound,
)
from .executor import Executor, TaskRegistry
from .models import FailKind, NodeSpec, NodeState, TriggerSource, WorkflowSpec
from .store import Store

logger = logging.getLogger("scheduler.engine")

_TICK_SECONDS = 0.05
_TERMINAL_INSTANCE = {"SUCCEEDED", "FAILED", "TIMED_OUT"}


def validate_workflow(spec: WorkflowSpec, task_names: set[str] | None) -> None:
    """Validate a workflow definition at submit time.

    Cyclic dependencies are rejected *here*, before any instance exists, so a
    bad definition can never reach runtime and deadlock.
    """
    if not spec.name:
        raise SchedulerError("workflow name must be non-empty")
    if not spec.nodes:
        raise SchedulerError(f"workflow {spec.name!r} has no nodes")
    by_name: dict[str, NodeSpec] = {}
    for node in spec.nodes:
        if node.name in by_name:
            raise SchedulerError(f"duplicate node name {node.name!r}")
        if node.max_retries < 0:
            raise SchedulerError(f"node {node.name!r}: max_retries must be >= 0")
        if node.retry_backoff_seconds < 0:
            raise SchedulerError(f"node {node.name!r}: backoff must be >= 0")
        by_name[node.name] = node
        if task_names is not None and node.task not in task_names:
            raise SchedulerError(
                f"node {node.name!r} references unregistered task {node.task!r}"
            )
    for node in spec.nodes:
        for dep in node.deps:
            if dep not in by_name:
                raise SchedulerError(
                    f"node {node.name!r} depends on unknown node {dep!r}"
                )
            if dep == node.name:
                raise SchedulerError(f"node {node.name!r} depends on itself")
    # DFS cycle detection (three-color).
    WHITE, GRAY, BLACK = 0, 1, 2
    color = {name: WHITE for name in by_name}

    def visit(name: str, stack: list[str]) -> None:
        color[name] = GRAY
        stack.append(name)
        for dep in by_name[name].deps:
            if color[dep] == GRAY:
                cycle = stack[stack.index(dep):] + [dep]
                raise SchedulerError(
                    "cyclic dependency rejected at submit time: "
                    + " -> ".join(cycle)
                )
            if color[dep] == WHITE:
                visit(dep, stack)
        stack.pop()
        color[name] = BLACK

    for name in by_name:
        if color[name] == WHITE:
            visit(name, [])


def _backoff_delay(node: NodeSpec, failed_attempt: int) -> float:
    """Exponential backoff for the given (1-based) failed attempt, capped."""
    base = node.retry_backoff_seconds
    if base <= 0:
        return 0.0
    delay = base * (2 ** (failed_attempt - 1))
    cap = node.retry_backoff_max_seconds
    if cap is not None:
        delay = min(delay, cap)
    return delay


def _transitive_dependents(
    nodes: dict[str, NodeSpec], failed: str
) -> list[str]:
    """All nodes that (transitively) depend on ``failed``."""
    result: set[str] = set()
    frontier = [failed]
    while frontier:
        current = frontier.pop()
        for name, node in nodes.items():
            if current in node.deps and name not in result:
                result.add(name)
                frontier.append(name)
    return sorted(result)


class _NodeOutcome:
    __slots__ = ("kind", "result", "error")

    def __init__(self, kind: str, *, result: str | None = None, error: str = ""):
        self.kind = kind  # SUCCEEDED | RETRY | BUSINESS | TIMEOUT
        self.result = result
        self.error = error


class Scheduler:
    def __init__(
        self,
        store: Store,
        registry: TaskRegistry | None = None,
        max_workers: int = 4,
        clock: Callable[[], float] | None = None,
        auto_recover: bool = True,
        timeout_poll_seconds: float = 0.1,
    ) -> None:
        self.store = store
        self.registry = registry or TaskRegistry()
        self.executor = Executor(self.registry)
        self._clock = clock or time.time
        self._pool = ThreadPoolExecutor(
            max_workers=max_workers, thread_name_prefix="wf-node"
        )
        self._drivers: set[str] = set()
        self._driver_threads: list[threading.Thread] = []
        self._drivers_lock = threading.Lock()
        self._wake = threading.Condition()
        self._shutdown = False
        self._timeout_poll = timeout_poll_seconds
        if auto_recover:
            self.recover()
        self._monitor = threading.Thread(
            target=self._timeout_monitor, name="wf-timeout-sweep", daemon=True
        )
        self._monitor.start()

    # ------------------------------------------------------------ registration
    def register_workflow(self, spec: WorkflowSpec) -> None:
        validate_workflow(spec, set(self.registry.names()))
        self.store.register_workflow(spec)
        logger.info(
            "workflow registered: name=%s nodes=%d instance_timeout=%s",
            spec.name, len(spec.nodes), spec.instance_timeout_seconds,
        )

    # --------------------------------------------------------------- triggering
    def trigger(
        self,
        workflow_name: str,
        source: str = TriggerSource.MANUAL.value,
        window_key: str | None = None,
    ) -> str:
        """Trigger a run. Manual triggers always start a fresh instance."""
        spec = self._require_workflow(workflow_name)
        deadline = self._deadline(spec)
        instance_id = self.store.create_instance(
            workflow_name, source, window_key, deadline
        )
        self.store.append_audit(
            instance_id, None, "TRIGGER",
            f"source={source} window_key={window_key} reason=manual trigger",
            source,
        )
        logger.info(
            "trigger: workflow=%s instance=%s source=%s window=%s",
            workflow_name, instance_id, source, window_key,
        )
        self._spawn_driver(instance_id)
        return instance_id

    def trigger_scheduled(
        self,
        workflow_name: str,
        window_key: str,
        source: str = TriggerSource.SCHEDULE.value,
    ) -> tuple[str, bool]:
        """Trigger for a scheduling/backfill window.

        The ``(workflow, window_key)`` unique key makes overlapping schedule
        and backfill calls idempotent: they share one instance and the window
        executes once. Returns ``(instance_id, created)``.
        """
        spec = self._require_workflow(workflow_name)
        deadline = self._deadline(spec)
        instance_id, created = self.store.get_or_create_scheduled_instance(
            workflow_name, source, window_key, deadline
        )
        if created:
            self.store.append_audit(
                instance_id, None, "TRIGGER",
                f"source={source} window_key={window_key} reason=new window",
                source,
            )
            logger.info(
                "scheduled trigger: workflow=%s instance=%s source=%s window=%s",
                workflow_name, instance_id, source, window_key,
            )
        else:
            existing = self.store.get_instance(instance_id)
            self.store.append_audit(
                instance_id, None, "DEDUP",
                f"source={source} window_key={window_key} "
                f"reason=window already exists state={existing['state']}",
                source,
            )
            logger.info(
                "scheduled trigger deduplicated: workflow=%s instance=%s "
                "source=%s window=%s state=%s",
                workflow_name, instance_id, source, window_key,
                existing["state"],
            )
        self._spawn_driver(instance_id)
        return instance_id, created

    # ------------------------------------------------------------- advancement
    def advance_instance(self, instance_id: str) -> None:
        """Public entry point: advance one instance if not already advancing.

        Safe to call repeatedly and concurrently; only one driver wins the
        lock. Mostly useful after manual recovery with ``auto_recover=False``.
        """
        self._spawn_driver(instance_id)

    def _spawn_driver(self, instance_id: str) -> None:
        with self._drivers_lock:
            if self._shutdown or instance_id in self._drivers:
                return
            inst = self.store.get_instance(instance_id)
            if inst is None or inst["state"] in _TERMINAL_INSTANCE:
                return
            if not self.store.try_lock_instance(instance_id):
                logger.info(
                    "advance skipped: instance=%s reason=lock held by another driver",
                    instance_id,
                )
                return
            self._drivers.add(instance_id)
        thread = threading.Thread(
            target=self._run_driver, args=(instance_id,),
            name=f"wf-driver-{instance_id[:8]}", daemon=True,
        )
        with self._drivers_lock:
            self._driver_threads.append(thread)
        thread.start()

    def _run_driver(self, instance_id: str) -> None:
        try:
            self._drive(instance_id)
        except Exception as exc:
            if self.store.closed:
                # Hard shutdown simulated/real: the store is gone, so this
                # orphaned driver cannot commit anything. Recovery resumes
                # the instance in a fresh process.
                logger.info(
                    "driver exits on closed store: instance=%s", instance_id
                )
            else:
                logger.exception("driver crashed: instance=%s", instance_id)
        finally:
            if not self.store.closed:
                self.store.unlock_instance(instance_id)
            with self._drivers_lock:
                self._drivers.discard(instance_id)

    def _drive(self, instance_id: str) -> None:
        spec = self._spec_of(instance_id)
        nodes_spec = {n.name: n for n in spec.nodes}
        inst = self.store.get_instance(instance_id)
        self.store.mark_instance_running(instance_id)
        self.store.append_audit(
            instance_id, None, "INSTANCE_RUNNING",
            f"trigger={inst['trigger_source']} reason=driver acquired lock",
            inst["trigger_source"],
        )
        logger.info("driver start: instance=%s workflow=%s", instance_id, spec.name)
        while not self._shutdown:
            inst = self.store.get_instance(instance_id)
            if inst is None:
                return
            if inst["state"] in _TERMINAL_INSTANCE:
                logger.info(
                    "driver stop: instance=%s reason=terminal state=%s",
                    instance_id, inst["state"],
                )
                return

            now = self._clock()
            if self._past_deadline(inst, now):
                self._finalize_timeout(instance_id, inst, "instance deadline exceeded")
                return

            nodes = {n["node_name"]: n for n in self.store.list_nodes(instance_id)}

            # 1) failure propagation: skip everything behind a FAILED node
            failed = [n for n in nodes.values() if n["state"] == "FAILED"]
            if failed:
                root = sorted(failed, key=lambda n: n["node_name"])[0]
                self._propagate_failure(
                    instance_id, nodes_spec, nodes,
                    root["node_name"], root.get("fail_kind") or "BUSINESS",
                    root.get("error") or "",
                )
                return

            # 2) activate PENDING nodes whose deps have all succeeded
            ready_pending: list[str] = []
            blocked = False
            for name, node in nodes.items():
                if node["state"] != "PENDING":
                    continue
                dep_nodes = nodes_spec[name].deps
                deps_states = [nodes[d]["state"] for d in dep_nodes]
                if all(s == "SUCCEEDED" for s in deps_states):
                    ready_pending.append(name)
                else:
                    blocked = True
            if ready_pending:
                self.store.activate_ready_nodes(instance_id, ready_pending)
                self.store.append_audit(
                    instance_id, None, "READY_BATCH",
                    f"nodes={','.join(sorted(ready_pending))} "
                    "reason=all deps SUCCEEDED", None,
                )
                nodes = {n["node_name"]: n for n in self.store.list_nodes(instance_id)}

            # 3) claim all currently runnable READY nodes (backoff gate open)
            claimed: list[dict[str, Any]] = []
            gated: list[dict[str, Any]] = []
            for node in nodes.values():
                if node["state"] != "READY":
                    continue
                if node["not_before_ts"] <= now:
                    if self.store.claim_node(instance_id, node["node_name"], now):
                        claimed.append(
                            self.store.get_node(instance_id, node["node_name"])
                        )
                else:
                    gated.append(node)

            if claimed:
                failure = self._run_batch(instance_id, inst, nodes_spec, claimed)
                if failure is not None:
                    kind, node_name, error = failure
                    nodes_after = {
                        n["node_name"]: n
                        for n in self.store.list_nodes(instance_id)
                    }
                    self._propagate_failure(
                        instance_id, nodes_spec, nodes_after,
                        node_name, kind, error,
                    )
                    return
                # loop: newly unblocked nodes may now be ready
                continue

            # 4) nothing runnable right now
            latest = self.store.list_nodes(instance_id)
            if all(n["state"] == "SUCCEEDED" for n in latest):
                self.store.finish_instance(instance_id, "SUCCEEDED", "NONE", None)
                self.store.append_audit(
                    instance_id, None, "INSTANCE_SUCCEEDED",
                    "reason=all nodes SUCCEEDED", None,
                )
                logger.info(
                    "instance succeeded: instance=%s workflow=%s",
                    instance_id, spec.name,
                )
                return
            if all(n["state"] in ("SUCCEEDED", "SKIPPED", "FAILED") for n in latest):
                # Defensive: failure path normally finalizes explicitly.
                self.store.finish_instance(
                    instance_id, "FAILED", "BUSINESS",
                    "all nodes settled without success",
                )
                return

            # waiting: backoff gate, or retry deadline
            wait_for = _TICK_SECONDS
            gated_pending = [
                n["not_before_ts"] - now for n in gated if n["not_before_ts"] > now
            ]
            if gated_pending:
                wait_for = min(_TICK_SECONDS, min(gated_pending))
            self._interruptible_sleep(max(wait_for, 0.001), instance_id)

    def _run_batch(
        self,
        instance_id: str,
        inst: dict[str, Any],
        nodes_spec: dict[str, NodeSpec],
        claimed: list[dict[str, Any]],
    ) -> tuple[str, str, str] | None:
        """Run one batch of concurrently claimed nodes.

        Returns ``(fail_kind, node_name, error)`` for the first terminal node
        failure (business error, retries exhausted, or node timeout), else
        ``None`` (every node succeeded or was requeued for retry).
        """
        results: dict[str, Any] = self._gather_results(instance_id)
        futures: dict[Future[Any], tuple[str, int, float | None]] = {}
        for node in claimed:
            name = node["node_name"]
            attempt = node["attempt"]
            spec = nodes_spec[name]
            # Node-level timeout only when explicitly configured; the
            # instance deadline is enforced separately (TIMED_OUT state).
            node_deadline = (
                node["started_at"] + spec.timeout_seconds
                if spec.timeout_seconds is not None
                else None
            )
            ctx = {
                "instance_id": instance_id,
                "workflow": inst["workflow_name"],
                "node": name,
                "attempt": attempt,
                "trigger_source": inst["trigger_source"],
                "window_key": inst["window_key"],
                "results": dict(results),
            }
            self.store.append_audit(
                instance_id, name, "NODE_RUNNING",
                f"attempt={attempt} trigger={inst['trigger_source']}",
                inst["trigger_source"],
            )
            logger.info(
                "node running: instance=%s node=%s attempt=%d source=%s",
                instance_id, name, attempt, inst["trigger_source"],
            )
            fut = self._pool.submit(self.executor.run_once, node["task"], ctx)
            futures[fut] = (name, attempt, node_deadline)

        first_failure: tuple[str, str, str] | None = None
        while futures:
            now = self._clock()
            inst_now = self.store.get_instance(instance_id)
            if self._past_deadline(inst_now, now):
                self._finalize_timeout(
                    instance_id, inst_now, "instance deadline exceeded while running"
                )
                # Driver ends; late worker results are rejected by CAS.
                return None  # type: ignore[return-value]
            done, _ = wait(tuple(futures), timeout=_TICK_SECONDS)
            # Re-check the instance deadline first: instance timeout always
            # wins over a coincident node timeout.
            inst_after_wait = self.store.get_instance(instance_id)
            if self._past_deadline(inst_after_wait, self._clock()):
                self._finalize_timeout(
                    instance_id, inst_after_wait,
                    "instance deadline exceeded while running",
                )
                return None
            for fut in done:
                name, attempt, _node_deadline = futures.pop(fut)
                outcome = self._resolve_future(fut)
                failure = self._apply_outcome(
                    instance_id, nodes_spec[name], attempt, outcome
                )
                if failure is not None and first_failure is None:
                    first_failure = failure
            # node-level timeouts for still-running futures
            now = self._clock()
            for fut, (name, attempt, node_deadline) in list(futures.items()):
                if node_deadline is not None and now >= node_deadline:
                    del futures[fut]
                    err = f"node {name!r} exceeded timeout"
                    try:
                        self.store.fail_node(
                            instance_id, name, err,
                            fail_kind=FailKind.TIMEOUT.value,
                            expected_attempt=attempt,
                        )
                    except SchedulerError:
                        # State already changed (e.g. concurrent settle); late
                        # result will be ignored by CAS.
                        pass
                    self.store.append_audit(
                        instance_id, name, "NODE_FAILED",
                        f"attempt={attempt} fail_kind=TIMEOUT reason={err}",
                        None,
                    )
                    logger.warning(
                        "node timeout: instance=%s node=%s attempt=%d",
                        instance_id, name, attempt,
                    )
                    if first_failure is None:
                        first_failure = (FailKind.TIMEOUT.value, name, err)
        return first_failure

    @staticmethod
    def _resolve_future(fut: Future[Any]) -> _NodeOutcome:
        exc = fut.exception()
        if exc is None:
            return _NodeOutcome("SUCCEEDED", result=fut.result())
        if isinstance(exc, TaskError):
            return _NodeOutcome(
                "RETRY" if exc.retryable else "BUSINESS", error=str(exc)
            )
        return _NodeOutcome("RETRY", error=f"{type(exc).__name__}: {exc}")

    def _apply_outcome(
        self, instance_id: str, node: NodeSpec, attempt: int, outcome: _NodeOutcome
    ) -> tuple[str, str, str] | None:
        name = node.name
        if outcome.kind == "SUCCEEDED":
            try:
                self.store.complete_node(
                    instance_id, name, outcome.result or "null",
                    expected_attempt=attempt,
                )
            except SchedulerError as exc:
                logger.warning(
                    "stale node result ignored: instance=%s node=%s attempt=%d: %s",
                    instance_id, name, attempt, exc,
                )
                return None
            self.store.append_audit(
                instance_id, name, "NODE_SUCCEEDED",
                f"attempt={attempt}", None,
            )
            logger.info("node succeeded: instance=%s node=%s", instance_id, name)
            return None

        if outcome.kind == "RETRY" and attempt <= node.max_retries:
            delay = _backoff_delay(node, attempt)
            not_before = self._clock() + delay
            try:
                self.store.requeue_node(
                    instance_id, name, outcome.error, not_before,
                    expected_attempt=attempt,
                )
            except SchedulerError as exc:
                logger.warning("stale retry ignored: instance=%s node=%s: %s",
                               instance_id, name, exc)
                return None
            self.store.append_audit(
                instance_id, name, "NODE_RETRY",
                f"attempt={attempt} next_attempt={attempt + 1} "
                f"backoff_seconds={delay:.3f} reason=retryable error: "
                f"{outcome.error}", None,
            )
            logger.info(
                "node retry scheduled: instance=%s node=%s attempt=%d "
                "backoff=%.3fs reason=%s",
                instance_id, name, attempt, delay, outcome.error,
            )
            return None

        kind = (
            FailKind.BUSINESS.value
            if outcome.kind == "BUSINESS"
            else FailKind.BUSINESS.value
        )
        if outcome.kind == "RETRY":
            error = f"retries exhausted after {attempt} attempt(s): {outcome.error}"
        else:
            error = outcome.error
        try:
            self.store.fail_node(
                instance_id, name, error,
                fail_kind=kind, expected_attempt=attempt,
            )
        except SchedulerError as exc:
            logger.warning("stale failure ignored: instance=%s node=%s: %s",
                           instance_id, name, exc)
            return None
        self.store.append_audit(
            instance_id, name, "NODE_FAILED",
            f"attempt={attempt} fail_kind=BUSINESS reason={error}", None,
        )
        logger.warning(
            "node failed permanently: instance=%s node=%s attempt=%d reason=%s",
            instance_id, name, attempt, error,
        )
        return FailKind.BUSINESS.value, name, error

    # --------------------------------------------------------------- finalizing
    def _propagate_failure(
        self,
        instance_id: str,
        nodes_spec: dict[str, NodeSpec],
        nodes: dict[str, dict[str, Any]],
        failed_node: str,
        fail_kind: str,
        error: str,
    ) -> None:
        blocked_names = _transitive_dependents(nodes_spec, failed_node)
        downstream_skip = [
            n for n in blocked_names
            if nodes[n]["state"] in ("PENDING", "READY")
        ]
        reason = f"upstream {failed_node!r} failed ({fail_kind}): {error}"
        self.store.mark_nodes_skipped(instance_id, sorted(downstream_skip), reason)
        for name in sorted(downstream_skip):
            self.store.append_audit(
                instance_id, name, "NODE_SKIPPED",
                f"reason={reason}", None,
            )
            logger.info(
                "node skipped: instance=%s node=%s reason=upstream %s failed",
                instance_id, name, failed_node,
            )

        # Everything still unsettled in a now-terminal instance (e.g. a sibling
        # node waiting behind its retry backoff, or another RUNNING batch mate)
        # is closed as SKIPPED so no node is left in a non-terminal state.
        fresh = {n["node_name"]: n for n in self.store.list_nodes(instance_id)}
        fresh = {n["node_name"]: n for n in self.store.list_nodes(instance_id)}
        remaining_waiting = [
            name for name, node in fresh.items()
            if node["state"] in ("PENDING", "READY")
        ]
        remaining_running = [
            name for name, node in fresh.items() if node["state"] == "RUNNING"
        ]
        cancel_reason = f"instance finalizing after {failed_node!r} failure"
        self.store.mark_nodes_skipped(
            instance_id, sorted(remaining_waiting), cancel_reason
        )
        self.store.force_fail_running_nodes(
            instance_id, sorted(remaining_running), cancel_reason,
            fail_kind=FailKind.TIMEOUT.value,
        )
        for name in sorted(remaining_waiting):
            self.store.append_audit(
                instance_id, name, "NODE_SKIPPED",
                f"reason={cancel_reason}", None,
            )
        for name in sorted(remaining_running):
            self.store.append_audit(
                instance_id, name, "NODE_FAILED",
                f"fail_kind=TIMEOUT reason={cancel_reason}", None,
            )
        skipped_all = sorted(
            set(downstream_skip) | set(remaining_waiting) | set(remaining_running)
        )

        state = "FAILED"
        self.store.finish_instance(instance_id, state, fail_kind, error)
        self.store.append_audit(
            instance_id, None, "INSTANCE_FAILED",
            f"fail_kind={fail_kind} failed_node={failed_node} "
            f"skipped={','.join(skipped_all) or '-'} reason={error}",
            None,
        )
        logger.warning(
            "instance failed: instance=%s fail_kind=%s failed_node=%s",
            instance_id, fail_kind, failed_node,
        )

    def _finalize_timeout(
        self, instance_id: str, inst: dict[str, Any], reason: str
    ) -> None:
        # Any node still RUNNING is marked FAILED/TIMEOUT; SUCCEEDED nodes are
        # left untouched, so completed side effects are never duplicated.
        active = [
            n["node_name"]
            for n in self.store.list_nodes(instance_id)
            if n["state"] == "RUNNING"
        ]
        self.store.force_fail_running_nodes(
            instance_id, active, reason, FailKind.TIMEOUT.value
        )
        for name in active:
            self.store.append_audit(
                instance_id, name, "NODE_FAILED",
                f"fail_kind=TIMEOUT reason={reason}", None,
            )
        self.store.mark_nodes_skipped(
            instance_id,
            [n["node_name"] for n in self.store.list_nodes(instance_id)
             if n["state"] in ("PENDING", "READY")],
            f"instance timed out: {reason}",
        )
        self.store.finish_instance(
            instance_id, "TIMED_OUT", FailKind.TIMEOUT.value, reason
        )
        self.store.append_audit(
            instance_id, None, "INSTANCE_TIMED_OUT",
            f"fail_kind=TIMEOUT reason={reason}", None,
        )
        logger.warning("instance timed out: instance=%s reason=%s",
                       instance_id, reason)

    # ----------------------------------------------------------------- recovery
    def recover(self) -> list[str]:
        """Resume unfinished instances after a process restart.

        Stale locks (held by the dead process) are cleared; nodes left
        RUNNING by the crash are reset to READY and retried, while
        SUCCEEDED nodes stay SUCCEEDED — no step runs twice.
        """
        self.store.force_clear_all_locks()
        ids = self.store.list_unfinished_instances()
        recovered: list[str] = []
        for instance_id in ids:
            reset = self.store.reset_interrupted_nodes(instance_id)
            inst = self.store.get_instance(instance_id)
            self.store.append_audit(
                instance_id, None, "RECOVERY",
                f"source=RECOVERY reset_running={','.join(reset) or '-'} "
                f"state={inst['state']} reason=process recovery",
                TriggerSource.RECOVERY.value,
            )
            logger.info(
                "recovery: instance=%s reset_nodes=%s", instance_id, reset,
            )
            recovered.append(instance_id)
        for instance_id in recovered:
            self._spawn_driver(instance_id)
        return recovered

    # ------------------------------------------------------------ timeout sweep
    def sweep_timeouts(self) -> list[str]:
        """Mark deadline-exceeded unfinished instances.

        An instance already being driven is handled by its own driver; here we
        only drive unclaimed ones (e.g. nothing triggered them this process).
        """
        now = self._clock()
        timed_out = self.store.list_timed_out_instances(now)
        marked: list[str] = []
        for instance_id in timed_out:
            if not self.store.try_lock_instance(instance_id):
                continue  # active driver will time it out on its next tick
            try:
                inst = self.store.get_instance(instance_id)
                if inst is None or inst["state"] in _TERMINAL_INSTANCE:
                    continue
                self._finalize_timeout(
                    instance_id, inst,
                    "instance deadline exceeded (swept by monitor)",
                )
                marked.append(instance_id)
            finally:
                # No driver owns it; release immediately.
                if instance_id not in self._drivers:
                    self.store.unlock_instance(instance_id)
        return marked

    def _timeout_monitor(self) -> None:
        while not self._shutdown:
            try:
                self.sweep_timeouts()
            except Exception:
                logger.exception("timeout sweep error")
            with self._wake:
                self._wake.wait(timeout=self._timeout_poll)

    # ------------------------------------------------------------------ status
    def get_status(self, instance_id: str) -> dict[str, Any]:
        inst = self.store.get_instance(instance_id)
        if inst is None:
            raise WorkflowNotFound(f"instance {instance_id!r} not found")
        nodes = self.store.list_nodes(instance_id)
        audit = self.store.list_audit(instance_id)
        node_summaries = [
            {
                "node": n["node_name"],
                "state": n["state"],
                "attempt": n["attempt"],
                "fail_kind": n["fail_kind"] if n["state"] == "FAILED" else "NONE",
                "error": n["error"],
            }
            for n in nodes
        ]
        state = inst["state"]
        return {
            "instance_id": instance_id,
            "workflow": inst["workflow_name"],
            "state": state,
            "terminal": state in _TERMINAL_INSTANCE,
            "fail_kind": inst["fail_kind"],
            "error": inst["error"],
            "trigger_source": inst["trigger_source"],
            "window_key": inst["window_key"],
            "deadline_ts": inst["deadline_ts"],
            "nodes": node_summaries,
            "audit": [
                {
                    "seq": a["id"],
                    "node": a["node_name"],
                    "event": a["event"],
                    "detail": a["detail"],
                    "trigger_source": a["trigger_source"],
                }
                for a in audit
            ],
        }

    def wait_until_terminal(self, instance_id: str, timeout: float = 10.0) -> dict[str, Any]:
        """Test/operational helper: block until a stable terminal state."""
        deadline = self._clock() + timeout
        while self._clock() < deadline:
            status = self.get_status(instance_id)
            if status["terminal"]:
                return status
            time.sleep(0.01)
        raise TimeoutError(f"instance {instance_id} did not finish within {timeout}s")

    # ------------------------------------------------------------------- utils
    def _require_workflow(self, name: str) -> WorkflowSpec:
        spec = self.store.get_workflow(name)
        if spec is None:
            raise WorkflowNotFound(f"workflow {name!r} is not registered")
        return spec

    def _spec_of(self, instance_id: str) -> WorkflowSpec:
        inst = self.store.get_instance(instance_id)
        return self._require_workflow(inst["workflow_name"])

    def _deadline(self, spec: WorkflowSpec) -> float | None:
        if spec.instance_timeout_seconds is None:
            return None
        return self._clock() + spec.instance_timeout_seconds

    def _past_deadline(self, inst: dict[str, Any], now: float) -> bool:
        return inst["deadline_ts"] is not None and now >= inst["deadline_ts"]

    def _gather_results(self, instance_id: str) -> dict[str, Any]:
        import json

        results: dict[str, Any] = {}
        for n in self.store.list_nodes(instance_id):
            if n["state"] == "SUCCEEDED" and n["result_json"] is not None:
                try:
                    results[n["node_name"]] = json.loads(n["result_json"])
                except (ValueError, TypeError):
                    results[n["node_name"]] = n["result_json"]
        return results

    def _interruptible_sleep(self, seconds: float, instance_id: str) -> None:
        end = self._clock() + seconds
        while not self._shutdown:
            remaining = end - self._clock()
            if remaining <= 0:
                return
            inst = self.store.get_instance(instance_id)
            if inst is None or inst["state"] in _TERMINAL_INSTANCE:
                return
            time.sleep(min(0.02, remaining))

    def shutdown(self, wait: bool = True) -> None:
        self._shutdown = True
        with self._wake:
            self._wake.notify_all()
        if wait:
            for thread in list(self._driver_threads):
                if thread.is_alive() and thread is not threading.current_thread():
                    thread.join(timeout=3.0)
            self._monitor.join(timeout=1.0)
        self._pool.shutdown(wait=False, cancel_futures=True)
