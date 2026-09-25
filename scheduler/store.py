"""SQLite-backed durable state store.

Persistence model
------------------
* ``workflows``  : registered workflow definitions (spec as JSON)
* ``instances``  : one row per workflow run; terminal states are stable
* ``nodes``      : one row per (instance, node); CAS updates guard against
                   double advancement by concurrent workers / re-entrant calls
* ``audit_log``  : append-only, explainable state-transition trace
* ``instance_locks`` : durable advisory locks so that only one advancer at a
                   time drives an instance (also across threads/processes)

Wall-clock time (``time.time``) is stored, so backoff deadlines and instance
timeouts remain meaningful after a process restart.  A clock callable can be
injected for deterministic tests.
"""
from __future__ import annotations

import json
import os
import sqlite3
import threading
import uuid
from collections.abc import Callable
from typing import Any

from .errors import ValidationError
from .models import NodeSpec, WorkflowSpec

_SCHEMA = """
CREATE TABLE IF NOT EXISTS workflows (
    name        TEXT PRIMARY KEY,
    spec_json   TEXT NOT NULL,
    created_at  REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS instances (
    id                TEXT PRIMARY KEY,
    workflow_name     TEXT NOT NULL REFERENCES workflows(name),
    state             TEXT NOT NULL,
    trigger_source    TEXT NOT NULL,
    window_key        TEXT,
    fail_kind         TEXT NOT NULL DEFAULT 'NONE',
    error             TEXT,
    deadline_ts       REAL,
    created_at        REAL NOT NULL,
    updated_at        REAL NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS ux_instances_window
    ON instances(workflow_name, window_key) WHERE window_key IS NOT NULL;
CREATE TABLE IF NOT EXISTS nodes (
    instance_id      TEXT NOT NULL REFERENCES instances(id),
    node_name        TEXT NOT NULL,
    task             TEXT NOT NULL,
    deps_json        TEXT NOT NULL DEFAULT '[]',
    state            TEXT NOT NULL,
    attempt          INTEGER NOT NULL DEFAULT 0,
    max_retries      INTEGER NOT NULL DEFAULT 0,
    backoff_seconds  REAL NOT NULL DEFAULT 0,
    timeout_seconds  REAL,
    not_before_ts    REAL NOT NULL DEFAULT 0,
    result_json      TEXT,
    error            TEXT,
    fail_kind        TEXT NOT NULL DEFAULT 'BUSINESS',
    started_at       REAL,
    finished_at      REAL,
    PRIMARY KEY (instance_id, node_name)
);
CREATE TABLE IF NOT EXISTS audit_log (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    instance_id     TEXT NOT NULL,
    node_name       TEXT,
    event           TEXT NOT NULL,
    detail          TEXT NOT NULL DEFAULT '',
    trigger_source  TEXT,
    ts              REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS instance_locks (
    instance_id  TEXT PRIMARY KEY,
    holder       TEXT NOT NULL,
    acquired_at  REAL NOT NULL
);
"""

_UNFINISHED_STATES = ("PENDING", "RUNNING")
_TERMINAL_STATES = ("SUCCEEDED", "FAILED", "TIMED_OUT")


def _spec_to_json(spec: WorkflowSpec) -> str:
    return json.dumps(
        {
            "name": spec.name,
            "instance_timeout_seconds": spec.instance_timeout_seconds,
            "nodes": [
                {
                    "name": n.name,
                    "task": n.task,
                    "deps": list(n.deps),
                    "max_retries": n.max_retries,
                    "retry_backoff_seconds": n.retry_backoff_seconds,
                    "retry_backoff_max_seconds": n.retry_backoff_max_seconds,
                    "timeout_seconds": n.timeout_seconds,
                }
                for n in spec.nodes
            ],
        },
        sort_keys=True,
    )


def _spec_from_json(raw: str) -> WorkflowSpec:
    data = json.loads(raw)
    return WorkflowSpec(
        name=data["name"],
        instance_timeout_seconds=data.get("instance_timeout_seconds"),
        nodes=tuple(
            NodeSpec(
                name=n["name"],
                task=n["task"],
                deps=tuple(n.get("deps", [])),
                max_retries=n.get("max_retries", 0),
                retry_backoff_seconds=n.get("retry_backoff_seconds", 0.0),
                retry_backoff_max_seconds=n.get("retry_backoff_max_seconds", 60.0),
                timeout_seconds=n.get("timeout_seconds"),
            )
            for n in data["nodes"]
        ),
    )


class Store:
    """Thread-safe SQLite wrapper. All writes are serialized by a process lock."""

    def __init__(
        self,
        path: str = ":memory:",
        clock: Callable[[], float] | None = None,
    ) -> None:
        self.path = path
        self._clock = clock or __import__("time").time
        self._lock = threading.RLock()
        self._closed = False
        self._conn = sqlite3.connect(path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        with self._lock:
            self._conn.executescript(_SCHEMA)
            self._conn.commit()
        self.holder_id = f"{os.getpid()}:{uuid.uuid4().hex[:8]}"

    # ------------------------------------------------------------------ utils
    def _now(self) -> float:
        return self._clock()

    @property
    def closed(self) -> bool:
        return self._closed

    def _begin(self) -> None:
        self._conn.execute("BEGIN IMMEDIATE")

    @staticmethod
    def new_instance_id() -> str:
        return uuid.uuid4().hex

    def close(self) -> None:
        with self._lock:
            if self._closed:
                return
            self._closed = True
            self._conn.close()

    # -------------------------------------------------------------- workflows
    def register_workflow(self, spec: WorkflowSpec) -> None:
        raw = _spec_to_json(spec)
        now = self._now()
        with self._lock:
            row = self._conn.execute(
                "SELECT spec_json FROM workflows WHERE name=?", (spec.name,)
            ).fetchone()
            if row is not None and row["spec_json"] != raw:
                raise ValidationError(
                    f"workflow {spec.name!r} already registered with a "
                    "different definition"
                )
            if row is None:
                self._conn.execute(
                    "INSERT INTO workflows(name, spec_json, created_at) VALUES(?,?,?)",
                    (spec.name, raw, now),
                )
                self._conn.commit()

    def get_workflow(self, name: str) -> WorkflowSpec | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT spec_json FROM workflows WHERE name=?", (name,)
            ).fetchone()
        return _spec_from_json(row["spec_json"]) if row else None

    # -------------------------------------------------------------- instances
    def create_instance(
        self,
        workflow_name: str,
        trigger_source: str,
        window_key: str | None,
        deadline_ts: float | None,
    ) -> str:
        """Create an instance plus its node rows. Caller guarantees no duplicate
        window key (manual triggers use ``window_key=None``)."""
        spec = self.get_workflow(workflow_name)
        if spec is None:
            raise ValidationError(f"workflow {workflow_name!r} is not registered")
        instance_id = self.new_instance_id()
        now = self._now()
        with self._lock:
            self._begin()
            try:
                self._conn.execute(
                    """INSERT INTO instances(id, workflow_name, state,
                           trigger_source, window_key, fail_kind, deadline_ts,
                           created_at, updated_at)
                       VALUES(?,?,?,?,?,?,?,?,?)""",
                    (
                        instance_id,
                        workflow_name,
                        "PENDING",
                        trigger_source,
                        window_key,
                        "NONE",
                        deadline_ts,
                        now,
                        now,
                    ),
                )
                self._conn.executemany(
                    """INSERT INTO nodes(instance_id, node_name, task, deps_json,
                           state, max_retries, backoff_seconds, timeout_seconds,
                           not_before_ts)
                       VALUES(?,?,?,?,?,?,?,?,?)""",
                    [
                        (
                            instance_id,
                            n.name,
                            n.task,
                            json.dumps(list(n.deps)),
                            "PENDING",
                            n.max_retries,
                            n.retry_backoff_seconds,
                            n.timeout_seconds,
                            0.0,
                        )
                        for n in spec.nodes
                    ],
                )
                self._conn.commit()
            except Exception:
                self._conn.rollback()
                raise
        return instance_id

    def get_or_create_scheduled_instance(
        self,
        workflow_name: str,
        trigger_source: str,
        window_key: str,
        deadline_ts: float | None,
    ) -> tuple[str, bool]:
        """Idempotent per-(workflow, window) instance creation.

        Returns ``(instance_id, created)``. Overlapping backfill/schedule
        windows resolve to the same instance, so a window is never executed
        twice. In-process callers are serialized by the store lock;
        cross-process races are settled by the partial unique index.
        """
        with self._lock:
            row = self._conn.execute(
                "SELECT id FROM instances WHERE workflow_name=? AND window_key=?",
                (workflow_name, window_key),
            ).fetchone()
            if row is not None:
                return row["id"], False
            try:
                instance_id = self.create_instance(
                    workflow_name, trigger_source, window_key, deadline_ts
                )
            except sqlite3.IntegrityError:
                row = self._conn.execute(
                    "SELECT id FROM instances WHERE workflow_name=? AND window_key=?",
                    (workflow_name, window_key),
                ).fetchone()
                return row["id"], False
            return instance_id, True

    def get_instance(self, instance_id: str) -> dict[str, Any] | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM instances WHERE id=?", (instance_id,)
            ).fetchone()
        return dict(row) if row else None

    def list_instances(self, workflow_name: str | None = None) -> list[dict[str, Any]]:
        sql = "SELECT * FROM instances"
        args: tuple[Any, ...] = ()
        if workflow_name:
            sql += " WHERE workflow_name=?"
            args = (workflow_name,)
        sql += " ORDER BY created_at, id"
        with self._lock:
            rows = self._conn.execute(sql, args).fetchall()
        return [dict(r) for r in rows]

    def list_unfinished_instances(self) -> list[str]:
        with self._lock:
            rows = self._conn.execute(
                f"SELECT id FROM instances WHERE state IN {_UNFINISHED_STATES}"
            ).fetchall()
        return [r["id"] for r in rows]

    def list_timed_out_instances(self, now_ts: float) -> list[str]:
        with self._lock:
            rows = self._conn.execute(
                f"""SELECT id FROM instances
                    WHERE state IN {_UNFINISHED_STATES}
                      AND deadline_ts IS NOT NULL AND deadline_ts <= ?""",
                (now_ts,),
            ).fetchall()
        return [r["id"] for r in rows]

    def finish_instance(
        self,
        instance_id: str,
        state: str,
        fail_kind: str,
        error: str | None,
    ) -> None:
        if state not in _TERMINAL_STATES:
            raise ValueError(f"finish_instance called with non-terminal state {state}")
        now = self._now()
        with self._lock:
            self._conn.execute(
                """UPDATE instances SET state=?, fail_kind=?, error=?, updated_at=?
                   WHERE id=? AND state IN ('PENDING','RUNNING')""",
                (state, fail_kind, error, now, instance_id),
            )
            self._conn.commit()

    def mark_instance_running(self, instance_id: str) -> None:
        now = self._now()
        with self._lock:
            self._conn.execute(
                """UPDATE instances SET state='RUNNING', updated_at=?
                   WHERE id=? AND state='PENDING'""",
                (now, instance_id),
            )
            self._conn.commit()

    # ------------------------------------------------------------------ nodes
    def list_nodes(self, instance_id: str) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM nodes WHERE instance_id=? ORDER BY rowid",
                (instance_id,),
            ).fetchall()
        return [dict(r) for r in rows]

    def get_node(self, instance_id: str, node_name: str) -> dict[str, Any] | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM nodes WHERE instance_id=? AND node_name=?",
                (instance_id, node_name),
            ).fetchone()
        return dict(row) if row else None

    def activate_ready_nodes(self, instance_id: str, ready_names: list[str]) -> None:
        """PENDING -> READY for nodes whose deps have all succeeded."""
        if not ready_names:
            return
        now = self._now()
        with self._lock:
            self._conn.executemany(
                """UPDATE nodes SET state='READY', not_before_ts=?, error=NULL
                   WHERE instance_id=? AND node_name=? AND state='PENDING'""",
                [(0.0, instance_id, name) for name in ready_names],
            )
            self._conn.commit()

    def claim_node(self, instance_id: str, node_name: str, now_ts: float) -> bool:
        """CAS: READY (and backoff elapsed) -> RUNNING.

        Returns True only for the single caller that wins, guaranteeing a node
        body is never executed twice concurrently or after completion.
        """
        with self._lock:
            cur = self._conn.execute(
                """UPDATE nodes
                      SET state='RUNNING', attempt=attempt+1, started_at=?,
                          error=NULL
                    WHERE instance_id=? AND node_name=?
                      AND state='READY' AND not_before_ts <= ?""",
                (now_ts, instance_id, node_name, now_ts),
            )
            self._conn.commit()
            return cur.rowcount == 1

    def complete_node(
        self,
        instance_id: str,
        node_name: str,
        result_json: str,
        expected_attempt: int | None = None,
    ) -> None:
        """CAS: RUNNING -> SUCCEEDED. A completed node is final."""
        now = self._now()
        sql = """UPDATE nodes SET state='SUCCEEDED', result_json=?,
                        finished_at=?, error=NULL
                  WHERE instance_id=? AND node_name=? AND state='RUNNING'"""
        args: list[Any] = [result_json, now, instance_id, node_name]
        if expected_attempt is not None:
            sql += " AND attempt=?"
            args.append(expected_attempt)
        with self._lock:
            cur = self._conn.execute(sql, args)
            self._conn.commit()
        if cur.rowcount == 0:
            raise ValidationError(
                f"node {node_name!r} cannot be completed: not RUNNING"
                + (" or attempt changed" if expected_attempt is not None else "")
            )

    def requeue_node(
        self,
        instance_id: str,
        node_name: str,
        error: str,
        not_before_ts: float,
        expected_attempt: int | None = None,
    ) -> None:
        """CAS: RUNNING -> READY with backoff gate (retryable failure)."""
        now = self._now()
        sql = """UPDATE nodes SET state='READY', error=?, not_before_ts=?
                 WHERE instance_id=? AND node_name=? AND state='RUNNING'"""
        args: list[Any] = [error, not_before_ts, instance_id, node_name]
        if expected_attempt is not None:
            sql += " AND attempt=?"
            args.append(expected_attempt)
        with self._lock:
            cur = self._conn.execute(sql, args)
            self._conn.commit()
        if cur.rowcount == 0:
            raise ValidationError(
                f"node {node_name!r} cannot be requeued: not RUNNING"
                + (" or attempt changed" if expected_attempt is not None else "")
            )

    def fail_node(
        self,
        instance_id: str,
        node_name: str,
        error: str,
        fail_kind: str = "BUSINESS",
        expected_attempt: int | None = None,
    ) -> None:
        """CAS: RUNNING -> FAILED (non-retryable or retries exhausted).

        ``expected_attempt`` guards against a late worker result landing after
        the node was reset/reclaimed during recovery.
        """
        now = self._now()
        sql = """UPDATE nodes SET state='FAILED', error=?, fail_kind=?, finished_at=?
                 WHERE instance_id=? AND node_name=? AND state='RUNNING'"""
        args: list[Any] = [error, fail_kind, now, instance_id, node_name]
        if expected_attempt is not None:
            sql += " AND attempt=?"
            args.append(expected_attempt)
        with self._lock:
            cur = self._conn.execute(sql, args)
            self._conn.commit()
        if cur.rowcount == 0:
            raise ValidationError(
                f"node {node_name!r} cannot be failed: not RUNNING"
                + (" or attempt changed" if expected_attempt is not None else "")
            )

    def mark_nodes_skipped(self, instance_id: str, names: list[str], reason: str) -> None:
        """PENDING/READY -> SKIPPED because an upstream failed."""
        if not names:
            return
        with self._lock:
            self._conn.executemany(
                """UPDATE nodes SET state='SKIPPED', error=?
                    WHERE instance_id=? AND node_name=?
                      AND state IN ('PENDING','READY')""",
                [(reason, instance_id, n) for n in names],
            )
            self._conn.commit()

    def force_fail_running_nodes(
        self,
        instance_id: str,
        names: list[str],
        error: str,
        fail_kind: str = "TIMEOUT",
    ) -> None:
        """RUNNING -> FAILED (used by timeout finalization / crash recovery).

        Unlike :meth:`fail_node` this is not attempt-guarded: the driver owns
        the instance lock and the worker's late result is ignored by the CAS.
        """
        if not names:
            return
        now = self._now()
        with self._lock:
            self._conn.executemany(
                """UPDATE nodes SET state='FAILED', error=?, fail_kind=?,
                          finished_at=?
                    WHERE instance_id=? AND node_name=? AND state='RUNNING'""",
                [(error, fail_kind, now, instance_id, n) for n in names],
            )
            self._conn.commit()

    def reset_interrupted_nodes(self, instance_id: str) -> list[str]:
        """Recovery: nodes left RUNNING by a crashed process -> READY.

        SUCCEEDED nodes are untouched, so completed steps never re-execute.
        """
        now = self._now()
        with self._lock:
            rows = self._conn.execute(
                """SELECT node_name FROM nodes
                    WHERE instance_id=? AND state='RUNNING'""",
                (instance_id,),
            ).fetchall()
            names = [r["node_name"] for r in rows]
            self._conn.executemany(
                """UPDATE nodes SET state='READY', error='interrupted by restart',
                          not_before_ts=?
                    WHERE instance_id=? AND node_name=? AND state='RUNNING'""",
                [(now, instance_id, n) for n in names],
            )
            # An instance that died mid-flight goes back to RUNNING (not
            # terminal) so recovery will drive it forward.
            self._conn.execute(
                """UPDATE instances SET state='RUNNING', updated_at=?
                    WHERE id=? AND state IN ('PENDING','RUNNING')""",
                (now, instance_id),
            )
            self._conn.commit()
        return names

    # ------------------------------------------------------------------ locks
    def try_lock_instance(self, instance_id: str) -> bool:
        now = self._now()
        with self._lock:
            try:
                self._conn.execute(
                    "INSERT INTO instance_locks(instance_id, holder, acquired_at) VALUES(?,?,?)",
                    (instance_id, self.holder_id, now),
                )
                self._conn.commit()
                return True
            except sqlite3.IntegrityError:
                return False

    def unlock_instance(self, instance_id: str) -> None:
        with self._lock:
            if self._closed:
                # Process is shutting down; stale locks are cleared by the
                # next process in recover().
                return
            self._conn.execute(
                "DELETE FROM instance_locks WHERE instance_id=? AND holder=?",
                (instance_id, self.holder_id),
            )
            self._conn.commit()

    def force_clear_all_locks(self) -> None:
        """Used at process startup: previous holder processes are gone."""
        with self._lock:
            self._conn.execute("DELETE FROM instance_locks")
            self._conn.commit()

    # ------------------------------------------------------------------ audit
    def append_audit(
        self,
        instance_id: str,
        node_name: str | None,
        event: str,
        detail: str,
        trigger_source: str | None = None,
    ) -> None:
        now = self._now()
        with self._lock:
            self._conn.execute(
                """INSERT INTO audit_log(instance_id, node_name, event, detail,
                          trigger_source, ts) VALUES(?,?,?,?,?,?)""",
                (instance_id, node_name, event, detail, trigger_source, now),
            )
            self._conn.commit()

    def list_audit(self, instance_id: str) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(
                """SELECT id, instance_id, node_name, event, detail,
                          trigger_source, ts
                     FROM audit_log WHERE instance_id=? ORDER BY id""",
                (instance_id,),
            ).fetchall()
        return [dict(r) for r in rows]
