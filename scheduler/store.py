"""基于 SQLite 的本地持久化状态存储。

所有写操作在事务内完成，保证并发触发与进程重启后状态一致。
"""
from __future__ import annotations

import sqlite3
import threading

from .models import (
    RunInstance,
    RunState,
    TaskRun,
    TaskState,
    TERMINAL_RUN_STATES,
    TriggerSource,
)

_SCHEMA = """
CREATE TABLE IF NOT EXISTS runs (
    run_id TEXT PRIMARY KEY,
    workflow_name TEXT NOT NULL,
    window TEXT NOT NULL,
    source TEXT NOT NULL,
    state TEXT NOT NULL,
    created_at REAL NOT NULL,
    deadline REAL NOT NULL,
    finished_at REAL,
    failure_reason TEXT,
    UNIQUE (workflow_name, window)
);
CREATE TABLE IF NOT EXISTS task_runs (
    run_id TEXT NOT NULL,
    task_name TEXT NOT NULL,
    state TEXT NOT NULL,
    attempts INTEGER NOT NULL,
    next_attempt_at REAL,
    error TEXT,
    error_kind TEXT,
    started_at REAL,
    finished_at REAL,
    PRIMARY KEY (run_id, task_name)
);
"""


class StateStore:
    """线程安全的 SQLite 状态存储。"""

    def __init__(self, db_path: str = ":memory:") -> None:
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        with self._lock, self._conn:
            self._conn.executescript(_SCHEMA)

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    # ---- 写接口 ----
    def insert_run_if_absent(self, run: RunInstance, tasks: list[TaskRun]) -> bool:
        """幂等插入实例及其任务；同 (workflow, window) 已存在时返回 False。"""
        with self._lock, self._conn:
            cur = self._conn.execute(
                "INSERT OR IGNORE INTO runs VALUES (?,?,?,?,?,?,?,?,?)",
                (run.run_id, run.workflow_name, run.window, run.source.value,
                 run.state.value, run.created_at, run.deadline,
                 run.finished_at, run.failure_reason),
            )
            if cur.rowcount == 0:
                return False
            for t in tasks:
                self._conn.execute(
                    "INSERT INTO task_runs VALUES (?,?,?,?,?,?,?,?,?)",
                    (t.run_id, t.task_name, t.state.value, t.attempts,
                     t.next_attempt_at, t.error, t.error_kind,
                     t.started_at, t.finished_at),
                )
            return True

    def save_task(self, task: TaskRun) -> None:
        with self._lock, self._conn:
            self._conn.execute(
                "UPDATE task_runs SET state=?, attempts=?, next_attempt_at=?,"
                " error=?, error_kind=?, started_at=?, finished_at=?"
                " WHERE run_id=? AND task_name=?",
                (task.state.value, task.attempts, task.next_attempt_at,
                 task.error, task.error_kind, task.started_at, task.finished_at,
                 task.run_id, task.task_name),
            )

    def save_run(self, run: RunInstance) -> None:
        with self._lock, self._conn:
            self._conn.execute(
                "UPDATE runs SET state=?, finished_at=?, failure_reason=?"
                " WHERE run_id=?",
                (run.state.value, run.finished_at, run.failure_reason, run.run_id),
            )

    # ---- 读接口 ----
    def get_run(self, run_id: str) -> RunInstance | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM runs WHERE run_id=?", (run_id,)).fetchone()
        return self._to_run(row) if row else None

    def get_run_by_window(self, workflow_name: str, window: str) -> RunInstance | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM runs WHERE workflow_name=? AND window=?",
                (workflow_name, window)).fetchone()
        return self._to_run(row) if row else None

    def list_tasks(self, run_id: str) -> list[TaskRun]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM task_runs WHERE run_id=? ORDER BY task_name",
                (run_id,)).fetchall()
        return [self._to_task(r) for r in rows]

    def list_unfinished_runs(self) -> list[RunInstance]:
        terminal = tuple(s.value for s in TERMINAL_RUN_STATES)
        with self._lock:
            rows = self._conn.execute(
                f"SELECT * FROM runs WHERE state NOT IN ({','.join('?' * len(terminal))})",
                terminal).fetchall()
        return [self._to_run(r) for r in rows]

    # ---- 行转换 ----
    @staticmethod
    def _to_run(row: sqlite3.Row) -> RunInstance:
        return RunInstance(
            run_id=row["run_id"], workflow_name=row["workflow_name"],
            window=row["window"], source=TriggerSource(row["source"]),
            state=RunState(row["state"]), created_at=row["created_at"],
            deadline=row["deadline"], finished_at=row["finished_at"],
            failure_reason=row["failure_reason"],
        )

    @staticmethod
    def _to_task(row: sqlite3.Row) -> TaskRun:
        return TaskRun(
            run_id=row["run_id"], task_name=row["task_name"],
            state=TaskState(row["state"]), attempts=row["attempts"],
            next_attempt_at=row["next_attempt_at"], error=row["error"],
            error_kind=row["error_kind"], started_at=row["started_at"],
            finished_at=row["finished_at"],
        )
