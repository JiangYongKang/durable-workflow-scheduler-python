"""基于 SQLite 的本地持久化状态存储。

所有写操作在事务内完成，保证并发触发与进程重启后状态一致。
"""
from __future__ import annotations

import sqlite3
import threading
from collections.abc import Callable

from .models import (
    RunInstance,
    RunState,
    TaskRun,
    TaskState,
    TERMINAL_RUN_STATES,
    TERMINAL_TASK_STATES,
    TriggerSource,
)

TERMINAL_VALUES = tuple(s.value for s in TERMINAL_RUN_STATES)
TERMINAL_TASK_VALUES = tuple(s.value for s in TERMINAL_TASK_STATES)

_SCHEMA = """
CREATE TABLE IF NOT EXISTS runs (
    run_id TEXT PRIMARY KEY,
    workflow_name TEXT NOT NULL,
    window TEXT NOT NULL,
    source TEXT NOT NULL,
    attempt INTEGER NOT NULL DEFAULT 1,
    state TEXT NOT NULL,
    created_at REAL NOT NULL,
    deadline REAL NOT NULL,
    finished_at REAL,
    failure_reason TEXT,
    UNIQUE (workflow_name, window, attempt)
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

# 旧版 runs 表缺少 attempt 列且唯一约束为 (workflow_name, window)，
# 这里做一次性就地迁移：重建表并迁移数据（旧数据 attempt 记为 1）。
_MIGRATE_RUNS = """
ALTER TABLE runs RENAME TO runs_old;
""" + _SCHEMA.split("CREATE TABLE IF NOT EXISTS task_runs")[0] + """
INSERT INTO runs (run_id, workflow_name, window, source, attempt, state,
                  created_at, deadline, finished_at, failure_reason)
SELECT run_id, workflow_name, window, source, 1, state,
       created_at, deadline, finished_at, failure_reason FROM runs_old;
DROP TABLE runs_old;
"""


class StateStore:
    """线程安全的 SQLite 状态存储。"""

    def __init__(self, db_path: str = ":memory:") -> None:
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        with self._lock, self._conn:
            # 旧库迁移：runs 表无 attempt 列时重建
            cols = {r[1] for r in
                    self._conn.execute("PRAGMA table_info(runs)")}
            if cols and "attempt" not in cols:
                self._conn.executescript(_MIGRATE_RUNS)
            self._conn.executescript(_SCHEMA)

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    # ---- 写接口 ----
    def insert_run_if_absent(self, run: RunInstance, tasks: list[TaskRun]) -> bool:
        """幂等插入实例及其任务；同 (workflow, window, attempt) 已存在时返回 False。"""
        with self._lock, self._conn:
            cur = self._conn.execute(
                "INSERT OR IGNORE INTO runs VALUES (?,?,?,?,?,?,?,?,?,?)",
                (run.run_id, run.workflow_name, run.window, run.source.value,
                 run.attempt, run.state.value, run.created_at, run.deadline,
                 run.finished_at, run.failure_reason),
            )
            if cur.rowcount == 0:
                return False
            self._insert_tasks(tasks)
            return True

    def insert_next_attempt(self, run: RunInstance, tasks: list[TaskRun],
                            run_id_for_attempt: Callable[[int], str],
                            ) -> RunInstance | None:
        """重跑：原子地插入同一窗口的下一次尝试。

        在单个事务内先读最新一次尝试：
        - 最新尝试仍在进行（非终态）→ 不插入，返回 None（调用方去重）；
        - 否则分配 attempt = 最新 + 1，由 run_id_for_attempt 生成 run_id
          并写入新实例；并发重跑在 store 锁内串行，只有第一个能插入成功。
        """
        with self._lock, self._conn:
            row = self._conn.execute(
                "SELECT * FROM runs WHERE workflow_name=? AND window=?"
                " ORDER BY attempt DESC LIMIT 1",
                (run.workflow_name, run.window)).fetchone()
            if row is not None and row["state"] not in TERMINAL_VALUES:
                return None
            run.attempt = 1 if row is None else row["attempt"] + 1
            run.run_id = run_id_for_attempt(run.attempt)
            for t in tasks:
                t.run_id = run.run_id
            cur = self._conn.execute(
                "INSERT OR IGNORE INTO runs VALUES (?,?,?,?,?,?,?,?,?,?)",
                (run.run_id, run.workflow_name, run.window, run.source.value,
                 run.attempt, run.state.value, run.created_at, run.deadline,
                 run.finished_at, run.failure_reason),
            )
            if cur.rowcount == 0:  # 极端情况下 run_id 冲突，视为未插入
                return None
            self._insert_tasks(tasks)
            return run

    def _insert_tasks(self, tasks: list[TaskRun]) -> None:
        for t in tasks:
            self._conn.execute(
                "INSERT INTO task_runs VALUES (?,?,?,?,?,?,?,?,?)",
                (t.run_id, t.task_name, t.state.value, t.attempts,
                 t.next_attempt_at, t.error, t.error_kind,
                 t.started_at, t.finished_at),
            )

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

    def finalize_run(self, run: RunInstance) -> bool:
        """仅当实例仍为 RUNNING 时写入终态（超时/成功/失败）。

        与并发的作废操作互斥：若实例已被作废（或暂停），返回 False，
        调用方不得覆盖其状态。
        """
        with self._lock, self._conn:
            cur = self._conn.execute(
                "UPDATE runs SET state=?, finished_at=?, failure_reason=?"
                " WHERE run_id=? AND state=?",
                (run.state.value, run.finished_at, run.failure_reason,
                 run.run_id, RunState.RUNNING.value),
            )
            return cur.rowcount > 0

    # ---- 运维操作（原子写入，不经过引擎主锁，慢任务执行中也能立即生效）----
    def cancel_run(self, run_id: str, now: float, reason: str) -> RunInstance | None:
        """作废实例：非终态任务置 CANCELLED，实例置 CANCELLED（终态）。

        单事务完成；已终态的实例不做任何修改并返回 None。
        已成功/已失败等终态任务保持原样，不回滚。
        """
        with self._lock, self._conn:
            row = self._conn.execute(
                "SELECT * FROM runs WHERE run_id=?", (run_id,)).fetchone()
            if row is None or row["state"] in TERMINAL_VALUES:
                return None
            placeholders = ",".join("?" * len(TERMINAL_TASK_VALUES))
            self._conn.execute(
                f"UPDATE task_runs SET state=?, error_kind='cancelled',"
                f" error=?, finished_at=? WHERE run_id=?"
                f" AND state NOT IN ({placeholders})",
                (TaskState.CANCELLED.value, reason, now, run_id,
                 *TERMINAL_TASK_VALUES),
            )
            self._conn.execute(
                "UPDATE runs SET state=?, finished_at=?, failure_reason=?"
                " WHERE run_id=?",
                (RunState.CANCELLED.value, now, reason, run_id),
            )
            return self.get_run(run_id)

    def transition_run_state(self, run_id: str, from_state: RunState,
                             to_state: RunState) -> RunInstance | None:
        """比较并交换实例状态（暂停/恢复）；前置状态不匹配返回 None。"""
        with self._lock, self._conn:
            cur = self._conn.execute(
                "UPDATE runs SET state=? WHERE run_id=? AND state=?",
                (to_state.value, run_id, from_state.value),
            )
            if cur.rowcount == 0:
                return None
            return self.get_run(run_id)

    # ---- 读接口 ----
    def get_run(self, run_id: str) -> RunInstance | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM runs WHERE run_id=?", (run_id,)).fetchone()
        return self._to_run(row) if row else None

    def get_run_by_window(self, workflow_name: str, window: str) -> RunInstance | None:
        """返回该窗口最新一次尝试的实例。"""
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM runs WHERE workflow_name=? AND window=?"
                " ORDER BY attempt DESC LIMIT 1",
                (workflow_name, window)).fetchone()
        return self._to_run(row) if row else None

    def list_attempts(self, workflow_name: str, window: str) -> list[RunInstance]:
        """列出该窗口的全部尝试（含历史重跑结果），按 attempt 升序。"""
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM runs WHERE workflow_name=? AND window=?"
                " ORDER BY attempt ASC",
                (workflow_name, window)).fetchall()
        return [self._to_run(r) for r in rows]

    def get_task(self, run_id: str, task_name: str) -> TaskRun | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM task_runs WHERE run_id=? AND task_name=?",
                (run_id, task_name)).fetchone()
        return self._to_task(row) if row else None

    def list_tasks(self, run_id: str) -> list[TaskRun]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM task_runs WHERE run_id=? ORDER BY task_name",
                (run_id,)).fetchall()
        return [self._to_task(r) for r in rows]

    def list_unfinished_runs(self) -> list[RunInstance]:
        with self._lock:
            rows = self._conn.execute(
                f"SELECT * FROM runs WHERE state NOT IN"
                f" ({','.join('?' * len(TERMINAL_VALUES))})",
                TERMINAL_VALUES).fetchall()
        return [self._to_run(r) for r in rows]

    # ---- 行转换 ----
    @staticmethod
    def _to_run(row: sqlite3.Row) -> RunInstance:
        return RunInstance(
            run_id=row["run_id"], workflow_name=row["workflow_name"],
            window=row["window"], source=TriggerSource(row["source"]),
            attempt=row["attempt"],
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
