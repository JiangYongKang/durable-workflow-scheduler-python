"""基于 SQLite 的本地持久化状态存储。

所有写操作在事务内完成，保证并发触发与进程重启后状态一致。

去重约束：
- 同一 (workflow, window) 可以保留多轮「尝试」（首次 + 重跑），历史终态
  尝试不被覆盖；
- 但同一窗口同时只允许存在一个活跃尝试（RUNNING/PAUSED），由部分唯一索引
  在数据库层兜底，配合引擎锁实现并发重跑的「同时只跑一遍 / 同波合并」。
"""
from __future__ import annotations

import sqlite3
import threading

from .models import (
    ACTIVE_RUN_STATES,
    TERMINAL_TASK_STATES,
    RunInstance,
    RunState,
    TaskRun,
    TaskState,
    TriggerSource,
)

_RUN_COLUMNS = (
    "run_id, workflow_name, window, source, state, attempt, "
    "created_at, deadline, finished_at, failure_reason"
)
_TASK_COLUMNS = (
    "run_id, task_name, state, attempts, next_attempt_at, error, error_kind, "
    "started_at, finished_at"
)
# 仅活跃尝试占窗口：并发重跑由该索引在库层面合并为一次
_ACTIVE_INDEX = (
    "CREATE UNIQUE INDEX IF NOT EXISTS idx_runs_active_window ON runs"
    " (workflow_name, window) WHERE state IN ('RUNNING', 'PAUSED')"
)
_ACTIVE_STATES = tuple(s.value for s in ACTIVE_RUN_STATES)
_NON_TERMINAL_TASK_STATES = tuple(
    s.value for s in TaskState if s not in TERMINAL_TASK_STATES
)


class StateStore:
    """线程安全的 SQLite 状态存储。"""

    def __init__(self, db_path: str = ":memory:") -> None:
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        with self._lock, self._conn:
            self._conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS runs (
                    run_id TEXT PRIMARY KEY,
                    workflow_name TEXT NOT NULL,
                    window TEXT NOT NULL,
                    source TEXT NOT NULL,
                    state TEXT NOT NULL,
                    attempt INTEGER NOT NULL DEFAULT 1,
                    created_at REAL NOT NULL,
                    deadline REAL NOT NULL,
                    finished_at REAL,
                    failure_reason TEXT
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
            )
            self._migrate_if_needed()

    def _migrate_if_needed(self) -> None:
        """旧版 runs 表使用全量 UNIQUE(workflow_name, window) 且无 attempt 列，
        原地迁移为新表结构并把历史记录标记为第 1 次尝试。"""
        cols = {r["name"] for r in self._conn.execute(
            "PRAGMA table_info(runs)").fetchall()}
        if "attempt" not in cols:
            self._conn.executescript(
                """
                ALTER TABLE runs RENAME TO runs_old;
                CREATE TABLE runs (
                    run_id TEXT PRIMARY KEY,
                    workflow_name TEXT NOT NULL,
                    window TEXT NOT NULL,
                    source TEXT NOT NULL,
                    state TEXT NOT NULL,
                    attempt INTEGER NOT NULL DEFAULT 1,
                    created_at REAL NOT NULL,
                    deadline REAL NOT NULL,
                    finished_at REAL,
                    failure_reason TEXT
                );
                INSERT INTO runs
                    (run_id, workflow_name, window, source, state, attempt,
                     created_at, deadline, finished_at, failure_reason)
                SELECT run_id, workflow_name, window, source, state, 1,
                       created_at, deadline, finished_at, failure_reason
                FROM runs_old;
                DROP TABLE runs_old;
                """
            )
        self._conn.execute(_ACTIVE_INDEX)

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    # ---- 写接口 ----
    def _insert_run_rows(self, run: RunInstance, tasks: list[TaskRun]) -> None:
        self._conn.execute(
            f"INSERT INTO runs ({_RUN_COLUMNS}) VALUES (?,?,?,?,?,?,?,?,?,?)",
            (run.run_id, run.workflow_name, run.window, run.source.value,
             run.state.value, run.attempt, run.created_at, run.deadline,
             run.finished_at, run.failure_reason),
        )
        for t in tasks:
            self._conn.execute(
                f"INSERT INTO task_runs ({_TASK_COLUMNS}) VALUES (?,?,?,?,?,?,?,?,?)",
                (t.run_id, t.task_name, t.state.value, t.attempts,
                 t.next_attempt_at, t.error, t.error_kind,
                 t.started_at, t.finished_at),
            )

    def insert_run_if_absent(self, run: RunInstance, tasks: list[TaskRun]) -> bool:
        """首次触发语义：窗口下没有任何尝试时插入并返回 True；

        窗口下已存在尝试（含终态）则返回 False，由调用方返回既有实例，
        保持原有触发去重语义。
        """
        with self._lock, self._conn:
            exists = self._conn.execute(
                "SELECT 1 FROM runs WHERE workflow_name=? AND window=? LIMIT 1",
                (run.workflow_name, run.window)).fetchone()
            if exists:
                return False
            self._insert_run_rows(run, tasks)
            return True

    def insert_rerun_if_no_active(self, run: RunInstance,
                                   tasks: list[TaskRun]) -> bool:
        """重跑语义：窗口下存在活跃尝试（RUNNING/PAUSED）时不插入（同波/并发
        重跑合并为一次）；否则插入新一轮尝试，历史尝试原样保留。"""
        placeholders = ",".join("?" * len(_ACTIVE_STATES))
        with self._lock, self._conn:
            active = self._conn.execute(
                f"SELECT 1 FROM runs WHERE workflow_name=? AND window=?"
                f" AND state IN ({placeholders}) LIMIT 1",
                (run.workflow_name, run.window, *_ACTIVE_STATES)).fetchone()
            if active:
                return False
            self._insert_run_rows(run, tasks)
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

    def save_task_if_state(self, task: TaskRun,
                           expected: frozenset[TaskState]) -> bool:
        """条件更新任务：仅当库中状态仍属于 expected 时写入。

        作废可能在任务执行期间把任务改为 CANCELLED；用该守卫保证慢任务执行
        完返回时不会把作废结果覆盖回去。
        """
        states = tuple(s.value for s in expected)
        placeholders = ",".join("?" * len(states))
        with self._lock, self._conn:
            cur = self._conn.execute(
                f"UPDATE task_runs SET state=?, attempts=?, next_attempt_at=?,"
                f" error=?, error_kind=?, started_at=?, finished_at=?"
                f" WHERE run_id=? AND task_name=? AND state IN ({placeholders})",
                (task.state.value, task.attempts, task.next_attempt_at,
                 task.error, task.error_kind, task.started_at, task.finished_at,
                 task.run_id, task.task_name, *states),
            )
            return cur.rowcount == 1

    def save_run(self, run: RunInstance) -> None:
        with self._lock, self._conn:
            self._conn.execute(
                "UPDATE runs SET state=?, finished_at=?, failure_reason=?"
                " WHERE run_id=?",
                (run.state.value, run.finished_at, run.failure_reason, run.run_id),
            )

    def save_run_if_state(self, run: RunInstance,
                          expected: frozenset[RunState]) -> bool:
        """条件更新实例：暂停/作废可能已改变实例状态，推进侧用该守卫避免
        把人工操作结果覆盖回 RUNNING/终态。"""
        states = tuple(s.value for s in expected)
        placeholders = ",".join("?" * len(states))
        with self._lock, self._conn:
            cur = self._conn.execute(
                f"UPDATE runs SET state=?, finished_at=?, failure_reason=?"
                f" WHERE run_id=? AND state IN ({placeholders})",
                (run.state.value, run.finished_at, run.failure_reason,
                 run.run_id, *states),
            )
            return cur.rowcount == 1

    def mark_cancelled(self, run_id: str, now: float, reason: str) -> bool:
        """把实例及所有未进入终态的任务原子地置为 CANCELLED。

        已完成（SUCCEEDED/FAILED/BLOCKED/TIMEOUT/CANCELLED）的步骤保持原样。
        返回是否实际发生了作废（幂等重复作废返回 False）。
        """
        task_ph = ",".join("?" * len(_NON_TERMINAL_TASK_STATES))
        with self._lock, self._conn:
            cur = self._conn.execute(
                "UPDATE runs SET state=?, finished_at=?, failure_reason=?"
                " WHERE run_id=? AND state NOT IN ('CANCELLED','SUCCEEDED',"
                " 'FAILED','TIMEOUT')",
                (RunState.CANCELLED.value, now, reason, run_id),
            )
            changed = cur.rowcount == 1
            self._conn.execute(
                f"UPDATE task_runs SET state=?, error=?, error_kind=?, finished_at=?"
                f" WHERE run_id=? AND state IN ({task_ph})",
                (TaskState.CANCELLED.value, reason, "cancelled", now,
                 run_id, *_NON_TERMINAL_TASK_STATES),
            )
            return changed

    def mark_paused(self, run_id: str) -> bool:
        """RUNNING -> PAUSED；其他状态不动（暂停只对运行中实例有效）。"""
        with self._lock, self._conn:
            cur = self._conn.execute(
                "UPDATE runs SET state=? WHERE run_id=? AND state=?",
                (RunState.PAUSED.value, run_id, RunState.RUNNING.value))
            return cur.rowcount == 1

    def mark_resumed(self, run_id: str) -> bool:
        """PAUSED -> RUNNING；重启后暂停状态原样保留，只有显式恢复才推进。"""
        with self._lock, self._conn:
            cur = self._conn.execute(
                "UPDATE runs SET state=? WHERE run_id=? AND state=?",
                (RunState.RUNNING.value, run_id, RunState.PAUSED.value))
            return cur.rowcount == 1

    def reset_running_tasks(self, run_id: str, reason: str) -> int:
        """把停留在 RUNNING 的任务重置为 READY（重启边界的 at-least-once）。"""
        with self._lock, self._conn:
            cur = self._conn.execute(
                "UPDATE task_runs SET state=?, attempts=attempts, error=?,"
                " started_at=NULL WHERE run_id=? AND state=?",
                (TaskState.READY.value, reason, run_id, TaskState.RUNNING.value),
            )
            return cur.rowcount

    # ---- 读接口 ----
    def get_run(self, run_id: str) -> RunInstance | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM runs WHERE run_id=?", (run_id,)).fetchone()
        return self._to_run(row) if row else None

    def get_run_by_window(self, workflow_name: str, window: str) -> RunInstance | None:
        """窗口下最新一次尝试（attempt 最大），保持原有调用习惯。"""
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM runs WHERE workflow_name=? AND window=?"
                " ORDER BY attempt DESC LIMIT 1",
                (workflow_name, window)).fetchone()
        return self._to_run(row) if row else None

    def get_active_run_by_window(self, workflow_name: str,
                                  window: str) -> RunInstance | None:
        """窗口下当前活跃尝试（RUNNING/PAUSED）；无则 None。"""
        placeholders = ",".join("?" * len(_ACTIVE_STATES))
        with self._lock:
            row = self._conn.execute(
                f"SELECT * FROM runs WHERE workflow_name=? AND window=?"
                f" AND state IN ({placeholders}) ORDER BY attempt DESC LIMIT 1",
                (workflow_name, window, *_ACTIVE_STATES)).fetchone()
        return self._to_run(row) if row else None

    def list_runs_by_window(self, workflow_name: str,
                             window: str) -> list[RunInstance]:
        """列出窗口下全部尝试（按尝试序号升序），历史结果不被覆盖、可对比。"""
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM runs WHERE workflow_name=? AND window=?"
                " ORDER BY attempt ASC",
                (workflow_name, window)).fetchall()
        return [self._to_run(r) for r in rows]

    def list_tasks(self, run_id: str) -> list[TaskRun]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM task_runs WHERE run_id=? ORDER BY task_name",
                (run_id,)).fetchall()
        return [self._to_task(r) for r in rows]

    def list_running_runs(self) -> list[RunInstance]:
        """仅返回 RUNNING 实例：PAUSED 不参与调度推进，重启恢复也不自动推进。"""
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM runs WHERE state=?",
                (RunState.RUNNING.value,)).fetchall()
        return [self._to_run(r) for r in rows]

    def list_paused_runs(self) -> list[RunInstance]:
        """返回 PAUSED 实例（重启边界需要重置其 stale RUNNING 任务）。"""
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM runs WHERE state=?",
                (RunState.PAUSED.value,)).fetchall()
        return [self._to_run(r) for r in rows]

    # ---- 行转换 ----
    @staticmethod
    def _to_run(row: sqlite3.Row) -> RunInstance:
        return RunInstance(
            run_id=row["run_id"], workflow_name=row["workflow_name"],
            window=row["window"], source=TriggerSource(row["source"]),
            state=RunState(row["state"]), attempt=row["attempt"],
            created_at=row["created_at"], deadline=row["deadline"],
            finished_at=row["finished_at"], failure_reason=row["failure_reason"],
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
