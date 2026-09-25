"""Local service entry point.

A file-backed SQLite store keeps durable state under ``./data/scheduler.db``,
so restarting this process lets the engine resume unfinished instances
(``auto_recover=True``) without re-running completed steps.
"""
from __future__ import annotations

import os

import uvicorn

from scheduler.api import create_app
from scheduler.engine import Scheduler
from scheduler.executor import TaskRegistry
from scheduler.store import Store

DB_PATH = os.environ.get("SCHEDULER_DB", "data/scheduler.db")


def build_scheduler(db_path: str = DB_PATH) -> Scheduler:
    if db_path != ":memory:":
        os.makedirs(os.path.dirname(db_path) or ".", exist_ok=True)
    store = Store(db_path)
    registry = TaskRegistry()
    scheduler = Scheduler(store, registry, auto_recover=True)
    return scheduler


scheduler = build_scheduler()
app = create_app(scheduler)


@app.on_event("shutdown")
def _shutdown() -> None:  # pragma: no cover - operational hook
    scheduler.shutdown()


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=int(os.environ.get("PORT", "8000")))
