"""FastAPI HTTP surface for the workflow scheduler.

Endpoints
---------
POST /workflows                      register a workflow definition
                                     (cyclic deps rejected here -> 400)
POST /workflows/{name}/trigger       manual trigger (fresh instance each call)
POST /workflows/{name}/scheduled     schedule/backfill trigger for a window
                                     (overlapping windows dedupe -> one run)
POST /workflows/{name}/backfill      backfill every window in [start, end]
GET  /instances                      list instances (optionally by workflow)
GET  /instances/{id}                 stable terminal state + audit trail
POST /recover                        resume unfinished instances in this store

The scheduler instance is attached to ``app.state``. Tests build their own
scheduler and pass it to :func:`create_app`.
"""
from __future__ import annotations

from typing import Any

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from .engine import Scheduler
from .errors import SchedulerError, WorkflowNotFound
from .models import NodeSpec, WorkflowSpec, TriggerSource
from .windows import to_ts, windows_between


class NodeIn(BaseModel):
    name: str
    task: str
    deps: list[str] = Field(default_factory=list)
    max_retries: int = 3
    retry_backoff_seconds: float = 0.0
    retry_backoff_max_seconds: float | None = 60.0
    timeout_seconds: float | None = None


class WorkflowIn(BaseModel):
    name: str
    nodes: list[NodeIn]
    instance_timeout_seconds: float | None = None


class TriggerIn(BaseModel):
    source: str = TriggerSource.MANUAL.value


class ScheduledIn(BaseModel):
    ts: str | float
    length: str = "hour"
    source: str = TriggerSource.SCHEDULE.value


class BackfillIn(BaseModel):
    start: str | float
    end: str | float
    length: str = "hour"


def create_app(scheduler: Scheduler | None = None) -> FastAPI:
    app = FastAPI(title="durable-workflow-scheduler", version="0.1.0")
    app.state.scheduler = scheduler

    def _sched(self_app: Any = None) -> Scheduler:
        sched = app.state.scheduler
        if sched is None:
            raise HTTPException(status_code=503, detail="scheduler not configured")
        return sched

    @app.post("/workflows", status_code=201)
    def register_workflow(payload: WorkflowIn) -> dict[str, Any]:
        sched = _sched()
        spec = WorkflowSpec(
            name=payload.name,
            instance_timeout_seconds=payload.instance_timeout_seconds,
            nodes=tuple(
                NodeSpec(
                    name=n.name,
                    task=n.task,
                    deps=tuple(n.deps),
                    max_retries=n.max_retries,
                    retry_backoff_seconds=n.retry_backoff_seconds,
                    retry_backoff_max_seconds=n.retry_backoff_max_seconds,
                    timeout_seconds=n.timeout_seconds,
                )
                for n in payload.nodes
            ),
        )
        try:
            sched.register_workflow(spec)
        except SchedulerError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return {"registered": payload.name, "nodes": len(payload.nodes)}

    @app.post("/workflows/{name}/trigger", status_code=202)
    def trigger(name: str, payload: TriggerIn | None = None) -> dict[str, Any]:
        sched = _sched()
        source = payload.source if payload else TriggerSource.MANUAL.value
        try:
            instance_id = sched.trigger(name, source=source)
        except WorkflowNotFound as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        return {"instance_id": instance_id}

    @app.post("/workflows/{name}/scheduled", status_code=202)
    def trigger_scheduled(name: str, payload: ScheduledIn) -> dict[str, Any]:
        sched = _sched()
        window = windows_between(to_ts(payload.ts), to_ts(payload.ts), payload.length)[0]
        try:
            instance_id, created = sched.trigger_scheduled(
                name, window.key, source=payload.source
            )
        except WorkflowNotFound as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        return {
            "instance_id": instance_id,
            "created": created,
            "window_key": window.key,
            "deduplicated": not created,
        }

    @app.post("/workflows/{name}/backfill", status_code=202)
    def backfill(name: str, payload: BackfillIn) -> dict[str, Any]:
        sched = _sched()
        windows = windows_between(to_ts(payload.start), to_ts(payload.end), payload.length)
        results = []
        try:
            for window in windows:
                instance_id, created = sched.trigger_scheduled(
                    name, window.key, source=TriggerSource.BACKFILL.value
                )
                results.append(
                    {"window_key": window.key, "instance_id": instance_id,
                     "created": created, "deduplicated": not created}
                )
        except WorkflowNotFound as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        return {
            "windows": len(results),
            "created": sum(1 for r in results if r["created"]),
            "deduplicated": sum(1 for r in results if r["deduplicated"]),
            "runs": results,
        }

    @app.get("/instances")
    def list_instances(workflow: str | None = None) -> dict[str, Any]:
        sched = _sched()
        return {"instances": sched.store.list_instances(workflow)}

    @app.get("/instances/{instance_id}")
    def get_instance(instance_id: str) -> dict[str, Any]:
        sched = _sched()
        try:
            return sched.get_status(instance_id)
        except WorkflowNotFound as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

    @app.post("/recover", status_code=200)
    def recover() -> dict[str, Any]:
        sched = _sched()
        recovered = sched.recover()
        return {"recovered": len(recovered), "instance_ids": recovered}

    @app.get("/health")
    def health() -> dict[str, str]:
        return {"status": "ok"}

    return app
