"""FastAPI 服务：对外暴露本地工作流调度与执行引擎。"""
from __future__ import annotations

import logging
import os

from fastapi import FastAPI
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from scheduler.engine import Engine
from scheduler.errors import (
    CycleDetectedError,
    FatalTaskError,
    RetryableTaskError,
    RunControlError,
    RunNotFoundError,
    UnknownTaskError,
)
from scheduler.models import (
    TERMINAL_RUN_STATES,
    RetryPolicy,
    RunState,
    TaskDef,
    TriggerSource,
    WorkflowDef,
)
from scheduler.store import StateStore

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(name)s %(message)s")
logger = logging.getLogger("scheduler.api")

DB_PATH = os.environ.get("SCHEDULER_DB", "scheduler.db")

store = StateStore(DB_PATH)
engine = Engine(store)

app = FastAPI(title="durable-workflow-scheduler")


class TaskDefIn(BaseModel):
    name: str
    depends_on: list[str] = Field(default_factory=list)
    max_attempts: int = 3
    backoff_seconds: float = 1.0


class WorkflowIn(BaseModel):
    name: str
    tasks: list[TaskDefIn]
    run_timeout_seconds: float = 3600.0


class TriggerIn(BaseModel):
    workflow: str
    window: str
    source: TriggerSource = TriggerSource.MANUAL


class HandlerIn(BaseModel):
    task: str
    succeed: bool = True
    error: str = "injected failure"
    fatal: bool = True


class BackfillIn(BaseModel):
    workflow: str
    windows: list[str]


class RerunIn(BaseModel):
    workflow: str
    window: str


class CancelIn(BaseModel):
    reason: str | None = None


class WindowOpIn(BaseModel):
    workflow: str
    window: str
    reason: str | None = None


def _task_dto(task) -> dict:
    return {"task": task.task_name, "state": task.state.value,
            "attempts": task.attempts, "error": task.error,
            "error_kind": task.error_kind}


def _run_dto(run: RunState, tasks: list | None = None) -> dict:
    dto = {"run_id": run.run_id, "workflow": run.workflow_name,
           "window": run.window, "source": run.source.value,
           "state": run.state.value, "attempt": run.attempt,
           "terminal": run.state in TERMINAL_RUN_STATES,
           "failure_reason": run.failure_reason}
    if tasks is not None:
        dto["tasks"] = [_task_dto(t) for t in tasks]
    return dto


@app.get("/")
def read_root():
    return {"service": "durable-workflow-scheduler", "db": DB_PATH}


@app.post("/workflows/handlers")
def register_handler(body: HandlerIn):
    """为本地演示注册任务执行器：可指定成功或注入可重试/致命失败。"""
    if body.succeed:
        def handler() -> None:
            logger.info("[执行] task=%s 成功", body.task)
    elif body.fatal:
        def handler() -> None:
            raise FatalTaskError(body.error)
    else:
        def handler() -> None:
            raise RetryableTaskError(body.error)
    engine.register_handler(body.task, handler)
    return {"registered": True, "task": body.task}


@app.post("/workflows")
def register_workflow(body: WorkflowIn):
    wf = WorkflowDef(
        name=body.name,
        tasks=tuple(TaskDef(
            name=t.name, depends_on=tuple(t.depends_on),
            retry_policy=RetryPolicy(t.max_attempts, t.backoff_seconds))
            for t in body.tasks),
        run_timeout_seconds=body.run_timeout_seconds,
    )
    try:
        engine.register_workflow(wf)
    except (CycleDetectedError, UnknownTaskError) as exc:
        return {"accepted": False, "error": str(exc)}
    return {"accepted": True, "workflow": body.name}


@app.post("/trigger")
def trigger(body: TriggerIn):
    run = engine.trigger(body.workflow, body.window, body.source)
    engine.tick()
    fresh, tasks = engine.get_status(run.run_id)
    return _run_dto(fresh, tasks)


@app.post("/backfill")
def backfill(body: BackfillIn):
    runs = engine.backfill(body.workflow, body.windows)
    engine.tick()
    out = []
    for r in runs:
        fresh, tasks = engine.get_status(r.run_id)
        out.append(_run_dto(fresh, tasks))
    return out


@app.post("/recover")
def recover():
    run_ids = engine.recover()
    return {"recovered": run_ids}


@app.post("/runs/{run_id}/cancel")
def cancel_run(run_id: str, body: CancelIn | None = None):
    """作废未收尾实例：立即生效，已完成步骤不回滚，终态 CANCELLED。"""
    try:
        run = engine.cancel_run(run_id, body.reason if body else None)
    except RunNotFoundError as exc:
        return JSONResponse(status_code=404,
                            content={"found": False, "error": str(exc)})
    except RunControlError as exc:
        return JSONResponse(status_code=409,
                            content={"found": True, "ok": False, "error": str(exc)})
    fresh, tasks = engine.get_status(run.run_id)
    return {"ok": True, **_run_dto(fresh, tasks)}


@app.post("/runs/{run_id}/pause")
def pause_run(run_id: str):
    """暂停运行中实例：保留进度与重试节奏，暂停期间不推进。"""
    try:
        run = engine.pause_run(run_id)
    except RunNotFoundError as exc:
        return JSONResponse(status_code=404,
                            content={"found": False, "error": str(exc)})
    except RunControlError as exc:
        return JSONResponse(status_code=409,
                            content={"found": True, "ok": False, "error": str(exc)})
    fresh, tasks = engine.get_status(run.run_id)
    return {"ok": True, **_run_dto(fresh, tasks)}


@app.post("/runs/{run_id}/resume")
def resume_run(run_id: str):
    """恢复暂停实例并推进一轮；重启后只有显式恢复才会推进。"""
    try:
        run = engine.resume_run(run_id)
    except RunNotFoundError as exc:
        return JSONResponse(status_code=404,
                            content={"found": False, "error": str(exc)})
    except RunControlError as exc:
        return JSONResponse(status_code=409,
                            content={"found": True, "ok": False, "error": str(exc)})
    engine.tick()
    fresh, tasks = engine.get_status(run.run_id)
    return {"ok": True, **_run_dto(fresh, tasks)}


@app.post("/rerun")
def rerun(body: RerunIn):
    """对已收尾窗口重跑一次：新建尝试，历史尝试保留；活跃尝试存在时合并。"""
    try:
        run = engine.rerun(body.workflow, body.window)
    except RunNotFoundError as exc:
        return JSONResponse(status_code=404,
                            content={"found": False, "error": str(exc)})
    except RunControlError as exc:
        return JSONResponse(status_code=409,
                            content={"found": True, "ok": False, "error": str(exc)})
    engine.tick()
    fresh, tasks = engine.get_status(run.run_id)
    return _run_dto(fresh, tasks)


@app.post("/ops/cancel")
def ops_cancel(body: WindowOpIn):
    """按窗口作废最新尝试：立即生效，已完成步骤不回滚，终态 CANCELLED。"""
    try:
        run = engine.cancel_window(body.workflow, body.window, body.reason)
    except RunNotFoundError as exc:
        return JSONResponse(status_code=404,
                            content={"found": False, "error": str(exc)})
    except RunControlError as exc:
        return JSONResponse(status_code=409,
                            content={"found": True, "ok": False, "error": str(exc)})
    fresh, tasks = engine.get_status(run.run_id)
    return {"ok": True, **_run_dto(fresh, tasks)}


@app.post("/ops/pause")
def ops_pause(body: WindowOpIn):
    """按窗口暂停最新尝试：保留进度与重试节奏，暂停期间不推进。"""
    try:
        run = engine.pause_window(body.workflow, body.window)
    except RunNotFoundError as exc:
        return JSONResponse(status_code=404,
                            content={"found": False, "error": str(exc)})
    except RunControlError as exc:
        return JSONResponse(status_code=409,
                            content={"found": True, "ok": False, "error": str(exc)})
    fresh, tasks = engine.get_status(run.run_id)
    return {"ok": True, **_run_dto(fresh, tasks)}


@app.post("/ops/resume")
def ops_resume(body: WindowOpIn):
    """按窗口恢复暂停尝试并推进一轮；重启后只有显式恢复才会推进。"""
    try:
        run = engine.resume_window(body.workflow, body.window)
    except RunNotFoundError as exc:
        return JSONResponse(status_code=404,
                            content={"found": False, "error": str(exc)})
    except RunControlError as exc:
        return JSONResponse(status_code=409,
                            content={"found": True, "ok": False, "error": str(exc)})
    engine.tick()
    fresh, tasks = engine.get_status(run.run_id)
    return {"ok": True, **_run_dto(fresh, tasks)}


@app.get("/workflows/{workflow}/windows/{window}/attempts")
def list_attempts(workflow: str, window: str):
    """列出窗口下全部尝试（按序号升序），方便对比两次运行差异。"""
    attempts = engine.list_attempts(workflow, window)
    if not attempts:
        return JSONResponse(status_code=404,
                            content={"found": False, "workflow": workflow,
                                     "window": window, "attempts": []})
    return {"found": True, "workflow": workflow, "window": window,
            "attempts": [
                {"attempt": r.attempt, **_run_dto(r, tasks)}
                for r, tasks in attempts]}


@app.post("/tick")
def tick():
    engine.tick()
    return {"ok": True}


@app.get("/runs/{run_id}")
def get_run(run_id: str):
    status = engine.get_status(run_id)
    if status is None:
        return {"found": False}
    run, tasks = status
    return {"found": True, **_run_dto(run, tasks)}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="127.0.0.1", port=8000)
