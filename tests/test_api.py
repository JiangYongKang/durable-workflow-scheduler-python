"""FastAPI 服务层测试：提交校验、触发去重、恢复、状态查询。"""
from __future__ import annotations

import importlib

import pytest


@pytest.fixture
def client(monkeypatch, tmp_path):
    monkeypatch.setenv("SCHEDULER_DB", str(tmp_path / "api.db"))
    main = importlib.import_module("main")
    importlib.reload(main)
    from fastapi.testclient import TestClient
    with TestClient(main.app) as c:
        yield c


def _register(client, name="wf", **kw):
    tasks = kw.pop("tasks", [
        {"name": "a"},
        {"name": "b", "depends_on": ["a"]},
    ])
    return client.post("/workflows", json={"name": name, "tasks": tasks, **kw})


def test_root(client):
    assert client.get("/").json()["service"] == "durable-workflow-scheduler"


def test_cycle_rejected_via_api(client):
    body = {"name": "cyc", "tasks": [
        {"name": "a", "depends_on": ["b"]},
        {"name": "b", "depends_on": ["a"]}]}
    resp = client.post("/workflows", json=body).json()
    assert resp["accepted"] is False and "环形依赖" in resp["error"]


def test_trigger_and_query_terminal(client):
    _register(client)
    client.post("/workflows/handlers", json={"task": "a", "succeed": False})
    client.post("/workflows/handlers", json={"task": "b", "succeed": True})
    r1 = client.post("/trigger", json={"workflow": "wf", "window": "d1"}).json()
    # a 致命失败 -> b BLOCKED，终态稳定可查询
    assert r1["state"] == "FAILED"
    states = {t["task"]: t["state"] for t in r1["tasks"]}
    assert states == {"a": "FAILED", "b": "BLOCKED"}

    got = client.get("/runs/wf:d1").json()
    assert got["found"] and got["state"] == "FAILED"
    assert got["terminal"] is True
    # 重复提交终态不变
    r2 = client.post("/trigger", json={"workflow": "wf", "window": "d1"}).json()
    assert r2["state"] == "FAILED"


def test_backfill_dedup_and_recover(client):
    _register(client, name="bf")
    client.post("/workflows/handlers", json={"task": "a", "succeed": True})
    client.post("/workflows/handlers", json={"task": "b", "succeed": True})
    client.post("/trigger", json={"workflow": "bf", "window": "d1"})
    resp = client.post("/backfill", json={
        "workflow": "bf", "windows": ["d1", "d2"]}).json()
    assert {r["run_id"] for r in resp} == {"bf:d1", "bf:d2"}
    again = client.post("/backfill", json={
        "workflow": "bf", "windows": ["d1", "d2"]}).json()
    assert len(again) == 2
    rec = client.post("/recover").json()
    # 两个实例均已终态，恢复不再推进
    assert set(rec["recovered"]) == set()


def test_unknown_run_returns_not_found(client):
    assert client.get("/runs/nope").json() == {"found": False}


def _register_flaky(client, name="flaky"):
    """注册一个会可重试失败的流程：触发后实例停留在 RUNNING（退避中）。"""
    client.post("/workflows", json={"name": name, "tasks": [
        {"name": "f", "max_attempts": 3, "backoff_seconds": 100},
    ]})
    client.post("/workflows/handlers", json={
        "task": "f", "succeed": False, "fatal": False})


def test_pause_resume_cancel_via_api(client):
    _register_flaky(client)
    r = client.post("/trigger", json={"workflow": "flaky", "window": "d1"}).json()
    assert r["state"] == "RUNNING"  # 退避中，实例仍在运行

    p = client.post("/runs/flaky:d1/pause").json()
    assert p["state"] == "PAUSED" and p["terminal"] is False
    # 暂停中 tick 不推进
    client.post("/tick")
    assert client.get("/runs/flaky:d1").json()["state"] == "PAUSED"

    # 重复暂停 -> 409
    assert client.post("/runs/flaky:d1/pause").status_code == 409

    rs = client.post("/runs/flaky:d1/resume").json()
    assert rs["state"] == "RUNNING"

    c = client.post("/runs/flaky:d1/cancel",
                    json={"reason": "人工介入"}).json()
    assert c["state"] == "CANCELLED" and c["terminal"] is True
    assert c["failure_reason"].startswith("cancelled:")
    assert c["tasks"][0]["state"] == "CANCELLED"
    # 已收尾不能再作废/暂停
    assert client.post("/runs/flaky:d1/cancel").status_code == 409
    assert client.post("/runs/flaky:d1/pause").status_code == 409
    # 不存在的实例 -> 404
    assert client.post("/runs/flaky:ghost/cancel").status_code == 404


def test_rerun_via_api_keeps_history(client):
    _register(client)  # wf: a -> b
    client.post("/workflows/handlers",
                json={"task": "a", "succeed": False, "error": "v1 broken"})
    client.post("/workflows/handlers", json={"task": "b", "succeed": True})
    r1 = client.post("/trigger", json={"workflow": "wf", "window": "d1"}).json()
    assert r1["state"] == "FAILED" and r1["attempt"] == 1

    # 修复后重跑：新尝试成功，历史保留
    client.post("/workflows/handlers", json={"task": "a", "succeed": True})
    r2 = client.post("/rerun", json={"workflow": "wf", "window": "d1"}).json()
    assert r2["state"] == "SUCCEEDED"
    assert r2["attempt"] == 2 and r2["source"] == "RERUN"
    assert r2["run_id"] == "wf:d1#2"

    old = client.get("/runs/wf:d1").json()
    assert old["state"] == "FAILED" and "v1 broken" in old["failure_reason"]

    attempts = client.get("/workflows/wf/windows/d1/runs").json()["attempts"]
    assert [(a["attempt"], a["state"]) for a in attempts] == [
        (1, "FAILED"), (2, "SUCCEEDED")]

    # 普通触发仍去重到最新尝试，不产生第三次
    r3 = client.post("/trigger", json={"workflow": "wf", "window": "d1"}).json()
    assert r3["run_id"] == "wf:d1#2"
    assert len(client.get("/workflows/wf/windows/d1/runs")
               .json()["attempts"]) == 2
