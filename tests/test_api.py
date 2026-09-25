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
