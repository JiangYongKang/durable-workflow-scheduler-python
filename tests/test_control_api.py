"""人工运维操作的 HTTP 接口测试：作废 / 暂停恢复 / 重跑 / 尝试列表。"""
from __future__ import annotations

import importlib

import pytest


@pytest.fixture
def client(monkeypatch, tmp_path):
    monkeypatch.setenv("SCHEDULER_DB", str(tmp_path / "api-ops.db"))
    main = importlib.import_module("main")
    importlib.reload(main)
    from fastapi.testclient import TestClient
    with TestClient(main.app) as c:
        yield c


def test_cancel_via_api_distinguishable_keeps_attempts_and_validates(client):
    """作废接口：退避中的运行实例作废后 CANCELLED、attempts 保留、幂等；
    对其他终态作废返回 409；未知窗口 404。"""
    client.post("/workflows", json={
        "name": "flaky",
        "tasks": [{"name": "f", "max_attempts": 3, "backoff_seconds": 100}]})
    client.post("/workflows/handlers", json={
        "task": "f", "succeed": False, "fatal": False, "error": "transient"})
    r = client.post("/trigger", json={"workflow": "flaky", "window": "d2"}).json()
    assert r["state"] == "RUNNING" and r["tasks"][0]["attempts"] == 1

    out = client.post("/ops/cancel", json={
        "workflow": "flaky", "window": "d2", "reason": "先停掉"}).json()
    assert out["ok"] is True and out["state"] == "CANCELLED"
    assert out["failure_reason"] == "cancelled: 先停掉"
    assert out["terminal"] is True
    t = out["tasks"][0]
    assert t["state"] == "CANCELLED" and t["error_kind"] == "cancelled"
    assert t["attempts"] == 1

    # 查询一眼可区分；重复作废幂等
    assert client.get("/runs/flaky:d2").json()["state"] == "CANCELLED"
    again = client.post("/ops/cancel",
                        json={"workflow": "flaky", "window": "d2"})
    assert again.status_code == 200 and again.json()["state"] == "CANCELLED"

    # 已成功的窗口不能作废 -> 409
    client.post("/workflows", json={"name": "done", "tasks": [{"name": "x"}]})
    client.post("/workflows/handlers", json={"task": "x", "succeed": True})
    client.post("/trigger", json={"workflow": "done", "window": "z1"})
    assert client.post("/ops/cancel", json={
        "workflow": "done", "window": "z1"}).status_code == 409
    # 未知 -> 404
    assert client.post("/ops/cancel", json={
        "workflow": "flaky", "window": "nope"}).status_code == 404


def test_pause_resume_via_api(client):
    """暂停恢复接口：退避中暂停后 tick 不推进；恢复后状态回到 RUNNING；
    对终态实例暂停/恢复返回 409；重复暂停幂等。"""
    client.post("/workflows", json={
        "name": "flaky",
        "tasks": [{"name": "f", "max_attempts": 3, "backoff_seconds": 100}]})
    client.post("/workflows/handlers", json={
        "task": "f", "succeed": False, "fatal": False, "error": "transient"})
    client.post("/trigger", json={"workflow": "flaky", "window": "p1"})

    paused = client.post("/ops/pause",
                         json={"workflow": "flaky", "window": "p1"}).json()
    assert paused["ok"] and paused["state"] == "PAUSED"
    assert paused["tasks"][0]["attempts"] == 1
    client.post("/tick")
    assert client.get("/runs/flaky:p1").json()["state"] == "PAUSED"
    # 重复暂停幂等
    assert client.post("/ops/pause", json={
        "workflow": "flaky", "window": "p1"}).json()["state"] == "PAUSED"

    resumed = client.post("/ops/resume",
                          json={"workflow": "flaky", "window": "p1"}).json()
    assert resumed["state"] == "RUNNING"

    client.post("/workflows", json={"name": "done", "tasks": [{"name": "x"}]})
    client.post("/workflows/handlers", json={"task": "x", "succeed": True})
    client.post("/trigger", json={"workflow": "done", "window": "z1"})
    assert client.post("/ops/pause", json={
        "workflow": "done", "window": "z1"}).status_code == 409
    assert client.post("/ops/resume", json={
        "workflow": "done", "window": "z1"}).status_code == 409
    assert client.post("/ops/resume", json={
        "workflow": "flaky", "window": "ghost"}).status_code == 404


def test_recover_does_not_resume_paused_via_api(client):
    """恢复接口：暂停实例在 /recover 后仍保持 PAUSED，不会被自动跑起来。"""
    client.post("/workflows", json={
        "name": "flaky",
        "tasks": [{"name": "f", "max_attempts": 3, "backoff_seconds": 100}]})
    client.post("/workflows/handlers", json={
        "task": "f", "succeed": False, "fatal": False, "error": "transient"})
    client.post("/trigger", json={"workflow": "flaky", "window": "p2"})
    client.post("/ops/pause", json={"workflow": "flaky", "window": "p2"})
    rec = client.post("/recover").json()
    assert "flaky:p2" not in rec["recovered"]
    assert client.get("/runs/flaky:p2").json()["state"] == "PAUSED"


def test_rerun_via_api_keeps_history_and_coalesces(client):
    """重跑接口：失败后重跑成功，attempts 列表保留两次可对比；第 3 次尝试暂停
    期间重跑请求合并；普通触发仍去重返回最新尝试；未知窗口 404。"""
    client.post("/workflows", json={
        "name": "wf", "tasks": [{"name": "a"}]})
    client.post("/workflows/handlers", json={
        "task": "a", "succeed": False, "error": "biz bug"})
    first = client.post("/trigger", json={"workflow": "wf", "window": "d1"}).json()
    assert first["state"] == "FAILED" and first["attempt"] == 1

    # 修复后重跑
    client.post("/workflows/handlers", json={"task": "a", "succeed": True})
    second = client.post("/rerun", json={"workflow": "wf", "window": "d1"}).json()
    assert second["run_id"] == "wf:d1#2" and second["attempt"] == 2
    assert second["source"] == "RERUN" and second["state"] == "SUCCEEDED"

    att = client.get("/workflows/wf/windows/d1/attempts").json()
    assert att["found"]
    assert [a["attempt"] for a in att["attempts"]] == [1, 2]
    assert [a["state"] for a in att["attempts"]] == ["FAILED", "SUCCEEDED"]
    assert "biz bug" in att["attempts"][0]["tasks"][0]["error"]
    assert att["attempts"][1]["tasks"][0]["state"] == "SUCCEEDED"

    # 成功后继续重跑为第 3 次
    third = client.post("/rerun", json={"workflow": "wf", "window": "d1"}).json()
    assert third["attempt"] == 3 and third["state"] == "SUCCEEDED"

    # 终态再重跑 -> 第 4 次；先把处理器换成可重试失败，让第 4 次停在退避中保持活跃
    client.post("/workflows/handlers", json={
        "task": "a", "succeed": False, "fatal": False, "error": "retrying"})
    # backoff 默认 1s，/rerun 自带的 tick 会立刻执行一次后进入退避
    fourth = client.post("/rerun", json={"workflow": "wf", "window": "d1"}).json()
    assert fourth["attempt"] == 4 and fourth["state"] == "RUNNING"
    client.post("/ops/pause", json={"workflow": "wf", "window": "d1"})
    merged = client.post("/rerun", json={"workflow": "wf", "window": "d1"}).json()
    assert merged["attempt"] == 4 and merged["state"] == "PAUSED"

    # 普通 trigger 仍去重返回最新尝试
    trig = client.post("/trigger", json={"workflow": "wf", "window": "d1"}).json()
    assert trig["attempt"] == 4 and trig["state"] == "PAUSED"

    assert client.post("/rerun", json={"workflow": "wf", "window": "nope"}
                       ).status_code == 404
    assert client.get("/workflows/wf/windows/nope/attempts").status_code == 404


def test_cancel_run_id_endpoint_still_works(client):
    """兼容：按 run_id 的作废端点（首次尝试无 # 后缀）仍然可用。"""
    client.post("/workflows", json={"name": "s", "tasks": [{"name": "a"}]})
    client.post("/workflows/handlers", json={
        "task": "a", "succeed": False, "fatal": False, "error": "t"})
    client.post("/trigger", json={"workflow": "s", "window": "w"})
    resp = client.post("/runs/s:w/cancel", json={"reason": "stop"})
    assert resp.status_code == 200 and resp.json()["state"] == "CANCELLED"
