"""Backfill window overlap semantics and the HTTP surface end to end."""
from __future__ import annotations

from datetime import datetime, timezone

from fastapi.testclient import TestClient

from scheduler.api import create_app
from scheduler.models import NodeSpec, TriggerSource, WorkflowSpec
from scheduler.windows import window_for, windows_between

from .conftest import dump_audit


WORKFLOW_DEF = {
    "name": "hourly",
    "instance_timeout_seconds": 10,
    "nodes": [
        {"name": "extract", "task": "extract", "max_retries": 1,
         "retry_backoff_seconds": 0.0},
        {"name": "load", "task": "load", "deps": ["extract"]},
    ],
}


def _client(sched):
    return TestClient(create_app(sched))


def test_schedule_then_backfill_same_windows_dedupe(make_scheduler) -> None:
    runs = {"n": 0}
    import threading
    lock = threading.Lock()

    def task(ctx):
        with lock:
            runs["n"] += 1
        return {"ok": True}

    sched = make_scheduler(max_workers=4)
    sched.registry.register("extract", task)
    sched.registry.register("load", task)
    sched.register_workflow(WorkflowSpec(name="hourly", nodes=(
        NodeSpec(name="extract", task="extract", max_retries=1),
        NodeSpec(name="load", task="load", deps=("extract",)),
    )))

    ts10 = datetime(2026, 9, 25, 10, 30, tzinfo=timezone.utc).timestamp()
    ts11 = datetime(2026, 9, 25, 11, 30, tzinfo=timezone.utc).timestamp()
    w10 = window_for(ts10, "hour")
    w11 = window_for(ts11, "hour")

    # normal schedule already ran the 10:00 window
    iid10, created10 = sched.trigger_scheduled(
        "hourly", w10.key, source=TriggerSource.SCHEDULE.value
    )
    assert created10
    sched.wait_until_terminal(iid10, 5)

    # backfill covers 10:00 and 11:00: 10:00 dedupes, 11:00 is new
    outcomes = []
    for w in windows_between(ts10, ts11, "hour"):
        iid, created = sched.trigger_scheduled(
            "hourly", w.key, source=TriggerSource.BACKFILL.value
        )
        outcomes.append((w.key, iid, created))
    assert [c for _, _, c in outcomes] == [False, True], outcomes
    iid11 = next(iid for _, iid, created in outcomes if created)
    st11 = sched.wait_until_terminal(iid11, 5)
    assert st11["state"] == "SUCCEEDED"
    st10 = sched.get_status(iid10)
    assert st10["state"] == "SUCCEEDED"
    # two windows * two nodes, nothing executed twice
    assert runs["n"] == 4, runs
    dump_audit(sched, iid10, iid11)
    # audit shows the trigger source for each window
    sources10 = {a["trigger_source"] for a in st10["audit"] if a["event"]}
    assert "SCHEDULE" in sources10
    sources11 = {a["trigger_source"] for a in st11["audit"] if a["event"]}
    assert "BACKFILL" in sources11
    dedup = [a for a in st10["audit"] if a["event"] == "DEDUP"]
    assert any(a["trigger_source"] == "BACKFILL" for a in dedup)


def test_http_full_lifecycle(make_scheduler) -> None:
    sched = make_scheduler()
    sched.registry.register("extract", lambda ctx: {"rows": 7})
    sched.registry.register("load", lambda ctx: None)
    client = _client(sched)

    assert client.get("/health").json() == {"status": "ok"}

    r = client.post("/workflows", json=WORKFLOW_DEF)
    assert r.status_code == 201, r.text

    # cyclic workflow is rejected at submit time
    cyc = {
        "name": "cyc",
        "nodes": [
            {"name": "a", "task": "extract", "deps": ["b"]},
            {"name": "b", "task": "load", "deps": ["a"]},
        ],
    }
    r = client.post("/workflows", json=cyc)
    assert r.status_code == 400 and "cyclic" in r.json()["detail"]

    r = client.post("/workflows/hourly/trigger", json={"source": "MANUAL"})
    assert r.status_code == 202
    iid = r.json()["instance_id"]

    status = sched.wait_until_terminal(iid, 5)
    r = client.get(f"/instances/{iid}")
    body = r.json()
    assert body["state"] == "SUCCEEDED" and body["terminal"] is True
    assert {n["state"] for n in body["nodes"]} == {"SUCCEEDED"}
    events = [a["event"] for a in body["audit"]]
    assert events[0] == "TRIGGER"
    assert "INSTANCE_SUCCEEDED" in events
    assert status["state"] == "SUCCEEDED"

    r = client.get("/instances/does-not-exist")
    assert r.status_code == 404
    r = client.post("/workflows/missing/trigger")
    assert r.status_code == 404


def test_http_scheduled_and_backfill_dedupe(make_scheduler) -> None:
    sched = make_scheduler()
    sched.registry.register("extract", lambda ctx: None)
    sched.registry.register("load", lambda ctx: None)
    client = _client(sched)
    client.post("/workflows", json=WORKFLOW_DEF)

    ts = "2026-09-25T10:45:00+00:00"
    r1 = client.post("/workflows/hourly/scheduled",
                     json={"ts": ts, "length": "hour", "source": "SCHEDULE"})
    assert r1.status_code == 202 and r1.json()["created"] is True
    iid = r1.json()["instance_id"]

    r2 = client.post("/workflows/hourly/scheduled",
                     json={"ts": ts, "length": "hour", "source": "SCHEDULE"})
    assert r2.json()["created"] is False and r2.json()["instance_id"] == iid

    r3 = client.post(
        "/workflows/hourly/backfill",
        json={"start": "2026-09-25T10:05:00+00:00",
              "end": "2026-09-25T10:55:00+00:00", "length": "hour"},
    )
    body = r3.json()
    assert body["windows"] == 1 and body["created"] == 0
    assert body["deduplicated"] == 1
    assert body["runs"][0]["instance_id"] == iid

    sched.wait_until_terminal(iid, 5)


def test_http_recover_endpoint(make_scheduler) -> None:
    sched = make_scheduler()
    sched.registry.register("extract", lambda ctx: None)
    sched.registry.register("load", lambda ctx: None)
    client = _client(sched)
    client.post("/workflows", json=WORKFLOW_DEF)
    r = client.post("/recover")
    assert r.status_code == 200 and r.json()["recovered"] == 0


def test_status_terminal_flag_is_stable(make_scheduler) -> None:
    sched = make_scheduler()
    sched.registry.register("extract", lambda ctx: None)
    sched.registry.register("load", lambda ctx: None)
    client = _client(sched)
    client.post("/workflows", json=WORKFLOW_DEF)
    iid = client.post("/workflows/hourly/trigger").json()["instance_id"]
    sched.wait_until_terminal(iid, 5)
    for _ in range(3):
        body = client.get(f"/instances/{iid}").json()
        assert body["terminal"] is True and body["state"] == "SUCCEEDED"
