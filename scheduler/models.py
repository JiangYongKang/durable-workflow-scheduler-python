"""Core data types and state enums."""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum


class NodeState(str, Enum):
    PENDING = "PENDING"
    READY = "READY"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"          # terminal business failure (non-retryable or retries exhausted)
    SKIPPED = "SKIPPED"        # upstream failed, never executed


class InstanceState(str, Enum):
    PENDING = "PENDING"        # created, not yet advanced
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"          # terminal business failure
    TIMED_OUT = "TIMED_OUT"    # terminal, deadline exceeded (distinguishable from FAILED)


class FailKind(str, Enum):
    NONE = "NONE"
    BUSINESS = "BUSINESS"      # non-retryable or retries exhausted
    TIMEOUT = "TIMEOUT"        # deadline exceeded


class TriggerSource(str, Enum):
    MANUAL = "MANUAL"
    SCHEDULE = "SCHEDULE"
    RECOVERY = "RECOVERY"
    BACKFILL = "BACKFILL"


@dataclass(frozen=True)
class NodeSpec:
    name: str
    task: str
    deps: tuple[str, ...] = ()
    max_retries: int = 3
    retry_backoff_seconds: float = 0.0
    # Upper bound for exponential backoff: attempts back off
    # retry_backoff_seconds * 2**(attempt-1), capped here.
    retry_backoff_max_seconds: float | None = 60.0
    timeout_seconds: float | None = None


@dataclass(frozen=True)
class WorkflowSpec:
    name: str
    nodes: tuple[NodeSpec, ...]
    instance_timeout_seconds: float | None = None
