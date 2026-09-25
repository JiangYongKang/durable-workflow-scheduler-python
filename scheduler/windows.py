"""Scheduling-window helpers.

A workflow may run on a fixed cadence (e.g. hourly windows). Both the normal
schedule tick and a later backfill call derive the *same* window key from the
same wall-clock instant, so the unique ``(workflow, window_key)`` constraint
makes overlapping schedule/backfill executions collapse onto one instance.

Window semantics
----------------
Windows are aligned to epoch: for a window length of ``W`` seconds, the window
containing timestamp ``t`` starts at ``floor(t / W) * W``. The window key is
``"w{length}:{window_start_iso}"``.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

_ALIGN_SECONDS = {
    "minute": 60,
    "hour": 3600,
    "day": 86400,
    "week": 604800,
}


@dataclass(frozen=True)
class Window:
    key: str
    start_ts: float
    end_ts: float
    length_seconds: int

    @property
    def start_iso(self) -> str:
        return datetime.fromtimestamp(self.start_ts, tz=timezone.utc).isoformat()


def _length_seconds(length: str) -> int:
    if length not in _ALIGN_SECONDS:
        raise ValueError(
            f"unsupported window length {length!r}; expected one of "
            f"{sorted(_ALIGN_SECONDS)}"
        )
    return _ALIGN_SECONDS[length]


def window_for(
    ts: float,
    length: str = "hour",
    *,
    length_seconds: int | None = None,
) -> Window:
    """Return the aligned window containing wall-clock timestamp ``ts``."""
    wlen = length_seconds if length_seconds is not None else _length_seconds(length)
    if wlen <= 0:
        raise ValueError("window length must be positive")
    start = (int(ts) // wlen) * wlen
    return Window(
        key=f"w{wlen}:{datetime.fromtimestamp(start, tz=timezone.utc).isoformat()}",
        start_ts=float(start),
        end_ts=float(start + wlen),
        length_seconds=wlen,
    )


def windows_between(
    start_ts: float,
    end_ts: str | float,
    length: str = "hour",
    *,
    length_seconds: int | None = None,
) -> list[Window]:
    """Enumerate complete/partial windows covering ``[start_ts, end_ts]``.

    Used by backfill: the caller triggers one instance per window; already
    existing windows (normal schedule ran them) are deduplicated by the store.
    """
    if isinstance(end_ts, str):
        end_ts = datetime.fromisoformat(end_ts.replace("Z", "+00:00")).timestamp()
    if end_ts < start_ts:
        raise ValueError("end_ts precedes start_ts")
    wlen = length_seconds if length_seconds is not None else _length_seconds(length)
    first = window_for(start_ts, length_seconds=wlen)
    last = window_for(end_ts, length_seconds=wlen)
    out: list[Window] = []
    cur = first.start_ts
    while cur <= last.start_ts:
        out.append(
            Window(
                key=f"w{wlen}:{datetime.fromtimestamp(cur, tz=timezone.utc).isoformat()}",
                start_ts=float(cur),
                end_ts=float(cur + wlen),
                length_seconds=wlen,
            )
        )
        cur += wlen
    return out


def to_ts(value: str | float | datetime) -> float:
    if isinstance(value, datetime):
        return value.timestamp()
    if isinstance(value, (int, float)):
        return float(value)
    return datetime.fromisoformat(str(value).replace("Z", "+00:00")).timestamp()


def iso(ts: float) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()
