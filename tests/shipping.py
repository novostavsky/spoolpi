"""Helpers shared by the shipper and invariant tests."""

from __future__ import annotations

import time
from collections.abc import Callable
from pathlib import Path

from spool.core.buffer import INFLIGHT, PENDING, Buffer
from spool.core.reading import Reading
from spool.core.shipper import Backoff
from spool.sinks.memory import MemorySink

FAST_BACKOFF = Backoff(initial_s=0.005, max_s=0.05)


def reading(step: int, boot_id: str = "boot") -> Reading:
    return Reading("s", float(step), None, step, step, boot_id)


def fill(db: Path, n: int, start: int = 0) -> None:
    with Buffer(db) as b:
        b.append([reading(i) for i in range(start, start + n)])


def drain(db: Path, timeout_s: float = 20.0) -> None:
    deadline = time.monotonic() + timeout_s
    with Buffer(db) as b:
        while True:
            counts = b.counts()
            if counts[PENDING] == 0 and counts[INFLIGHT] == 0:
                return
            if time.monotonic() > deadline:
                raise AssertionError(f"buffer not drained: {counts}")
            time.sleep(0.01)


def steps(sink: MemorySink) -> list[int]:
    return [int(e.reading.value or 0) for e in sink.received]


def assert_invariants(sink: MemorySink, n: int) -> None:
    """No loss, and each dedupe key names exactly one reading (and vice versa)."""
    step_by_key: dict[tuple[str, int], int] = {}
    key_by_step: dict[int, tuple[str, int]] = {}
    for e in sink.received:
        step = int(e.reading.value or 0)
        assert step_by_key.setdefault(e.key, step) == step, f"key {e.key} names two readings"
        assert key_by_step.setdefault(step, e.key) == e.key, f"step {step} shipped under two keys"
    assert set(key_by_step) == set(range(n)), "readings lost"


def wait_for(predicate: Callable[[], bool], timeout_s: float = 5.0) -> None:
    deadline = time.monotonic() + timeout_s
    while not predicate():
        if time.monotonic() > deadline:
            raise AssertionError("condition not reached")
        time.sleep(0.005)
