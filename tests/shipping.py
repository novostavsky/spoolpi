"""Helpers shared by the shipper and invariant tests."""

from __future__ import annotations

import time
from collections.abc import Callable
from pathlib import Path

from spoolpi.core.buffer import INFLIGHT, PENDING, Buffer
from spoolpi.core.reading import Reading
from spoolpi.core.retention import GapRecord
from spoolpi.core.shipper import Backoff
from spoolpi.sinks.base import Envelope
from spoolpi.sinks.memory import MemorySink

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
            gaps_left = b.unshipped_gaps()
            if counts[PENDING] == 0 and counts[INFLIGHT] == 0 and gaps_left == 0:
                return
            if time.monotonic() > deadline:
                raise AssertionError(f"buffer not drained: {counts}, {gaps_left} gaps unshipped")
            time.sleep(0.01)


def reading_envelopes(sink: MemorySink) -> list[tuple[Envelope, Reading]]:
    return [(e, e.payload) for e in sink.received if isinstance(e.payload, Reading)]


def gap_envelopes(sink: MemorySink) -> list[tuple[Envelope, GapRecord]]:
    return [(e, e.payload) for e in sink.received if isinstance(e.payload, GapRecord)]


def steps(sink: MemorySink) -> list[int]:
    return [int(r.value or 0) for _, r in reading_envelopes(sink)]


def assert_invariants(sink: MemorySink, n: int) -> None:
    """Every reading is delivered or counted in a gap, and keys are dedupe-safe.

    Each (buffer_id, seq) names exactly one record, and each reading travels
    under exactly one key, however often it is resent.
    """
    step_by_key: dict[tuple[str, int], int] = {}
    key_by_step: dict[int, tuple[str, int]] = {}
    for e, r in reading_envelopes(sink):
        step = int(r.value or 0)
        assert step_by_key.setdefault(e.key, step) == step, f"key {e.key} names two readings"
        assert key_by_step.setdefault(step, e.key) == e.key, f"step {step} shipped under two keys"
    gap_by_key: dict[tuple[str, int], GapRecord] = {}
    for e, g in gap_envelopes(sink):
        assert e.key not in step_by_key, f"key {e.key} names a reading and a gap"
        assert gap_by_key.setdefault(e.key, g) == g, f"gap {e.key} resent with different content"
    assert set(key_by_step) <= set(range(n))
    gap_count = sum(g.count for g in gap_by_key.values())
    delivered = len(key_by_step)
    assert delivered + gap_count == n, f"{delivered} delivered + {gap_count} in gaps != {n} written"


def wait_for(predicate: Callable[[], bool], timeout_s: float = 5.0) -> None:
    deadline = time.monotonic() + timeout_s
    while not predicate():
        if time.monotonic() > deadline:
            raise AssertionError("condition not reached")
        time.sleep(0.005)
