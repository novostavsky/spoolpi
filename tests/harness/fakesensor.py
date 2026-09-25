"""Deterministic, verifiable reading stream for crash tests.

The stream emits value ``n`` at logical step ``n``. A gap or a duplicate in
whatever storage received the stream is therefore detectable by inspecting
the set of steps alone -- no separate bookkeeping of "what was sent" is
needed.
"""

from __future__ import annotations

import itertools
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class FakeReading:
    step: int
    value: float


class FakeSensor:
    """Emits ``FakeReading(step=n, value=float(n))`` for n = start, start+1, ..."""

    def __init__(self, start: int = 0) -> None:
        self._counter = itertools.count(start)

    def read(self) -> FakeReading:
        n = next(self._counter)
        return FakeReading(step=n, value=float(n))


def find_gaps(steps: set[int]) -> list[int]:
    """Missing integers in [0, max(steps)]. Empty if steps is empty or contiguous."""
    if not steps:
        return []
    return sorted(set(range(max(steps) + 1)) - steps)
