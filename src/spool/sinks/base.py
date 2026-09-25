"""The sink contract.

A sink receives a batch of envelopes and reports which ones it durably accepted.
Anything not in the returned ``AckSet`` is resent later, and so is the whole
batch if ``send`` raises. Delivery is at-least-once, so a sink (or whatever
sits behind it) must deduplicate on ``(buffer_id, seq)``.
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from typing import Protocol

from spool.core.reading import Reading
from spool.core.retention import GapRecord


@dataclass(frozen=True, slots=True)
class Envelope:
    """One record for the sink. Gap records share the reading key space."""

    buffer_id: str
    seq: int
    payload: Reading | GapRecord

    @property
    def key(self) -> tuple[str, int]:
        return (self.buffer_id, self.seq)


@dataclass(frozen=True, slots=True)
class AckSet:
    """The seqs of the envelopes a sink durably accepted; any subset of the batch."""

    accepted: frozenset[int] = frozenset()

    @classmethod
    def of(cls, seqs: Iterable[int]) -> AckSet:
        return cls(frozenset(seqs))

    @classmethod
    def all(cls, batch: Sequence[Envelope]) -> AckSet:
        return cls.of(e.seq for e in batch)

    @classmethod
    def none(cls) -> AckSet:
        return cls()

    def __contains__(self, seq: object) -> bool:
        return seq in self.accepted

    def __len__(self) -> int:
        return len(self.accepted)


def to_wire(e: Envelope) -> dict[str, object]:
    """The plain-JSON wire form. ``buffer_id`` + ``seq`` is the dedupe key."""
    head: dict[str, object] = {"buffer_id": e.buffer_id, "seq": e.seq}
    p = e.payload
    if isinstance(p, Reading):
        return head | {
            "type": "reading",
            "sensor_id": p.sensor_id,
            "value": p.value,
            "unit": p.unit,
            "mono_ns": p.mono_ns,
            "wall_ns": p.wall_ns,
            "boot_id": p.boot_id,
            "ts_quality": p.ts_quality,
            "qc_flag": p.qc_flag,
            "qc_tests": list(p.qc_tests),
        }
    return head | {
        "type": "gap",
        "sensor_id": p.sensor_id,
        "boot_id": p.boot_id,
        "from_mono_ns": p.from_mono_ns,
        "to_mono_ns": p.to_mono_ns,
        "reason": p.reason,
        "count": p.count,
    }


class SinkError(Exception):
    """A send failed; the whole batch will be retried."""


class Sink(Protocol):
    def send(self, batch: Sequence[Envelope]) -> AckSet: ...

    def close(self) -> None: ...
