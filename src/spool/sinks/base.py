"""The sink contract.

A sink receives a batch of envelopes and reports which ones it durably accepted,
and which it refuses for good. Anything in neither set is resent later, and so
is the whole batch if ``send`` raises. Delivery is at-least-once, so a sink (or
whatever sits behind it) must deduplicate on ``(buffer_id, seq)``.

``rejected`` is for a record that can never succeed, such as one a server fails
to validate. Anything systemic (auth, schema, endpoint, network) must raise
instead: rejected rows are quarantined and recorded as a gap, not resent.
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
    """Seqs the sink durably accepted, and seqs it refuses permanently.

    Each may be any subset of the batch; the rest is retried.
    """

    accepted: frozenset[int] = frozenset()
    rejected: frozenset[int] = frozenset()

    def __post_init__(self) -> None:
        if both := self.accepted & self.rejected:
            raise ValueError(f"seqs both accepted and rejected: {sorted(both)[:5]}")

    @classmethod
    def of(cls, accepted: Iterable[int], rejected: Iterable[int] = ()) -> AckSet:
        return cls(frozenset(accepted), frozenset(rejected))

    @classmethod
    def all(cls, batch: Sequence[Envelope]) -> AckSet:
        return cls.of(e.seq for e in batch)

    @classmethod
    def none(cls) -> AckSet:
        return cls()

    def __contains__(self, seq: object) -> bool:
        """Whether ``seq`` was accepted."""
        return seq in self.accepted

    def __len__(self) -> int:
        """How many were accepted."""
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
    """``send`` may be abandoned by a timeout while still running, and ``close``
    may then be called concurrently with it: don't free what a send still uses."""

    def send(self, batch: Sequence[Envelope]) -> AckSet: ...

    def close(self) -> None: ...
