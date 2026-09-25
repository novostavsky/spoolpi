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


@dataclass(frozen=True, slots=True)
class Envelope:
    buffer_id: str
    seq: int
    reading: Reading

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


class SinkError(Exception):
    """A send failed; the whole batch will be retried."""


class Sink(Protocol):
    def send(self, batch: Sequence[Envelope]) -> AckSet: ...

    def close(self) -> None: ...
