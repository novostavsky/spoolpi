"""What happens when the buffer reaches its cap, and the gap records that say so.

A gap record is data, not a log line: it is stored in the same transaction
that discards the readings, gets a seq, and ships to the sink like a reading.
"""

from __future__ import annotations

import enum
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Final

from spool.core.reading import Reading

REASON_DROP_OLDEST: Final = "retention:drop_oldest"
REASON_BACKPRESSURE: Final = "backpressure"


class Policy(enum.StrEnum):
    DROP_OLDEST = "drop_oldest"
    HALT_AND_ALARM = "halt_and_alarm"


@dataclass(frozen=True, slots=True)
class Retention:
    policy: Policy
    max_rows: int  # unacked readings (pending + inflight)

    def __post_init__(self) -> None:
        if self.max_rows < 1:
            raise ValueError(f"retention max_rows must be at least 1, got {self.max_rows}")


@dataclass(frozen=True, slots=True)
class GapRecord:
    """``count`` readings were discarded, all sampled within [from, to] of ``boot_id``.

    Other readings from that interval may still have been delivered; the count
    is exact, the interval is a bound.
    """

    sensor_id: str | None  # None = all sensors
    boot_id: str
    from_mono_ns: int
    to_mono_ns: int
    reason: str
    count: int


class BufferFull(Exception):
    """halt_and_alarm refused a write. The batch was discarded and recorded as a gap.

    ``discarded`` is the whole batch, which can include readings written before
    the one whose write raised.
    """

    def __init__(self, message: str, discarded: int) -> None:
        super().__init__(message)
        self.discarded = discarded


def summarize(readings: Iterable[Reading], reason: str) -> list[GapRecord]:
    """One gap record per (sensor, boot), in first-seen order."""
    groups: dict[tuple[str, str], list[int]] = {}
    for r in readings:
        groups.setdefault((r.sensor_id, r.boot_id), []).append(r.mono_ns)
    return [
        GapRecord(sensor, boot, min(monos), max(monos), reason, len(monos))
        for (sensor, boot), monos in groups.items()
    ]
