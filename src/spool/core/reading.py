from __future__ import annotations

from dataclasses import dataclass
from typing import Final

TS_UNSYNCED: Final = 0  # sampled before NTP sync, wall clock untrustworthy
TS_CORRECTED: Final = 1  # retroactively fixed once an anchor was available
TS_SYNCED: Final = 2  # clock was synced at sample time

QC_NOT_EVALUATED: Final = 2  # QARTOD


@dataclass(frozen=True, slots=True)
class Reading:
    sensor_id: str
    value: float | None  # None is legitimate: the sensor read failed
    unit: str | None
    mono_ns: int  # CLOCK_BOOTTIME at sample
    wall_ns: int  # CLOCK_REALTIME at sample, may be wrong
    boot_id: str
    ts_quality: int = TS_UNSYNCED
    qc_flag: int = QC_NOT_EVALUATED
    qc_tests: tuple[str, ...] = ()
