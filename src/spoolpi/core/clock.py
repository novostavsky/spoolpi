"""Boot identity, clock sampling, NTP sync detection, and wall-clock anchoring.

Every reading carries CLOCK_BOOTTIME (``mono_ns``), which is trustworthy within a
boot, and CLOCK_REALTIME (``wall_ns``), which is not trustworthy until NTP has
synchronised. Once it has, ``ClockAnchor`` records ``offset = wall - mono`` and
readings sampled earlier in the same boot can be corrected to ``mono + offset``.
"""

from __future__ import annotations

import ctypes
import ctypes.util
import logging
import subprocess
import threading
import time
import uuid
from dataclasses import dataclass, replace
from typing import Final, Literal, Protocol

from spoolpi.core.reading import TS_CORRECTED, TS_SYNCED, TS_UNSYNCED, Reading

log = logging.getLogger("spoolpi.clock")

TIME_ERROR: Final = 5
STA_UNSYNC: Final = 0x0040
STA_NANO: Final = 0x2000
MAXFREQ_SCALED: Final = 500 << 16  # constant `tolerance` the kernel always reports

_BOOT_ID_PATH: Final = "/proc/sys/kernel/random/boot_id"


# --- boot identity ----------------------------------------------------------


def _read_boot_id() -> str:
    try:
        with open(_BOOT_ID_PATH) as f:
            return f.read().strip()
    except OSError:
        # Unique per process: correction is then refused across restarts, never misapplied.
        return f"no-boot-id-{uuid.uuid4()}"


BOOT_ID: Final = _read_boot_id()


# --- clocks -----------------------------------------------------------------


def _pick_mono_clock() -> int:
    try:
        clk: int = time.CLOCK_BOOTTIME
        time.clock_gettime_ns(clk)
        return clk
    except (AttributeError, OSError):
        # CLOCK_MONOTONIC stops during suspend, so offsets drift across a suspend.
        return time.CLOCK_MONOTONIC


MONO_CLOCK: Final = _pick_mono_clock()


def mono_ns() -> int:
    return time.clock_gettime_ns(MONO_CLOCK)


def wall_ns() -> int:
    return time.time_ns()


# --- adjtimex ---------------------------------------------------------------


class _Timeval(ctypes.Structure):
    _fields_ = [("tv_sec", ctypes.c_long), ("tv_usec", ctypes.c_long)]


class _Timex(ctypes.Structure):
    # glibc's legacy `struct timex`, the layout the plain `adjtimex` symbol uses on
    # both 32- and 64-bit. `long` tracks the ABI word size automatically.
    _fields_ = [
        ("modes", ctypes.c_uint),
        ("offset", ctypes.c_long),
        ("freq", ctypes.c_long),
        ("maxerror", ctypes.c_long),
        ("esterror", ctypes.c_long),
        ("status", ctypes.c_int),
        ("constant", ctypes.c_long),
        ("precision", ctypes.c_long),
        ("tolerance", ctypes.c_long),
        ("time", _Timeval),
        ("tick", ctypes.c_long),
        ("ppsfreq", ctypes.c_long),
        ("jitter", ctypes.c_long),
        ("shift", ctypes.c_int),
        ("stabil", ctypes.c_long),
        ("jitcnt", ctypes.c_long),
        ("calcnt", ctypes.c_long),
        ("errcnt", ctypes.c_long),
        ("stbcnt", ctypes.c_long),
        ("tai", ctypes.c_int),
        ("_reserved", ctypes.c_int * 11),
    ]


EXPECTED_TIMEX_SIZE: Final = {8: 208, 4: 128}[ctypes.sizeof(ctypes.c_long)]


class _GuardedTimex(ctypes.Structure):
    # Slack after the struct so a libc with a larger layout can't write past our buffer.
    _fields_ = [("tx", _Timex), ("_guard", ctypes.c_char * 256)]


def _load_libc() -> ctypes.CDLL | None:
    try:
        return ctypes.CDLL(ctypes.util.find_library("c") or "libc.so.6", use_errno=True)
    except OSError:
        return None


_libc = _load_libc()


def _adjtimex_read() -> tuple[int, _Timex]:
    if _libc is None or not hasattr(_libc, "adjtimex"):
        raise OSError("adjtimex unavailable")
    buf = _GuardedTimex()  # modes=0: read-only query, no privileges needed
    rc = int(_libc.adjtimex(ctypes.byref(buf)))
    if rc < 0:
        raise OSError(ctypes.get_errno(), "adjtimex failed")
    return rc, buf.tx


def timex_plausible(tx: _Timex, now_s: float) -> bool:
    """Canary checks that fail if the struct layout doesn't match what libc wrote."""
    sub_second_limit = 1_000_000_000 if tx.status & STA_NANO else 1_000_000
    return bool(
        tx.tolerance == MAXFREQ_SCALED
        and 1 <= tx.tick <= 1_000_000
        and 0 <= tx.time.tv_usec < sub_second_limit
        # Also trips on 32-bit tv_sec after 2038 overflow, which is the right outcome.
        and abs(tx.time.tv_sec - now_s) < 60
    )


def _adjtimex_usable() -> bool:
    if ctypes.sizeof(_Timex) != EXPECTED_TIMEX_SIZE:
        return False
    try:
        _, tx = _adjtimex_read()
    except OSError:
        return False
    return timex_plausible(tx, time.time())


def _synced_via_adjtimex() -> bool:
    # Same rule systemd uses for NTPSynchronized.
    rc, tx = _adjtimex_read()
    return rc != TIME_ERROR and not int(tx.status) & STA_UNSYNC


def _synced_via_timedatectl() -> bool:
    """False when timedatectl is missing or fails: unknown sync state is treated as unsynced."""
    try:
        out = subprocess.run(
            ["timedatectl", "show", "-p", "NTPSynchronized", "--value"],
            capture_output=True,
            text=True,
            timeout=5,
            check=True,
        ).stdout
    except (OSError, subprocess.SubprocessError):
        return False
    return out.strip() == "yes"


SyncBackend = Literal["adjtimex", "timedatectl"]
SYNC_BACKEND: Final[SyncBackend] = "adjtimex" if _adjtimex_usable() else "timedatectl"
if SYNC_BACKEND != "adjtimex":
    log.warning("adjtimex unusable on this system, falling back to timedatectl")


def clock_synced() -> bool:
    if SYNC_BACKEND == "adjtimex":
        try:
            return _synced_via_adjtimex()
        except OSError:
            pass
    return _synced_via_timedatectl()


# --- anchoring --------------------------------------------------------------


class ClockSource(Protocol):
    @property
    def boot_id(self) -> str: ...
    def mono_ns(self) -> int: ...
    def wall_ns(self) -> int: ...
    def synced(self) -> bool: ...


class SystemClock:
    @property
    def boot_id(self) -> str:
        return BOOT_ID

    def mono_ns(self) -> int:
        return mono_ns()

    def wall_ns(self) -> int:
        return wall_ns()

    def synced(self) -> bool:
        return clock_synced()


@dataclass(frozen=True, slots=True)
class Anchor:
    boot_id: str
    offset_ns: int  # wall - mono, valid for any mono_ns in this boot
    measured_at_mono_ns: int


@dataclass(frozen=True, slots=True)
class _State:
    synced: bool
    anchor: Anchor | None


def measure_offset_ns(source: ClockSource, tries: int = 5) -> int:
    """wall - mono, sampled between two mono reads; keeps the tightest bracket."""
    best_width: int | None = None
    best = 0
    for _ in range(tries):
        m0 = source.mono_ns()
        w = source.wall_ns()
        m1 = source.mono_ns()
        width = m1 - m0
        if best_width is None or width < best_width:
            best_width = width
            best = w - (m0 + m1) // 2
    return best


class ClockAnchor:
    """Tracks sync state and the wall-minus-mono offset of the current boot.

    ``poll()`` does one observation; ``start()`` runs it on a daemon thread. State
    is swapped as a single immutable object, so readers need no lock.
    """

    def __init__(
        self,
        source: ClockSource | None = None,
        *,
        step_threshold_ns: int = 20_000_000,
    ) -> None:
        self._source: ClockSource = source if source is not None else SystemClock()
        self._step_threshold_ns = step_threshold_ns
        self._state = _State(synced=False, anchor=None)
        self.step_count = 0
        self.last_step_ns = 0
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    @property
    def synced(self) -> bool:
        return self._state.synced

    @property
    def anchor(self) -> Anchor | None:
        return self._state.anchor

    def poll(self) -> Anchor | None:
        synced = self._source.synced()
        anchor = self._state.anchor
        if synced:
            offset = measure_offset_ns(self._source)
            if anchor is None:
                log.info("clock synchronised, anchor offset %d ns", offset)
            elif abs(offset - anchor.offset_ns) > self._step_threshold_ns:
                # NTP steps in either direction; the newest offset is the better one.
                self.step_count += 1
                self.last_step_ns = offset - anchor.offset_ns
                log.warning("wall clock stepped by %+d ns", self.last_step_ns)
            anchor = Anchor(self._source.boot_id, offset, self._source.mono_ns())
        # When sync is lost the previous anchor is kept: mono is still good, wall is suspect.
        self._state = _State(synced=synced, anchor=anchor)
        return anchor

    def stamp(self) -> tuple[int, int, int]:
        """(mono_ns, wall_ns, ts_quality) for a reading sampled now."""
        quality = TS_SYNCED if self._state.synced else TS_UNSYNCED
        return self._source.mono_ns(), self._source.wall_ns(), quality

    def correct(self, reading: Reading) -> Reading:
        """Rewrite wall_ns of an unsynced reading from this boot; otherwise return it as-is."""
        anchor = self._state.anchor
        if reading.ts_quality != TS_UNSYNCED or anchor is None or reading.boot_id != anchor.boot_id:
            return reading
        return replace(reading, wall_ns=reading.mono_ns + anchor.offset_ns, ts_quality=TS_CORRECTED)

    def start(self, interval_s: float = 1.0) -> None:
        self.poll()
        self._stop.clear()
        self._thread = threading.Thread(
            target=self._run, args=(interval_s,), name="spoolpi-clock-anchor", daemon=True
        )
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join()
            self._thread = None

    def _run(self, interval_s: float) -> None:
        while not self._stop.wait(interval_s):
            try:
                self.poll()
            except Exception:
                # A dead poller would silently freeze sync state; keep going.
                log.exception("clock poll failed")
