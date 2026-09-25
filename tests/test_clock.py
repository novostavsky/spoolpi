from __future__ import annotations

import ctypes
import json
import subprocess
import sys
import textwrap
import time

import pytest
from hypothesis import given
from hypothesis import strategies as st

from spool.core import clock
from spool.core.clock import (
    EXPECTED_TIMEX_SIZE,
    ClockAnchor,
    SystemClock,
    _Timex,
    measure_offset_ns,
    timex_plausible,
)
from spool.core.reading import TS_CORRECTED, TS_SYNCED, TS_UNSYNCED, Reading
from tests.harness.fakeclock import (
    NS_PER_S,
    FakeClock,
    run_in_time_namespace,
    time_namespaces_available,
)

linux_only = pytest.mark.skipif(not sys.platform.startswith("linux"), reason="Linux only")

TRUE_OFFSET_NS = 1_790_000_000 * NS_PER_S + 123_456_789  # a plausible 2026 epoch offset


def _reading(fake: FakeClock, anchor: ClockAnchor) -> Reading:
    mono, wall, quality = anchor.stamp()
    return Reading("t1", 1.0, "C", mono, wall, fake.boot_id, ts_quality=quality)


# --- adjtimex / sync detection ---------------------------------------------


def test_timex_struct_matches_abi_size() -> None:
    assert ctypes.sizeof(_Timex) == EXPECTED_TIMEX_SIZE


@linux_only
def test_adjtimex_is_usable_on_this_kernel() -> None:
    assert clock.SYNC_BACKEND == "adjtimex"
    _, tx = clock._adjtimex_read()
    assert timex_plausible(tx, time.time())


def test_zeroed_struct_fails_canaries() -> None:
    assert not timex_plausible(_Timex(), time.time())


@linux_only
def test_wrong_time_fails_canaries() -> None:
    _, tx = clock._adjtimex_read()
    assert not timex_plausible(tx, time.time() + 3600)


@linux_only
def test_adjtimex_agrees_with_timedatectl() -> None:
    try:
        subprocess.run(["timedatectl", "status"], capture_output=True, check=True, timeout=5)
    except (OSError, subprocess.SubprocessError):
        pytest.skip("timedatectl not available")
    assert clock._synced_via_adjtimex() == clock._synced_via_timedatectl()


def test_falls_back_to_timedatectl_when_adjtimex_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    def boom() -> bool:
        raise OSError("nope")

    monkeypatch.setattr(clock, "_synced_via_adjtimex", boom)
    monkeypatch.setattr(clock, "_synced_via_timedatectl", lambda: True)
    assert clock.clock_synced() is True


@pytest.mark.parametrize(
    ("stdout", "exc", "expected"),
    [
        ("yes\n", None, True),
        ("no\n", None, False),
        ("", FileNotFoundError("timedatectl"), False),
        ("", subprocess.TimeoutExpired("timedatectl", 5), False),
    ],
)
def test_timedatectl_parsing(
    monkeypatch: pytest.MonkeyPatch, stdout: str, exc: Exception | None, expected: bool
) -> None:
    def fake_run(*args: object, **kwargs: object) -> subprocess.CompletedProcess[str]:
        if exc is not None:
            raise exc
        return subprocess.CompletedProcess(args, 0, stdout=stdout, stderr="")

    monkeypatch.setattr(clock.subprocess, "run", fake_run)
    assert clock._synced_via_timedatectl() is expected


def test_mono_clock_falls_back_without_boottime(monkeypatch: pytest.MonkeyPatch) -> None:
    real = time.clock_gettime_ns

    def no_boottime(clk: int) -> int:
        if clk == time.CLOCK_BOOTTIME:
            raise OSError(22, "Invalid argument")
        return real(clk)

    monkeypatch.setattr(clock.time, "clock_gettime_ns", no_boottime)
    assert clock._pick_mono_clock() == time.CLOCK_MONOTONIC


@linux_only
def test_boot_id_is_the_kernel_one() -> None:
    with open("/proc/sys/kernel/random/boot_id") as f:
        assert clock.BOOT_ID == f.read().strip()


# --- anchoring with a scripted clock ----------------------------------------


def test_sync_transition_anchors_offset_and_corrects_earlier_readings() -> None:
    fake = FakeClock()  # wall == mono: stamped in the 1970s
    anchor = ClockAnchor(fake)
    assert anchor.poll() is None

    early = _reading(fake, anchor)
    assert early.ts_quality == TS_UNSYNCED
    assert early.wall_ns < NS_PER_S * 60  # the classic 1970 timestamp

    fake.advance(60 * NS_PER_S)
    fake.ntp_step(TRUE_OFFSET_NS)
    result = anchor.poll()

    assert result is not None
    assert abs(result.offset_ns - TRUE_OFFSET_NS) < 1_000_000
    fixed = anchor.correct(early)
    assert fixed.ts_quality == TS_CORRECTED
    assert fixed.wall_ns == early.mono_ns + TRUE_OFFSET_NS
    assert anchor.step_count == 0

    later = _reading(fake, anchor)
    assert later.ts_quality == TS_SYNCED
    assert anchor.correct(later) is later


@given(
    pre_sync_mono=st.lists(st.integers(0, 10**6 * NS_PER_S), min_size=1, max_size=50),
    true_offset=st.integers(-(10**9) * NS_PER_S, 10**10 * NS_PER_S),
)
def test_correction_is_exact_for_any_offset(pre_sync_mono: list[int], true_offset: int) -> None:
    fake = FakeClock()
    anchor = ClockAnchor(fake)
    readings = [Reading("s", None, None, m, m, fake.boot_id) for m in pre_sync_mono]
    fake.ntp_step(true_offset)
    anchor.poll()
    for r in readings:
        assert anchor.correct(r).wall_ns == r.mono_ns + true_offset


@pytest.mark.parametrize("step_ns", [-5 * NS_PER_S, 5 * NS_PER_S])
def test_step_after_sync_is_detected_in_either_direction(step_ns: int) -> None:
    fake = FakeClock()
    fake.ntp_step(TRUE_OFFSET_NS)
    anchor = ClockAnchor(fake)
    anchor.poll()

    fake.advance(NS_PER_S)
    fake.wall_offset_ns += step_ns
    result = anchor.poll()

    assert anchor.step_count == 1
    assert anchor.last_step_ns == step_ns
    assert result is not None and result.offset_ns == TRUE_OFFSET_NS + step_ns


def test_slew_below_threshold_is_not_a_step() -> None:
    fake = FakeClock()
    fake.ntp_step(TRUE_OFFSET_NS)
    anchor = ClockAnchor(fake)
    anchor.poll()
    fake.wall_offset_ns += 500_000  # 0.5 ms, well within slewing
    anchor.poll()
    assert anchor.step_count == 0


def test_readings_from_another_boot_are_not_corrected() -> None:
    fake = FakeClock(boot_id="boot-A")
    anchor = ClockAnchor(fake)
    old = _reading(fake, anchor)
    fake.reboot("boot-B")
    fake.ntp_step(TRUE_OFFSET_NS)
    anchor.poll()
    assert anchor.correct(old) is old


def test_anchor_kept_when_sync_lost() -> None:
    fake = FakeClock()
    fake.ntp_step(TRUE_OFFSET_NS)
    anchor = ClockAnchor(fake)
    first = anchor.poll()
    fake.is_synced = False
    fake.wall_offset_ns = 0  # someone sets the clock back to 1970
    assert anchor.poll() == first
    assert not anchor.synced


def test_background_thread_picks_up_sync() -> None:
    fake = FakeClock()
    anchor = ClockAnchor(fake)
    anchor.start(interval_s=0.01)
    try:
        assert anchor.anchor is None
        fake.ntp_step(TRUE_OFFSET_NS)
        deadline = time.monotonic() + 2
        while anchor.anchor is None and time.monotonic() < deadline:
            time.sleep(0.01)
        assert anchor.anchor is not None
        assert anchor.anchor.offset_ns == TRUE_OFFSET_NS
    finally:
        anchor.stop()


# --- acceptance: real kernel, manipulated clock via time namespace -----------

_CHILD = textwrap.dedent(
    """
    import json
    from spool.core.clock import ClockAnchor, SystemClock

    class InjectedSync(SystemClock):
        flag = False
        def synced(self):
            return self.flag

    src = InjectedSync()
    anchor = ClockAnchor(src)
    before = anchor.poll()
    src.flag = True
    after = anchor.poll()
    print(json.dumps({"anchored_before_sync": before is not None, "offset_ns": after.offset_ns}))
    """
)


@linux_only
@pytest.mark.skipif(
    sys.platform.startswith("linux") and not time_namespaces_available(),
    reason="unprivileged time namespaces unavailable",
)
@pytest.mark.parametrize("shift_s", [1000, 86_400 * 3])
def test_acceptance_offset_in_time_namespace_within_1ms(shift_s: int) -> None:
    proc = run_in_time_namespace([sys.executable, "-c", _CHILD], shift_s)
    host_offset = measure_offset_ns(SystemClock())
    child = json.loads(proc.stdout)

    assert child["anchored_before_sync"] is False
    # CLOCK_BOOTTIME is shifted forward in the namespace, so wall - mono shrinks by exactly that.
    injected = shift_s * NS_PER_S
    assert abs((host_offset - child["offset_ns"]) - injected) < 1_000_000
