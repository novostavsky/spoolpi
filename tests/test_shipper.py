from __future__ import annotations

import random
import sqlite3
import time
from collections.abc import Iterator, Sequence
from pathlib import Path
from typing import Any

import pytest

from spoolpi.core import shipper as shipper_mod
from spoolpi.core.buffer import ACKED, INFLIGHT, PENDING, REJECTED, BatchWriter, Buffer
from spoolpi.core.clock import ClockAnchor
from spoolpi.core.reading import TS_CORRECTED, Reading
from spoolpi.core.retention import GapRecord, Policy, Retention
from spoolpi.core.shipper import Backoff, Shipper
from spoolpi.sinks.base import AckSet, Envelope
from spoolpi.sinks.memory import Hang, MemorySink, Partial, Raise, Reject
from tests.harness.fakeclock import NS_PER_S, FakeClock
from tests.shipping import FAST_BACKOFF, assert_invariants, drain, fill, reading, steps, wait_for


@pytest.fixture
def db(tmp_path: Path) -> Path:
    return tmp_path / "b.db"


@pytest.fixture
def running() -> Iterator[list[Shipper]]:
    started: list[Shipper] = []
    yield started
    for s in started:
        s.stop(timeout_s=2)


def ship(db: Path, sink: MemorySink, running: list[Shipper], **kw: object) -> Shipper:
    opts: dict[str, object] = {
        "batch_size": 100,
        "send_timeout_s": 0.2,
        "backoff": FAST_BACKOFF,
        "poll_interval_s": 0.02,
        "purge_interval_s": 3600,
    }
    opts.update(kw)
    s = Shipper(db, sink, **opts)  # type: ignore[arg-type]
    s.start()
    running.append(s)
    return s


# --- each MemorySink mode (M4 acceptance) -----------------------------------


def test_accept(db: Path, running: list[Shipper]) -> None:
    fill(db, 250)
    sink = MemorySink()
    s = ship(db, sink, running)
    drain(db)
    assert steps(sink) == list(range(250))
    assert s.stats.acked == 250


def test_reject_retries_in_order_without_duplicates(db: Path, running: list[Shipper]) -> None:
    fill(db, 150)
    sink = MemorySink([Reject(), Reject()])
    ship(db, sink, running)
    drain(db)
    assert steps(sink) == list(range(150))


def test_partial_prefix_keeps_order(db: Path, running: list[Shipper]) -> None:
    fill(db, 200)
    sink = MemorySink([Partial.first(30)] * 3)
    ship(db, sink, running)
    drain(db)
    assert steps(sink) == list(range(200))


def test_partial_non_prefix_delivers_everything_once(db: Path, running: list[Shipper]) -> None:
    fill(db, 200)
    sink = MemorySink([Partial.where(lambda e: e.seq % 2 == 0)] * 2)
    ship(db, sink, running)
    drain(db)
    assert sorted(steps(sink)) == list(range(200))


def test_raise_releases_and_retries(db: Path, running: list[Shipper]) -> None:
    fill(db, 150)
    sink = MemorySink([Raise(), Raise(ConnectionError("uplink down")), Raise()])
    s = ship(db, sink, running)
    drain(db)
    assert steps(sink) == list(range(150))
    assert s.stats.failed_sends == 3


def test_hang_times_out_and_is_resent(db: Path, running: list[Shipper]) -> None:
    fill(db, 150)
    sink = MemorySink([Hang()])
    s = ship(db, sink, running)
    drain(db)
    assert s.stats.timeouts == 1
    assert steps(sink) == list(range(150))


def test_hang_that_delivers_late_is_a_duplicate_not_a_collision(
    db: Path, running: list[Shipper]
) -> None:
    fill(db, 150)
    sink = MemorySink([Hang(for_s=0.4, late_accept=True)])
    ship(db, sink, running)
    drain(db)
    wait_for(lambda: len(sink.received) == 250)  # the late batch of 100 lands too
    assert_invariants(sink, 150)


def test_acks_for_seqs_never_sent_are_ignored(db: Path, running: list[Shipper]) -> None:
    class Overclaiming(MemorySink):
        def send(self, batch: Sequence[Envelope]) -> AckSet:
            got = super().send(batch)
            return AckSet.of({*got.accepted, 10**9})

    fill(db, 50)
    sink = Overclaiming()
    s = ship(db, sink, running)
    drain(db)
    assert s.stats.acked == 50


# --- backoff ----------------------------------------------------------------


def test_backoff_grows_is_capped_and_jittered() -> None:
    b = Backoff(initial_s=1, max_s=8, multiplier=2, immediate_retries=0)
    rng = random.Random(0)
    for failures, base in [(1, 1), (2, 2), (3, 4), (4, 8), (5, 8), (10_000, 8)]:
        for _ in range(50):
            assert base / 2 <= b.delay(failures, rng) <= base


def test_first_failures_of_a_streak_retry_immediately() -> None:
    b = Backoff(initial_s=1, max_s=8, multiplier=2, immediate_retries=2)
    rng = random.Random(0)
    assert b.delay(1, rng) == b.delay(2, rng) == 0.0
    for failures, base in [(3, 1), (4, 2), (5, 4), (10_000, 8)]:
        assert base / 2 <= b.delay(failures, rng) <= base


def test_an_outage_costs_only_the_immediate_retries_before_backing_off(
    db: Path, running: list[Shipper]
) -> None:
    fill(db, 10)
    sink = MemorySink(default=Raise())
    ship(db, sink, running, backoff=Backoff(initial_s=1.0, max_s=1.0, immediate_retries=2))
    wait_for(lambda: sink.calls == 3)  # the first try plus two immediate retries
    time.sleep(0.3)
    assert sink.calls == 3  # then the backoff (at least 0.5 s) holds


def test_failure_backoff_is_not_cut_short_by_notify(db: Path, running: list[Shipper]) -> None:
    fill(db, 10)
    sink = MemorySink([Raise()])
    backoff = Backoff(initial_s=0.6, max_s=0.6, immediate_retries=0)
    s = ship(db, sink, running, backoff=backoff)
    wait_for(lambda: sink.calls == 1)
    for _ in range(10):
        s.notify()
        time.sleep(0.01)
    assert sink.calls == 1  # still backing off (at least 0.3 s)


# --- lifecycle --------------------------------------------------------------


def test_stop_during_hang_returns_in_time_and_releases_rows(db: Path) -> None:
    fill(db, 10)
    sink = MemorySink([Hang()])
    s = Shipper(db, sink, send_timeout_s=30, poll_interval_s=0.02)
    s.start()
    wait_for(lambda: sink.calls == 1)
    t0 = time.monotonic()
    assert s.stop(timeout_s=1.0)
    assert time.monotonic() - t0 < 1.0
    with Buffer(db) as b:
        assert b.counts() == {PENDING: 10, INFLIGHT: 0, ACKED: 0, REJECTED: 0}


def test_stop_lets_the_batch_in_flight_finish(db: Path) -> None:
    fill(db, 10)
    sink = MemorySink(latency_s=0.3)
    s = Shipper(db, sink, poll_interval_s=0.02)
    s.start()
    wait_for(lambda: sink.calls == 1)
    assert s.stop(timeout_s=5)
    with Buffer(db) as b:
        assert b.counts()[ACKED] == 10


def test_writer_is_not_blocked_by_a_hung_sink(db: Path, running: list[Shipper]) -> None:
    fill(db, 10)
    sink = MemorySink([Hang()])
    ship(db, sink, running, send_timeout_s=30)
    wait_for(lambda: sink.calls == 1)
    t0 = time.monotonic()
    with Buffer(db) as b:
        w = BatchWriter(b, max_rows=50)
        for i in range(2000):
            w.write(reading(10 + i))
        w.flush()
    assert time.monotonic() - t0 < 2.0


def test_notify_wakes_an_idle_shipper(db: Path, running: list[Shipper]) -> None:
    Buffer(db).close()
    sink = MemorySink()
    s = ship(db, sink, running, poll_interval_s=10)
    time.sleep(0.1)
    fill(db, 5)
    s.notify()
    wait_for(lambda: len(sink.received) == 5, timeout_s=1.0)


def test_hung_sends_are_capped(db: Path, running: list[Shipper]) -> None:
    fill(db, 10)
    sink = MemorySink([Hang()] * 5)
    s = ship(db, sink, running, send_timeout_s=0.05, max_abandoned_sends=2)
    wait_for(lambda: s.stats.failed_sends >= 4)
    assert sink.calls == 2


def test_a_failed_buffer_open_is_retried_not_fatal(
    db: Path, running: list[Shipper], monkeypatch: pytest.MonkeyPatch
) -> None:
    # Regression: an exception opening the buffer ended the shipper thread, and with
    # it all delivery, without a trace.
    fill(db, 20)
    real = shipper_mod.Buffer
    calls = {"n": 0}

    def flaky_open(path: Path, **kwargs: Any) -> Buffer:
        calls["n"] += 1
        if calls["n"] == 1:
            raise sqlite3.OperationalError("database is locked")
        return real(path, **kwargs)

    monkeypatch.setattr(shipper_mod, "Buffer", flaky_open)
    sink = MemorySink()
    s = ship(db, sink, running)
    wait_for(lambda: s.stats.acked == 20)  # acked is counted after the sink returns
    assert calls["n"] == 2 and len(sink.received) == 20


def test_acked_rows_are_purged_on_cadence(db: Path, running: list[Shipper]) -> None:
    fill(db, 50)
    ship(db, MemorySink(), running, purge_interval_s=0.05)
    drain(db)
    with Buffer(db) as b:
        wait_for(lambda: b.counts()[ACKED] == 0)


def test_purge_keeps_up_beyond_one_chunk(db: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("spoolpi.core.shipper._PURGE_CHUNK", 7)
    fill(db, 50)
    with Buffer(db) as b:
        b.ack(s.id for s in b.claim(50))
    s = Shipper(db, MemorySink(), purge_interval_s=0.0, poll_interval_s=0.02)
    s.start()
    try:
        with Buffer(db) as b:
            wait_for(lambda: b.counts()[ACKED] == 0, timeout_s=1.0)
    finally:
        s.stop(timeout_s=2)


def test_unsynced_readings_are_corrected_at_ship_time(db: Path, running: list[Shipper]) -> None:
    clock = FakeClock()
    anchor = ClockAnchor(clock)
    with Buffer(db) as b:
        b.append([reading(i, boot_id=clock.boot_id) for i in range(5)])
    offset = 1_790_000_000 * NS_PER_S
    clock.ntp_step(offset)
    anchor.poll()
    sink = MemorySink()
    ship(db, sink, running, anchor=anchor)
    drain(db)
    for e in sink.received:
        assert isinstance(e.payload, Reading)
        assert e.payload.ts_quality == TS_CORRECTED
        assert e.payload.wall_ns == e.payload.mono_ns + offset


# --- holding unsynced readings -------------------------------------------------


def _unsynced(db: Path, n: int) -> tuple[FakeClock, ClockAnchor]:
    clock = FakeClock()
    anchor = ClockAnchor(clock)
    anchor.poll()  # not synced: no anchor yet
    with Buffer(db) as b:
        b.append([reading(i, boot_id=clock.boot_id) for i in range(n)])
    return clock, anchor


def test_hold_waits_for_sync_then_ships_corrected(db: Path, running: list[Shipper]) -> None:
    clock, anchor = _unsynced(db, 30)
    sink = MemorySink()
    ship(db, sink, running, anchor=anchor, hold_unsynced_s=30)
    time.sleep(0.3)
    assert sink.received == []  # held: shipping now would send them uncorrected
    offset = 1_790_000_000 * NS_PER_S
    clock.ntp_step(offset)
    anchor.poll()
    drain(db)
    assert steps(sink) == list(range(30))
    for e in sink.received:
        assert isinstance(e.payload, Reading)
        assert e.payload.ts_quality == TS_CORRECTED
        assert e.payload.wall_ns == e.payload.mono_ns + offset


def test_hold_runs_out_and_ships_uncorrected(db: Path, running: list[Shipper]) -> None:
    _, anchor = _unsynced(db, 10)
    sink = MemorySink()
    started = time.monotonic()
    ship(db, sink, running, anchor=anchor, hold_unsynced_s=0.3)
    drain(db)
    assert time.monotonic() - started >= 0.3
    assert steps(sink) == list(range(10))
    assert all(isinstance(e.payload, Reading) and e.payload.ts_quality == 0 for e in sink.received)


def test_no_hold_when_the_clock_is_already_synced(db: Path, running: list[Shipper]) -> None:
    clock = FakeClock(synced=True)
    anchor = ClockAnchor(clock)
    anchor.poll()
    fill(db, 10)
    sink = MemorySink()
    started = time.monotonic()
    ship(db, sink, running, anchor=anchor, hold_unsynced_s=30)
    drain(db)
    assert time.monotonic() - started < 5


def test_hold_zero_ships_at_once(db: Path, running: list[Shipper]) -> None:
    _, anchor = _unsynced(db, 10)
    sink = MemorySink()
    started = time.monotonic()
    ship(db, sink, running, anchor=anchor, hold_unsynced_s=0)
    drain(db)
    assert time.monotonic() - started < 5
    assert steps(sink) == list(range(10))


def test_gap_records_ship_during_the_hold(db: Path, running: list[Shipper]) -> None:
    clock = FakeClock()
    anchor = ClockAnchor(clock)
    anchor.poll()
    with Buffer(db, retention=Retention(Policy.DROP_OLDEST, 5)) as b:
        b.append([reading(i, boot_id=clock.boot_id) for i in range(8)])  # drops 3 into a gap
    sink = MemorySink()
    ship(db, sink, running, anchor=anchor, hold_unsynced_s=30)
    wait_for(lambda: any(isinstance(e.payload, GapRecord) for e in sink.received))
    time.sleep(0.2)
    assert all(isinstance(e.payload, GapRecord) for e in sink.received)  # readings still held
