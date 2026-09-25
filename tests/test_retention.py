from __future__ import annotations

import contextlib
import random
import sqlite3
import sys
from pathlib import Path

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from spool.core.buffer import INFLIGHT, PENDING, BatchWriter, Buffer
from spool.core.reading import Reading
from spool.core.retention import (
    REASON_BACKPRESSURE,
    REASON_DROP_OLDEST,
    BufferFull,
    GapRecord,
    Policy,
    Retention,
    summarize,
)
from spool.core.shipper import Shipper
from spool.sinks.memory import MemorySink, Raise
from tests.harness.crash import run_until_killed
from tests.shipping import FAST_BACKOFF, assert_invariants, drain, gap_envelopes

ROOT = Path(__file__).resolve().parent.parent


def r(step: int, sensor: str = "s1", boot: str = "boot") -> Reading:
    return Reading(sensor, float(step), None, step, step, boot)


def drop(cap: int) -> Retention:
    return Retention(Policy.DROP_OLDEST, cap)


def halt(cap: int) -> Retention:
    return Retention(Policy.HALT_AND_ALARM, cap)


def stored_steps(b: Buffer) -> list[int]:
    return [int(v) for (v,) in b._conn.execute("SELECT value FROM readings ORDER BY id")]


def test_retention_rejects_nonpositive_cap() -> None:
    with pytest.raises(ValueError, match="at least 1"):
        Retention(Policy.DROP_OLDEST, 0)


def test_summarize_groups_by_sensor_and_boot() -> None:
    got = summarize([r(5), r(1, "s2"), r(3), r(9, boot="b2")], "x")
    assert got == [
        GapRecord("s1", "boot", 3, 5, "x", 2),
        GapRecord("s2", "boot", 1, 1, "x", 1),
        GapRecord("s1", "b2", 9, 9, "x", 1),
    ]


# --- drop_oldest --------------------------------------------------------------


def test_drop_oldest_keeps_newest_and_counts_exactly(tmp_path: Path) -> None:
    with Buffer(tmp_path / "b.db", retention=drop(10)) as b:
        for start in range(0, 25, 5):
            b.append([r(i) for i in range(start, start + 5)])
        assert stored_steps(b) == list(range(15, 25))
        assert b.unacked() == 10
        assert b.gaps() == [GapRecord("s1", "boot", 0, 14, REASON_DROP_OLDEST, 15)]


def test_drop_oldest_gaps_per_sensor_and_boot(tmp_path: Path) -> None:
    with Buffer(tmp_path / "b.db", retention=drop(4)) as b:
        b.append([r(0, "a"), r(1, "b"), r(2, "a", "b2"), r(3, "a")])
        b.append([r(4), r(5), r(6)])
        assert sorted(b.gaps(), key=lambda g: (g.sensor_id or "", g.boot_id)) == [
            GapRecord("a", "b2", 2, 2, REASON_DROP_OLDEST, 1),
            GapRecord("a", "boot", 0, 0, REASON_DROP_OLDEST, 1),
            GapRecord("b", "boot", 1, 1, REASON_DROP_OLDEST, 1),
        ]


def test_drop_oldest_never_evicts_inflight_rows(tmp_path: Path) -> None:
    with Buffer(tmp_path / "b.db", retention=drop(10)) as b:
        b.append([r(i) for i in range(10)])
        b.claim(8)
        b.append([r(i) for i in range(10, 15)])
        # Only 2 pending rows could go, so the 3 oldest incoming were dropped too.
        assert stored_steps(b) == [0, 1, 2, 3, 4, 5, 6, 7, 13, 14]
        assert b.counts()[INFLIGHT] == 8
        assert sum(g.count for g in b.gaps()) == 5


def test_gap_is_frozen_once_sealed(tmp_path: Path) -> None:
    with Buffer(tmp_path / "b.db", retention=drop(2)) as b:
        b.append([r(i) for i in range(4)])  # drops 0, 1
        sealed = b.claim_gaps(10)
        assert [g.gap.count for g in sealed] == [2]
        b.release_gaps([g.id for g in sealed])
        b.append([r(4), r(5)])  # drops 2, 3 into a new gap
        again = b.claim_gaps(10)
        assert [(g.seq, g.gap.count) for g in again] == [(sealed[0].seq, 2), (again[1].seq, 2)]
        assert again[1].seq != sealed[0].seq


def test_gap_seqs_never_collide_with_reading_seqs(tmp_path: Path) -> None:
    with Buffer(tmp_path / "b.db", retention=drop(3)) as b:
        for i in range(0, 30, 3):
            b.append([r(i), r(i + 1), r(i + 2)])
            b.claim_gaps(10)
        reading_seqs = {s for (s,) in b._conn.execute("SELECT seq FROM readings")}
        gap_seqs = {s for (s,) in b._conn.execute("SELECT seq FROM gaps")}
        assert gap_seqs and not reading_seqs & gap_seqs


# --- halt_and_alarm -------------------------------------------------------------


def test_halt_refuses_whole_batch_and_records_backpressure(tmp_path: Path) -> None:
    with Buffer(tmp_path / "b.db", retention=halt(10)) as b:
        b.append([r(i) for i in range(8)])
        with pytest.raises(BufferFull):
            b.append([r(i) for i in range(8, 11)])
        assert stored_steps(b) == list(range(8))
        assert b.gaps() == [GapRecord("s1", "boot", 8, 10, REASON_BACKPRESSURE, 3)]
        b.ack(s.id for s in b.claim(5))
        b.append([r(11), r(12)])  # room again
        assert b.unacked() == 5


def test_batch_writer_drops_a_refused_batch(tmp_path: Path) -> None:
    with Buffer(tmp_path / "b.db", retention=halt(3)) as b:
        w = BatchWriter(b, max_rows=2)
        w.write(r(0))
        w.write(r(1))
        w.write(r(2))
        with pytest.raises(BufferFull):
            w.write(r(3))
        assert w.pending_count == 0  # not retried: it's already counted in a gap
        w.flush()
        assert stored_steps(b) == [0, 1]
        assert sum(g.count for g in b.gaps()) == 2


# --- acceptance: exact accounting -----------------------------------------------

ops = st.lists(
    st.one_of(
        st.tuples(st.just("append"), st.integers(1, 12)),
        st.tuples(st.just("claim"), st.integers(1, 12)),
        st.tuples(st.just("ack"), st.integers(0, 12)),
        st.tuples(st.just("release"), st.integers(0, 12)),
        st.tuples(st.just("purge"), st.just(0)),
        st.tuples(st.just("recover"), st.just(0)),
    ),
    max_size=60,
)


@settings(max_examples=150, deadline=None)
@given(policy=st.sampled_from(list(Policy)), cap=st.integers(1, 20), script=ops)
def test_acceptance_gap_counts_exactly_match_discarded_rows(
    tmp_path_factory: pytest.TempPathFactory,
    policy: Policy,
    cap: int,
    script: list[tuple[str, int]],
) -> None:
    db = tmp_path_factory.mktemp("ret") / "b.db"
    written = acked = 0
    inflight: list[int] = []
    with Buffer(db, retention=Retention(policy, cap)) as b:
        for op, k in script:
            if op == "append":
                with contextlib.suppress(BufferFull):
                    b.append([r(written + i) for i in range(k)])
                written += k
            elif op == "claim":
                inflight += [s.id for s in b.claim(k)]
            elif op == "ack":
                acked += b.ack(inflight[:k])
                inflight = inflight[k:]
            elif op == "release":
                b.release(inflight[:k])
                inflight = inflight[k:]
            elif op == "purge":
                b.purge_acked()
            else:
                b.recover_inflight()
                inflight = []
            counts = b.counts()
            assert b.unacked() == counts[PENDING] + counts[INFLIGHT] <= cap
            discarded = sum(g.count for g in b.gaps())
            assert counts[PENDING] + counts[INFLIGHT] + acked + discarded == written


@pytest.mark.parametrize("policy", list(Policy))
def test_end_to_end_outage_fills_buffer_and_gaps_ship(tmp_path: Path, policy: Policy) -> None:
    db = tmp_path / "b.db"
    n = 2000
    sink = MemorySink([Raise()] * 40)  # uplink down for a while
    with Buffer(db, retention=Retention(policy, 300)) as buf:
        shipper = Shipper(
            db, sink, batch_size=50, backoff=FAST_BACKOFF, poll_interval_s=0.02, send_timeout_s=1
        )
        shipper.start()
        try:
            w = BatchWriter(buf, max_rows=20)
            for i in range(n):
                with contextlib.suppress(BufferFull):
                    w.write(r(i))
            with contextlib.suppress(BufferFull):
                w.flush()
            shipper.notify()
            drain(db)
        finally:
            shipper.stop(timeout_s=2)
    assert gap_envelopes(sink), "the outage should have produced gap records"
    assert_invariants(sink, n)


# --- crash safety: eviction and its gap commit together --------------------------

SPAN = 1_000_000
CAP = 100


def _crash_cycles(tmp_path: Path, cycles: int, seed: int) -> None:
    db = tmp_path / "buffer.db"
    log = tmp_path / "child.out"
    rng = random.Random(seed)
    committed = 0
    for i in range(cycles):
        start = i * SPAN
        argv = [sys.executable, "-m", "tests.harness.buffer_child", str(db), str(start)]
        argv += ["20", "50", str(CAP)]
        run_until_killed(argv, rng.uniform(0.05, 0.5), cwd=ROOT, stdout_path=log)
        conn = sqlite3.connect(db)
        try:
            assert conn.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
            # drop_oldest keeps the newest rows, so this cycle's last commit survives.
            (newest,) = conn.execute(
                "SELECT max(value) FROM readings WHERE value >= ? AND value < ?",
                (start, start + SPAN),
            ).fetchone()
            committed += 0 if newest is None else int(newest) - start + 1
            (rows,) = conn.execute("SELECT count(*) FROM readings").fetchone()
            (dropped,) = conn.execute("SELECT coalesce(sum(count), 0) FROM gaps").fetchone()
        finally:
            conn.close()
        assert rows + dropped == committed, f"cycle {i}: {rows} kept + {dropped} dropped"


def test_retention_crash_smoke(tmp_path: Path) -> None:
    _crash_cycles(tmp_path, cycles=20, seed=3)


@pytest.mark.slow
def test_retention_300_crash_cycles(tmp_path: Path) -> None:
    seed = random.randrange(2**32)
    print(f"seed={seed}")
    _crash_cycles(tmp_path, cycles=300, seed=seed)
