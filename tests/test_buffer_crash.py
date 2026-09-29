from __future__ import annotations

import os
import random
import sqlite3
import sys
from dataclasses import dataclass, field
from pathlib import Path

import pytest

from spoolpi.core.buffer import INFLIGHT
from tests.harness.crash import (
    crash_seed,
    has_table,
    open_if_created,
    run_until_killed,
    wait_for_ready,
)

ROOT = Path(__file__).resolve().parent.parent
MAX_ROWS = 20
WAL_AUTOCHECKPOINT = 50  # pages; small so kills also land mid-checkpoint
SPAN = 1_000_000  # each cycle writes steps [i * SPAN, (i + 1) * SPAN)
# How often the full checks run (integrity_check and a whole-table audit). Both read the
# whole database, which grows every cycle, so checking every cycle makes a long run
# quadratic: 10,000 cycles took hours. Corruption and missing rows persist, so a periodic
# check still detects them, just not in the same cycle (replay the seed to find it).
# Up to 1,000 cycles (every CI run) the full checks still run after every cycle.
FULL_CHECK_EVERY_LONG = 100


@dataclass
class Tally:
    cycles: int = 0
    rows: int = 0
    tail_lost: list[int] = field(default_factory=list)
    killed_with_inflight: int = 0
    max_seq: int = -1  # highest seq seen so far; the next cycle's rows are all above it
    per_cycle: dict[int, int] = field(default_factory=dict)  # cycle start -> rows stored
    full_checks: int = 0


def _last(lines: list[str], tag: str, default: int) -> int:
    values = [int(line[2:]) for line in lines if line.startswith(f"{tag} ")]
    return max(values, default=default)


def _audit_all(conn: sqlite3.Connection, tally: Tally) -> None:
    """Whole-table checks: every cycle holds exactly the rows counted for it, once each."""
    assert conn.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
    total, distinct = conn.execute(
        "SELECT count(*), count(DISTINCT value) FROM readings"
    ).fetchone()
    assert total == distinct == tally.rows, (
        f"{total} rows ({distinct} distinct), tally {tally.rows}"
    )
    found = {
        int(lo) - int(lo) % SPAN: (n, int(lo), int(hi))
        for n, lo, hi in conn.execute(
            f"SELECT count(*), min(value), max(value) FROM readings GROUP BY CAST(value / {SPAN} AS INTEGER)"
        )
    }
    expected = {s: (n, s, s + n - 1) for s, n in tally.per_cycle.items() if n}
    assert found == expected, "a cycle's rows changed after it was verified"
    tally.full_checks += 1


def _verify_cycle(db: Path, start: int, log: Path, tally: Tally, *, full: bool) -> None:
    lines = log.read_text().splitlines()
    last_written = _last(lines, "w", start - 1)
    last_committed = _last(lines, "c", start - 1)

    steps: list[int] = []
    new_max_seq = tally.max_seq
    inflight = 0
    conn = open_if_created(db)  # None: killed before it created the buffer
    if conn is not None:
        try:
            if has_table(conn, "readings"):
                # Only this cycle's rows, via the UNIQUE index on seq: seq follows write
                # order across restarts, so they're exactly the rows above the last max.
                rows = conn.execute(
                    "SELECT seq, value FROM readings WHERE seq > ?", (tally.max_seq,)
                ).fetchall()
                assert all(start <= v < start + SPAN for _, v in rows), (
                    "a row from an earlier cycle was stored again under a new seq"
                )
                steps = sorted(int(v) for _, v in rows)
                new_max_seq = max((s for s, _ in rows), default=tally.max_seq)
                inflight = conn.execute(
                    "SELECT count(*) FROM readings WHERE state = ?", (INFLIGHT,)
                ).fetchone()[0]
            elif full:
                assert conn.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
        finally:
            conn.close()

    stored_last = start + len(steps) - 1
    assert steps == list(range(start, stored_last + 1)), "gap or duplicate inside a cycle"
    assert stored_last >= last_committed, "a reported commit was lost"
    # write() may commit and be killed before reporting, so one step past last_written is fine.
    assert stored_last <= last_written + 1
    tail = last_written - stored_last
    assert tail <= MAX_ROWS, f"lost {tail} rows, more than one batch"

    tally.cycles += 1
    tally.rows += len(steps)
    tally.tail_lost.append(max(tail, 0))
    tally.killed_with_inflight += inflight > 0
    tally.max_seq = new_max_seq
    tally.per_cycle[start] = len(steps)

    if full and (conn := open_if_created(db)) is not None:
        try:
            if has_table(conn, "readings"):
                _audit_all(conn, tally)
        finally:
            conn.close()


def _run(tmp_path: Path, cycles: int, seed: int) -> Tally:
    db = tmp_path / "buffer.db"
    log = tmp_path / "child.out"
    rng = random.Random(seed)
    tally = Tally()
    every = 1 if cycles <= 1000 else FULL_CHECK_EVERY_LONG
    for i in range(cycles):
        start = i * SPAN
        argv = [
            sys.executable,
            "-m",
            "tests.harness.buffer_child",
            str(db),
            str(start),
            str(MAX_ROWS),
            str(WAL_AUTOCHECKPOINT),
        ]
        run_until_killed(
            argv, rng.uniform(0.05, 0.5), cwd=ROOT, stdout_path=log, after_ready=wait_for_ready(rng)
        )
        _verify_cycle(db, start, log, tally, full=(i + 1) % every == 0 or i == cycles - 1)
    return tally


def _summary(t: Tally, seed: int) -> str:
    lost = sum(t.tail_lost)
    return (
        f"seed={seed} cycles={t.cycles} rows={t.rows} tail_lost_total={lost} "
        f"tail_lost_max={max(t.tail_lost, default=0)} "
        f"cycles_killed_with_inflight={t.killed_with_inflight} full_checks={t.full_checks}"
    )


def test_crash_cycles_smoke(tmp_path: Path) -> None:
    tally = _run(tmp_path, cycles=25, seed=1)
    print(_summary(tally, 1))
    assert tally.rows > 0


@pytest.mark.parametrize(
    "fault",
    [
        # A row of an earlier cycle vanishes: invisible to the per-cycle check, which
        # only reads rows above the last seq.
        "DELETE FROM readings WHERE id = (SELECT min(id) FROM readings)",
        # An earlier cycle's reading is stored again under a new seq, after a newer
        # cycle's rows: the same value twice.
        (
            "INSERT INTO readings (seq, sensor_id, value, unit, mono_ns, wall_ns, boot_id, "
            "ts_quality, qc_flag, qc_tests) SELECT (SELECT max(seq) FROM readings) + 1, "
            "sensor_id, value, unit, mono_ns, wall_ns, boot_id, ts_quality, qc_flag, qc_tests "
            "FROM readings WHERE id = (SELECT min(id) FROM readings)"
        ),
    ],
    ids=["missing-row", "duplicate-row"],
)
def test_the_periodic_audit_catches_what_the_per_cycle_check_cannot(
    tmp_path: Path, fault: str
) -> None:
    # The checker itself under test: long runs only audit every 100 cycles, so the audit
    # must catch damage to earlier cycles that the cheap per-cycle check can't see.
    tally = Tally()
    cycle = 0
    while not tally.rows:  # a cycle can die before its first commit; go until one stores rows
        (tmp_path / str(cycle)).mkdir()
        tally = _run(tmp_path / str(cycle), cycles=3, seed=cycle)
        cycle += 1
    db = tmp_path / str(cycle - 1) / "buffer.db"
    conn = sqlite3.connect(db)
    try:
        _audit_all(conn, tally)  # the real run passes
        conn.execute(fault)
        conn.commit()
        with pytest.raises(AssertionError):
            _audit_all(conn, tally)
    finally:
        conn.close()


@pytest.mark.slow
def test_acceptance_1000_crash_cycles(tmp_path: Path) -> None:
    seed = crash_seed()
    print(f"crash-seed buffer={seed}")  # replay with SPOOLPI_CRASH_SEED
    # 1,000 by default; the nightly run sets SPOOLPI_CRASH_CYCLES=10000.
    tally = _run(tmp_path, cycles=int(os.environ.get("SPOOLPI_CRASH_CYCLES", "1000")), seed=seed)
    print(_summary(tally, seed))
    assert tally.killed_with_inflight > 0, "kills never landed mid-flight; test is too weak"
