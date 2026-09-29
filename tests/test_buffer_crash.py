from __future__ import annotations

import os
import random
import sqlite3
import sys
from dataclasses import dataclass, field
from pathlib import Path

import pytest

from spoolpi.core.buffer import INFLIGHT
from tests.harness.crash import crash_seed, run_until_killed

ROOT = Path(__file__).resolve().parent.parent
MAX_ROWS = 20
WAL_AUTOCHECKPOINT = 50  # pages; small so kills also land mid-checkpoint
SPAN = 1_000_000  # each cycle writes steps [i * SPAN, (i + 1) * SPAN)


@dataclass
class Tally:
    cycles: int = 0
    rows: int = 0
    tail_lost: list[int] = field(default_factory=list)
    killed_with_inflight: int = 0


def _last(lines: list[str], tag: str, default: int) -> int:
    values = [int(line[2:]) for line in lines if line.startswith(f"{tag} ")]
    return max(values, default=default)


def _verify_cycle(db: Path, start: int, log: Path, tally: Tally) -> None:
    lines = log.read_text().splitlines()
    last_written = _last(lines, "w", start - 1)
    last_committed = _last(lines, "c", start - 1)

    conn = sqlite3.connect(db)
    try:
        assert conn.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
        steps = sorted(
            int(v)
            for (v,) in conn.execute(
                "SELECT value FROM readings WHERE value >= ? AND value < ?",
                (start, start + SPAN),
            )
        )
        inflight = conn.execute(
            "SELECT count(*) FROM readings WHERE state = ?", (INFLIGHT,)
        ).fetchone()[0]
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


def _run(tmp_path: Path, cycles: int, seed: int) -> Tally:
    db = tmp_path / "buffer.db"
    log = tmp_path / "child.out"
    rng = random.Random(seed)
    tally = Tally()
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
        run_until_killed(argv, rng.uniform(0.05, 0.5), cwd=ROOT, stdout_path=log)
        _verify_cycle(db, start, log, tally)
    return tally


def _summary(t: Tally, seed: int) -> str:
    lost = sum(t.tail_lost)
    return (
        f"seed={seed} cycles={t.cycles} rows={t.rows} tail_lost_total={lost} "
        f"tail_lost_max={max(t.tail_lost, default=0)} "
        f"cycles_killed_with_inflight={t.killed_with_inflight}"
    )


def test_crash_cycles_smoke(tmp_path: Path) -> None:
    tally = _run(tmp_path, cycles=25, seed=1)
    print(_summary(tally, 1))
    assert tally.rows > 0


@pytest.mark.slow
def test_acceptance_1000_crash_cycles(tmp_path: Path) -> None:
    seed = crash_seed()
    print(f"crash-seed buffer={seed}")  # replay with SPOOLPI_CRASH_SEED
    # 1,000 by default; the nightly run sets SPOOLPI_CRASH_CYCLES=10000.
    tally = _run(tmp_path, cycles=int(os.environ.get("SPOOLPI_CRASH_CYCLES", "1000")), seed=seed)
    print(_summary(tally, seed))
    assert tally.killed_with_inflight > 0, "kills never landed mid-flight; test is too weak"
