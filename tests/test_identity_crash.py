from __future__ import annotations

import random
import sqlite3
import sys
from pathlib import Path

import pytest

from tests.harness.crash import crash_seed, run_until_killed

ROOT = Path(__file__).resolve().parent.parent
SPAN = 1_000_000
BLOCK_SIZE = 50  # small, so most cycles cross block boundaries and restarts waste part of one


def _collect(db: Path, log: Path, seq_to_step: dict[int, int]) -> None:
    pairs = []
    for line in log.read_text().splitlines():
        _, seq, step = line.split()
        pairs.append((int(seq), int(step)))
    conn = sqlite3.connect(db)
    try:
        pairs += [(s, int(v)) for s, v in conn.execute("SELECT seq, value FROM readings")]
    finally:
        conn.close()
    for seq, step in pairs:
        # Resending the same row under the same seq is fine (at-least-once);
        # the same seq on a different row is a collision at the sink.
        assert seq_to_step.setdefault(seq, step) == step, f"seq {seq} reissued"


def _run(tmp_path: Path, cycles: int, seed: int) -> dict[int, int]:
    db = tmp_path / "buffer.db"
    log = tmp_path / "child.out"
    rng = random.Random(seed)
    seq_to_step: dict[int, int] = {}
    for i in range(cycles):
        argv = [sys.executable, "-m", "tests.harness.seq_child", str(db), str(i * SPAN)]
        argv.append(str(BLOCK_SIZE))
        run_until_killed(argv, rng.uniform(0.1, 0.5), cwd=ROOT, stdout_path=log)
        _collect(db, log, seq_to_step)
    return seq_to_step


def _check_order(seq_to_step: dict[int, int]) -> None:
    by_step = [seq for _, seq in sorted((step, seq) for seq, step in seq_to_step.items())]
    assert by_step == sorted(by_step), "seq order disagrees with write order"


def test_seq_crash_smoke(tmp_path: Path) -> None:
    seen = _run(tmp_path, cycles=10, seed=2)
    _check_order(seen)
    assert seen


@pytest.mark.slow
def test_acceptance_100_kill_restart_cycles_never_reissue_seq(tmp_path: Path) -> None:
    seed = crash_seed()
    print(f"crash-seed seq={seed}")
    seen = _run(tmp_path, cycles=100, seed=seed)
    _check_order(seen)
    print(f"distinct seqs={len(seen)} max seq={max(seen)}")
