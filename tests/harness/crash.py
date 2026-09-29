"""SIGKILL supervisor for crash-safety testing.

Runs a target command as a subprocess, lets it run for a given duration,
then SIGKILLs it -- no clean shutdown -- and reports what happened. Used to
drive both the Week 0 naive-spike measurement and the later M0 buffer crash
tests against the same mechanism.
"""

from __future__ import annotations

import os
import random
import sqlite3
import subprocess
import sys
import time
from contextlib import nullcontext
from dataclasses import dataclass
from pathlib import Path
from typing import Final


def crash_seed() -> int:
    """Seed for a crash-test run: random, or SPOOLPI_CRASH_SEED to replay a failure."""
    if fixed := os.environ.get("SPOOLPI_CRASH_SEED"):
        return int(fixed)
    return random.randrange(2**32)


@dataclass(frozen=True, slots=True)
class CrashResult:
    kill_after_s: float
    returncode: int | None
    was_killed: bool


READY: Final = b"ready\n"


def run_until_killed(
    argv: list[str],
    kill_after: float,
    *,
    cwd: Path | None = None,
    kill_timeout: float = 5.0,
    stdout_path: Path | None = None,
    after_ready: bool = False,
    ready_timeout: float = 60.0,
) -> CrashResult:
    """Start ``argv``, wait ``kill_after`` seconds, SIGKILL it.

    With ``after_ready`` the clock starts once the child has written ``READY`` to
    ``stdout_path``, so the kill lands in the part of its life under test, even
    on hardware where starting Python alone takes longer than ``kill_after``.
    """
    # A file, not a pipe: what the child wrote survives the kill and can't block it.
    with open(stdout_path, "wb") if stdout_path is not None else nullcontext() as out:
        proc = subprocess.Popen(argv, cwd=cwd, stdout=out)
        if after_ready:
            assert stdout_path is not None, "after_ready needs stdout_path"
            deadline = time.monotonic() + ready_timeout
            while READY not in stdout_path.read_bytes() and proc.poll() is None:
                if time.monotonic() > deadline:
                    proc.kill()
                    raise TimeoutError(f"child not ready within {ready_timeout} s: {argv}")
                time.sleep(0.005)
        time.sleep(kill_after)
        was_killed = proc.poll() is None
        if was_killed:
            proc.kill()  # SIGKILL, not SIGTERM -- no clean shutdown
        proc.wait(timeout=kill_timeout)
    return CrashResult(kill_after_s=kill_after, returncode=proc.returncode, was_killed=was_killed)


def wait_for_ready(rng: random.Random) -> bool:
    """``after_ready`` for one cycle: mostly True, but one cycle in ten kills the child
    during start-up (imports, opening or creating the buffer), which needs testing too."""
    return rng.random() >= 0.1


def open_if_created(db: Path) -> sqlite3.Connection | None:
    """Connect to the child's buffer, or None if it never got as far as creating it.

    Never creates the file: `sqlite3.connect` would leave an empty non-WAL database
    behind, which is not the state a killed child leaves.
    """
    return sqlite3.connect(db) if db.exists() else None


def has_table(conn: sqlite3.Connection, name: str) -> bool:
    """False when the child was killed before its schema transaction committed."""
    row = conn.execute("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?", (name,))
    return row.fetchone() is not None


def run_crash_cycles(
    argv: list[str],
    *,
    cycles: int,
    min_kill_after: float,
    max_kill_after: float,
    cwd: Path | None = None,
    seed: int | None = None,
) -> list[CrashResult]:
    rng = random.Random(seed)
    results = []
    for _ in range(cycles):
        kill_after = rng.uniform(min_kill_after, max_kill_after)
        results.append(run_until_killed(argv, kill_after, cwd=cwd))
    return results


if __name__ == "__main__":
    # Ad-hoc manual use: python crash.py <kill_after_s> -- <argv...>
    kill_after = float(sys.argv[1])
    argv = sys.argv[3:]  # skip the "--" separator
    result = run_until_killed(argv, kill_after)
    print(result)
