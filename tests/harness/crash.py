"""SIGKILL supervisor for crash-safety testing.

Runs a target command as a subprocess, lets it run for a given duration,
then SIGKILLs it -- no clean shutdown -- and reports what happened. Used to
drive both the Week 0 naive-spike measurement and the later M0 buffer crash
tests against the same mechanism.
"""

from __future__ import annotations

import os
import random
import subprocess
import sys
import time
from contextlib import nullcontext
from dataclasses import dataclass
from pathlib import Path


def crash_seed() -> int:
    """Seed for a crash-test run: random, or SPOOL_CRASH_SEED to replay a failure."""
    if fixed := os.environ.get("SPOOL_CRASH_SEED"):
        return int(fixed)
    return random.randrange(2**32)


@dataclass(frozen=True, slots=True)
class CrashResult:
    kill_after_s: float
    returncode: int | None
    was_killed: bool


def run_until_killed(
    argv: list[str],
    kill_after: float,
    *,
    cwd: Path | None = None,
    kill_timeout: float = 5.0,
    stdout_path: Path | None = None,
) -> CrashResult:
    # A file, not a pipe: what the child wrote survives the kill and can't block it.
    with open(stdout_path, "wb") if stdout_path is not None else nullcontext() as out:
        proc = subprocess.Popen(argv, cwd=cwd, stdout=out)
        time.sleep(kill_after)
        was_killed = proc.poll() is None
        if was_killed:
            proc.kill()  # SIGKILL, not SIGTERM -- no clean shutdown
        proc.wait(timeout=kill_timeout)
    return CrashResult(kill_after_s=kill_after, returncode=proc.returncode, was_killed=was_killed)


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
