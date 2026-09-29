"""Check that `systemctl restart` loses nothing (plan §9: the unit survives restarts).

Runs `spoolpi run` with the fake source as a transient systemd user unit,
restarts it a few times, stops it, then checks every run's readings: each run
counts 0, 1, 2, ... from its own seq block, and every value must be delivered
to the jsonl sink or still be in the buffer, with no holes.

Needs a systemd user manager (`systemctl --user is-system-running`).
Usage: python bench/systemd_restart_check.py [restarts]
"""

from __future__ import annotations

import json
import sqlite3
import subprocess
import sys
import tempfile
import time
from collections import defaultdict
from pathlib import Path

UNIT = "spoolpi-restart-check"
SEQ_BLOCK = 1000  # Buffer's default block size: each process starts a new block
CONFIG = """\
[buffer]
path = "buf.db"
[retention]
policy = "drop_oldest"
max_rows = 1000000
[batch]
max_rows = 20
max_delay_s = 0.2
[shipper]
poll_interval_s = 0.1
stop_timeout_s = 5
[sink]
type = "jsonl"
path = "out.jsonl"
[source]
type = "fake"
rate_hz = 100
"""


def systemctl(*args: str) -> None:
    subprocess.run(["systemctl", "--user", *args], check=True)


def main() -> int:
    restarts = int(sys.argv[1]) if len(sys.argv) > 1 else 3
    work = Path(tempfile.mkdtemp(prefix="spoolpi-systemd-"))
    (work / "spoolpi.toml").write_text(CONFIG)
    subprocess.run(
        [
            "systemd-run",
            "--user",
            f"--unit={UNIT}",
            "--property=KillSignal=SIGTERM",
            "--property=TimeoutStopSec=15",
            f"--working-directory={work}",
            sys.executable,
            "-m",
            "spoolpi",
            "run",
            str(work / "spoolpi.toml"),
        ],
        check=True,
    )
    try:
        for _ in range(restarts):
            time.sleep(2)
            systemctl("restart", UNIT)
        time.sleep(2)
    finally:
        systemctl("stop", UNIT)

    shipped = [json.loads(line) for line in (work / "out.jsonl").read_text().splitlines()]
    conn = sqlite3.connect(work / "buf.db")
    kept = conn.execute("SELECT seq, value FROM readings WHERE state != 2").fetchall()
    conn.close()

    runs: dict[int, set[int]] = defaultdict(set)
    for seq, value in [(r["seq"], r["value"]) for r in shipped] + kept:
        runs[seq // SEQ_BLOCK].add(int(value))
    ok = len(runs) == restarts + 1
    for block, values in sorted(runs.items()):
        complete = values == set(range(max(values) + 1))
        ok &= complete
        print(f"run {block}: {len(values)} readings, {'complete' if complete else 'HOLES'}")
    print(f"{len(shipped)} shipped, {len(kept)} still buffered, workdir {work}")
    print("OK: nothing lost across restarts" if ok else "FAILED")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
