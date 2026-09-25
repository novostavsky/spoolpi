"""Drive the naive spike through repeated SIGKILL cycles and measure damage.

Same DB and events file persist across all cycles, simulating a long-running
deployment that keeps losing power. Reports:

  - readings committed to SQLite but never delivered to the HTTP sink (loss)
  - readings delivered more than once (duplication)
  - SQLite integrity_check failures (corruption)

Usage: python run_naive_spike_crash_test.py [cycles] [seed]
"""

from __future__ import annotations

import json
import random
import socket
import sqlite3
import sys
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from tests.harness.crash import run_until_killed  # noqa: E402

SPIKE = ROOT / "bench" / "naive_spike.py"


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def integrity_ok(db_path: Path) -> bool:
    if not db_path.exists():
        return True
    try:
        conn = sqlite3.connect(db_path)
        result = conn.execute("PRAGMA integrity_check").fetchone()[0]
        conn.close()
        return result == "ok"
    except sqlite3.DatabaseError:
        return False


def main() -> None:
    cycles = int(sys.argv[1]) if len(sys.argv) > 1 else 100
    seed = int(sys.argv[2]) if len(sys.argv) > 2 else 0

    workdir = ROOT / "bench" / "spike_run"
    workdir.mkdir(parents=True, exist_ok=True)
    db_path = workdir / "spike.db"
    events_path = workdir / "events.jsonl"
    db_path.unlink(missing_ok=True)
    events_path.unlink(missing_ok=True)

    rng = random.Random(seed)
    corruptions = 0

    for i in range(cycles):
        port = free_port()
        kill_after = rng.uniform(0.05, 1.5)
        run_until_killed(
            [sys.executable, str(SPIKE), str(db_path), str(events_path), str(port)],
            kill_after,
        )
        if not integrity_ok(db_path):
            corruptions += 1
            print(f"cycle {i}: CORRUPTION DETECTED", file=sys.stderr)

    conn = sqlite3.connect(db_path)
    committed_steps = {row[0] for row in conn.execute("SELECT step FROM readings")}
    conn.close()

    delivered_steps = []
    if events_path.exists():
        for line in events_path.read_text().splitlines():
            if line.strip():
                delivered_steps.append(json.loads(line)["step"])

    delivered_counts = Counter(delivered_steps)
    lost = committed_steps - set(delivered_counts)
    duplicated = {step: n for step, n in delivered_counts.items() if n > 1}

    total = len(committed_steps)
    report = {
        "cycles": cycles,
        "seed": seed,
        "total_committed_readings": total,
        "lost_count": len(lost),
        "loss_rate": (len(lost) / total) if total else 0.0,
        "duplicated_count": len(duplicated),
        "corruption_count": corruptions,
    }
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
