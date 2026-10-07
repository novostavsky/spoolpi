"""The spike rerun: drive bench/spoolpi_spike.py through SIGKILL cycles, like the naive spike.

Same kill schedule as run_naive_spike_crash_test.py (uniform 0.05-1.5 s, seeded),
the same buffer persisting across all cycles, and an HTTP receiver that stays up
throughout. After the last kill, one uninterrupted --drain run lets everything
committed reach the receiver. Reports:

  lost                     committed to the buffer but never delivered
  transport_redeliveries   extra copies of a record under the same (buffer_id, seq):
                           at-least-once delivery, removed mechanically by the key
  duplicates_after_dedupe  readings still repeated once deduplicated by key (i.e. the
                           same reading shipped under two different keys)
  corrupted                PRAGMA integrity_check failures on the buffer

Usage: python bench/run_spoolpi_spike_crash_test.py [cycles] [seed]
"""

from __future__ import annotations

import json
import random
import sqlite3
import subprocess
import sys
import threading
from collections import Counter, defaultdict
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from tests.harness.crash import run_until_killed

CHILD = ROOT / "bench" / "spoolpi_spike.py"


class Receiver:
    """An HTTP ingest endpoint that accepts every batch and remembers every record."""

    def __init__(self) -> None:
        self.records: list[dict[str, object]] = []
        lock = threading.Lock()
        records = self.records

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self) -> None:
                body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
                with lock:
                    records.extend(body["records"])
                self.send_response(200)
                self.send_header("Content-Length", "0")
                self.end_headers()

            def log_message(self, *_args: object) -> None:
                pass

        self._httpd = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.url = f"http://127.0.0.1:{self._httpd.server_address[1]}/ingest"
        threading.Thread(target=self._httpd.serve_forever, daemon=True).start()


def write_config(workdir: Path, url: str) -> Path:
    cfg = workdir / "spoolpi.toml"
    cfg.write_text(
        f"""
[buffer]
path = "buffer.db"
[retention]
policy = "drop_oldest"
max_rows = 10000000
[batch]
max_rows = 15        # the naive spike commits every 15 rows too
max_delay_s = 1.0
[shipper]
batch_size = 100
send_timeout_s = 5
poll_interval_s = 0.05
backoff_initial_s = 0.05
backoff_max_s = 1
[sink]
type = "http"
url = "{url}"
timeout_s = 2
"""
    )
    return cfg


def integrity_ok(db: Path) -> bool:
    if not db.exists():
        return True
    try:
        conn = sqlite3.connect(db)
        try:
            return conn.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
        finally:
            conn.close()
    except sqlite3.DatabaseError:
        return False


def main() -> None:
    cycles = int(sys.argv[1]) if len(sys.argv) > 1 else 100
    seed = int(sys.argv[2]) if len(sys.argv) > 2 else 0
    workdir = ROOT / "bench" / "spoolpi_spike_run"
    workdir.mkdir(parents=True, exist_ok=True)
    for f in workdir.glob("buffer.db*"):
        f.unlink()

    receiver = Receiver()
    cfg = write_config(workdir, receiver.url)
    db = workdir / "buffer.db"
    rng = random.Random(seed)
    corrupted = 0
    for i in range(cycles):
        run_until_killed([sys.executable, str(CHILD), str(cfg)], rng.uniform(0.05, 1.5), cwd=ROOT)
        if not integrity_ok(db):
            corrupted += 1
            print(f"cycle {i}: CORRUPTION DETECTED", file=sys.stderr)
    drained = subprocess.run(
        [sys.executable, str(CHILD), str(cfg), "--drain"], cwd=ROOT, check=False
    )

    conn = sqlite3.connect(db)
    committed = {int(v) for (v,) in conn.execute("SELECT value FROM readings")}
    conn.close()

    readings = [r for r in receiver.records if r["type"] == "reading"]
    per_key = Counter((r["buffer_id"], r["seq"]) for r in readings)
    keys_per_step: dict[int, set[tuple[object, object]]] = defaultdict(set)
    for r in readings:
        keys_per_step[int(r["value"])].add((r["buffer_id"], r["seq"]))  # type: ignore[call-overload]
    delivered_steps = set(keys_per_step)

    report = {
        "cycles": cycles,
        "seed": seed,
        "final_drain_completed": drained.returncode == 0,
        "total_committed_readings": len(committed),
        "lost_count": len(committed - delivered_steps),
        "loss_rate": len(committed - delivered_steps) / len(committed) if committed else 0.0,
        "delivered_records": len(readings),
        "transport_redeliveries": sum(n - 1 for n in per_key.values()),
        "duplicates_after_dedupe": sum(1 for keys in keys_per_step.values() if len(keys) > 1),
        "corruption_count": corrupted,
    }
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
