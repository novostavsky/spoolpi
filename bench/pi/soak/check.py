"""Seven-day soak: the verdict so far. Run as pi on the Pi, any time:

    python3 ~/spoolpi/bench/pi/soak/check.py [~/soak]

Delivery: every reading the producer wrote is either received or counted in a
received gap record, with no holes inside a run. A run's last readings may be
missing only at its very end:
  - the live run: still in flight;
  - a run that ended with a stop: readings still in the pipe at SIGTERM;
  - a run that ended in a power cut: up to one uncommitted batch, and the
    producer's log is up to 60 s behind.
Trends: SpoolPi's memory, virtual size and threads (a leak shows here first),
restarts, buffer backlog, card writes, outages.
"""

from __future__ import annotations

import json
import sqlite3
import sys
import time
from collections import defaultdict
from pathlib import Path

RUN_SPAN = 1_000_000_000
TAIL_OK = 200  # readings at a run's end that may be legitimately missing (see above)


def producer_runs(log: Path) -> dict[int, dict[str, object]]:
    runs: dict[int, dict[str, object]] = {}
    for line in log.read_text().splitlines() if log.exists() else []:
        parts = line.split()
        t, run = int(parts[0]), int(parts[2])
        r = runs.setdefault(run, {"start": t, "written": 0, "ended": False})
        if parts[3] == "written":
            r["written"] = int(parts[4])
        elif parts[3] == "end":
            r["written"], r["ended"] = int(parts[5]), True
    return runs


def main() -> None:
    soak = Path(sys.argv[1] if len(sys.argv) > 1 else "~/soak").expanduser()
    db = sqlite3.connect(f"file:{soak / 'receiver.db'}?mode=ro", uri=True)
    runs = producer_runs(soak / "producer.log")
    started = (soak / "soak.log").read_text().split()[0] if (soak / "soak.log").exists() else None
    elapsed_h = (time.time() - int(started)) / 3600 if started else 0.0
    print(f"soak running {elapsed_h:.1f} h ({elapsed_h / 24:.2f} days)")

    steps: dict[int, set[int]] = defaultdict(set)
    quality: dict[int, int] = defaultdict(int)
    for value, q in db.execute("SELECT value, ts_quality FROM records WHERE type = 'reading'"):
        steps[int(value) // RUN_SPAN].add(int(value) % RUN_SPAN)
        quality[q] += 1
    gaps = db.execute(
        "SELECT reason, count(*), sum(count) FROM records WHERE type = 'gap' GROUP BY reason"
    ).fetchall()
    stats = dict(db.execute("SELECT key, value FROM stats"))

    ok = True
    holes_total = tail_total = 0
    live = max(runs, default=None)
    print(f"\nruns: {len(runs)} (each is one start of the service)")
    for run in sorted(runs):
        r = runs[run]
        got = steps.get(run, set())
        top = max(got, default=-1)
        holes = (top + 1) - len(got)  # missing below the highest received step
        written = int(r["written"])  # type: ignore[call-overload]
        tail = max(0, written - (top + 1))
        how = (
            "live"
            if run == live and not r["ended"]
            else ("stopped" if r["ended"] else "power cut?")
        )
        flag = ""
        if holes:
            flag, ok = "  <-- HOLES", False
        elif how != "live" and tail > TAIL_OK:
            flag, ok = "  <-- large tail", False
        holes_total += holes
        tail_total += tail if how != "live" else 0
        print(
            f"  run {run:3d} ({how:10s}): written >= {written:8d}, received {len(got):8d}, "
            f"holes {holes}, tail {tail}{flag}"
        )
    print(f"\ngap records received: {gaps or 'none'}")
    print(
        f"duplicates absorbed by (buffer_id, seq): {stats.get('duplicates', 0)}; unparseable: {stats.get('bad', 0)}"
    )
    print(f"ts_quality of received readings: {dict(sorted(quality.items()))}")

    samples = (
        [json.loads(x) for x in (soak / "metrics.jsonl").read_text().splitlines()]
        if (soak / "metrics.jsonl").exists()
        else []
    )
    if samples:
        # Only samples of the Python process (comm "spoolpi"); the first sample of the
        # 09-29 run measured the unit's /bin/sh wrapper and has no "comm".
        sp = [s["spoolpi"] for s in samples if s["spoolpi"].get("comm") == "spoolpi"]

        def mb(key: str) -> str:
            v = [x[key] / 1024 for x in sp]
            return f"first {v[0]:.1f}, last {v[-1]:.1f}, max {max(v):.1f} MB"

        print(
            f"\nmetrics: {len(samples)} samples over {(samples[-1]['t'] - samples[0]['t']) / 3600:.1f} h"
        )
        if sp:
            print(f"  SpoolPi RSS:     {mb('VmRSS')}")
            print(f"  SpoolPi VmSize:  {mb('VmSize')}   (steady = no stack/address-space leak)")
            print(f"  SpoolPi threads: max {max(x['Threads'] for x in sp)}")
        last = samples[-1]
        print(
            f"  restarts: spoolpi {last['units']['spoolpi.service'].get('NRestarts')}, "
            f"receiver {last['units']['spoolpi-soak-receiver.service'].get('NRestarts')}"
        )
        boots = {s["boot_id"] for s in samples}
        print(f"  boots seen: {len(boots)}")
        pend = [
            dict(map(tuple, s["buffer_states"])).get(0, 0)
            for s in samples
            if isinstance(s["buffer_states"], list)
        ]
        if pend:
            print(f"  buffer pending: max {max(pend):,}, now {pend[-1]:,}")
        print(f"  files now: {last['files']}")
        by_boot: dict[str, list[int]] = defaultdict(list)
        for s in samples:
            by_boot[s["boot_id"]].append(s["card_sectors_written"])
        written_mb = sum((max(v) - min(v)) * 512 / 2**20 for v in by_boot.values())
        span_d = max(1e-9, (samples[-1]["t"] - samples[0]["t"]) / 86400)
        print(f"  SD card writes: {written_mb:,.0f} MB total, ~{written_mb / span_d:,.0f} MB/day")
    outages = (
        (soak / "outages.log").read_text().splitlines() if (soak / "outages.log").exists() else []
    )
    print(
        f"\noutages: {sum(1 for x in outages if ' start ' in x)} started, {sum(1 for x in outages if x.endswith('end'))} ended"
    )

    print(
        f"\nVERDICT: {'OK' if ok else 'PROBLEM'} — holes {holes_total}, "
        f"tail at stops/cuts {tail_total} readings"
    )


if __name__ == "__main__":
    main()
