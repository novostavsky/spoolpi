"""Commit latency on the device's storage (run it on the SD card you'll deploy on).

Measures, in a scratch directory next to the buffer's future home:
  1. raw write + fsync of a 4 KiB block (the storage's floor);
  2. SpoolPi buffer commits (Buffer.append) at several batch sizes, with the
     default synchronous=NORMAL and with synchronous=FULL (fsync per commit,
     which is what a power-safe mode would cost).

Usage: python bench/pi/fsync_latency.py [directory]     default: ~/spoolpi-bench
"""

from __future__ import annotations

import os
import statistics
import sys
import tempfile
import time
from pathlib import Path

from spoolpi.core.buffer import Buffer
from spoolpi.core.reading import Reading

BATCHES = (1, 10, 50, 200)
COMMITS = 150  # per configuration; enough for a stable p90 without wearing the card


def pct(values: list[float], q: float) -> float:
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, int(q * len(ordered)))]


def summary(ms: list[float]) -> str:
    return (
        f"p50 {statistics.median(ms):7.2f}  p90 {pct(ms, 0.9):7.2f}  "
        f"p99 {pct(ms, 0.99):7.2f}  max {max(ms):7.2f} ms"
    )


def raw_fsync(directory: Path) -> list[float]:
    path = directory / "raw.bin"
    block = os.urandom(4096)
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o644)
    try:
        out = []
        for _ in range(COMMITS):
            t0 = time.perf_counter()
            os.write(fd, block)
            os.fsync(fd)
            out.append((time.perf_counter() - t0) * 1000)
        return out
    finally:
        os.close(fd)
        path.unlink()


def commits(directory: Path, batch: int, synchronous: str) -> list[float]:
    db = directory / f"buffer-{batch}-{synchronous}.db"
    out = []
    with Buffer(db, durability="power" if synchronous == "FULL" else "process") as buf:
        n = 0
        for _ in range(COMMITS):
            readings = [
                Reading(f"s{i % 10}", 21.5, "C", n + i, 1_790_000_000 * 10**9 + n + i, "boot")
                for i in range(batch)
            ]
            n += batch
            t0 = time.perf_counter()
            buf.append(readings)
            out.append((time.perf_counter() - t0) * 1000)
    for f in directory.glob(f"{db.name}*"):
        f.unlink()
    return out


def main() -> None:
    base = Path(sys.argv[1] if len(sys.argv) > 1 else Path.home() / "spoolpi-bench")
    base.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(dir=base) as tmp:
        directory = Path(tmp)
        device = os.stat(directory).st_dev
        print(f"storage: {directory} (device {os.major(device)}:{os.minor(device)})")
        print(f"raw 4 KiB write+fsync      {summary(raw_fsync(directory))}")
        for synchronous in ("NORMAL", "FULL"):
            for batch in BATCHES:
                ms = commits(directory, batch, synchronous)
                per_s = batch / (statistics.median(ms) / 1000)
                print(
                    f"commit {synchronous:6} batch {batch:3}  {summary(ms)}   "
                    f"~{per_s:>9,.0f} readings/s"
                )


if __name__ == "__main__":
    main()
