"""Memory of the `spoolpi run` daemon at a steady load (Definition of Done: < 30 MB RSS
on a Zero 2 W at 10 sensors x 1 Hz, measured).

Starts `spoolpi run` with the jsonl sink, feeds it SENSORS readings per second on
stdin for DURATION seconds, and samples VmRSS / VmHWM / threads from /proc every
few seconds.

Usage: python bench/pi/rss.py [duration_s] [sensors] [hz]     default: 300 10 1
"""

from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import time
from pathlib import Path

CONFIG = """\
[buffer]
path = "buffer.db"
[retention]
policy = "drop_oldest"
max_rows = 1000000
[sink]
type = "jsonl"
path = "out.jsonl"
"""


def status(pid: int) -> dict[str, int]:
    fields = {}
    for line in Path(f"/proc/{pid}/status").read_text().splitlines():
        key, _, value = line.partition(":")
        if key in ("VmRSS", "VmHWM", "Threads"):
            fields[key] = int(value.split()[0])
    return fields


def main() -> None:
    duration = float(sys.argv[1]) if len(sys.argv) > 1 else 300
    sensors = int(sys.argv[2]) if len(sys.argv) > 2 else 10
    hz = float(sys.argv[3]) if len(sys.argv) > 3 else 1
    with tempfile.TemporaryDirectory() as tmp:
        cfg = Path(tmp) / "spoolpi.toml"
        cfg.write_text(CONFIG)
        exe = Path(sys.executable).with_name("spoolpi")
        proc = subprocess.Popen(
            [str(exe), "run", str(cfg)],
            stdin=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True,
        )
        assert proc.stdin is not None
        samples: list[dict[str, int]] = []
        start = time.monotonic()
        n = 0
        next_sample = start
        while time.monotonic() - start < duration:
            for s in range(sensors):
                proc.stdin.write(
                    json.dumps({"sensor_id": f"s{s}", "value": n * 0.1, "unit": "C"}) + "\n"
                )
            proc.stdin.flush()
            n += 1
            if time.monotonic() >= next_sample:
                samples.append(status(proc.pid))
                next_sample += 5
            time.sleep(1 / hz)
        final = status(proc.pid)
        proc.stdin.close()
        proc.wait(timeout=60)
        delivered = sum(1 for _ in (Path(tmp) / "out.jsonl").open())

    rss = [s["VmRSS"] for s in samples]
    print(
        f"load: {sensors} sensors x {hz:g} Hz for {duration:.0f} s -> {n * sensors} readings, {delivered} delivered"
    )
    print(
        f"RSS (MB): start {rss[0] / 1024:.1f}  after 60 s {rss[min(12, len(rss) - 1)] / 1024:.1f}  "
        f"end {rss[-1] / 1024:.1f}  peak (VmHWM) {final['VmHWM'] / 1024:.1f}"
    )
    print(f"threads: {final['Threads']}")
    growth = (rss[-1] - rss[min(12, len(rss) - 1)]) / 1024
    print(f"growth after warm-up: {growth:+.2f} MB over {duration - 60:.0f} s")


if __name__ == "__main__":
    main()
