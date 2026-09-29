"""Seven-day soak: one metrics sample, appended as a JSON line. Run as root by a timer.

Databases are opened read-only (mode=ro): as root, opening SpoolPi's buffer
read-write could create -wal/-shm files the service's DynamicUser can't write.
Also copies the producer's log (in SpoolPi's private state directory) where
the pi user can read it.

Usage: python metrics.py <out_dir>
"""

from __future__ import annotations

import json
import shutil
import sqlite3
import subprocess
import sys
import time
from pathlib import Path

STATE = Path("/var/lib/private/spoolpi")  # DynamicUser's StateDirectory
BUFFER = STATE / "buffer.db"


def proc_status(pid: int) -> dict[str, int]:
    out = {}
    for line in Path(f"/proc/{pid}/status").read_text().splitlines():
        key, _, value = line.partition(":")
        if key in ("VmRSS", "VmHWM", "VmSize", "Threads"):
            out[key] = int(value.split()[0])
    return out


def spoolpi_pid() -> int | None:
    r = subprocess.run(
        ["pgrep", "-f", "-o", "spoolpi run /etc/spoolpi/spoolpi.toml"],
        capture_output=True,
        text=True,
        check=False,
    )
    return int(r.stdout.split()[0]) if r.stdout.strip() else None


def unit(name: str, *props: str) -> dict[str, str]:
    r = subprocess.run(
        ["systemctl", "show", name, *(f"-p{p}" for p in props)],
        capture_output=True,
        text=True,
        check=False,
    )
    return dict(line.split("=", 1) for line in r.stdout.splitlines() if "=" in line)


def ro_query(db: Path, sql: str) -> list[tuple[object, ...]] | str:
    try:
        conn = sqlite3.connect(f"file:{db}?mode=ro", uri=True, timeout=5)
        try:
            return conn.execute(sql).fetchall()
        finally:
            conn.close()
    except sqlite3.Error as e:
        return f"error: {e}"


def size(p: Path) -> int | None:
    return p.stat().st_size if p.exists() else None


def main() -> None:
    out = Path(sys.argv[1])
    sample: dict[str, object] = {"t": round(time.time())}
    sample["uptime_s"] = round(float(Path("/proc/uptime").read_text().split()[0]))
    sample["boot_id"] = Path("/proc/sys/kernel/random/boot_id").read_text().strip()
    pid = spoolpi_pid()
    sample["spoolpi"] = {"pid": pid, **(proc_status(pid) if pid else {})}
    sample["units"] = {
        u: unit(u, "ActiveState", "NRestarts")
        for u in ("spoolpi.service", "mosquitto.service", "spoolpi-soak-receiver.service")
    }
    sample["buffer_states"] = ro_query(
        BUFFER, "SELECT state, count(*) FROM readings GROUP BY state"
    )
    sample["buffer_gaps"] = ro_query(
        BUFFER, "SELECT state, count(*), sum(count) FROM gaps GROUP BY state"
    )
    sample["files"] = {
        "buffer": size(BUFFER),
        "wal": size(STATE / "buffer.db-wal"),
        "receiver": size(out / "receiver.db"),
    }
    sample["received"] = ro_query(
        out / "receiver.db", "SELECT type, count(*) FROM records GROUP BY type"
    )
    # Sectors written to the SD card since boot (field 7 of /sys/block/*/stat, 512 bytes each).
    sample["card_sectors_written"] = int(Path("/sys/block/mmcblk0/stat").read_text().split()[6])
    with open(out / "metrics.jsonl", "a") as f:
        f.write(json.dumps(sample) + "\n")
    log = STATE / "producer.log"
    if log.exists():
        shutil.copyfile(log, out / "producer.log")
        shutil.chown(out / "producer.log", "pi", "pi")


if __name__ == "__main__":
    main()
