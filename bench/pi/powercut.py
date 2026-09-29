"""Power-cut test, controller side (runs on the dev machine, drives the Pi over SSH).

Each cycle:
  1. start powercut_child.py on the Pi, reading its "c <step>" commit reports;
  2. after a random 5-120 s, reset the Pi instantly with sysrq 'b' (no sync,
     no unmount: the page cache is lost, as in a power cut);
  3. once the Pi is back, read what the buffer actually holds.

lost = last step the Pi reported committed - last step found in the buffer.

Needs on the Pi: the repo deployed (bench/pi/deploy.sh) and passwordless
`sudo tee /proc/sysrq-trigger`.

Usage: python bench/pi/powercut.py <cycles> <power|process> [host] [seed]
       (NORMAL / FULL, the SQLite modes behind them, are accepted too)
"""

from __future__ import annotations

import json
import random
import subprocess
import sys
import threading
import time

HOST = "spoolpi-zero"
DB = "spoolpi-powercut/buffer.db"
SPAN = 1_000_000
PY = "cd ~/spoolpi && PATH=$HOME/spoolpi/.venv/bin:$PATH"


def ssh(host: str, command: str, timeout: float = 60) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["ssh", "-o", "ConnectTimeout=10", host, command],
        capture_output=True,
        text=True,
        timeout=timeout,
        check=False,
    )


def wait_until_up(host: str, timeout: float = 300) -> float:
    t0 = time.monotonic()
    time.sleep(10)  # it's going down; don't catch it before the reset lands
    while time.monotonic() - t0 < timeout:
        try:
            if ssh(host, "true", timeout=15).returncode == 0:
                return time.monotonic() - t0
        except subprocess.TimeoutExpired:
            pass
        time.sleep(5)
    raise TimeoutError(f"{host} didn't come back within {timeout} s")


INSPECT = """
import json, sqlite3, sys
db, lo, hi = sys.argv[1], int(sys.argv[2]), int(sys.argv[3])
conn = sqlite3.connect(db)
ok = conn.execute("PRAGMA integrity_check").fetchone()[0]
(top,) = conn.execute("SELECT max(value) FROM readings WHERE value >= ? AND value < ?", (lo, hi)).fetchone()
(count,) = conn.execute("SELECT count(*) FROM readings WHERE value >= ? AND value < ?", (lo, hi)).fetchone()
print(json.dumps({"integrity": ok, "top": top, "count": count}))
"""


def boot_id(host: str) -> str:
    return ssh(host, "cat /proc/sys/kernel/random/boot_id", timeout=15).stdout.strip()


def cycle(host: str, i: int, synchronous: str, rng: random.Random) -> dict[str, object]:
    start = i * SPAN
    # Let the boot finish and get its own writes onto the card first. Otherwise a cut can
    # land in the OS's boot-time writes: on 09-29 one left NetworkManager's netplan files
    # empty and the Pi without Wi-Fi for good (docs/hardware.md). We test SpoolPi, not that.
    ssh(host, "systemctl is-system-running --wait >/dev/null; sync", timeout=300)
    boot_before = boot_id(host)
    child = subprocess.Popen(
        [
            "ssh",
            host,
            f"{PY} && mkdir -p ~/spoolpi-powercut && exec python bench/pi/powercut_child.py ~/{DB} {start} {synchronous}",
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        text=True,
    )
    reported = {"last": start - 1, "ready": False}

    def read() -> None:
        assert child.stdout is not None
        for line in child.stdout:
            if line.startswith("c "):
                reported["last"] = int(line[2:])
            elif line.startswith("ready"):
                reported["ready"] = True

    reader = threading.Thread(target=read, daemon=True)
    reader.start()
    deadline = time.monotonic() + 120
    while not reported["ready"]:
        if time.monotonic() > deadline or child.poll() is not None:
            child.kill()
            raise RuntimeError("the writer on the Pi didn't start (is the repo deployed?)")
        time.sleep(0.1)
    run_for = rng.uniform(5, 120)
    time.sleep(run_for)
    # The reset: no sync, no unmount. Our SSH session just dies without a
    # FIN, so this call hangs until the timeout; that's the expected outcome.
    try:
        subprocess.run(
            ["ssh", host, "echo b | sudo -n tee /proc/sysrq-trigger"],
            capture_output=True,
            timeout=10,
            check=False,
        )
    except subprocess.TimeoutExpired:
        pass
    last_reported = reported["last"]
    child.kill()
    boot_s = wait_until_up(host)
    if boot_id(host) == boot_before:
        raise RuntimeError("the Pi didn't reboot (is the sysrq sudo rule in place?)")
    probe = ssh(host, f"{PY} && python -c '{INSPECT}' ~/{DB} {start} {start + SPAN}")
    found = json.loads(probe.stdout)
    top = start - 1 if found["top"] is None else int(found["top"])
    return {
        "cycle": i,
        "ran_s": round(run_for, 1),
        "reported_committed": last_reported - start + 1,
        "found": found["count"],
        "lost": max(0, int(last_reported) - top),
        "integrity": found["integrity"],
        "boot_s": round(boot_s),
    }


def main() -> None:
    cycles, synchronous = int(sys.argv[1]), sys.argv[2].upper()
    host = sys.argv[3] if len(sys.argv) > 3 else HOST
    seed = int(sys.argv[4]) if len(sys.argv) > 4 else random.randrange(2**32)
    rng = random.Random(seed)
    print(f"power-cut test: {cycles} cycles, synchronous={synchronous}, seed={seed}", flush=True)
    ssh(host, "rm -rf ~/spoolpi-powercut && sync")
    results = []
    stopped = ""
    for i in range(cycles):
        try:
            r = cycle(host, i, synchronous, rng)
        except TimeoutError as e:
            # The Pi didn't come back: stop rather than lose track of it; keep what we have.
            stopped = f"; STOPPED at cut {i}: {e}"
            break
        results.append(r)
        print(json.dumps(r), flush=True)
    if not results:
        print(f"SUMMARY synchronous={synchronous}: no completed cuts{stopped}", flush=True)
        sys.exit(1)
    lost = [int(r["lost"]) for r in results]  # type: ignore[call-overload]
    bad = [r for r in results if r["integrity"] != "ok"]
    print(
        f"SUMMARY synchronous={synchronous}: {len(results)} cuts, lost committed readings "
        f"max {max(lost)}, mean {sum(lost) / len(lost):.1f}, cuts with any loss "
        f"{sum(1 for x in lost if x)}; integrity failures {len(bad)}{stopped}",
        flush=True,
    )
    sys.exit(1 if stopped else 0)


if __name__ == "__main__":
    main()
