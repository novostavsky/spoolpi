"""Power-cut test, device side: write readings and report each commit on stdout.

Runs on the Pi (started over SSH by bench/pi/powercut.py). After every commit it
prints "c <last committed step>" and flushes, so the controller on the other
end of the SSH connection knows what SpoolPi said was committed at the moment
the power went.

Usage: python bench/pi/powercut_child.py <db> <start_step> <power|process> [readings_per_s]
       (NORMAL and FULL are accepted too: the SQLite modes the two durabilities use)
"""

from __future__ import annotations

import sys
import time

from spoolpi.core.buffer import BatchWriter, Buffer, Durability
from spoolpi.core.clock import BOOT_ID, mono_ns, wall_ns
from spoolpi.core.reading import Reading

_ALIASES: dict[str, Durability] = {
    "power": "power",
    "full": "power",
    "process": "process",
    "normal": "process",
}


def main() -> None:
    db, start, durability = sys.argv[1], int(sys.argv[2]), _ALIASES[sys.argv[3].lower()]
    rate = float(sys.argv[4]) if len(sys.argv) > 4 else 10.0
    buf = Buffer(db, durability=durability)
    writer = BatchWriter(buf)  # production defaults: 50 rows / 1 s
    print("ready", flush=True)
    n = start
    next_at = time.monotonic()
    while True:
        writer.write(Reading(f"s{n % 10}", float(n), "C", mono_ns(), wall_ns(), BOOT_ID))
        if writer.pending_count == 0:
            print(f"c {n}", flush=True)
        n += 1
        next_at += 1 / rate
        time.sleep(max(0.0, next_at - time.monotonic()))


if __name__ == "__main__":
    main()
