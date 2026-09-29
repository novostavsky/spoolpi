"""Crash-test child for sequence allocation.

Writes step ``n`` (as ``value=float(n)``) from ``start`` while a shipper thread
claims rows, reports each as ``s <seq> <step>``, acks or releases them, and
purges acked rows aggressively, so a reissued seq can't be caught by the
table's UNIQUE constraint and only shows up in the shipped log.

Usage: python -m tests.harness.seq_child <db> <start> <block_size>
"""

from __future__ import annotations

import os
import random
import sys
import threading
import time

from spoolpi.core.buffer import BatchWriter, Buffer
from spoolpi.core.clock import BOOT_ID, mono_ns, wall_ns
from spoolpi.core.reading import Reading


def shipper(db: str) -> None:
    buf = Buffer(db)
    rng = random.Random()
    while True:
        batch = buf.claim(10)
        if not batch:
            time.sleep(0.005)
            continue
        lines = "".join(f"s {s.seq} {int(s.reading.value or 0)}\n" for s in batch)
        os.write(1, lines.encode())
        ids = [s.id for s in batch]
        if rng.random() < 0.2:
            buf.release(ids)
        else:
            buf.ack(ids)
            buf.purge_acked()


def main() -> None:
    db, start, block_size = sys.argv[1], int(sys.argv[2]), int(sys.argv[3])
    buf = Buffer(db, seq_block_size=block_size)
    buf.recover_inflight()
    threading.Thread(target=shipper, args=(db,), daemon=True).start()
    writer = BatchWriter(buf, max_rows=7, max_delay_s=0.05)
    n = start
    while True:
        writer.write(Reading("fake", float(n), None, mono_ns(), wall_ns(), BOOT_ID))
        n += 1
        time.sleep(0.001)


if __name__ == "__main__":
    main()
