"""Crash-test child for the buffer.

Writes step ``n`` as ``value=float(n)`` through a BatchWriter, starting at
``start``, while a second thread with its own handle claims, acks and releases
rows. Progress goes to stdout with unbuffered writes so it survives SIGKILL:

    w <n>   write(n) returned
    c <n>   the batch ending at n is committed

Usage: python -m tests.harness.buffer_child <db> <start> <max_rows> <wal_autocheckpoint>
"""

from __future__ import annotations

import os
import random
import sys
import threading
import time

from spool.core.buffer import BatchWriter, Buffer
from spool.core.clock import BOOT_ID, mono_ns, wall_ns
from spool.core.reading import Reading


def report(tag: str, step: int) -> None:
    os.write(1, f"{tag} {step}\n".encode())


def shipper(db: str, wal_autocheckpoint: int) -> None:
    buf = Buffer(db, wal_autocheckpoint=wal_autocheckpoint)
    rng = random.Random()
    while True:
        batch = buf.claim(10)
        if not batch:
            time.sleep(0.005)
            continue
        time.sleep(0.002)  # the "network call", made with no transaction open
        ids = [s.id for s in batch]
        if rng.random() < 0.2:
            buf.release(ids)
        else:
            buf.ack(ids)


def main() -> None:
    db, start, max_rows, ckpt = sys.argv[1], int(sys.argv[2]), int(sys.argv[3]), int(sys.argv[4])
    buf = Buffer(db, wal_autocheckpoint=ckpt)
    buf.recover_inflight()
    threading.Thread(target=shipper, args=(db, ckpt), daemon=True).start()

    writer = BatchWriter(buf, max_rows=max_rows, max_delay_s=0.05)
    n = start
    while True:
        writer.write(Reading("fake", float(n), None, mono_ns(), wall_ns(), BOOT_ID))
        report("w", n)
        if writer.pending_count == 0:
            report("c", n)
        n += 1
        time.sleep(0.002)


if __name__ == "__main__":
    main()
