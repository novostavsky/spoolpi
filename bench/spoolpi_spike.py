"""The spike rerun: the naive spike's fake-sensor loop, through SpoolPi instead.

Same cadence as naive_spike.py (one reading every 10 ms), and the same resume
rule: after a restart, continue from the last *committed* step + 1. Ships over
the HTTP sink.

Usage: python bench/spoolpi_spike.py <config.toml>            # run until killed
       python bench/spoolpi_spike.py <config.toml> --drain    # ship everything, exit
"""

from __future__ import annotations

import sqlite3
import sys
import time

from spoolpi import SpoolPi, load_config


def last_committed_step(db: str) -> int:
    try:
        conn = sqlite3.connect(db)
        try:
            (top,) = conn.execute("SELECT max(value) FROM readings").fetchone()
        finally:
            conn.close()
    except sqlite3.OperationalError:  # no buffer yet
        return -1
    return -1 if top is None else int(top)


def main() -> None:
    config = load_config(sys.argv[1])
    if "--drain" in sys.argv:
        with SpoolPi(config) as spoolpi:
            ok = spoolpi.drain(60)
        sys.exit(0 if ok else 1)

    n = last_committed_step(str(config.buffer_path)) + 1
    with SpoolPi(config) as spoolpi:
        while True:
            spoolpi.write("fake", float(n))
            n += 1
            time.sleep(0.01)
            spoolpi.tick()


if __name__ == "__main__":
    main()
