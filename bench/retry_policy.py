"""Throughput under random send failures, for different numbers of immediate retries.

Usage: python bench/retry_policy.py
"""

from __future__ import annotations

import random
import sys
import tempfile
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from spool.core.buffer import Buffer
from spool.core.shipper import Backoff, Shipper
from spool.sinks.memory import Accept, Behavior, MemorySink, Raise
from tests.shipping import drain, reading

REGIMES = {
    # name: (uplink latency, backoff initial, backoff max, readings, failure rates)
    "fast: 5 ms uplink, backoff 5-100 ms": (0.005, 0.005, 0.1, 3000, (0.0, 0.25, 0.5, 0.75)),
    "production backoff: 50 ms uplink, backoff 0.5-60 s": (0.05, 0.5, 60.0, 1000, (0.0, 0.25, 0.5)),
}


def rate(
    latency: float, initial: float, cap: float, n: int, p: float, immediate: int
) -> tuple[float, int]:
    with tempfile.TemporaryDirectory() as tmp:
        db = Path(tmp) / "b.db"
        with Buffer(db) as b:
            b.append([reading(i) for i in range(n)])
        rng = random.Random(7)
        script: list[Behavior] = [Raise() if rng.random() < p else Accept() for _ in range(20000)]
        sink = MemorySink(script, latency_s=latency)
        backoff = Backoff(initial_s=initial, max_s=cap, immediate_retries=immediate)
        shipper = Shipper(db, sink, batch_size=50, backoff=backoff, poll_interval_s=0.02, rng=rng)
        t0 = time.monotonic()
        shipper.start()
        try:
            drain(db, timeout_s=600)
        finally:
            shipper.stop(timeout_s=5)
        return n / (time.monotonic() - t0), sink.calls


def main() -> None:
    for name, (latency, initial, cap, n, rates) in REGIMES.items():
        print(f"\n{name} ({n} readings, batches of 50)")
        print("immediate | " + " | ".join(f"fail {p:.0%}" for p in rates))
        for immediate in (0, 1, 2, 3):
            cells = []
            for p in rates:
                r, calls = rate(latency, initial, cap, n, p, immediate)
                cells.append(f"{r:7,.0f}/s ({calls} sends)")
            print(f"{immediate:9} | " + " | ".join(cells))


if __name__ == "__main__":
    main()
