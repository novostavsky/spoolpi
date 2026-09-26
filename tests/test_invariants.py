"""The two properties that matter, end to end through buffer, shipper and sink:

1. No loss: every reading written is delivered at least once.
2. Dedupe-safe: each (buffer_id, seq) key names exactly one reading, and each
   reading is shipped under exactly one key, however often it is resent.
"""

from __future__ import annotations

import random
import time
from pathlib import Path

import pytest

from spool.core.buffer import BatchWriter, Buffer
from spool.core.reading import Reading
from spool.core.shipper import Backoff, Shipper
from spool.sinks.memory import Accept, Behavior, Hang, MemorySink, Partial, Poison, Raise, Reject
from tests.shipping import FAST_BACKOFF, assert_invariants, drain, fill, reading


def _chaos(rng: random.Random, n: int) -> list[Behavior]:
    options: list[Behavior] = [
        Accept(),
        Reject(),
        Raise(),
        Partial.first(rng.randrange(1, 40)),
        Partial.where(lambda e: e.seq % 3 != 0),
        Hang(for_s=0.15, late_accept=True),
        Hang(for_s=0.15),
    ]
    return [rng.choice(options) for _ in range(n)]


@pytest.mark.parametrize("seed", [0, 1, 2])
def test_invariants_under_chaos_with_concurrent_writer(tmp_path: Path, seed: int) -> None:
    db = tmp_path / "b.db"
    n = 3000
    rng = random.Random(seed)
    sink = MemorySink(_chaos(rng, 300))
    with Buffer(db) as buf:
        shipper = Shipper(
            db,
            sink,
            batch_size=50,
            send_timeout_s=0.05,
            backoff=FAST_BACKOFF,
            poll_interval_s=0.02,
            max_abandoned_sends=8,
            rng=rng,
        )
        shipper.start()
        try:
            writer = BatchWriter(buf, max_rows=20)
            for i in range(n):
                writer.write(reading(i))
                if writer.pending_count == 0:
                    shipper.notify()
            writer.flush()
            shipper.notify()
            drain(db, timeout_s=60)
        finally:
            shipper.stop(timeout_s=2)
    assert_invariants(sink, n)


def test_invariants_with_poison_rows_and_a_concurrent_writer(tmp_path: Path) -> None:
    db = tmp_path / "b.db"
    n = 2000
    rng = random.Random(5)
    # Consistent per record, like a real validating server: the same rows always fail.
    poison = Poison(lambda e: isinstance(e.payload, Reading) and e.payload.value % 97 == 0)
    flaky: list[Behavior] = [rng.choice([poison, Raise(), Reject()]) for _ in range(100)]
    sink = MemorySink(flaky, default=poison)
    with Buffer(db) as buf:
        shipper = Shipper(db, sink, batch_size=50, backoff=FAST_BACKOFF, poll_interval_s=0.02)
        shipper.start()
        try:
            writer = BatchWriter(buf, max_rows=20)
            for i in range(n):
                writer.write(reading(i))
                if writer.pending_count == 0:
                    shipper.notify()
            writer.flush()
            shipper.notify()
            drain(db, timeout_s=60)
        finally:
            shipper.stop(timeout_s=2)
    assert shipper.stats.rejected == len(range(0, n, 97))
    assert_invariants(sink, n)


def _drain_rate(tmp_path: Path, failure_rate: float, n: int, seed: int) -> float:
    db = tmp_path / f"rate-{failure_rate}.db"
    fill(db, n)
    rng = random.Random(seed)
    script: list[Behavior] = [
        Raise() if rng.random() < failure_rate else Accept() for _ in range(5000)
    ]
    sink = MemorySink(script, latency_s=0.005)  # a 5 ms uplink round trip
    shipper = Shipper(
        db,
        sink,
        batch_size=50,
        backoff=Backoff(initial_s=0.005, max_s=0.1),
        poll_interval_s=0.02,
        rng=rng,
    )
    t0 = time.monotonic()
    shipper.start()
    try:
        drain(db, timeout_s=60)
    finally:
        shipper.stop(timeout_s=2)
    elapsed = time.monotonic() - t0
    assert_invariants(sink, n)
    assert len(sink.received) == n  # Raise delivers nothing, so no duplicates either
    return n / elapsed


def test_acceptance_throughput_degrades_smoothly_with_random_failures(tmp_path: Path) -> None:
    n = 3000
    rates = {p: _drain_rate(tmp_path, p, n, seed=7) for p in (0.0, 0.25, 0.5, 0.75)}
    print("  ".join(f"fail={p:.0%}: {r:,.0f} rows/s" for p, r in rates.items()))
    # Smooth: more failure never helps (with timing slack), and 50% failure costs
    # a bounded factor rather than stalling delivery.
    assert rates[0.25] <= rates[0.0] * 1.2
    assert rates[0.5] <= rates[0.25] * 1.2
    assert rates[0.75] <= rates[0.5] * 1.2
    # At p=0.5 each doubling level below the cap adds the same expected backoff per
    # delivered batch (0.5**k * 2**(k-1) is constant): ~5 levels here, ~13 ms per
    # 5 ms send, so ~20% of baseline is what the backoff model predicts.
    assert rates[0.5] >= rates[0.0] * 0.1
