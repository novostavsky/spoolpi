"""Records a sink refuses for good: quarantined, counted in a gap, never resent,
and never mistaken for a systemic failure's worth of data."""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

import pytest

from spoolpi import SpoolPi, load_config
from spoolpi.core import buffer as buffer_mod
from spoolpi.core.buffer import ACKED, INFLIGHT, PENDING, REJECTED, Buffer
from spoolpi.core.reading import Reading
from spoolpi.core.retention import REASON_REJECTED, GapRecord, Policy, Retention
from spoolpi.core.shipper import Shipper
from spoolpi.sinks.base import AckSet, Envelope
from spoolpi.sinks.memory import Accept, MemorySink, Poison
from tests.shipping import FAST_BACKOFF, assert_invariants, drain, fill, gap_envelopes, reading


def is_step(*steps: int) -> Poison:
    return Poison(lambda e: isinstance(e.payload, Reading) and e.payload.value in steps)


@pytest.fixture
def running() -> Iterator[list[Shipper]]:
    started: list[Shipper] = []
    yield started
    for s in started:
        s.stop(timeout_s=2)


def ship(db: Path, sink: MemorySink, running: list[Shipper], **kw: object) -> Shipper:
    opts: dict[str, object] = {"backoff": FAST_BACKOFF, "poll_interval_s": 0.02}
    opts.update(kw)
    s = Shipper(db, sink, **opts)  # type: ignore[arg-type]
    s.start()
    running.append(s)
    return s


# --- AckSet / MemorySink ---------------------------------------------------------


def test_ackset_refuses_a_seq_both_accepted_and_rejected() -> None:
    with pytest.raises(ValueError, match="both accepted and rejected"):
        AckSet.of([1, 2], rejected=[2])


def test_memory_sink_poison_rejects_matches_and_accepts_the_rest() -> None:
    batch = [Envelope("b", i, reading(i)) for i in range(4)]
    sink = MemorySink([is_step(1, 3)])
    assert sink.send(batch) == AckSet.of([0, 2], rejected=[1, 3])
    assert [e.seq for e in sink.received] == [0, 2]
    assert [e.seq for e in sink.rejected] == [1, 3]


# --- buffer ----------------------------------------------------------------------


def test_reject_quarantines_and_records_an_exact_gap(tmp_path: Path) -> None:
    with Buffer(tmp_path / "b.db", retention=Retention(Policy.DROP_OLDEST, 100)) as b:
        b.append([reading(i) for i in range(10)])
        claimed = b.claim(10)
        assert b.reject([claimed[2].id, claimed[5].id, 10**9]) == 2
        b.ack(s.id for i, s in enumerate(claimed) if i not in (2, 5))
        assert b.counts() == {PENDING: 0, INFLIGHT: 0, ACKED: 8, REJECTED: 2}
        assert b.unacked() == 0
        assert b.gaps() == [GapRecord("s", "boot", 2, 5, REASON_REJECTED, 2)]
        # Quarantined rows never come back: not on claim, not on restart recovery.
        b.recover_inflight()
        assert b.claim(10) == []


def test_reject_only_touches_inflight_rows(tmp_path: Path) -> None:
    with Buffer(tmp_path / "b.db") as b:
        b.append([reading(i) for i in range(3)])
        pending_id = b.claim(1)[0].id + 1
        assert b.reject([pending_id]) == 0
        assert b.gaps() == []


def test_quarantine_is_trimmed_to_its_newest_rows(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(buffer_mod, "QUARANTINE_KEEP", 3)
    with Buffer(tmp_path / "b.db") as b:
        b.append([reading(i) for i in range(8)])
        b.reject(s.id for s in b.claim(8))
        b.purge_acked()
        kept = [int(v) for (v,) in b._conn.execute("SELECT value FROM readings ORDER BY id")]
        assert kept == [5, 6, 7]
        assert sum(g.count for g in b.gaps()) == 8  # the accounting outlives the rows


def test_quarantined_gaps_do_not_count_as_unshipped(tmp_path: Path) -> None:
    with Buffer(tmp_path / "b.db", retention=Retention(Policy.DROP_OLDEST, 2)) as b:
        b.append([reading(i) for i in range(4)])
        gaps = b.claim_gaps(10)
        assert b.unshipped_gaps() == 1
        assert b.reject_gaps(g.id for g in gaps) == 1
        assert b.unshipped_gaps() == 0


# --- poison input ------------------------------------------------------------------


def test_spoolpi_refuses_text_it_could_never_store(tmp_path: Path) -> None:
    cfg = tmp_path / "spoolpi.toml"
    cfg.write_text(
        '[buffer]\npath = "b.db"\n[retention]\npolicy = "drop_oldest"\nmax_rows = 100\n'
        '[sink]\ntype = "jsonl"\npath = "out.jsonl"\n'
    )
    sink = MemorySink()
    with SpoolPi(load_config(cfg), sink) as spoolpi:
        with pytest.raises(ValueError, match="not valid UTF-8"):
            spoolpi.write("\ud800", 1.0)
        spoolpi.write("t1", 2.0)  # the batch wasn't poisoned by the refused write
        assert spoolpi.drain(5)
    assert [e.payload.sensor_id for e in sink.received if isinstance(e.payload, Reading)] == ["t1"]


# --- shipper ---------------------------------------------------------------------


def test_poison_row_is_quarantined_and_the_rest_flows(
    tmp_path: Path, running: list[Shipper]
) -> None:
    db = tmp_path / "b.db"
    fill(db, 200)
    sink = MemorySink(default=is_step(13, 150))
    s = ship(db, sink, running, batch_size=50)
    drain(db)
    assert s.stats.rejected == 2
    assert s.stats.failed_sends == 0  # a poison row must not look like an outage
    gaps = [g for _, g in gap_envelopes(sink)]
    assert {g.reason for g in gaps} == {REASON_REJECTED}
    assert sum(g.count for g in gaps) == 2
    assert_invariants(sink, 200)
    with Buffer(db) as b:
        assert b.counts()[REJECTED] == 2


def test_rejecting_a_whole_batch_is_treated_as_an_outage(
    tmp_path: Path, running: list[Shipper]
) -> None:
    db = tmp_path / "b.db"
    fill(db, 30)
    everything = Poison(lambda _e: True)
    sink = MemorySink([everything] * 4)  # e.g. a schema change on the server, then fixed
    s = ship(db, sink, running, batch_size=10)
    drain(db)
    assert s.stats.rejected == 0
    assert sink.calls >= 5
    assert sorted(
        int(r.value or 0) for e in sink.received if isinstance(r := e.payload, Reading)
    ) == list(range(30))


def test_a_lone_poison_row_is_still_rejected(tmp_path: Path, running: list[Shipper]) -> None:
    db = tmp_path / "b.db"
    fill(db, 1)
    sink = MemorySink(default=is_step(0))
    s = ship(db, sink, running)
    drain(db)
    assert s.stats.rejected == 1


def test_a_rejected_gap_is_quarantined_and_readings_keep_flowing(
    tmp_path: Path, running: list[Shipper]
) -> None:
    db = tmp_path / "b.db"
    with Buffer(db, retention=Retention(Policy.DROP_OLDEST, 5)) as b:
        b.append([reading(i) for i in range(8)])  # 3 dropped into one gap
    sink = MemorySink([Poison(lambda e: isinstance(e.payload, GapRecord))], default=Accept())
    s = ship(db, sink, running)
    drain(db)
    assert s.stats.rejected == 1
    assert sorted(
        int(r.value or 0) for e in sink.received if isinstance(r := e.payload, Reading)
    ) == [
        3,
        4,
        5,
        6,
        7,
    ]
    with Buffer(db) as b:
        assert b._conn.execute("SELECT state FROM gaps").fetchall() == [(REJECTED,)]
