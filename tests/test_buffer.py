from __future__ import annotations

import sqlite3
import threading
from collections.abc import Iterator
from pathlib import Path

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from spoolpi.core import buffer as buffer_mod
from spoolpi.core.buffer import ACKED, INFLIGHT, PENDING, REJECTED, BatchWriter, Buffer
from spoolpi.core.reading import TS_CORRECTED, Reading

INT64 = st.integers(-(2**63), 2**63 - 1)
TEXT = st.text(alphabet=st.characters(blacklist_categories=("Cs",)))


def r(n: int, value: float | None = None) -> Reading:
    return Reading("s1", float(n) if value is None else value, "C", n, n, "boot")


@pytest.fixture
def buf(tmp_path: Path) -> Iterator[Buffer]:
    b = Buffer(tmp_path / "b.db")
    yield b
    b.close()


readings = st.builds(
    Reading,
    sensor_id=TEXT,
    value=st.none() | st.floats(allow_nan=False, allow_infinity=False),
    unit=st.none() | TEXT,
    mono_ns=INT64,
    wall_ns=INT64,
    boot_id=TEXT,
    ts_quality=st.integers(0, 2),
    qc_flag=st.integers(0, 9),
    qc_tests=st.lists(TEXT, max_size=4).map(tuple),
)


@settings(suppress_health_check=[HealthCheck.function_scoped_fixture], max_examples=50)
@given(batch=st.lists(readings, min_size=1, max_size=20))
def test_roundtrip_is_exact(buf: Buffer, batch: list[Reading]) -> None:
    buf.append(batch)
    got = buf.claim(len(batch))
    assert [s.reading for s in got] == batch
    buf.ack(s.id for s in got)
    buf.purge_acked()


def test_pragmas(buf: Buffer) -> None:
    conn = buf._conn
    assert conn.execute("PRAGMA journal_mode").fetchone()[0] == "wal"
    assert conn.execute("PRAGMA synchronous").fetchone()[0] == 2  # FULL: durability="power"
    assert conn.execute("PRAGMA wal_autocheckpoint").fetchone()[0] == 1000
    assert conn.execute("PRAGMA busy_timeout").fetchone()[0] == 5000


def test_process_durability_uses_normal(tmp_path: Path) -> None:
    with Buffer(tmp_path / "b.db", durability="process") as b:
        assert b._conn.execute("PRAGMA synchronous").fetchone()[0] == 1  # NORMAL


def test_seq_reservation_keeps_the_handles_durability(tmp_path: Path) -> None:
    # The reservation switches to FULL for its own commit and must switch back.
    for durability, expected in (("process", 1), ("power", 2)):
        with Buffer(tmp_path / f"{durability}.db", durability=durability) as b:
            b.append([Reading("s", 1.0, None, 1, 1, "boot")])
            assert b._conn.execute("PRAGMA synchronous").fetchone()[0] == expected


def test_unknown_durability_is_refused(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="durability"):
        Buffer(tmp_path / "b.db", durability="sometimes")  # type: ignore[arg-type]


def test_failed_read_is_stored_and_shipped(buf: Buffer) -> None:
    failed = Reading("s1", None, None, 1, 1, "boot")
    buf.append([failed])
    assert buf.claim(1)[0].reading == failed


@pytest.mark.parametrize("bad", [float("nan"), float("inf"), float("-inf")])
def test_non_finite_values_are_stored_as_failed_reads(buf: Buffer, bad: float) -> None:
    buf.append([r(1, value=bad)])
    assert buf.claim(1)[0].reading.value is None


def test_claim_is_ordered_and_exclusive(buf: Buffer) -> None:
    buf.append([r(i) for i in range(10)])
    first = buf.claim(4)
    second = buf.claim(4)
    assert [s.reading.mono_ns for s in first] == [0, 1, 2, 3]
    assert [s.reading.mono_ns for s in second] == [4, 5, 6, 7]
    assert buf.counts() == {PENDING: 2, INFLIGHT: 8, ACKED: 0, REJECTED: 0}


def test_release_puts_rows_back_in_original_order(buf: Buffer) -> None:
    buf.append([r(i) for i in range(6)])
    a = buf.claim(3)
    buf.claim(3)
    assert buf.release(s.id for s in a) == 3
    assert [s.reading.mono_ns for s in buf.claim(10)] == [0, 1, 2]


def test_ack_only_moves_inflight_rows(buf: Buffer) -> None:
    buf.append([r(i) for i in range(3)])
    claimed = buf.claim(2)
    assert buf.ack([claimed[0].id, claimed[0].id + 99]) == 1
    assert buf.ack([claimed[0].id]) == 0  # already acked
    assert buf.counts() == {PENDING: 1, INFLIGHT: 1, ACKED: 1, REJECTED: 0}


def test_recover_inflight(tmp_path: Path) -> None:
    with Buffer(tmp_path / "b.db") as b:
        b.append([r(i) for i in range(5)])
        b.claim(3)
    with Buffer(tmp_path / "b.db") as b:
        assert b.counts()[INFLIGHT] == 3  # opening alone must not touch inflight rows
        assert b.recover_inflight() == 3
        assert [s.reading.mono_ns for s in b.claim(10)] == [0, 1, 2, 3, 4]


def test_purge_removes_only_acked_and_respects_limit(buf: Buffer) -> None:
    buf.append([r(i) for i in range(10)])
    claimed = buf.claim(6)
    buf.ack(s.id for s in claimed)
    assert buf.purge_acked(limit=4) == 4
    assert buf.counts() == {PENDING: 4, INFLIGHT: 0, ACKED: 2, REJECTED: 0}
    assert buf.purge_acked() == 2
    assert buf.counts() == {PENDING: 4, INFLIGHT: 0, ACKED: 0, REJECTED: 0}


def test_claim_leaves_no_transaction_open(tmp_path: Path, buf: Buffer) -> None:
    buf.append([r(i) for i in range(2000)])
    buf.claim(100)
    assert not buf._conn.in_transaction
    # Nothing pins the WAL, so a full checkpoint can complete from another connection.
    other = sqlite3.connect(tmp_path / "b.db")
    busy, _, _ = other.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
    other.close()
    assert busy == 0


def test_many_handles_can_open_a_brand_new_database_at_once(tmp_path: Path) -> None:
    # Regression: racing WAL switches on a new file raised "database is locked".
    for attempt in range(5):
        db = tmp_path / f"race{attempt}.db"
        gate = threading.Barrier(8)
        errors: list[BaseException] = []

        def open_it(
            path: Path = db,
            barrier: threading.Barrier = gate,
            failures: list[BaseException] = errors,
        ) -> None:
            barrier.wait()
            try:
                Buffer(path).close()
            except sqlite3.Error as e:
                failures.append(e)

        threads = [threading.Thread(target=open_it) for _ in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert errors == []


def test_handle_refuses_use_from_another_thread(buf: Buffer) -> None:
    refused: list[sqlite3.ProgrammingError] = []

    def use() -> None:
        try:
            buf.counts()
        except sqlite3.ProgrammingError as e:
            refused.append(e)

    t = threading.Thread(target=use)
    t.start()
    t.join()
    assert len(refused) == 1


def test_failed_append_rolls_back_whole_batch(buf: Buffer) -> None:
    bad = Reading("s1", 1.0, None, 2**70, 0, "boot")  # overflows INTEGER
    with pytest.raises(OverflowError):
        buf.append([r(1), bad])
    assert not buf._conn.in_transaction
    assert buf.counts() == {PENDING: 0, INFLIGHT: 0, ACKED: 0, REJECTED: 0}


def test_rejects_unknown_schema_version(tmp_path: Path) -> None:
    conn = sqlite3.connect(tmp_path / "b.db")
    conn.execute("PRAGMA user_version = 99")
    conn.close()
    with pytest.raises(RuntimeError, match="schema version 99"):
        Buffer(tmp_path / "b.db")


def test_new_database_fsyncs_its_directory(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    synced: list[Path] = []
    monkeypatch.setattr(buffer_mod, "_fsync_dir", synced.append)
    Buffer(tmp_path / "b.db").close()
    Buffer(tmp_path / "b.db").close()
    assert synced == [tmp_path]


def test_concurrent_writer_and_shipper_deliver_everything_once(tmp_path: Path) -> None:
    db = tmp_path / "b.db"
    total = 3000
    Buffer(db).close()
    delivered: list[int] = []
    done = threading.Event()

    def ship() -> None:
        with Buffer(db) as b:
            while not (done.is_set() and b.counts()[PENDING] == 0):
                batch = b.claim(37)
                delivered.extend(s.reading.mono_ns for s in batch)
                b.ack(s.id for s in batch)

    t = threading.Thread(target=ship)
    t.start()
    with Buffer(db) as b:
        writer = BatchWriter(b, max_rows=25)
        for i in range(total):
            writer.write(r(i))
        writer.flush()
    done.set()
    t.join(timeout=30)
    assert delivered == list(range(total))


# --- BatchWriter -------------------------------------------------------------


class Ticker:
    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now


def test_batch_commits_at_max_rows(buf: Buffer) -> None:
    w = BatchWriter(buf, max_rows=3, max_delay_s=100)
    w.write(r(0))  # first write: nothing committed yet, so it goes straight away
    w.write(r(1))
    w.write(r(2))
    assert buf.counts()[PENDING] == 1 and w.pending_count == 2
    w.write(r(3))
    assert buf.counts()[PENDING] == 4 and w.pending_count == 0


def test_batch_commits_once_max_delay_since_last_commit(buf: Buffer) -> None:
    clock = Ticker()
    w = BatchWriter(buf, max_rows=100, max_delay_s=1.0, clock=clock)
    w.write(r(0))
    clock.now = 0.5
    w.write(r(1))
    assert buf.counts()[PENDING] == 1
    clock.now = 1.0
    w.write(r(2))
    assert buf.counts()[PENDING] == 3


def test_sparse_writes_commit_immediately(buf: Buffer) -> None:
    clock = Ticker()
    w = BatchWriter(buf, max_rows=100, max_delay_s=1.0, clock=clock)
    for minute in range(3):
        clock.now = 60.0 * minute
        w.write(r(minute))
        assert w.pending_count == 0  # never waits a minute for the next write


def test_flush_if_due_commits_an_aged_batch(buf: Buffer) -> None:
    clock = Ticker()
    w = BatchWriter(buf, max_rows=100, max_delay_s=1.0, clock=clock)
    w.write(r(0))
    clock.now = 0.2
    w.write(r(1))
    clock.now = 0.9
    assert not w.flush_if_due()
    clock.now = 1.2
    assert w.flush_if_due()
    assert buf.counts()[PENDING] == 2 and not w.flush_if_due()


def test_failed_flush_keeps_the_batch(buf: Buffer, monkeypatch: pytest.MonkeyPatch) -> None:
    w = BatchWriter(buf, max_rows=100)
    w.write(r(0))
    w.write(r(1))
    assert w.pending_count == 1

    def disk_full(_: object) -> None:
        raise sqlite3.OperationalError("database or disk is full")

    monkeypatch.setattr(buf, "append", disk_full)
    with pytest.raises(sqlite3.OperationalError):
        w.flush()
    assert w.pending_count == 1
    monkeypatch.undo()
    w.flush()
    assert buf.counts()[PENDING] == 2


def test_seq_follows_write_order_across_reopen(tmp_path: Path) -> None:
    db = tmp_path / "b.db"
    with Buffer(db, seq_block_size=4) as b:
        b.append([r(i) for i in range(3)])
        b.append([r(3)])
        first_id = b.buffer_id
    with Buffer(db, seq_block_size=4) as b:
        b.append([r(4), r(5)])
        assert b.buffer_id == first_id
        seqs = [s.seq for s in b.claim(10)]
    assert seqs == [0, 1, 2, 3, 4, 5]  # block of 4 was used up exactly, so no gap here


def test_reopen_mid_block_leaves_a_gap_not_a_reuse(tmp_path: Path) -> None:
    db = tmp_path / "b.db"
    with Buffer(db, seq_block_size=10) as b:
        b.append([r(0)])
    with Buffer(db, seq_block_size=10) as b:
        b.append([r(1)])
        assert [s.seq for s in b.claim(10)] == [0, 10]


def test_reader_handles_do_not_reserve_seqs(tmp_path: Path) -> None:
    db = tmp_path / "b.db"
    with Buffer(db) as b:
        b.claim(10)
        b.counts()
        assert b._seqs is None


def test_rejects_version_1_database(tmp_path: Path) -> None:
    conn = sqlite3.connect(tmp_path / "b.db")
    conn.execute("PRAGMA user_version = 1")
    conn.close()
    with pytest.raises(RuntimeError, match="schema version 1"):
        Buffer(tmp_path / "b.db")


def test_corrected_quality_survives_storage(buf: Buffer) -> None:
    corrected = Reading("s1", 1.0, "C", 5, 6, "boot", ts_quality=TS_CORRECTED)
    buf.append([corrected])
    assert buf.claim(1)[0].reading.ts_quality == TS_CORRECTED
