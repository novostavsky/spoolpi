from __future__ import annotations

import sqlite3
from collections.abc import Iterator
from pathlib import Path

import pytest
from hypothesis import given
from hypothesis import strategies as st

from spool.core.identity import SeqAllocator, device_id, init_meta, read_buffer_id


def _open(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(path, isolation_level=None)
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA synchronous = NORMAL")
    conn.execute("BEGIN IMMEDIATE")
    init_meta(conn)
    conn.execute("COMMIT")
    return conn


@pytest.fixture
def conn(tmp_path: Path) -> Iterator[sqlite3.Connection]:
    c = _open(tmp_path / "b.db")
    yield c
    c.close()


def test_takes_are_contiguous_within_a_process(conn: sqlite3.Connection) -> None:
    alloc = SeqAllocator(conn, block_size=10)
    got = alloc.take(7) + alloc.take(7) + alloc.take(1)
    assert got == list(range(15))


def test_take_larger_than_a_block(conn: sqlite3.Connection) -> None:
    alloc = SeqAllocator(conn, block_size=10)
    assert alloc.take(25) == list(range(25))
    assert alloc.take(1) == [25]


def test_restart_skips_the_unused_rest_of_the_block(conn: sqlite3.Connection) -> None:
    assert SeqAllocator(conn, block_size=10).take(3) == [0, 1, 2]
    assert SeqAllocator(conn, block_size=10).take(3) == [10, 11, 12]


def test_two_allocators_on_one_database_never_overlap(tmp_path: Path) -> None:
    a_conn, b_conn = _open(tmp_path / "b.db"), _open(tmp_path / "b.db")
    a, b = SeqAllocator(a_conn, block_size=5), SeqAllocator(b_conn, block_size=5)
    issued = []
    for _ in range(20):
        issued += a.take(3)
        issued += b.take(2)
    assert len(issued) == len(set(issued))


def test_reservation_restores_the_synchronous_setting(conn: sqlite3.Connection) -> None:
    SeqAllocator(conn, block_size=10).take(1)
    assert conn.execute("PRAGMA synchronous").fetchone()[0] == 1  # NORMAL
    assert not conn.in_transaction


def test_rejects_connection_with_implicit_transactions(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="autocommit"):
        SeqAllocator(sqlite3.connect(tmp_path / "x.db"))


@given(ops=st.lists(st.one_of(st.integers(1, 40), st.just(0)), max_size=60))
def test_no_seq_is_ever_issued_twice_across_restarts(
    tmp_path_factory: pytest.TempPathFactory, ops: list[int]
) -> None:
    # 0 means "restart": a new allocator on the same database.
    path = tmp_path_factory.mktemp("seq") / "b.db"
    conn = _open(path)
    alloc = SeqAllocator(conn, block_size=16)
    issued: list[int] = []
    for op in ops:
        if op == 0:
            alloc = SeqAllocator(conn, block_size=16)
        else:
            issued += alloc.take(op)
    conn.close()
    assert issued == sorted(set(issued)), "reused or out of order"


def _buffer_id(path: Path) -> str:
    conn = _open(path)
    try:
        return read_buffer_id(conn)
    finally:
        conn.close()


def test_buffer_id_is_stable_but_new_per_database(tmp_path: Path) -> None:
    db = tmp_path / "a.db"
    first = _buffer_id(db)
    assert _buffer_id(db) == first
    db.unlink()
    assert _buffer_id(db) != first


def test_device_id_is_stable_and_does_not_leak_machine_id(tmp_path: Path) -> None:
    mid = tmp_path / "machine-id"
    mid.write_text("0123456789abcdef0123456789abcdef\n")
    got = device_id(mid)
    assert got == device_id(mid)
    assert len(got) == 32
    assert "0123456789abcdef" not in got


def test_device_id_falls_back_to_hostname(tmp_path: Path) -> None:
    missing, empty = tmp_path / "nope", tmp_path / "empty"
    empty.write_text("")
    assert device_id(missing) == device_id(empty)
