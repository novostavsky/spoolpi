"""Sequence allocation and identity.

A sink deduplicates on ``(buffer_id, seq)``. ``seq`` comes from a high-water mark
persisted in the ``meta`` table and reserved in blocks, so a crash costs at most
one block of unused numbers (gaps are legal) and never reissues one.
``buffer_id`` is a fresh UUID each time a buffer database is created, so a
recreated database can't collide with keys the old one already shipped.
"""

from __future__ import annotations

import hashlib
import hmac
import socket
import sqlite3
import uuid
from pathlib import Path
from typing import Final

META_SCHEMA: Final = (
    "CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value ANY NOT NULL) STRICT"
)


def init_meta(conn: sqlite3.Connection) -> None:
    """Create the meta table and its rows. Run inside the schema transaction."""
    conn.execute(META_SCHEMA)
    conn.execute(
        "INSERT OR IGNORE INTO meta (key, value) VALUES ('buffer_id', ?)", (str(uuid.uuid4()),)
    )
    conn.execute("INSERT OR IGNORE INTO meta (key, value) VALUES ('seq_hwm', 0)")


def read_buffer_id(conn: sqlite3.Connection) -> str:
    return str(conn.execute("SELECT value FROM meta WHERE key = 'buffer_id'").fetchone()[0])


class SeqAllocator:
    """Hands out sequence numbers from blocks reserved in their own transaction.

    Each reservation reads the persisted high-water mark under a write lock, so
    two allocators on the same database still get disjoint blocks.
    """

    def __init__(self, conn: sqlite3.Connection, *, block_size: int = 1000) -> None:
        if conn.isolation_level is not None:
            raise ValueError("SeqAllocator needs an autocommit connection (isolation_level=None)")
        self._conn = conn
        self._block_size = block_size
        self._next = 0
        self._end = 0

    def take(self, n: int) -> list[int]:
        out: list[int] = []
        while len(out) < n:
            if self._next == self._end:
                self._reserve(max(self._block_size, n - len(out)))
            k = min(n - len(out), self._end - self._next)
            out.extend(range(self._next, self._next + k))
            self._next += k
        return out

    def _reserve(self, size: int) -> None:
        conn = self._conn
        previous = int(conn.execute("PRAGMA synchronous").fetchone()[0])
        # FULL fsyncs this commit. Under NORMAL a power cut could roll back a
        # reservation whose numbers already shipped, and they'd be issued again.
        conn.execute("PRAGMA synchronous = FULL")
        try:
            conn.execute("BEGIN IMMEDIATE")
            try:
                hwm = int(
                    conn.execute("SELECT value FROM meta WHERE key = 'seq_hwm'").fetchone()[0]
                )
                conn.execute("UPDATE meta SET value = ? WHERE key = 'seq_hwm'", (hwm + size,))
            except BaseException:
                conn.execute("ROLLBACK")
                raise
            conn.execute("COMMIT")
        finally:
            conn.execute(f"PRAGMA synchronous = {previous}")
        self._next, self._end = hwm, hwm + size


def device_id(machine_id_path: str | Path = "/etc/machine-id") -> str:
    """A stable per-host ID derived from machine-id, which shouldn't leave the host raw.

    Falls back to the hostname when machine-id is missing or empty.
    """
    try:
        raw = Path(machine_id_path).read_text().strip()
    except OSError:
        raw = ""
    key = (raw or socket.gethostname()).encode()
    return hmac.new(key, b"spool", hashlib.sha256).hexdigest()[:32]
