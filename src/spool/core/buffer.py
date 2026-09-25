"""SQLite WAL buffer: the durable queue between the writer and the shipper.

Rows move ``pending -> inflight -> acked``. Delivery is at-least-once by design:
rows a shipper claimed but never acked (because the process died between send
and ack) go back to pending via ``recover_inflight()`` and are sent again.

Durability: with ``synchronous=NORMAL`` every commit survives a process crash
(SIGKILL); on power loss the most recent commits may roll back, but the file
is never corrupted.
"""

from __future__ import annotations

import json
import os
import sqlite3
import time
from collections.abc import Callable, Iterable, Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from types import TracebackType
from typing import Any, Final, Self

from spool.core.identity import SeqAllocator, init_meta, read_buffer_id
from spool.core.reading import Reading

PENDING: Final = 0
INFLIGHT: Final = 1
ACKED: Final = 2

SCHEMA_VERSION: Final = 2

# `id` is internal row identity only. It is a plain rowid, so it can be reused
# after a purge; `seq` (with `buffer_id`) is what identifies a reading downstream.
_SCHEMA: Final = (
    """
    CREATE TABLE IF NOT EXISTS readings (
        id         INTEGER PRIMARY KEY,
        seq        INTEGER NOT NULL UNIQUE,
        sensor_id  TEXT    NOT NULL,
        value      REAL,
        unit       TEXT,
        mono_ns    INTEGER NOT NULL,
        wall_ns    INTEGER NOT NULL,
        boot_id    TEXT    NOT NULL,
        ts_quality INTEGER NOT NULL,
        qc_flag    INTEGER NOT NULL,
        qc_tests   TEXT    NOT NULL,
        state      INTEGER NOT NULL DEFAULT 0
    ) STRICT
    """,
    "CREATE INDEX IF NOT EXISTS readings_by_state ON readings (state, id)",
)

_COLUMNS: Final = "sensor_id, value, unit, mono_ns, wall_ns, boot_id, ts_quality, qc_flag, qc_tests"


@dataclass(frozen=True, slots=True)
class Stored:
    id: int
    seq: int
    reading: Reading


def _encode(r: Reading) -> tuple[object, ...]:
    # SQLite stores NaN as NULL, so a NaN value comes back as None (a failed read).
    return (
        r.sensor_id,
        r.value,
        r.unit,
        r.mono_ns,
        r.wall_ns,
        r.boot_id,
        r.ts_quality,
        r.qc_flag,
        json.dumps(list(r.qc_tests)),
    )


def _decode(row: Any) -> Stored:
    row_id, seq, sensor_id, value, unit, mono, wall, boot_id, ts_quality, qc_flag, qc_tests = row
    reading = Reading(
        sensor_id=sensor_id,
        value=value,
        unit=unit,
        mono_ns=mono,
        wall_ns=wall,
        boot_id=boot_id,
        ts_quality=ts_quality,
        qc_flag=qc_flag,
        qc_tests=tuple(json.loads(qc_tests)),
    )
    return Stored(id=row_id, seq=seq, reading=reading)


def _fsync_dir(path: Path) -> None:
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _connect(path: Path, wal_autocheckpoint: int) -> sqlite3.Connection:
    # isolation_level=None: we issue BEGIN IMMEDIATE / COMMIT ourselves.
    conn = sqlite3.connect(path, isolation_level=None, timeout=5.0)
    mode = conn.execute("PRAGMA journal_mode = WAL").fetchone()[0]
    if mode != "wal":
        conn.close()
        raise sqlite3.OperationalError(
            f"{path}: could not enable WAL mode (got {mode!r}); "
            "the buffer must live on a local filesystem"
        )
    conn.execute("PRAGMA synchronous = NORMAL")
    conn.execute(f"PRAGMA wal_autocheckpoint = {int(wal_autocheckpoint)}")
    conn.execute("PRAGMA busy_timeout = 5000")
    return conn


class Buffer:
    """A handle on the buffer database. Use one handle per thread.

    The underlying sqlite3 connection raises ``ProgrammingError`` if touched from
    a thread other than the one that created it.
    """

    def __init__(
        self,
        path: str | os.PathLike[str],
        *,
        wal_autocheckpoint: int = 1000,
        seq_block_size: int = 1000,
    ) -> None:
        self.path = Path(path)
        is_new = not self.path.exists()
        self._conn = _connect(self.path, wal_autocheckpoint)
        with self._write() as c:
            version = c.execute("PRAGMA user_version").fetchone()[0]
            if version not in (0, SCHEMA_VERSION):
                raise RuntimeError(
                    f"{self.path}: buffer schema version {version} is not supported "
                    f"(this Spool expects {SCHEMA_VERSION})"
                )
            for stmt in _SCHEMA:
                c.execute(stmt)
            init_meta(c)
            c.execute(f"PRAGMA user_version = {SCHEMA_VERSION}")
            self.buffer_id = read_buffer_id(c)
        if is_new:
            _fsync_dir(self.path.parent)
        self._seq_block_size = seq_block_size
        self._seqs: SeqAllocator | None = None  # only writer handles ever allocate

    @contextmanager
    def _write(self) -> Iterator[sqlite3.Connection]:
        # IMMEDIATE takes the write lock up front; a deferred txn that later
        # upgrades from read to write can fail with SQLITE_BUSY mid-transaction.
        self._conn.execute("BEGIN IMMEDIATE")
        try:
            yield self._conn
        except BaseException:
            self._conn.execute("ROLLBACK")
            raise
        self._conn.execute("COMMIT")

    def append(self, readings: Sequence[Reading]) -> None:
        if not readings:
            return
        if self._seqs is None:
            self._seqs = SeqAllocator(self._conn, block_size=self._seq_block_size)
        # Reserved in its own committed transaction first; if the insert then
        # fails, these numbers are simply never used.
        seqs = self._seqs.take(len(readings))
        with self._write() as c:
            c.executemany(
                f"INSERT INTO readings (seq, {_COLUMNS}) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                ((seq, *_encode(r)) for seq, r in zip(seqs, readings, strict=True)),
            )

    def claim(self, limit: int) -> list[Stored]:
        """Mark up to ``limit`` of the oldest pending rows inflight and return them.

        The transaction is closed before returning, so no snapshot is held while
        the caller talks to the network.
        """
        with self._write() as c:
            rows = c.execute(
                f"SELECT id, seq, {_COLUMNS} FROM readings WHERE state = ? ORDER BY id LIMIT ?",
                (PENDING, limit),
            ).fetchall()
            if rows:
                # The selected rows are exactly the pending rows with id <= the last one.
                c.execute(
                    "UPDATE readings SET state = ? WHERE state = ? AND id <= ?",
                    (INFLIGHT, PENDING, rows[-1][0]),
                )
        return [_decode(row) for row in rows]

    def ack(self, ids: Iterable[int]) -> int:
        return self._transition(ids, INFLIGHT, ACKED)

    def release(self, ids: Iterable[int]) -> int:
        """Return inflight rows to pending, e.g. after a failed send."""
        return self._transition(ids, INFLIGHT, PENDING)

    def _transition(self, ids: Iterable[int], src: int, dst: int) -> int:
        with self._write() as c:
            cur = c.executemany(
                "UPDATE readings SET state = ? WHERE id = ? AND state = ?",
                ((dst, i, src) for i in ids),
            )
            return cur.rowcount

    def recover_inflight(self) -> int:
        """Reset inflight rows to pending. Call once at process start, before any shipper runs."""
        with self._write() as c:
            return c.execute(
                "UPDATE readings SET state = ? WHERE state = ?", (PENDING, INFLIGHT)
            ).rowcount

    def purge_acked(self, limit: int = 10_000) -> int:
        """Delete up to ``limit`` of the oldest acked rows.

        Deliberately no VACUUM: on an SD card it rewrites the whole file. Freed
        pages are reused, so the file size plateaus instead of shrinking.
        """
        with self._write() as c:
            return c.execute(
                "DELETE FROM readings WHERE id IN "
                "(SELECT id FROM readings WHERE state = ? ORDER BY id LIMIT ?)",
                (ACKED, limit),
            ).rowcount

    def counts(self) -> dict[int, int]:
        rows = self._conn.execute("SELECT state, count(*) FROM readings GROUP BY state")
        found = {int(state): int(n) for state, n in rows}
        return {state: found.get(state, 0) for state in (PENDING, INFLIGHT, ACKED)}

    def close(self) -> None:
        self._conn.close()

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.close()


class BatchWriter:
    """Groups readings into one transaction per batch.

    A batch is committed once it reaches ``max_rows``, or on the first write after
    its oldest reading is ``max_delay_s`` old. A SIGKILL loses at most the uncommitted
    batch. Nothing flushes on a timer, so call ``flush()`` on shutdown or when idle.
    """

    def __init__(
        self,
        buffer: Buffer,
        *,
        max_rows: int = 50,
        max_delay_s: float = 1.0,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._buffer = buffer
        self._max_rows = max_rows
        self._max_delay_s = max_delay_s
        self._clock = clock
        self._pending: list[Reading] = []
        self._first_at = 0.0

    @property
    def pending_count(self) -> int:
        return len(self._pending)

    def write(self, reading: Reading) -> None:
        if not self._pending:
            self._first_at = self._clock()
        self._pending.append(reading)
        if (
            len(self._pending) >= self._max_rows
            or self._clock() - self._first_at >= self._max_delay_s
        ):
            self.flush()

    def flush(self) -> None:
        if self._pending:
            # If append raises, the batch is kept and retried on the next flush.
            self._buffer.append(self._pending)
            self._pending = []
