"""SQLite WAL buffer: the durable queue between the writer and the shipper.

Rows move ``pending -> inflight -> acked``. Delivery is at-least-once by design:
rows a shipper claimed but never acked (because the process died between send
and ack) go back to pending via ``recover_inflight()`` and are sent again.

Durability: with ``synchronous=NORMAL`` every commit survives a process crash
(SIGKILL); on power loss the most recent commits may roll back, but the file
is never corrupted.

Retention: with a cap, readings discarded to respect it are recorded as gap
records in the same transaction that discards them, so the counts are exact.
"""

from __future__ import annotations

import json
import logging
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
from spool.core.retention import (
    REASON_BACKPRESSURE,
    REASON_DROP_OLDEST,
    BufferFull,
    GapRecord,
    Policy,
    Retention,
    summarize,
)

log = logging.getLogger("spool.buffer")

PENDING: Final = 0
INFLIGHT: Final = 1
ACKED: Final = 2

SCHEMA_VERSION: Final = 3

# `id` is internal row identity only. It is a plain rowid, so it can be reused
# after a purge; `seq` (with `buffer_id`) is what identifies a record downstream.
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
    # A gap is mutable (merged into) while `seq` is NULL, and frozen once it is
    # sealed for shipping, so a resend can never carry a different count.
    """
    CREATE TABLE IF NOT EXISTS gaps (
        id           INTEGER PRIMARY KEY,
        seq          INTEGER UNIQUE,
        sensor_id    TEXT,
        boot_id      TEXT    NOT NULL,
        from_mono_ns INTEGER NOT NULL,
        to_mono_ns   INTEGER NOT NULL,
        reason       TEXT    NOT NULL,
        count        INTEGER NOT NULL,
        state        INTEGER NOT NULL DEFAULT 0
    ) STRICT
    """,
    "CREATE INDEX IF NOT EXISTS gaps_by_state ON gaps (state, id)",
    # Pending + inflight readings, maintained in the same transactions that change
    # them, so enforcing the cap never needs a COUNT(*) scan.
    "INSERT OR IGNORE INTO meta (key, value) VALUES ('unacked', 0)",
)

_COLUMNS: Final = "sensor_id, value, unit, mono_ns, wall_ns, boot_id, ts_quality, qc_flag, qc_tests"
_GAP_COLUMNS: Final = "sensor_id, boot_id, from_mono_ns, to_mono_ns, reason, count"


@dataclass(frozen=True, slots=True)
class Stored:
    id: int
    seq: int
    reading: Reading


@dataclass(frozen=True, slots=True)
class StoredGap:
    id: int
    seq: int
    gap: GapRecord


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


def _add_unacked(c: sqlite3.Connection, delta: int) -> None:
    if delta:
        c.execute("UPDATE meta SET value = value + ? WHERE key = 'unacked'", (delta,))


def _record_gaps(c: sqlite3.Connection, gaps: Iterable[GapRecord]) -> None:
    for g in gaps:
        open_gap = c.execute(
            "SELECT id FROM gaps WHERE seq IS NULL AND sensor_id IS ? AND boot_id = ? "
            "AND reason = ? ORDER BY id DESC LIMIT 1",
            (g.sensor_id, g.boot_id, g.reason),
        ).fetchone()
        if open_gap is not None:
            # Merging keeps a long outage to one row per (sensor, boot, reason).
            c.execute(
                "UPDATE gaps SET from_mono_ns = min(from_mono_ns, ?), "
                "to_mono_ns = max(to_mono_ns, ?), count = count + ? WHERE id = ?",
                (g.from_mono_ns, g.to_mono_ns, g.count, open_gap[0]),
            )
        else:
            c.execute(
                f"INSERT INTO gaps ({_GAP_COLUMNS}) VALUES (?, ?, ?, ?, ?, ?)",
                (g.sensor_id, g.boot_id, g.from_mono_ns, g.to_mono_ns, g.reason, g.count),
            )


class Buffer:
    """A handle on the buffer database. Use one handle per thread.

    The underlying sqlite3 connection raises ``ProgrammingError`` if touched from
    a thread other than the one that created it. ``retention`` only matters on
    the handle that writes; without it the buffer is unbounded.
    """

    def __init__(
        self,
        path: str | os.PathLike[str],
        *,
        retention: Retention | None = None,
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
            init_meta(c)
            for stmt in _SCHEMA:
                c.execute(stmt)
            c.execute(f"PRAGMA user_version = {SCHEMA_VERSION}")
            self.buffer_id = read_buffer_id(c)
        if is_new:
            _fsync_dir(self.path.parent)
        self._retention = retention
        self._seq_block_size = seq_block_size
        self._seqs: SeqAllocator | None = None  # created on first use; readers never need one
        self._over_cap = False

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

    def _take_seqs(self, n: int) -> list[int]:
        # Reserved in its own committed transaction, so never inside _write().
        if self._seqs is None:
            self._seqs = SeqAllocator(self._conn, block_size=self._seq_block_size)
        return self._seqs.take(n)

    # --- writing -------------------------------------------------------------

    def append(self, readings: Sequence[Reading]) -> None:
        """Store a batch atomically, applying retention.

        Raises ``BufferFull`` under halt_and_alarm when the batch doesn't fit. The
        batch is then discarded and recorded as a gap: don't retry it.
        """
        if not readings:
            return
        seqs = self._take_seqs(len(readings))  # unused if the batch is refused or evicted
        incoming = list(zip(seqs, readings, strict=True))
        retention = self._retention
        refused: BufferFull | None = None
        overflow = 0
        with self._write() as c:
            if retention is not None:
                overflow = self._unacked(c) + len(incoming) - retention.max_rows
            if retention is not None and overflow > 0:
                if retention.policy is Policy.HALT_AND_ALARM:
                    _record_gaps(c, summarize(readings, REASON_BACKPRESSURE))
                    refused = BufferFull(
                        f"{self.path}: buffer at its cap of {retention.max_rows} unacked "
                        f"readings; discarded {len(readings)} and recorded a gap"
                    )
                else:
                    incoming = self._evict(c, incoming, overflow)
            if refused is None:
                c.executemany(
                    f"INSERT INTO readings (seq, {_COLUMNS}) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    ((seq, *_encode(r)) for seq, r in incoming),
                )
                _add_unacked(c, len(incoming))
        self._note_pressure(overflow > 0)
        if refused is not None:
            raise refused  # after the commit, so the gap record is kept

    def _unacked(self, c: sqlite3.Connection) -> int:
        return int(c.execute("SELECT value FROM meta WHERE key = 'unacked'").fetchone()[0])

    def _evict(
        self, c: sqlite3.Connection, incoming: list[tuple[int, Reading]], overflow: int
    ) -> list[tuple[int, Reading]]:
        """Drop the ``overflow`` oldest pending readings, then incoming ones if short.

        Inflight rows are never evicted: the sink may already have them, and
        counting them as lost would make the gap count wrong.
        """
        row = c.execute(
            "SELECT id FROM readings WHERE state = ? ORDER BY id LIMIT 1 OFFSET ?",
            (PENDING, overflow - 1),
        ).fetchone()
        last = (
            row[0]
            if row is not None
            else c.execute("SELECT max(id) FROM readings WHERE state = ?", (PENDING,)).fetchone()[0]
        )
        evicted = 0
        if last is not None:
            groups = c.execute(
                "SELECT sensor_id, boot_id, min(mono_ns), max(mono_ns), count(*) FROM readings "
                "WHERE state = ? AND id <= ? GROUP BY sensor_id, boot_id ORDER BY min(id)",
                (PENDING, last),
            ).fetchall()
            _record_gaps(
                c, (GapRecord(s, b, lo, hi, REASON_DROP_OLDEST, n) for s, b, lo, hi, n in groups)
            )
            evicted = c.execute(
                "DELETE FROM readings WHERE state = ? AND id <= ?", (PENDING, last)
            ).rowcount
            _add_unacked(c, -evicted)
        short = overflow - evicted
        if short > 0:
            _record_gaps(c, summarize((r for _, r in incoming[:short]), REASON_DROP_OLDEST))
            incoming = incoming[short:]
        return incoming

    def _note_pressure(self, over_cap: bool) -> None:
        # Logged on transitions only; per-write logging would swamp journald.
        if over_cap and not self._over_cap:
            policy = self._retention.policy if self._retention is not None else None
            log.error("buffer reached its cap; %s is discarding readings", policy)
        elif not over_cap and self._over_cap:
            log.warning("buffer is below its cap again")
        self._over_cap = over_cap

    # --- shipping readings ---------------------------------------------------

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
        with self._write() as c:
            n = self._transition(c, "readings", ids, INFLIGHT, ACKED)
            _add_unacked(c, -n)
            return n

    def release(self, ids: Iterable[int]) -> int:
        """Return inflight rows to pending, e.g. after a failed send."""
        with self._write() as c:
            return self._transition(c, "readings", ids, INFLIGHT, PENDING)

    @staticmethod
    def _transition(
        c: sqlite3.Connection, table: str, ids: Iterable[int], src: int, dst: int
    ) -> int:
        cur = c.executemany(
            f"UPDATE {table} SET state = ? WHERE id = ? AND state = ?",
            ((dst, i, src) for i in ids),
        )
        return cur.rowcount

    # --- shipping gaps -------------------------------------------------------

    def claim_gaps(self, limit: int) -> list[StoredGap]:
        """Seal (assign a seq to) up to ``limit`` open gaps, then claim pending sealed ones."""
        unsealed = int(
            self._conn.execute("SELECT count(*) FROM gaps WHERE seq IS NULL").fetchone()[0]
        )
        seqs = self._take_seqs(min(unsealed, limit)) if unsealed else []
        with self._write() as c:
            ids = [
                row[0]
                for row in c.execute(
                    "SELECT id FROM gaps WHERE seq IS NULL ORDER BY id LIMIT ?", (len(seqs),)
                )
            ]
            c.executemany("UPDATE gaps SET seq = ? WHERE id = ?", zip(seqs, ids, strict=False))
            rows = c.execute(
                f"SELECT id, seq, {_GAP_COLUMNS} FROM gaps "
                "WHERE state = ? AND seq IS NOT NULL ORDER BY id LIMIT ?",
                (PENDING, limit),
            ).fetchall()
            c.executemany(
                "UPDATE gaps SET state = ? WHERE id = ?", ((INFLIGHT, row[0]) for row in rows)
            )
        return [StoredGap(row[0], row[1], GapRecord(*row[2:])) for row in rows]

    def ack_gaps(self, ids: Iterable[int]) -> int:
        with self._write() as c:
            return self._transition(c, "gaps", ids, INFLIGHT, ACKED)

    def release_gaps(self, ids: Iterable[int]) -> int:
        with self._write() as c:
            return self._transition(c, "gaps", ids, INFLIGHT, PENDING)

    # --- maintenance ---------------------------------------------------------

    def recover_inflight(self) -> int:
        """Reset inflight rows to pending. Call once at process start, before any shipper runs."""
        with self._write() as c:
            n = c.execute(
                "UPDATE readings SET state = ? WHERE state = ?", (PENDING, INFLIGHT)
            ).rowcount
            c.execute("UPDATE gaps SET state = ? WHERE state = ?", (PENDING, INFLIGHT))
            return n

    def purge_acked(self, limit: int = 10_000) -> int:
        """Delete up to ``limit`` of the oldest acked readings, and all acked gaps.

        Deliberately no VACUUM: on an SD card it rewrites the whole file. Freed
        pages are reused, so the file size plateaus instead of shrinking.
        """
        with self._write() as c:
            c.execute("DELETE FROM gaps WHERE state = ?", (ACKED,))
            return c.execute(
                "DELETE FROM readings WHERE id IN "
                "(SELECT id FROM readings WHERE state = ? ORDER BY id LIMIT ?)",
                (ACKED, limit),
            ).rowcount

    def counts(self) -> dict[int, int]:
        rows = self._conn.execute("SELECT state, count(*) FROM readings GROUP BY state")
        found = {int(state): int(n) for state, n in rows}
        return {state: found.get(state, 0) for state in (PENDING, INFLIGHT, ACKED)}

    def unacked(self) -> int:
        return self._unacked(self._conn)

    def unshipped_gaps(self) -> int:
        row = self._conn.execute("SELECT count(*) FROM gaps WHERE state != ?", (ACKED,))
        return int(row.fetchone()[0])

    def gaps(self) -> list[GapRecord]:
        """Every gap record still in the buffer, shipped or not, oldest first."""
        rows = self._conn.execute(f"SELECT {_GAP_COLUMNS} FROM gaps ORDER BY id")
        return [GapRecord(*row) for row in rows]

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
        if not self._pending:
            return
        try:
            self._buffer.append(self._pending)
        except BufferFull:
            # Already recorded as a gap; retrying would store it and double-count.
            self._pending = []
            raise
        # Any other error keeps the batch, and the next flush retries it.
        self._pending = []
