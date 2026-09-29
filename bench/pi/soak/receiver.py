"""Seven-day soak receiver: the reference consumer's MQTT side, storing into SQLite.

Reuses spoolpi.consumer.mqtt.MqttConsumer (persistent session, PUBACK only
after the batch is committed, stale acks dropped) and replaces only the storage:
a SQLite file with UNIQUE (buffer_id, seq), so resends are counted, not stored.

Usage: python receiver.py <db>
"""

from __future__ import annotations

import json
import logging
import signal
import sqlite3
import sys
import threading
from typing import Any

from spoolpi.consumer.mqtt import MqttConsumer

SCHEMA = """
CREATE TABLE IF NOT EXISTS records (
    buffer_id  TEXT    NOT NULL,
    seq        INTEGER NOT NULL,
    type       TEXT    NOT NULL,
    value      REAL,
    count      INTEGER,          -- gap records
    reason     TEXT,             -- gap records
    ts_quality INTEGER,
    wall_ns    INTEGER,
    mono_ns    INTEGER,
    boot_id    TEXT,
    received   REAL    NOT NULL DEFAULT (unixepoch('subsec')),
    PRIMARY KEY (buffer_id, seq)
) STRICT;
CREATE TABLE IF NOT EXISTS stats (key TEXT PRIMARY KEY, value INTEGER NOT NULL) STRICT;
INSERT OR IGNORE INTO stats VALUES ('duplicates', 0), ('bad', 0);
"""


class SqliteReceiver(MqttConsumer):
    def __init__(self, db: str) -> None:
        super().__init__(host="127.0.0.1", dsn="unused", client_id="spoolpi-soak-receiver")
        self.conn = sqlite3.connect(db, isolation_level=None)
        self.conn.execute("PRAGMA journal_mode = WAL")
        self.conn.execute("PRAGMA synchronous = FULL")  # acked means on disk
        self.conn.executescript(SCHEMA)

    def _store(self, conn: Any, batch: list[Any], stop: threading.Event) -> Any:
        dup = bad = 0
        c = self.conn
        c.execute("BEGIN IMMEDIATE")
        for m in batch:
            try:
                r = json.loads(m.incoming.payload)
                row = (
                    r["buffer_id"],
                    r["seq"],
                    r["type"],
                    r.get("value"),
                    r.get("count"),
                    r.get("reason"),
                    r.get("ts_quality"),
                    r.get("wall_ns"),
                    r.get("mono_ns"),
                    r.get("boot_id"),
                )
            except (ValueError, KeyError, TypeError):
                bad += 1
                continue
            cur = c.execute(
                "INSERT OR IGNORE INTO records (buffer_id, seq, type, value, count, reason, "
                "ts_quality, wall_ns, mono_ns, boot_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                row,
            )
            dup += cur.rowcount == 0
        c.execute("UPDATE stats SET value = value + ? WHERE key = 'duplicates'", (dup,))
        c.execute("UPDATE stats SET value = value + ? WHERE key = 'bad'", (bad,))
        c.execute("COMMIT")
        with self._lock:
            current = self._generation
        for m in batch:
            if m.generation == current and m.qos > 0:
                self._client.ack(m.mid, m.qos)
        return conn


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    receiver = SqliteReceiver(sys.argv[1])
    stop = threading.Event()
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, lambda *_: stop.set())
    receiver.run(stop)


if __name__ == "__main__":
    main()
