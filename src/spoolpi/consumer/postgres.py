"""Store SpoolPi records in Postgres, deduplicating on (buffer_id, seq).

Needs the ``consumer`` extra (psycopg >= 3.1).
"""

from __future__ import annotations

import json
import logging
import uuid
from collections.abc import Iterable
from dataclasses import dataclass
from importlib.resources import files
from typing import Any, Final

import psycopg

log = logging.getLogger("spoolpi.consumer")

SCHEMA_SQL: Final = files("spoolpi.consumer").joinpath("schema.sql").read_text()

_INT64: Final = range(-(2**63), 2**63)

_INSERT_READING: Final = """
    INSERT INTO spoolpi_readings (buffer_id, seq, device_id, sensor_id, value, unit, mono_ns,
                                wall_ns, boot_id, ts_quality, qc_flag, qc_tests)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (buffer_id, seq) DO NOTHING
"""
_INSERT_GAP: Final = """
    INSERT INTO spoolpi_gaps (buffer_id, seq, device_id, sensor_id, boot_id, from_mono_ns,
                            to_mono_ns, reason, count)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (buffer_id, seq) DO NOTHING
"""
_INSERT_DEAD: Final = (
    "INSERT INTO spoolpi_dead_letters (source, payload, error) VALUES (%s, %s, %s)"
)
# Keep the offset from the earliest trusted reading of a boot: it's the closest to
# the pre-sync readings it corrects.
_UPSERT_BOOT_CLOCK: Final = """
    INSERT INTO spoolpi_boot_clocks (boot_id, offset_ns, mono_ns) VALUES (%s, %s, %s)
    ON CONFLICT (boot_id) DO UPDATE
        SET offset_ns = excluded.offset_ns, mono_ns = excluded.mono_ns, updated_at = now()
        WHERE excluded.mono_ns < spoolpi_boot_clocks.mono_ns
"""
_CORRECT_UNSYNCED: Final = """
    UPDATE spoolpi_readings r
       SET wall_ns_device = r.wall_ns, wall_ns = r.mono_ns + c.offset_ns, ts_quality = 1
      FROM spoolpi_boot_clocks c
     WHERE r.ts_quality = 0 AND r.boot_id = c.boot_id AND r.boot_id = ANY(%s)
       AND r.mono_ns >= 0 AND r.mono_ns < %s
"""
_TS_UNSYNCED: Final = 0
# Offsets and mono_ns used for correction stay below 2**62, so mono_ns + offset_ns can't
# overflow bigint. A garbage record could otherwise fail the batch on every retry.
# Real values are far smaller: mono_ns ~ uptime, wall_ns ~ 1.8e18 in 2027.
_SANE: Final = range(2**62)

# Positions in a reading's insert parameters (see _row).
_MONO, _WALL, _BOOT, _QUALITY = 6, 7, 8, 9


class BadRecord(ValueError):
    pass


@dataclass(frozen=True, slots=True)
class Incoming:
    source: str  # where it came from, e.g. the MQTT topic
    payload: bytes


@dataclass(slots=True)
class StoreResult:
    readings: int = 0
    gaps: int = 0
    duplicates: int = 0
    dead_letters: int = 0
    corrected: int = 0  # earlier or incoming ts_quality 0 readings given their boot's offset


def apply_schema(conn: psycopg.Connection[Any]) -> None:
    with conn.transaction():
        conn.execute(SCHEMA_SQL.encode())


def _text(record: dict[str, Any], key: str, *, nullable: bool = False) -> str | None:
    v = record.get(key)
    if v is None and nullable:
        return None
    if not isinstance(v, str) or not v:
        raise BadRecord(f"{key} must be a non-empty string")
    if "\0" in v:
        raise BadRecord(f"{key} contains a NUL character, which Postgres text can't store")
    return v


def _int(record: dict[str, Any], key: str) -> int:
    v = record.get(key)
    if isinstance(v, bool) or not isinstance(v, int) or v not in _INT64:
        raise BadRecord(f"{key} must be a 64-bit integer")
    return v


def _row(record: Any) -> tuple[str, tuple[Any, ...]]:
    """(table kind, insert parameters) for one wire record, or BadRecord."""
    if not isinstance(record, dict):
        raise BadRecord("record is not a JSON object")
    try:
        buffer_id = str(uuid.UUID(str(record.get("buffer_id"))))
    except ValueError as e:
        raise BadRecord("buffer_id must be a UUID") from e
    seq = _int(record, "seq")
    if seq < 0:
        raise BadRecord("seq must not be negative")
    head = (buffer_id, seq, _text(record, "device_id"))
    kind = record.get("type")
    if kind == "reading":
        value = record.get("value")
        if value is not None and (isinstance(value, bool) or not isinstance(value, int | float)):
            raise BadRecord("value must be a number or null")
        tests = record.get("qc_tests", [])
        if not isinstance(tests, list) or not all(
            isinstance(t, str) and "\0" not in t for t in tests
        ):
            raise BadRecord("qc_tests must be a list of strings")
        quality = _int(record, "ts_quality")
        if quality not in (0, 1, 2):
            raise BadRecord("ts_quality must be 0, 1 or 2")
        return "reading", (
            *head,
            _text(record, "sensor_id"),
            None if value is None else float(value),
            _text(record, "unit", nullable=True),
            _int(record, "mono_ns"),
            _int(record, "wall_ns"),
            _text(record, "boot_id"),
            quality,
            _int(record, "qc_flag"),
            tests,
        )
    if kind == "gap":
        count = _int(record, "count")
        if count <= 0:
            raise BadRecord("count must be positive")
        return "gap", (
            *head,
            _text(record, "sensor_id", nullable=True),
            _text(record, "boot_id"),
            _int(record, "from_mono_ns"),
            _int(record, "to_mono_ns"),
            _text(record, "reason"),
            count,
        )
    raise BadRecord(f"unknown record type {kind!r}")


def store(conn: psycopg.Connection[Any], messages: Iterable[Incoming]) -> StoreResult:
    """Store a batch in one transaction. Duplicates are ignored, bad messages dead-lettered.

    Readings shipped before the device's clock synced (ts_quality 0) are corrected
    in the same transaction once any trusted reading of their boot has arrived,
    in this batch or an earlier one.

    Connection-level errors (psycopg.OperationalError) propagate: nothing is
    committed, and the caller should retry the whole batch.
    """
    result = StoreResult()
    offsets: dict[str, tuple[int, int]] = {}  # boot_id -> (mono_ns, offset_ns), earliest
    boots: set[str] = set()  # boots that got new readings, trusted or not
    with conn.transaction(), conn.cursor() as cur:
        for message in messages:
            try:
                kind, params = _row(json.loads(message.payload))
            except (ValueError, BadRecord) as e:  # JSONDecodeError is a ValueError
                cur.execute(_INSERT_DEAD, (message.source, message.payload, str(e)))
                result.dead_letters += 1
                continue
            try:
                # A savepoint per row: an error we didn't anticipate costs this
                # row, not the batch, so the consumer can never wedge on it.
                with conn.transaction():
                    cur.execute(_INSERT_READING if kind == "reading" else _INSERT_GAP, params)
            except (psycopg.DataError, psycopg.IntegrityError) as e:
                cur.execute(_INSERT_DEAD, (message.source, message.payload, str(e).strip()))
                result.dead_letters += 1
                continue
            if cur.rowcount == 0:
                result.duplicates += 1
            elif kind == "reading":
                result.readings += 1
                boot = params[_BOOT]
                boots.add(boot)
                mono, wall = params[_MONO], params[_WALL]
                trusted = params[_QUALITY] != _TS_UNSYNCED and mono in _SANE and wall in _SANE
                if trusted and (boot not in offsets or mono < offsets[boot][0]):
                    offsets[boot] = (mono, wall - mono)
            else:
                result.gaps += 1
        if boots:
            try:
                # Its own savepoint: a correction problem must never cost the batch.
                with conn.transaction():
                    for boot, (mono, offset) in offsets.items():
                        cur.execute(_UPSERT_BOOT_CLOCK, (boot, offset, mono))
                    cur.execute(_CORRECT_UNSYNCED, (sorted(boots), _SANE.stop))
                    result.corrected = cur.rowcount
            except (psycopg.DataError, psycopg.IntegrityError) as e:
                log.warning("couldn't correct unsynced timestamps in this batch: %s", e)
    return result
