from __future__ import annotations

import json
import threading
import time
import uuid
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

pytest.importorskip("psycopg")
pytest.importorskip("paho.mqtt")

import psycopg

from spoolpi.consumer.mqtt import MqttConsumer
from spoolpi.consumer.postgres import Incoming, apply_schema, store
from spoolpi.core.buffer import Buffer
from spoolpi.core.reading import TS_CORRECTED, Reading
from spoolpi.core.retention import GapRecord
from spoolpi.core.shipper import Shipper
from spoolpi.sinks.base import Envelope, to_wire
from spoolpi.sinks.mqtt import MqttSink
from tests.harness.broker import Broker, find_mosquitto
from tests.harness.postgres import Postgres, find_postgres
from tests.shipping import FAST_BACKOFF, drain, reading

pytestmark = pytest.mark.skipif(find_postgres() is None, reason="postgres not available")
needs_broker = pytest.mark.skipif(find_mosquitto() is None, reason="mosquitto not available")

BUF = str(uuid.uuid4())
WALL = 1_790_000_000 * 10**9  # 2026-09-21


@pytest.fixture(scope="session")
def server(tmp_path_factory: pytest.TempPathFactory) -> Iterator[Postgres]:
    with Postgres(tmp_path_factory.mktemp("pg")) as pg:
        yield pg


@pytest.fixture
def dsn(server: Postgres) -> Iterator[str]:
    name = f"t_{uuid.uuid4().hex[:12]}"
    with psycopg.connect(server.dsn, autocommit=True) as admin:
        admin.execute(f"CREATE DATABASE {name}")
    url = server.dsn.rsplit("/", 1)[0] + f"/{name}"
    with psycopg.connect(url) as conn:
        apply_schema(conn)
    yield url


def message(n: int, **overrides: Any) -> Incoming:
    reading_ = Reading("t1", float(n), "C", n, WALL + n, "boot", ts_quality=TS_CORRECTED)
    record = {"device_id": "dev1"} | to_wire(Envelope(BUF, n, reading_)) | overrides
    return Incoming("spoolpi/dev1/reading/t1", json.dumps(record).encode())


def rows(dsn: str, sql: str) -> list[tuple[Any, ...]]:
    with psycopg.connect(dsn) as conn:
        return conn.execute(sql).fetchall()


# --- schema and store ------------------------------------------------------------------


def test_schema_is_idempotent(dsn: str) -> None:
    with psycopg.connect(dsn) as conn:
        apply_schema(conn)
        apply_schema(conn)


def test_store_inserts_once_and_ignores_resends(dsn: str) -> None:
    batch = [message(i) for i in range(5)]
    with psycopg.connect(dsn) as conn:
        first = store(conn, batch)
        again = store(conn, batch + [message(5)])
    assert (first.readings, first.duplicates) == (5, 0)
    assert (again.readings, again.duplicates) == (1, 5)
    [(count, year, quality, tests)] = rows(
        dsn,
        "SELECT count(*), max(extract(year FROM ts)), max(ts_quality), max(qc_tests::text) "
        "FROM spoolpi_readings",
    )
    assert (count, int(year), quality, tests) == (6, 2026, TS_CORRECTED, "{}")


# --- correcting readings shipped before the device's clock synced ------------------------


def unsynced(n: int, **overrides: Any) -> Incoming:
    # Sampled before sync: the device's wall clock said 1970 + a bit.
    return message(n, ts_quality=0, wall_ns=n, **overrides)


def test_unsynced_readings_are_corrected_when_a_trusted_one_arrives_later(dsn: str) -> None:
    with psycopg.connect(dsn) as conn:
        first = store(conn, [unsynced(i) for i in range(5)])
        assert first.corrected == 0  # no trusted reading of that boot yet
        second = store(conn, [message(10)])  # mono 10, wall WALL + 10: offset WALL
    assert second.corrected == 5
    got = rows(
        dsn,
        "SELECT seq, wall_ns, ts_quality, wall_ns_device, extract(year FROM ts)::int "
        "FROM spoolpi_readings WHERE seq < 5 ORDER BY seq",
    )
    assert got == [(i, WALL + i, 1, i, 2026) for i in range(5)]


def test_unsynced_readings_arriving_after_the_offset_are_corrected_on_arrival(dsn: str) -> None:
    with psycopg.connect(dsn) as conn:
        store(conn, [message(10)])
        result = store(conn, [unsynced(i) for i in range(3)])
    assert result.corrected == 3
    assert rows(dsn, "SELECT count(*) FROM spoolpi_readings WHERE ts_quality = 0") == [(0,)]


def test_unsynced_and_trusted_in_one_batch(dsn: str) -> None:
    with psycopg.connect(dsn) as conn:
        result = store(conn, [unsynced(0), unsynced(1), message(2)])
    assert result.corrected == 2
    assert rows(dsn, "SELECT wall_ns FROM spoolpi_readings ORDER BY seq") == [
        (WALL,),
        (WALL + 1,),
        (WALL + 2,),
    ]


def test_other_boots_are_not_touched(dsn: str) -> None:
    with psycopg.connect(dsn) as conn:
        store(conn, [unsynced(0, boot_id="other-boot"), message(1)])
    assert rows(dsn, "SELECT ts_quality, wall_ns_device FROM spoolpi_readings WHERE seq = 0") == [
        (0, None)
    ]


def test_the_earliest_trusted_reading_sets_the_offset(dsn: str) -> None:
    with psycopg.connect(dsn) as conn:
        store(conn, [message(50, wall_ns=WALL + 50 + 7)])  # offset WALL + 7
        store(conn, [message(10)])  # earlier in the boot: offset WALL, replaces it
        store(conn, [message(90, wall_ns=WALL + 90 + 3)])  # later: kept out
    assert rows(dsn, "SELECT boot_id, offset_ns, mono_ns FROM spoolpi_boot_clocks") == [
        ("boot", WALL, 10)
    ]


def test_an_absurd_timestamp_is_not_used_and_does_not_wedge_the_batch(dsn: str) -> None:
    with psycopg.connect(dsn) as conn:
        result = store(conn, [unsynced(0), message(1, wall_ns=2**63 - 1)])
    assert (result.readings, result.corrected) == (2, 0)
    assert rows(dsn, "SELECT count(*) FROM spoolpi_boot_clocks") == [(0,)]


def test_gap_times_convert_through_the_boot_offset(dsn: str) -> None:
    # The query docs/guarantees.md gives for a gap's wall time.
    gap = GapRecord("t1", "boot", 3, 8, "backpressure", 4)
    gap_msg = Incoming(
        "spoolpi/dev1/gap/t1",
        json.dumps({"device_id": "dev1"} | to_wire(Envelope(BUF, 100, gap))).encode(),
    )
    with psycopg.connect(dsn) as conn:
        store(conn, [gap_msg, message(10)])
    [(start, end)] = rows(
        dsn,
        "SELECT g.from_mono_ns + c.offset_ns, g.to_mono_ns + c.offset_ns "
        "FROM spoolpi_gaps g JOIN spoolpi_boot_clocks c USING (boot_id)",
    )
    assert (start, end) == (WALL + 3, WALL + 8)


def test_failed_reads_and_gaps_are_stored(dsn: str) -> None:
    gap = GapRecord(None, "boot", 1, 9, "backpressure", 4)
    gap_msg = Incoming(
        "spoolpi/dev1/gap/_all",
        json.dumps({"device_id": "dev1"} | to_wire(Envelope(BUF, 100, gap))).encode(),
    )
    with psycopg.connect(dsn) as conn:
        result = store(conn, [message(1, value=None), gap_msg])
    assert (result.readings, result.gaps) == (1, 1)
    assert rows(dsn, "SELECT value FROM spoolpi_readings") == [(None,)]
    assert rows(dsn, "SELECT sensor_id, reason, count FROM spoolpi_gaps") == [
        (None, "backpressure", 4)
    ]


@pytest.mark.parametrize(
    ("payload", "error"),
    [
        (b"not json", "Expecting value"),
        (b"[1, 2]", "not a JSON object"),
        (json.dumps({"type": "reading"}).encode(), "buffer_id must be a UUID"),
        (message(1, buffer_id="nope").payload, "buffer_id must be a UUID"),
        (message(1, seq=-1).payload, "seq must not be negative"),
        (message(1, seq=2**63).payload, "64-bit integer"),
        (message(1, sensor_id="a\u0000b").payload, "NUL character"),
        (message(1, ts_quality=7).payload, "ts_quality"),
        (message(1, value="hot").payload, "value must be a number"),
        (message(1, type="event").payload, "unknown record type"),
        (message(1, type="gap", count=0).payload, "count must be positive"),
    ],
)
def test_bad_messages_are_dead_lettered_without_blocking_the_batch(
    dsn: str, payload: bytes, error: str
) -> None:
    with psycopg.connect(dsn) as conn:
        result = store(conn, [message(10), Incoming("spoolpi/x", payload), message(11)])
    assert (result.readings, result.dead_letters) == (2, 1)
    [(source, stored, reason)] = rows(
        dsn, "SELECT source, payload, error FROM spoolpi_dead_letters"
    )
    assert source == "spoolpi/x" and bytes(stored) == payload
    assert error in reason


# --- end to end: device -> MQTT -> consumer -> Postgres -----------------------------------


def start_consumer(
    broker: Broker, dsn: str, stop: threading.Event
) -> tuple[MqttConsumer, threading.Thread]:
    consumer = MqttConsumer(host="127.0.0.1", port=broker.port, dsn=dsn, client_id="it-consumer")
    t = threading.Thread(target=consumer.run, args=(stop,), daemon=True)
    t.start()
    return consumer, t


def wait_for_rows(dsn: str, n: int, timeout_s: float = 60) -> None:
    deadline = time.monotonic() + timeout_s
    while True:
        try:
            [(count,)] = rows(dsn, "SELECT count(*) FROM spoolpi_readings")
            if count >= n:
                return
        except psycopg.OperationalError:
            pass
        assert time.monotonic() < deadline, f"only {count} of {n} rows arrived"
        time.sleep(0.1)


def publish_from_device(tmp_path: Path, broker: Broker, n: int) -> Shipper:
    db = tmp_path / "device.db"
    with Buffer(db) as b:
        b.append([reading(i) for i in range(n)])
    sink = MqttSink(host="127.0.0.1", port=broker.port, device_id="dev1")
    shipper = Shipper(db, sink, batch_size=50, backoff=FAST_BACKOFF, poll_interval_s=0.02)
    shipper.start()
    return shipper


def assert_exactly_once(dsn: str, n: int) -> None:
    assert rows(dsn, "SELECT count(*), count(DISTINCT value) FROM spoolpi_readings") == [(n, n)]
    assert rows(dsn, "SELECT min(value), max(value) FROM spoolpi_readings") == [(0.0, float(n - 1))]


@needs_broker
def test_end_to_end_with_a_consumer_restart(tmp_path: Path, dsn: str) -> None:
    n = 400
    with Broker(tmp_path) as broker:
        stop = threading.Event()
        _, t = start_consumer(broker, dsn, stop)
        time.sleep(0.5)  # the persistent session exists from here on
        shipper = publish_from_device(tmp_path, broker, n)
        try:
            wait_for_rows(dsn, 50)
            stop.set()  # consumer goes away mid-stream; the broker keeps its session
            t.join(10)
            drain(tmp_path / "device.db")  # the device finishes shipping meanwhile
            stop = threading.Event()
            _, t = start_consumer(broker, dsn, stop)
            wait_for_rows(dsn, n)
        finally:
            shipper.stop(timeout_s=5)
            stop.set()
            t.join(10)
    assert_exactly_once(dsn, n)


@needs_broker
def test_end_to_end_through_a_database_outage(tmp_path: Path, dsn: str, server: Postgres) -> None:
    n = 300
    with Broker(tmp_path) as broker:
        stop = threading.Event()
        consumer, t = start_consumer(broker, dsn, stop)
        time.sleep(0.5)
        shipper = publish_from_device(tmp_path, broker, n)
        try:
            wait_for_rows(dsn, 30)
            server.stop()  # nothing can be stored, so nothing gets acked
            time.sleep(1.5)
            server.start()
            wait_for_rows(dsn, n)
        finally:
            shipper.stop(timeout_s=5)
            stop.set()
            t.join(10)
    assert_exactly_once(dsn, n)
    assert consumer.totals.dead_letters == 0
