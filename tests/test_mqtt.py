from __future__ import annotations

import json
import subprocess
import sys
import threading
import time
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

pytest.importorskip("paho.mqtt")

import paho.mqtt.client as mqtt
from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.reasoncodes import ReasonCode

from spool.core.buffer import REJECTED, BatchWriter, Buffer
from spool.core.reading import Reading
from spool.core.retention import REASON_REJECTED, GapRecord
from spool.core.shipper import Shipper
from spool.sinks.base import AckSet, Envelope, SinkError
from spool.sinks.mqtt import MqttSink, topic_part
from tests.harness.broker import Broker, find_mosquitto
from tests.shipping import FAST_BACKOFF, drain, reading

NEVER = -1  # scripted: no PUBACK ever arrives


# --- a scripted stand-in for paho's Client ------------------------------------------


class FakeInfo:
    def __init__(self, rc: int, mid: int) -> None:
        self.rc, self.mid = rc, mid


class FakeClient:
    """Enough of paho.mqtt.client.Client for MqttSink, with scripted PUBACKs."""

    def __init__(self, *, connect: bool = True, codes: list[int] | None = None, **kw: Any) -> None:
        self.kw = kw
        self.connect_now = connect
        self.codes = list(codes or [])
        self.published: list[tuple[str, bytes, Any]] = []
        self.pending: dict[int, int] = {}  # held PUBACKs, released by ack_late()
        self.next_mid = 1
        self.fail_publish_after: int | None = None
        self.on_connect: Any = None
        self.on_disconnect: Any = None
        self.on_publish: Any = None
        self.connect_args: tuple[Any, ...] = ()
        self.connect_kwargs: dict[str, Any] = {}

    def reconnect_delay_set(self, **_kw: Any) -> None: ...
    def tls_set(self, **_kw: Any) -> None: ...
    def username_pw_set(self, *_a: Any) -> None: ...

    def connect_async(self, *args: Any, **kwargs: Any) -> None:
        self.connect_args, self.connect_kwargs = args, kwargs

    def loop_start(self) -> None:
        if self.connect_now:
            self.fire_connect()

    def fire_connect(self) -> None:
        self.on_connect(self, None, None, ReasonCode(PacketTypes.CONNACK, identifier=0), None)

    def fire_disconnect(self) -> None:
        self.on_disconnect(self, None, None, ReasonCode(PacketTypes.DISCONNECT, identifier=0), None)

    def publish(self, topic: str, payload: bytes, qos: int, properties: Any = None) -> FakeInfo:
        if self.fail_publish_after is not None and len(self.published) >= self.fail_publish_after:
            return FakeInfo(mqtt.MQTT_ERR_NO_CONN, 0)
        self.published.append((topic, payload, properties))
        mid = self.next_mid
        self.next_mid += 1
        code = self.codes.pop(0) if self.codes else 0
        if code == NEVER:
            self.pending[mid] = 0
        else:
            self.on_publish(self, None, mid, ReasonCode(PacketTypes.PUBACK, identifier=code), None)
        return FakeInfo(mqtt.MQTT_ERR_SUCCESS, mid)

    def ack_late(self, mid: int, code: int = 0) -> None:
        self.pending.pop(mid, None)
        self.on_publish(self, None, mid, ReasonCode(PacketTypes.PUBACK, identifier=code), None)

    def disconnect(self) -> None: ...
    def loop_stop(self) -> None: ...


def make_sink(client: FakeClient, **kw: Any) -> MqttSink:
    opts: dict[str, Any] = {"host": "broker", "device_id": "dev/1", "ack_timeout_s": 0.2}
    opts.update(kw)

    def factory(**client_kw: Any) -> FakeClient:
        client.kw = client_kw
        return client

    return MqttSink(client_factory=factory, **opts)


def batch(*seqs: int, sensor: str = "t1") -> list[Envelope]:
    return [Envelope("buf", s, Reading(sensor, float(s), "C", s, s, "boot")) for s in seqs]


# --- unit ------------------------------------------------------------------------------


def test_all_acked_with_escaped_topics_and_json_payload() -> None:
    client = FakeClient()
    sink = make_sink(client)
    assert sink.send(batch(1, 2, sensor="rack/3+#")) == AckSet.of([1, 2])
    topic, payload, props = client.published[0]
    assert topic == "spool/dev%2F1/reading/rack%2F3%2B%23"
    record = json.loads(payload)
    assert record["device_id"] == "dev/1" and record["seq"] == 1 and record["value"] == 1.0
    assert record["type"] == "reading" and record["buffer_id"] == "buf"
    assert props.ContentType == "application/json" and props.PayloadFormatIndicator == 1
    assert client.kw["client_id"] == "spool-dev/1" and client.kw["protocol"] == mqtt.MQTTv5
    assert client.connect_kwargs == {"clean_start": True}


def test_gap_records_go_to_the_gap_topic() -> None:
    client = FakeClient()
    sink = make_sink(client)
    gap = Envelope("buf", 9, GapRecord(None, "boot", 1, 2, "backpressure", 5))
    sink.send([gap])
    assert client.published[0][0] == "spool/dev%2F1/gap/_all"


def test_reason_codes_map_to_accept_reject_retry() -> None:
    # 0x10 no subscribers (fine), 0x99/0x87/0x90 permanent, 0x97 quota and 0x80 retry.
    client = FakeClient(codes=[0x00, 0x10, 0x99, 0x87, 0x90, 0x97, 0x80])
    sink = make_sink(client)
    assert sink.send(batch(1, 2, 3, 4, 5, 6, 7)) == AckSet.of([1, 2], rejected=[3, 4, 5])


def test_unacked_messages_are_retried_and_late_pubacks_ignored() -> None:
    client = FakeClient(codes=[0, NEVER, 0])
    sink = make_sink(client)
    assert sink.send(batch(1, 2, 3)) == AckSet.of([1, 3])
    # paho reuses mids: a late PUBACK for mid 2 must not vouch for a new mid 2.
    client.ack_late(2)
    client.next_mid = 2
    client.codes = [NEVER]
    assert sink.send(batch(4)) == AckSet.none()


def test_publish_failure_mid_batch_leaves_the_rest_for_retry() -> None:
    client = FakeClient()
    client.fail_publish_after = 2
    sink = make_sink(client)
    assert sink.send(batch(1, 2, 3, 4)) == AckSet.of([1, 2])


def test_not_connected_raises_after_the_connect_timeout() -> None:
    sink = make_sink(FakeClient(connect=False), connect_timeout_s=0.1)
    t0 = time.monotonic()
    with pytest.raises(SinkError, match="not connected"):
        sink.send(batch(1))
    assert time.monotonic() - t0 < 1


def test_publishing_stops_while_disconnected_and_resumes_after() -> None:
    client = FakeClient()
    sink = make_sink(client, connect_timeout_s=0.1)
    client.fire_disconnect()
    with pytest.raises(SinkError):
        sink.send(batch(1))
    assert client.published == []  # nothing left queued in paho's memory
    client.fire_connect()
    assert sink.send(batch(1)) == AckSet.of([1])


def test_close_unblocks_a_send_waiting_for_pubacks() -> None:
    client = FakeClient(codes=[NEVER])
    sink = make_sink(client, ack_timeout_s=30)
    results: list[AckSet] = []
    t = threading.Thread(target=lambda: results.append(sink.send(batch(1))))
    t.start()
    time.sleep(0.1)
    sink.close()
    t.join(2)
    assert results == [AckSet.none()]


def test_mqtt_311_sends_no_v5_properties() -> None:
    client = FakeClient()
    sink = make_sink(client, protocol="3.1.1")
    sink.send(batch(1))
    assert client.published[0][2] is None
    assert client.kw["protocol"] == mqtt.MQTTv311 and client.connect_kwargs == {}


def test_topic_part_escapes_levels_and_wildcards() -> None:
    assert topic_part("a/b+c#d%e\0") == "a%2Fb%2Bc%23d%25e%00"


# --- integration with a real broker ------------------------------------------------------

needs_broker = pytest.mark.skipif(find_mosquitto() is None, reason="mosquitto not available")


class Subscriber:
    def __init__(self, port: int) -> None:
        self.records: list[dict[str, Any]] = []
        self._lock = threading.Lock()
        ready = threading.Event()
        self.client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
        self.client.on_connect = lambda c, *_: c.subscribe("spool/#", qos=1)
        self.client.on_subscribe = lambda *_: ready.set()
        self.client.on_message = self._on_message
        self.client.connect("127.0.0.1", port)
        self.client.loop_start()
        assert ready.wait(5), "subscriber didn't subscribe"

    def _on_message(self, _c: Any, _u: Any, msg: Any) -> None:
        with self._lock:
            self.records.append(json.loads(msg.payload))

    def readings(self) -> dict[int, float]:
        with self._lock:
            return {r["seq"]: r["value"] for r in self.records if r["type"] == "reading"}

    def gaps(self) -> list[dict[str, Any]]:
        with self._lock:
            return [r for r in self.records if r["type"] == "gap"]

    def close(self) -> None:
        self.client.disconnect()
        self.client.loop_stop()


@pytest.fixture
def broker(tmp_path: Path) -> Iterator[Broker]:
    with Broker(tmp_path) as b:
        yield b


def wait_until(predicate: Any, timeout_s: float = 20) -> None:
    deadline = time.monotonic() + timeout_s
    while not predicate():
        assert time.monotonic() < deadline, "timed out"
        time.sleep(0.05)


def run_shipper(db: Path, port: int, **sink_kw: Any) -> Shipper:
    sink = MqttSink(host="127.0.0.1", port=port, device_id="dev1", **sink_kw)
    s = Shipper(db, sink, batch_size=50, backoff=FAST_BACKOFF, poll_interval_s=0.02)
    s.start()
    return s


@needs_broker
def test_end_to_end_through_a_real_broker(tmp_path: Path, broker: Broker) -> None:
    sub = Subscriber(broker.port)
    db = tmp_path / "b.db"
    with Buffer(db) as b:
        b.append([reading(i) for i in range(300)])
    shipper = run_shipper(db, broker.port)
    try:
        drain(db)
        wait_until(lambda: len(sub.readings()) == 300)
    finally:
        shipper.stop(timeout_s=5)
        sub.close()
    assert sorted(sub.readings().values()) == [float(i) for i in range(300)]


@needs_broker
def test_broker_outage_loses_nothing(tmp_path: Path, broker: Broker) -> None:
    db = tmp_path / "b.db"
    n = 400
    sub = Subscriber(broker.port)
    shipper = run_shipper(db, broker.port, connect_timeout_s=0.5, ack_timeout_s=1)
    seen: dict[int, float] = {}
    try:
        with Buffer(db) as buf:
            w = BatchWriter(buf, max_rows=10, max_delay_s=0.05)
            for i in range(n):
                w.write(reading(i))
                if i == 100:
                    broker.stop()  # uplink gone while writing continues
                    time.sleep(0.3)  # let messages the broker already routed arrive
                    seen.update(sub.readings())
                    sub.close()
                if i == 300:
                    broker.start()
                    # Subscribes well before the shipper's reconnect (paho waits >= 1 s).
                    sub = Subscriber(broker.port)
                time.sleep(0.002)
            w.flush()
        drain(db, timeout_s=60)
        wait_until(lambda: len(seen.keys() | sub.readings().keys()) >= n)
    finally:
        shipper.stop(timeout_s=5)
        seen.update(sub.readings())
        sub.close()
    assert sorted(seen.values()) == [float(i) for i in range(n)]


@needs_broker
def test_cli_check_and_run_over_mqtt(tmp_path: Path, broker: Broker) -> None:
    cfg = tmp_path / "spool.toml"
    cfg.write_text(
        '[buffer]\npath = "b.db"\n[retention]\npolicy = "drop_oldest"\nmax_rows = 1000\n'
        f'[sink]\ntype = "mqtt"\nhost = "127.0.0.1"\nport = {broker.port}\n'
        '[device]\nid = "cli-dev"\n'
    )
    check = subprocess.run(
        [sys.executable, "-m", "spool", "check", str(cfg)],
        capture_output=True,
        text=True,
        check=False,
    )
    assert check.returncode == 0, check.stdout + check.stderr
    assert "broker reachable" in check.stdout

    sub = Subscriber(broker.port)
    lines = "".join(json.dumps({"sensor_id": "t", "value": i}) + "\n" for i in range(20))
    run = subprocess.run(
        [sys.executable, "-m", "spool", "run", str(cfg)],
        input=lines,
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )
    assert run.returncode == 0, run.stderr
    wait_until(lambda: len(sub.readings()) == 20)
    sub.close()
    assert {r["device_id"] for r in sub.records} == {"cli-dev"}


@needs_broker
def test_acl_denied_topic_is_rejected_and_reported_as_a_gap(tmp_path: Path) -> None:
    acl = "topic readwrite spool/#\ntopic deny spool/+/reading/secret\n"
    with Broker(tmp_path, acl=acl) as broker:
        sub = Subscriber(broker.port)
        db = tmp_path / "b.db"
        with Buffer(db) as b:
            b.append(
                [
                    Reading("secret" if i % 10 == 0 else "ok", float(i), None, i, i, "boot")
                    for i in range(50)
                ]
            )
        shipper = run_shipper(db, broker.port)
        try:
            drain(db)
            wait_until(lambda: len(sub.readings()) == 45 and sub.gaps())
        finally:
            shipper.stop(timeout_s=5)
            sub.close()
    assert shipper.stats.rejected == 5
    assert sum(g["count"] for g in sub.gaps()) == 5
    assert {g["reason"] for g in sub.gaps()} == {REASON_REJECTED}
    with Buffer(db) as b:
        assert b.counts()[REJECTED] == 5
