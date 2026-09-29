"""Reference consumer: MQTT -> Postgres, exactly-once storage from at-least-once delivery.

Subscribes with a persistent session (the broker holds messages while the
consumer is down), stores a batch in one transaction, and only then sends the
PUBACKs. A crash before commit means the broker redelivers; the (buffer_id, seq)
key turns that into a no-op. Nothing is acknowledged that isn't stored.

Needs the ``consumer`` extra (paho-mqtt >= 2.0, psycopg >= 3.1).
"""

from __future__ import annotations

import logging
import queue
import threading
import time
from dataclasses import dataclass
from typing import Any

import paho.mqtt.client as mqtt
import psycopg
from paho.mqtt.enums import CallbackAPIVersion
from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.properties import Properties

from spoolpi.consumer.postgres import Incoming, StoreResult, store

log = logging.getLogger("spoolpi.consumer")


@dataclass(frozen=True, slots=True)
class _Message:
    generation: int  # which MQTT connection delivered it
    mid: int
    qos: int
    incoming: Incoming


class MqttConsumer:
    def __init__(
        self,
        *,
        host: str,
        dsn: str,
        port: int = 1883,
        topic: str = "spoolpi/#",
        client_id: str = "spoolpi-consumer",
        session_expiry_s: int = 7 * 24 * 3600,
        batch_size: int = 500,
        max_delay_s: float = 0.2,
        tls: bool = False,
        ca_file: str | None = None,
        username: str | None = None,
        password: str | None = None,
    ) -> None:
        self.dsn = dsn
        self.topic = topic
        self.batch_size = batch_size
        self.max_delay_s = max_delay_s
        self.totals = StoreResult()
        self._queue: queue.Queue[_Message] = queue.Queue()
        self._generation = 0
        self._lock = threading.Lock()

        client = mqtt.Client(
            callback_api_version=CallbackAPIVersion.VERSION2,
            client_id=client_id,
            protocol=mqtt.MQTTv5,
        )
        client.manual_ack_set(True)  # PUBACK only after the rows are committed
        client.on_connect = self._on_connect
        client.on_message = self._on_message
        client.reconnect_delay_set(min_delay=1, max_delay=30)
        if tls:
            client.tls_set(ca_certs=ca_file)
        if username is not None:
            client.username_pw_set(username, password)
        props = Properties(PacketTypes.CONNECT)  # type: ignore[no-untyped-call]
        props.SessionExpiryInterval = session_expiry_s
        # Unacked messages the broker may have in flight to us. With manual acks this
        # caps the batch size; the broker's own limit (mosquitto: max_inflight_messages,
        # default 20) must be raised too for big batches.
        props.ReceiveMaximum = min(65535, max(batch_size, 20))
        # clean_start=False + a session expiry: the broker queues for us while we're down.
        client.connect_async(host, port, keepalive=60, clean_start=False, properties=props)
        self._client = client

    def _on_connect(self, client: Any, _u: Any, _flags: Any, reason_code: Any, _p: Any) -> None:
        if reason_code.is_failure:
            log.warning("broker refused the connection: %s", reason_code)
            return
        with self._lock:
            self._generation += 1
        client.subscribe(self.topic, qos=1)
        log.info("connected; subscribed to %s", self.topic)

    def _on_message(self, _c: Any, _u: Any, msg: Any) -> None:
        with self._lock:
            generation = self._generation
        self._queue.put(_Message(generation, msg.mid, msg.qos, Incoming(msg.topic, msg.payload)))

    def _next_batch(self, stop: threading.Event) -> list[_Message]:
        try:
            batch = [self._queue.get(timeout=self.max_delay_s)]
        except queue.Empty:
            return []
        deadline = time.monotonic() + self.max_delay_s
        while len(batch) < self.batch_size and not stop.is_set():
            # A 20 ms lull usually means the broker's in-flight window is full and
            # nothing more comes until we ack: commit now instead of waiting it out.
            wait = min(0.02, deadline - time.monotonic())
            try:
                batch.append(self._queue.get(timeout=max(0.0, wait)))
            except queue.Empty:
                break
        return batch

    def _store(
        self, conn: psycopg.Connection[Any] | None, batch: list[_Message], stop: threading.Event
    ) -> psycopg.Connection[Any] | None:
        """Store until it works, reconnecting as needed. Returns the live connection."""
        delay = 0.5
        while True:
            try:
                if conn is None or conn.closed:
                    conn = psycopg.connect(self.dsn)
                result = store(conn, [m.incoming for m in batch])
                break
            except psycopg.OperationalError as e:
                log.warning("database unavailable (%s); retrying in %.1f s", e, delay)
                if conn is not None:
                    conn.close()
                conn = None
                if stop.wait(delay):
                    return conn
                delay = min(delay * 2, 30.0)
        for field in ("readings", "gaps", "duplicates", "dead_letters"):
            setattr(self.totals, field, getattr(self.totals, field) + getattr(result, field))
        if result.dead_letters:
            log.warning(
                "%d messages could not be stored; see spoolpi_dead_letters", result.dead_letters
            )
        with self._lock:
            current = self._generation
        for m in batch:
            # A mid from an earlier connection may now name a different message on
            # this one: acking it could confirm something never stored. Drop those
            # acks; the broker redelivers and the primary key absorbs the repeat.
            if m.generation == current and m.qos > 0:
                self._client.ack(m.mid, m.qos)
        return conn

    def run(self, stop: threading.Event) -> None:
        self._client.loop_start()
        conn: psycopg.Connection[Any] | None = None
        try:
            while not stop.is_set():
                batch = self._next_batch(stop)
                if batch:
                    conn = self._store(conn, batch, stop)
        finally:
            self._client.disconnect()
            self._client.loop_stop()
            if conn is not None:
                conn.close()
