"""Publish each record as a JSON message over MQTT, QoS 1, acked on PUBACK.

Needs the ``mqtt`` extra (paho-mqtt >= 2.0).

A PUBACK means the broker took the message. The broker is not a buffer:
``send`` refuses to publish while disconnected, so nothing piles up in paho's
memory during an outage. Spool's own buffer holds it. With MQTT 5, PUBACK
reason codes separate a record the broker will never take (quarantined as a
rejection) from a transient refusal (retried).
"""

from __future__ import annotations

import json
import logging
import threading
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import Any, Final

from spool.core.reading import Reading
from spool.sinks.base import AckSet, Envelope, SinkError, to_wire

log = logging.getLogger("spool.sinks.mqtt")

# PUBACK reason codes that no retry can fix: not authorized (for this topic),
# topic name invalid, payload format invalid. Anything else >= 0x80 is retried;
# a broker refusing everything trips the shipper's whole-batch breaker instead.
PERMANENT_REASON_CODES: Final = frozenset({0x87, 0x90, 0x99})

_ESCAPES: Final = {"%": "%25", "/": "%2F", "+": "%2B", "#": "%23", "\0": "%00"}


def topic_part(text: str) -> str:
    """Escape a value so it stays a single, literal topic level."""
    return "".join(_ESCAPES.get(ch, ch) for ch in text)


class MqttSink:
    def __init__(
        self,
        *,
        host: str,
        device_id: str,
        port: int = 1883,
        topic: str = "spool/{device_id}/{type}/{sensor_id}",
        client_id: str | None = None,
        protocol: str = "5",
        keepalive_s: int = 60,
        connect_timeout_s: float = 3.0,
        ack_timeout_s: float = 5.0,
        tls: bool = False,
        ca_file: Path | None = None,
        username: str | None = None,
        password: str | None = None,
        client_factory: Callable[..., Any] | None = None,
    ) -> None:
        try:  # an optional extra, so imported only when this sink is used
            import paho.mqtt.client as mqtt
            from paho.mqtt.enums import CallbackAPIVersion
            from paho.mqtt.packettypes import PacketTypes
            from paho.mqtt.properties import Properties
        except ImportError as e:
            raise ImportError(
                "the mqtt sink needs paho-mqtt: install with `uv pip install 'spool[mqtt]'`"
            ) from e
        self._ok = mqtt.MQTT_ERR_SUCCESS
        self._properties_cls = Properties
        self._publish_packet = PacketTypes.PUBLISH
        self.host, self.port = host, port
        self.device_id = device_id
        self._topic = topic
        self._v5 = protocol == "5"
        self._connect_timeout_s = connect_timeout_s
        self._ack_timeout_s = ack_timeout_s

        self._cond = threading.Condition()
        self._connected = False
        self._closed = False
        self._results: dict[int, int] = {}  # mid -> PUBACK reason code
        self._abandoned: set[int] = set()  # mids we stopped waiting for

        factory = client_factory if client_factory is not None else mqtt.Client
        self._client = factory(
            callback_api_version=CallbackAPIVersion.VERSION2,
            client_id=client_id or f"spool-{device_id}",
            protocol=mqtt.MQTTv5 if self._v5 else mqtt.MQTTv311,
        )
        self._client.on_connect = self._on_connect
        self._client.on_disconnect = self._on_disconnect
        self._client.on_publish = self._on_publish
        self._client.reconnect_delay_set(min_delay=1, max_delay=30)
        if tls:
            self._client.tls_set(ca_certs=str(ca_file) if ca_file is not None else None)
        if username is not None:
            self._client.username_pw_set(username, password)
        # Background thread; paho reconnects on its own after a drop.
        connect_kw: dict[str, Any] = {"clean_start": True} if self._v5 else {}
        self._client.connect_async(host, port, keepalive_s, **connect_kw)
        self._client.loop_start()

    # --- paho callbacks (network thread) --------------------------------------

    def _on_connect(self, _c: Any, _u: Any, _flags: Any, reason_code: Any, _p: Any) -> None:
        with self._cond:
            self._connected = not reason_code.is_failure
            self._cond.notify_all()
        if reason_code.is_failure:
            log.warning(
                "MQTT broker %s:%d refused the connection: %s", self.host, self.port, reason_code
            )

    def _on_disconnect(self, _c: Any, _u: Any, _flags: Any, _rc: Any, _p: Any) -> None:
        with self._cond:
            self._connected = False
            self._cond.notify_all()

    def _on_publish(self, _c: Any, _u: Any, mid: int, reason_code: Any, _p: Any) -> None:
        with self._cond:
            if mid in self._abandoned:
                # paho reuses mids; a late PUBACK must never vouch for a newer message.
                self._abandoned.discard(mid)
                return
            self._results[mid] = int(reason_code.value)
            self._cond.notify_all()

    # --- Sink ------------------------------------------------------------------

    def is_connected(self, timeout_s: float = 0.0) -> bool:
        with self._cond:
            self._cond.wait_for(lambda: self._connected or self._closed, timeout=timeout_s)
            return self._connected and not self._closed

    def send(self, batch: Sequence[Envelope]) -> AckSet:
        if not self.is_connected(self._connect_timeout_s):
            raise SinkError(f"not connected to MQTT broker {self.host}:{self.port}")
        seq_by_mid: dict[int, int] = {}
        for e in batch:
            info = self._client.publish(
                self._topic_for(e), self._payload(e), qos=1, properties=self._properties()
            )
            if info.rc != self._ok:
                # Connection dropped mid-batch: whatever wasn't published is retried.
                break
            seq_by_mid[info.mid] = e.seq
        with self._cond:
            self._cond.wait_for(
                lambda: self._closed or all(m in self._results for m in seq_by_mid),
                timeout=self._ack_timeout_s,
            )
            codes: dict[int, int] = {}
            for mid, seq in seq_by_mid.items():
                if mid in self._results:
                    codes[seq] = self._results.pop(mid)
                else:
                    self._abandoned.add(mid)
        failures = {seq: code for seq, code in codes.items() if code >= 0x80}
        if failures:
            log.debug(
                "broker refused %d records: %s", len(failures), sorted(set(failures.values()))
            )
        return AckSet.of(
            (seq for seq, code in codes.items() if code < 0x80),
            (seq for seq, code in failures.items() if code in PERMANENT_REASON_CODES),
        )

    def close(self) -> None:
        with self._cond:
            self._closed = True
            self._cond.notify_all()
        self._client.disconnect()
        self._client.loop_stop()

    # --- encoding ----------------------------------------------------------------

    def _topic_for(self, e: Envelope) -> str:
        p = e.payload
        is_reading = isinstance(p, Reading)
        return self._topic.format(
            device_id=topic_part(self.device_id),
            buffer_id=topic_part(e.buffer_id),
            type="reading" if is_reading else "gap",
            sensor_id=topic_part(p.sensor_id) if p.sensor_id is not None else "_all",
        )

    def _payload(self, e: Envelope) -> bytes:
        record = {"device_id": self.device_id} | to_wire(e)
        return json.dumps(record, separators=(",", ":")).encode()

    def _properties(self) -> Any:
        if not self._v5:
            return None
        props = self._properties_cls(self._publish_packet)  # type: ignore[no-untyped-call]
        props.PayloadFormatIndicator = 1  # UTF-8; json.dumps output is ASCII
        props.ContentType = "application/json"
        return props
