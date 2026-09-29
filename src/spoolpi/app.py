"""The ``SpoolPi`` facade: one object that owns the writer, shipper and clock anchor.

    with SpoolPi.from_config("/etc/spoolpi/spoolpi.toml") as spoolpi:
        while running:
            spoolpi.write("temp", read_temp(), "C")

The sampling loop stays yours. ``write`` is cheap: it batches in memory and
commits per batch. Call it (and ``tick``/``flush``/``close``) from the thread
that created the SpoolPi; the buffer connection refuses other threads.
"""

from __future__ import annotations

import contextlib
import os
import time
from collections.abc import Iterator
from pathlib import Path
from types import TracebackType
from typing import Self

from spoolpi.config import Config, SinkConfig, load
from spoolpi.core import clock
from spoolpi.core.buffer import INFLIGHT, PENDING, BatchWriter, Buffer
from spoolpi.core.clock import BOOT_ID, ClockAnchor
from spoolpi.core.identity import device_id
from spoolpi.core.reading import Reading
from spoolpi.core.retention import BufferFull
from spoolpi.core.shipper import Backoff, Shipper
from spoolpi.sinks.base import Sink
from spoolpi.sinks.jsonl import JsonlSink


def _is_utf8(text: str) -> bool:
    try:
        text.encode()
    except UnicodeEncodeError:  # e.g. a lone surrogate from a JSON "\ud800" escape
        return False
    return True


def resolve_device_id(config: Config) -> str:
    return config.device_id if config.device_id is not None else device_id()


def build_sink(cfg: SinkConfig, device: str) -> Sink:
    """Construct the configured sink. config.load() has already validated it."""
    if cfg.type == "jsonl" and cfg.path is not None:
        return JsonlSink(cfg.path)
    if cfg.type == "mqtt" and cfg.mqtt is not None:
        from spoolpi.sinks.mqtt import MqttSink  # paho is an optional extra

        m = cfg.mqtt
        password = None
        if m.password_file is not None:
            password = m.password_file.read_text().rstrip("\r\n")
        return MqttSink(
            host=m.host,
            port=m.port,
            device_id=device,
            topic=m.topic,
            client_id=m.client_id,
            protocol=m.protocol,
            keepalive_s=m.keepalive_s,
            connect_timeout_s=m.connect_timeout_s,
            ack_timeout_s=m.ack_timeout_s,
            tls=m.tls,
            ca_file=m.ca_file,
            username=m.username,
            password=password,
        )
    if cfg.type == "http" and cfg.http is not None:
        from spoolpi.sinks.http import HttpSink  # httpx is an optional extra

        h = cfg.http
        token = h.token_file.read_text().strip() if h.token_file is not None else None
        return HttpSink(
            url=h.url,
            device_id=device,
            timeout_s=h.timeout_s,
            token=token,
            ca_file=h.ca_file,
            verify=h.verify,
            gzip=h.gzip,
        )
    raise ValueError(f"unsupported sink {cfg.type!r}")


class SpoolPi:
    def __init__(self, config: Config, sink: Sink | None = None) -> None:
        self.config = config
        self.device_id = resolve_device_id(config)
        self._sink = sink if sink is not None else build_sink(config.sink, self.device_id)
        self._buffer = Buffer(
            config.buffer_path,
            retention=config.retention,
            wal_autocheckpoint=config.wal_autocheckpoint,
        )
        # Rows a previous process claimed but never acked; resent (at-least-once).
        self._buffer.recover_inflight()
        self._writer = BatchWriter(
            self._buffer, max_rows=config.batch_max_rows, max_delay_s=config.batch_max_delay_s
        )
        self._anchor = ClockAnchor()
        # The timedatectl fallback spawns a process per poll; too costly every second on a Zero.
        self._anchor.start(interval_s=1.0 if clock.SYNC_BACKEND == "adjtimex" else 15.0)
        s = config.shipper
        self._shipper = Shipper(
            config.buffer_path,
            self._sink,
            batch_size=s.batch_size,
            send_timeout_s=s.send_timeout_s,
            backoff=Backoff(
                initial_s=s.backoff_initial_s,
                max_s=s.backoff_max_s,
                immediate_retries=s.immediate_retries,
            ),
            poll_interval_s=s.poll_interval_s,
            purge_interval_s=s.purge_interval_s,
            anchor=self._anchor,
        )
        self._shipper.start()
        self._closed = False
        # Readings refused at the cap under halt_and_alarm, all recorded as gaps.
        self.discarded = 0

    @classmethod
    def from_config(cls, path: str | os.PathLike[str]) -> SpoolPi:
        return cls(load(Path(path)))

    @property
    def buffer_id(self) -> str:
        return self._buffer.buffer_id

    def write(self, sensor_id: str, value: float | None, unit: str | None = None) -> None:
        """Record one sample now. ``value=None`` records a failed read, which still ships.

        Raises ``BufferFull`` under halt_and_alarm when the buffer is at its cap.
        """
        mono, wall, quality = self._anchor.stamp()
        self.write_reading(Reading(sensor_id, value, unit, mono, wall, BOOT_ID, quality))

    def write_reading(self, reading: Reading) -> None:
        """Raises ValueError for text SQLite can't store (not valid UTF-8)."""
        # Caught here, not at commit: a bad string would fail every commit of its
        # batch, and the writer would retry that batch forever.
        texts = [("sensor_id", reading.sensor_id), ("unit", reading.unit)]
        texts += [("qc_tests", t) for t in reading.qc_tests]
        for name, text in texts:
            if text is not None and not _is_utf8(text):
                raise ValueError(f"{name} {text!r} is not valid UTF-8 text")
        with self._counting_refusals():
            self._writer.write(reading)
        if self._writer.pending_count == 0:
            self._shipper.notify()

    def tick(self) -> None:
        """Commit a batch that has waited ``max_delay_s``. Call when idle between writes."""
        with self._counting_refusals():
            flushed = self._writer.flush_if_due()
        if flushed:
            self._shipper.notify()

    def flush(self) -> None:
        with self._counting_refusals():
            self._writer.flush()
        self._shipper.notify()

    @contextlib.contextmanager
    def _counting_refusals(self) -> Iterator[None]:
        try:
            yield
        except BufferFull as e:
            self.discarded += e.discarded
            raise

    def drain(self, timeout_s: float) -> bool:
        """Commit what's pending and wait until everything is shipped. True if it was.

        A refused final batch is already a gap record, and that gap is shipped too.
        """
        with contextlib.suppress(BufferFull):
            self.flush()
        deadline = time.monotonic() + timeout_s
        while True:
            counts = self._buffer.counts()
            if not counts[PENDING] and not counts[INFLIGHT] and not self._buffer.unshipped_gaps():
                return True
            if time.monotonic() >= deadline:
                return False
            time.sleep(0.05)

    def close(self, timeout_s: float | None = None) -> bool:
        """Commit what's pending, let the shipper finish its batch, and stop.

        True if the shipper stopped within the timeout. Rows not yet shipped stay
        in the buffer for the next run.
        """
        if self._closed:
            return True
        self._closed = True
        try:
            # A refusal here is already a gap record, and there's no caller left to alarm.
            with contextlib.suppress(BufferFull):
                self.flush()
        finally:
            timeout = timeout_s if timeout_s is not None else self.config.shipper.stop_timeout_s
            stopped = self._shipper.stop(timeout_s=timeout)
            self._anchor.stop()
            self._buffer.close()
        return stopped

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.close()
