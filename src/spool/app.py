"""The ``Spool`` facade: one object that owns the writer, shipper and clock anchor.

    with Spool.from_config("/etc/spool/spool.toml") as spool:
        while running:
            spool.write("temp", read_temp(), "C")

The sampling loop stays yours. ``write`` is cheap: it batches in memory and
commits per batch. Call it (and ``tick``/``flush``/``close``) from the thread
that created the Spool; the buffer connection refuses other threads.
"""

from __future__ import annotations

import os
import time
from pathlib import Path
from types import TracebackType
from typing import Self

from spool.config import Config, SinkConfig, load
from spool.core.buffer import INFLIGHT, PENDING, BatchWriter, Buffer
from spool.core.clock import BOOT_ID, ClockAnchor
from spool.core.reading import Reading
from spool.core.shipper import Backoff, Shipper
from spool.sinks.base import Sink
from spool.sinks.jsonl import JsonlSink


def build_sink(cfg: SinkConfig) -> Sink:
    if cfg.type == "jsonl" and cfg.path is not None:
        return JsonlSink(cfg.path)
    raise ValueError(f"unsupported sink {cfg.type!r}")  # config.load() already rejects these


class Spool:
    def __init__(self, config: Config, sink: Sink | None = None) -> None:
        self.config = config
        self._sink = sink if sink is not None else build_sink(config.sink)
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
        self._anchor.start()
        s = config.shipper
        self._shipper = Shipper(
            config.buffer_path,
            self._sink,
            batch_size=s.batch_size,
            send_timeout_s=s.send_timeout_s,
            backoff=Backoff(initial_s=s.backoff_initial_s, max_s=s.backoff_max_s),
            poll_interval_s=s.poll_interval_s,
            purge_interval_s=s.purge_interval_s,
            anchor=self._anchor,
        )
        self._shipper.start()
        self._closed = False

    @classmethod
    def from_config(cls, path: str | os.PathLike[str]) -> Spool:
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
        self._writer.write(reading)
        if self._writer.pending_count == 0:
            self._shipper.notify()

    def tick(self) -> None:
        """Commit a batch that has waited ``max_delay_s``. Call when idle between writes."""
        if self._writer.flush_if_due():
            self._shipper.notify()

    def flush(self) -> None:
        self._writer.flush()
        self._shipper.notify()

    def drain(self, timeout_s: float) -> bool:
        """Commit what's pending and wait until everything is shipped. True if it was."""
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
            self._writer.flush()
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
