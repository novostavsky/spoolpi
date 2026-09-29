"""Append records as JSON lines to a local file, fsynced before acking.

Useful as a real, durable sink for testing and for handing data to another
local process. Resends after a crash appear as repeated lines; readers must
deduplicate on (buffer_id, seq) like any SpoolPi consumer.
"""

from __future__ import annotations

import json
import logging
import os
import threading
from collections.abc import Sequence
from pathlib import Path

from spoolpi.sinks.base import AckSet, Envelope, SinkError, to_wire

log = logging.getLogger("spoolpi.sinks.jsonl")


class JsonlSink:
    def __init__(self, path: str | os.PathLike[str]) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        existed = self.path.exists()
        self._fd = os.open(self.path, os.O_WRONLY | os.O_APPEND | os.O_CREAT, 0o644)
        # Held for the whole write+fsync, so close() can't free the fd under a send.
        self._lock = threading.Lock()
        if not existed:
            dir_fd = os.open(self.path.parent, os.O_RDONLY)
            try:
                os.fsync(dir_fd)
            finally:
                os.close(dir_fd)

    def send(self, batch: Sequence[Envelope]) -> AckSet:
        data = "".join(json.dumps(to_wire(e), separators=(",", ":")) + "\n" for e in batch)
        view = memoryview(data.encode())
        with self._lock:
            if self._fd < 0:
                raise SinkError("sink is closed")
            # A crash mid-write can leave a torn last line; readers should skip one.
            while view:
                view = view[os.write(self._fd, view) :]
            os.fsync(self._fd)
        return AckSet.all(batch)

    def close(self) -> None:
        # A send the shipper gave up on may still be writing. Closing its fd under
        # it could let the OS hand the same number to another file (the buffer's,
        # say) and the late write would land there. Better to leak the fd.
        if not self._lock.acquire(timeout=5):
            log.warning("a send is still writing; leaving %s open", self.path)
            return
        try:
            if self._fd >= 0:
                os.close(self._fd)
                self._fd = -1
        finally:
            self._lock.release()
