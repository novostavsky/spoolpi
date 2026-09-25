"""Append records as JSON lines to a local file, fsynced before acking.

Useful as a real, durable sink for testing and for handing data to another
local process. Resends after a crash appear as repeated lines; readers must
deduplicate on (buffer_id, seq) like any Spool consumer.
"""

from __future__ import annotations

import json
import os
from collections.abc import Sequence
from pathlib import Path

from spool.sinks.base import AckSet, Envelope, to_wire


class JsonlSink:
    def __init__(self, path: str | os.PathLike[str]) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        existed = self.path.exists()
        self._fd = os.open(self.path, os.O_WRONLY | os.O_APPEND | os.O_CREAT, 0o644)
        if not existed:
            dir_fd = os.open(self.path.parent, os.O_RDONLY)
            try:
                os.fsync(dir_fd)
            finally:
                os.close(dir_fd)

    def send(self, batch: Sequence[Envelope]) -> AckSet:
        data = "".join(json.dumps(to_wire(e), separators=(",", ":")) + "\n" for e in batch)
        # A crash mid-write can leave a torn last line; readers should skip one.
        view = memoryview(data.encode())
        while view:
            view = view[os.write(self._fd, view) :]
        os.fsync(self._fd)
        return AckSet.all(batch)

    def close(self) -> None:
        if self._fd >= 0:
            os.close(self._fd)
            self._fd = -1
