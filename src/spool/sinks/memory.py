"""A recording sink whose behavior is scripted per call, for tests.

    sink = MemorySink([Accept(), Partial.first(3), Raise(), Hang(), Reject()])

Each ``send`` uses the next behavior in the script, then ``default`` once the
script runs out. Everything accepted is recorded in ``received``, in order and
including resends, so tests can check loss and duplication directly.
"""

from __future__ import annotations

import threading
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass, field
from typing import Final

from spool.sinks.base import AckSet, Envelope, SinkError


@dataclass(frozen=True, slots=True)
class Accept:
    pass


@dataclass(frozen=True, slots=True)
class Reject:
    """Returns an empty AckSet: the sink answered, but took nothing."""


@dataclass(frozen=True, slots=True)
class Partial:
    pick: Callable[[Sequence[Envelope]], Sequence[Envelope]]

    @classmethod
    def first(cls, n: int) -> Partial:
        return cls(lambda batch: batch[:n])

    @classmethod
    def where(cls, accept: Callable[[Envelope], bool]) -> Partial:
        return cls(lambda batch: [e for e in batch if accept(e)])


@dataclass(frozen=True, slots=True)
class Raise:
    error: Exception = field(default_factory=lambda: SinkError("scripted failure"))


@dataclass(frozen=True, slots=True)
class Hang:
    """Blocks until ``release_hung()`` or ``close()``, then raises SinkError."""


Behavior = Accept | Reject | Partial | Raise | Hang


_ACCEPT: Final = Accept()


class MemorySink:
    def __init__(self, script: Iterable[Behavior] = (), *, default: Behavior = _ACCEPT) -> None:
        self._script = list(script)
        self._default = default
        self._lock = threading.Lock()
        self._unhang = threading.Event()
        self.received: list[Envelope] = []
        self.calls = 0
        self.closed = False

    def send(self, batch: Sequence[Envelope]) -> AckSet:
        with self._lock:
            behavior = self._script.pop(0) if self._script else self._default
            self.calls += 1
        match behavior:
            case Accept():
                taken = list(batch)
            case Reject():
                taken = []
            case Partial():
                taken = list(behavior.pick(batch))
            case Raise():
                raise behavior.error
            case Hang():
                self._unhang.wait()
                raise SinkError("hung send released")
        with self._lock:
            self.received.extend(taken)
        return AckSet.all(taken)

    def release_hung(self) -> None:
        self._unhang.set()

    def close(self) -> None:
        self.closed = True
        self._unhang.set()

    def keys(self) -> list[tuple[str, int]]:
        with self._lock:
            return [e.key for e in self.received]
