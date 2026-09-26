"""A recording sink whose behavior is scripted per call, for tests.

    sink = MemorySink([Accept(), Partial.first(3), Raise(), Hang(), Reject()])

Each ``send`` uses the next behavior in the script, then ``default`` once the
script runs out. Everything accepted is recorded in ``received``, in order and
including resends, so tests can check loss and duplication directly.
"""

from __future__ import annotations

import threading
import time
from collections import deque
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
class Poison:
    """Rejects the envelopes ``is_poison`` matches, for good, and accepts the rest."""

    is_poison: Callable[[Envelope], bool]


@dataclass(frozen=True, slots=True)
class Raise:
    error: Exception = field(default_factory=lambda: SinkError("scripted failure"))


@dataclass(frozen=True, slots=True)
class Hang:
    """Blocks until ``release_hung()``, ``close()``, or ``for_s`` elapses.

    Then raises SinkError, or with ``late_accept`` records the batch anyway: a
    send the caller already gave up on that still got through.
    """

    for_s: float | None = None
    late_accept: bool = False


Behavior = Accept | Reject | Partial | Poison | Raise | Hang


_ACCEPT: Final = Accept()


class MemorySink:
    def __init__(
        self,
        script: Iterable[Behavior] = (),
        *,
        default: Behavior = _ACCEPT,
        latency_s: float = 0.0,
    ) -> None:
        self._script = deque(script)
        self._default = default
        self._latency_s = latency_s
        self._lock = threading.Lock()
        self._unhang = threading.Event()
        self.received: list[Envelope] = []
        self.rejected: list[Envelope] = []
        self.calls = 0
        self.closed = False

    def send(self, batch: Sequence[Envelope]) -> AckSet:
        with self._lock:
            behavior = self._script.popleft() if self._script else self._default
            self.calls += 1
        if self._latency_s:
            time.sleep(self._latency_s)
        match behavior:
            case Accept():
                taken = list(batch)
            case Reject():
                taken = []
            case Partial():
                taken = list(behavior.pick(batch))
            case Poison():
                poison = [e for e in batch if behavior.is_poison(e)]
                taken = [e for e in batch if not behavior.is_poison(e)]
                with self._lock:
                    self.received.extend(taken)
                    self.rejected.extend(poison)
                return AckSet.of((e.seq for e in taken), (e.seq for e in poison))
            case Raise():
                raise behavior.error
            case Hang():
                self._unhang.wait(behavior.for_s)
                if not behavior.late_accept:
                    raise SinkError("hung send released")
                taken = list(batch)
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
