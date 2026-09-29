"""The drain loop: claim a batch from the buffer, send it, ack what the sink took.

The shipper runs on its own thread with its own buffer handle, so the writer
never waits on it: the buffer is the queue. Rows the sink didn't accept go back
to pending and are retried first. A crash between send and ack means those rows
are sent again after restart; that's the at-least-once contract.

Timestamps are corrected at ship time, so a reading shipped before the clock
syncs goes out uncorrected. With ``hold_unsynced_s``, a shipper that starts
before the clock has synced holds readings back (gap records still ship) until
the clock syncs or the hold runs out, whichever comes first.
"""

from __future__ import annotations

import contextlib
import enum
import logging
import math
import os
import random
import sqlite3
import threading
import time
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from pathlib import Path

from spoolpi.core.buffer import Buffer, Durability
from spoolpi.core.clock import ClockAnchor
from spoolpi.core.reading import Reading
from spoolpi.core.retention import GapRecord
from spoolpi.sinks.base import AckSet, Envelope, Sink

log = logging.getLogger("spoolpi.shipper")

_WARN_INTERVAL_S = 60.0
_PURGE_CHUNK = 10_000


@dataclass(frozen=True, slots=True)
class Backoff:
    initial_s: float = 0.5
    max_s: float = 60.0
    multiplier: float = 2.0
    # The first failures of a streak are retried at once: on a flaky link most
    # streaks are that short. Measured (bench/retry_policy.py, 50 ms uplink,
    # 0.5-60 s backoff): at 25% send failure 0 -> 14% of full throughput, 2 -> 63%.
    # A real outage costs this many extra attempts before the backoff takes over.
    immediate_retries: int = 2

    def delay(self, failures: int, rng: random.Random) -> float:
        """Delay after ``failures`` consecutive failures (>= 1), jittered into [base/2, base]."""
        if failures <= self.immediate_retries:
            return 0.0
        # The cap is hit long before 64 doublings; the min avoids float overflow.
        exponent = min(failures - self.immediate_retries - 1, 64)
        base = min(self.max_s, self.initial_s * self.multiplier**exponent)
        return base / 2 + rng.uniform(0, base / 2)


@dataclass(slots=True)
class ShipperStats:
    sends: int = 0
    failed_sends: int = 0
    timeouts: int = 0
    acked: int = 0
    released: int = 0
    rejected: int = 0


class _Outcome(enum.Enum):
    EMPTY = enum.auto()
    PROGRESS = enum.auto()
    FAILED = enum.auto()


class Shipper:
    def __init__(
        self,
        db_path: str | os.PathLike[str],
        sink: Sink,
        *,
        batch_size: int = 100,
        send_timeout_s: float = 10.0,
        backoff: Backoff | None = None,
        poll_interval_s: float = 1.0,
        purge_interval_s: float = 60.0,
        max_abandoned_sends: int = 4,
        anchor: ClockAnchor | None = None,
        hold_unsynced_s: float = 0.0,
        durability: Durability = "power",
        rng: random.Random | None = None,
    ) -> None:
        """``hold_unsynced_s`` needs ``anchor``; 0 ships at once, as before."""
        self._db_path = Path(db_path)
        self._durability: Durability = durability
        self._hold_s = hold_unsynced_s if anchor is not None else 0.0
        self._hold_until: float | None = None  # set by start()
        self._holding = False
        self._sink = sink
        self._batch_size = batch_size
        self._send_timeout_s = send_timeout_s
        self._backoff = backoff if backoff is not None else Backoff()
        self._poll_interval_s = poll_interval_s
        self._purge_interval_s = purge_interval_s
        self._max_abandoned = max_abandoned_sends
        self._anchor = anchor
        self._rng = rng if rng is not None else random.Random()
        self._wake = threading.Event()
        self._stopping = threading.Event()
        self._stop_deadline: float | None = None
        self._abandoned: list[threading.Thread] = []
        self._thread: threading.Thread | None = None
        self._last_rejection_log = -math.inf
        self.stats = ShipperStats()

    def start(self) -> None:
        if self._hold_s > 0:
            self._hold_until = time.monotonic() + self._hold_s
        self._thread = threading.Thread(target=self._run, name="spoolpi-shipper", daemon=True)
        self._thread.start()

    def notify(self) -> None:
        """New rows were committed; ship now instead of at the next poll."""
        self._wake.set()

    def stop(self, timeout_s: float = 10.0) -> bool:
        """Finish or abandon the batch in flight, then exit. True if it exited in time.

        Rows still inflight after a missed deadline are recovered on next start.
        """
        # Leave part of the budget for releasing rows after abandoning a send.
        self._stop_deadline = time.monotonic() + timeout_s * 0.8
        self._stopping.set()
        self._wake.set()
        stopped = True
        if self._thread is not None:
            self._thread.join(timeout_s)
            stopped = not self._thread.is_alive()
        self._sink.close()
        return stopped

    def _run(self) -> None:
        # Opened inside the loop: a failed open (say, a locked brand-new file) must
        # be retried like anything else, not end the thread and with it all delivery.
        buf: Buffer | None = None
        failures = 0
        last_warning = -math.inf
        next_purge = time.monotonic() + self._purge_interval_s
        try:
            while not self._stopping.is_set():
                self._wake.clear()
                try:
                    if buf is None:
                        buf = Buffer(self._db_path, durability=self._durability)
                    outcome = self._ship_once(buf)
                    if time.monotonic() >= next_purge:
                        # Chunked so each delete transaction stays short, but repeated
                        # until caught up: at high rates one chunk per interval falls behind.
                        while (
                            buf.purge_acked(_PURGE_CHUNK) == _PURGE_CHUNK
                            and not self._stopping.is_set()
                        ):
                            pass
                        next_purge = time.monotonic() + self._purge_interval_s
                except Exception:
                    # A dead shipper thread would silently stop delivery; back off and retry.
                    log.exception("shipper iteration failed")
                    outcome = _Outcome.FAILED
                    # Rows claimed before the failure would otherwise stay inflight until
                    # restart. Safe: this thread is the only claimer, and no send is
                    # running (abandoned sends already released theirs).
                    if buf is not None:
                        with contextlib.suppress(sqlite3.Error):
                            buf.recover_inflight()
                if outcome is _Outcome.PROGRESS:
                    failures = 0
                elif outcome is _Outcome.EMPTY:
                    self._wake.wait(self._poll_interval_s)
                else:
                    failures += 1
                    # A flaky uplink starts a new failure streak every few sends; on a
                    # Zero, journald writes would outweigh the data itself.
                    now = time.monotonic()
                    if failures == 1 and now - last_warning >= _WARN_INTERVAL_S:
                        log.warning("send failed, backing off (repeats suppressed for 60 s)")
                        last_warning = now
                    # Only stop cuts a backoff short; new writes must not.
                    self._stopping.wait(self._backoff.delay(failures, self._rng))
        finally:
            if buf is not None:
                buf.close()

    def _ship_once(self, buf: Buffer) -> _Outcome:
        # Gaps go first: they're rare, small, and what an auditor looks for.
        gaps = buf.claim_gaps(self._batch_size)
        if gaps:
            return self._ship(
                [(g.id, Envelope(buf.buffer_id, g.seq, g.gap)) for g in gaps],
                buf.ack_gaps,
                buf.release_gaps,
                buf.reject_gaps,
            )
        if self._hold_readings():
            return _Outcome.EMPTY
        claimed = buf.claim(self._batch_size)
        if not claimed:
            return _Outcome.EMPTY
        return self._ship(
            [(s.id, Envelope(buf.buffer_id, s.seq, self._correct(s.reading))) for s in claimed],
            buf.ack,
            buf.release,
            buf.reject,
        )

    def _ship(
        self,
        rows: list[tuple[int, Envelope]],
        ack: Callable[[Iterable[int]], int],
        release: Callable[[Iterable[int]], int],
        reject: Callable[[Iterable[int]], int],
    ) -> _Outcome:
        id_by_seq = {env.seq: row_id for row_id, env in rows}
        batch = [env for _, env in rows]
        acks = self._send(batch)
        # Seqs the sink claims but we never sent are ignored.
        accepted = {q for q in acks.accepted if q in id_by_seq} if acks is not None else set()
        rejected = {q for q in acks.rejected if q in id_by_seq} if acks is not None else set()
        if len(batch) > 1 and len(rejected) == len(batch):
            # Every record refused at once looks systemic (schema, auth, endpoint),
            # not poison: keep the data and treat it as a failed send.
            self._log_rejection(
                "sink rejected all %d records of a batch; treating it as a failed send, "
                "not dropping data (a sink should raise for systemic errors)",
                len(batch),
            )
            rejected = set()
        rest = [row_id for seq, row_id in id_by_seq.items() if seq not in accepted | rejected]
        if accepted:
            self.stats.acked += ack(id_by_seq[seq] for seq in accepted)
        if rejected:
            n = reject(id_by_seq[seq] for seq in rejected)
            self.stats.rejected += n
            kind = "gap records" if isinstance(batch[0].payload, GapRecord) else "readings"
            self._log_rejection(
                "sink permanently rejected %d %s; quarantined in the buffer%s",
                n,
                kind,
                "" if kind == "readings" else ", and the sink's accounting is short by them",
            )
        if rest:
            self.stats.released += release(rest)
        return _Outcome.PROGRESS if accepted or rejected else _Outcome.FAILED

    def _log_rejection(self, message: str, *args: object) -> None:
        now = time.monotonic()
        if now - self._last_rejection_log >= _WARN_INTERVAL_S:
            log.error(message + " (repeats suppressed for 60 s)", *args)
            self._last_rejection_log = now

    def _hold_readings(self) -> bool:
        """True while readings wait for the clock: started unsynced, and the hold hasn't run out."""
        if self._hold_until is None:
            return False
        assert self._anchor is not None  # _hold_s is 0 without one
        if self._anchor.anchor is not None:
            if self._holding:
                log.info("clock synchronised; shipping held readings with corrected timestamps")
            self._hold_until = None
        elif time.monotonic() >= self._hold_until:
            log.warning(
                "clock still not synchronised after %g s; shipping readings with "
                "uncorrected timestamps (ts_quality 0)",
                self._hold_s,
            )
            self._hold_until = None
        elif not self._holding:
            log.info(
                "clock not synchronised; holding readings for up to %g s so their "
                "timestamps can be corrected",
                self._hold_s,
            )
            self._holding = True
        if self._hold_until is None:
            self._holding = False
            return False
        return True

    def _correct(self, reading: Reading) -> Reading:
        return self._anchor.correct(reading) if self._anchor is not None else reading

    def _send(self, batch: list[Envelope]) -> AckSet | None:
        """The sink's AckSet, or None if the send raised, timed out, or wasn't attempted."""
        for t in self._abandoned:
            if not t.is_alive():
                t.join()  # a hung send that finished late: free its stack (see below)
        self._abandoned = [t for t in self._abandoned if t.is_alive()]
        if len(self._abandoned) >= self._max_abandoned:
            log.warning("%d hung sends still running; not sending more", len(self._abandoned))
            self.stats.failed_sends += 1
            return None

        result: list[AckSet] = []
        done = threading.Event()

        def run() -> None:
            try:
                result.append(self._sink.send(batch))
            except Exception as e:  # noqa: BLE001 -- anything a sink raises is a failed send
                log.debug("send raised: %r", e)
            finally:
                done.set()

        self.stats.sends += 1
        sender = threading.Thread(target=run, name="spoolpi-send", daemon=True)
        sender.start()
        deadline = time.monotonic() + self._send_timeout_s
        # Short slices so a stop() deadline set mid-send is noticed promptly.
        while not done.wait(0.05):
            now = time.monotonic()
            stop_deadline = self._stop_deadline
            if now >= deadline or (stop_deadline is not None and now >= stop_deadline):
                self._abandoned.append(sender)
                self.stats.timeouts += 1
                self.stats.failed_sends += 1
                log.warning("send timed out; releasing batch of %d", len(batch))
                return None
        # Join, don't just drop it: on Python 3.13 an unjoined thread's stack (8 MB) stays
        # mapped for as long as its Thread object lives, and a failed send's exception can
        # keep that object alive in a reference cycle. On a 32-bit Pi that ran the process
        # out of address space after ~350 failed sends, i.e. during an uplink outage.
        # done is set in the thread's last statement, so this returns at once.
        sender.join()
        if not result:
            self.stats.failed_sends += 1
            return None
        return result[0]
