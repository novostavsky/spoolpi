"""``spool run | check | status <config>``."""

from __future__ import annotations

import argparse
import contextlib
import json
import logging
import os
import selectors
import signal
import sys
import threading
import time
from pathlib import Path
from typing import Any

from spool.app import Spool
from spool.config import Config, ConfigError, load
from spool.core import clock
from spool.core.buffer import ACKED, INFLIGHT, PENDING, Buffer
from spool.core.retention import BufferFull

log = logging.getLogger("spool.cli")

EXIT_OK = 0
EXIT_PROBLEM = 1
EXIT_CONFIG = 2


# --- run ------------------------------------------------------------------------


class _Counters:
    def __init__(self) -> None:
        self.received = 0
        self.bad_lines = 0


# Under halt_and_alarm any commit can be refused. The batch is already a gap
# record, logged by the buffer and counted by the Spool, so the daemon carries on.
_refusals = contextlib.suppress(BufferFull)


def _write(
    spool: Spool, counters: _Counters, sensor_id: str, value: float | None, unit: str | None
) -> None:
    counters.received += 1
    with _refusals:
        spool.write(sensor_id, value, unit)


def _note_bad_line(counters: _Counters, problem: object) -> None:
    counters.bad_lines += 1
    if counters.bad_lines == 1:
        log.warning("skipping bad input line (%s); further ones are only counted", problem)


def _run_fake(spool: Spool, config: Config, stop: threading.Event, counters: _Counters) -> None:
    interval = 1.0 / config.source.rate_hz
    n = 0
    next_at = time.monotonic()
    while not stop.is_set():
        _write(spool, counters, config.source.sensor_id, float(n), None)
        n += 1
        next_at += interval
        stop.wait(max(0.0, next_at - time.monotonic()))
        with _refusals:
            spool.tick()


class _BadLine(Exception):
    pass


def _parse_line(line: bytes) -> tuple[str, float | None, str | None]:
    try:
        obj: Any = json.loads(line)
    except ValueError as e:  # JSONDecodeError and UnicodeDecodeError are ValueErrors
        raise _BadLine(f"not JSON: {e}") from e
    if not isinstance(obj, dict):
        raise _BadLine("expected a JSON object")
    sensor_id, value, unit = obj.get("sensor_id"), obj.get("value"), obj.get("unit")
    if not isinstance(sensor_id, str) or not sensor_id:
        raise _BadLine("sensor_id must be a non-empty string")
    if value is not None and (isinstance(value, bool) or not isinstance(value, int | float)):
        raise _BadLine("value must be a number or null")
    if unit is not None and not isinstance(unit, str):
        raise _BadLine("unit must be a string or null")
    for name, text in (("sensor_id", sensor_id), ("unit", unit)):
        try:
            if text is not None:
                text.encode()
        except UnicodeEncodeError as e:  # a lone surrogate, from a "\ud800" escape
            raise _BadLine(f"{name} is not valid UTF-8 text") from e
    try:
        return sensor_id, None if value is None else float(value), unit
    except OverflowError as e:  # a JSON integer too big for a float
        raise _BadLine("value is out of range") from e


def _run_stdin(spool: Spool, stop: threading.Event, counters: _Counters) -> None:
    """JSON lines: {"sensor_id": "t1", "value": 21.5, "unit": "C"}. Ends at EOF."""
    fd = sys.stdin.fileno()
    sel = selectors.DefaultSelector()
    sel.register(fd, selectors.EVENT_READ)
    pending = b""

    def handle(line: bytes) -> None:
        if not line.strip():
            return
        try:
            sensor_id, value, unit = _parse_line(line)
        except _BadLine as e:
            _note_bad_line(counters, e)
            return
        _write(spool, counters, sensor_id, value, unit)

    # Poll with a timeout so SIGTERM and idle batch commits are noticed promptly.
    while not stop.is_set():
        if sel.select(timeout=0.2):
            chunk = os.read(fd, 65536)
            if not chunk:
                break
            *lines, pending = (pending + chunk).split(b"\n")
            for line in lines:
                handle(line)
        with _refusals:
            spool.tick()
    handle(pending)
    sel.close()


def _cmd_run(config: Config, _args: argparse.Namespace) -> int:
    stop = threading.Event()
    received_signal: list[int] = []

    def on_signal(signum: int, _frame: object) -> None:
        # Nothing else here: logging takes locks, and a signal landing while the
        # main thread holds one would deadlock.
        received_signal.append(signum)
        stop.set()

    signal.signal(signal.SIGTERM, on_signal)
    signal.signal(signal.SIGINT, on_signal)

    counters = _Counters()
    spool = Spool(config)
    log.info(
        "running: buffer %s (%s), sink %s, source %s",
        config.buffer_path,
        spool.buffer_id,
        config.sink.type,
        config.source.type,
    )
    try:
        if config.source.type == "fake":
            _run_fake(spool, config, stop, counters)
        else:
            _run_stdin(spool, stop, counters)
            if not stop.is_set():
                # Input ended: deliver it all now rather than on the next run.
                budget = config.shipper.stop_timeout_s
                if not spool.drain(budget):
                    log.warning(
                        "input ended; not everything shipped in %.0f s, rest stays buffered",
                        budget,
                    )
    finally:
        if received_signal:
            log.info("received %s, shutting down", signal.Signals(received_signal[0]).name)
        stopped = spool.close()
        log.info(
            "stopped: %d readings in, %d discarded at the cap (recorded as gaps), "
            "%d bad input lines%s",
            counters.received,
            spool.discarded,
            counters.bad_lines,
            "" if stopped else "; shipper didn't stop in time, unacked rows resend next run",
        )
    return EXIT_OK


# --- check / status ---------------------------------------------------------------


def _cmd_check(config: Config, _args: argparse.Namespace) -> int:
    ok = True
    print(f"config    ok: {config.file}")
    parent = config.buffer_path.parent
    writable = parent.is_dir() and os.access(parent, os.W_OK)
    ok &= writable
    print(
        f"buffer    {config.buffer_path} ({'directory writable' if writable else 'PROBLEM: directory missing or not writable'})"
    )
    r = config.retention
    print(f"retention {r.policy}, cap {r.max_rows:,} unacked readings")
    print(f"sink      {config.sink.type} -> {config.sink.path}")
    print(f"source    {config.source.type}")
    synced = clock.clock_synced()
    print(
        f"clock     {'synced' if synced else 'NOT synced (readings will be corrected once it is)'} via {clock.SYNC_BACKEND}"
    )
    return EXIT_OK if ok else EXIT_PROBLEM


def _status(config: Config) -> dict[str, object]:
    path = config.buffer_path
    if not path.exists():
        return {"buffer": str(path), "exists": False}
    with Buffer(path) as b:
        counts = b.counts()
        return {
            "buffer": str(path),
            "exists": True,
            "buffer_id": b.buffer_id,
            "pending": counts[PENDING],
            "inflight": counts[INFLIGHT],
            "acked_not_purged": counts[ACKED],
            "unacked": b.unacked(),
            "cap": config.retention.max_rows,
            "policy": str(config.retention.policy),
            "unshipped_gaps": b.unshipped_gaps(),
            # Shipped gaps are purged on the shipper's cadence, so this undercounts history.
            "discarded_in_buffered_gaps": sum(g.count for g in b.gaps()),
            "file_bytes": path.stat().st_size,
        }


def _cmd_status(config: Config, args: argparse.Namespace) -> int:
    status = _status(config)
    if args.json:
        print(json.dumps(status, indent=2))
    elif not status["exists"]:
        print(f"no buffer yet at {status['buffer']}")
    else:
        for key, value in status.items():
            if key != "exists":
                print(f"{key:18} {value:,}" if isinstance(value, int) else f"{key:18} {value}")
    return EXIT_OK


# --- entry point --------------------------------------------------------------------


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="spool", description="Crash-safe store-and-forward buffer"
    )
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("run", help="buffer readings from the configured source and ship them")
    sub.add_parser("check", help="validate the config and the environment")
    status = sub.add_parser("status", help="show what's in the buffer")
    status.add_argument("--json", action="store_true", help="machine-readable output")
    for p in sub.choices.values():
        p.add_argument("config", type=Path, help="path to spool.toml")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    try:
        config = load(args.config)
    except ConfigError as e:
        print(f"spool: {e}", file=sys.stderr)
        return EXIT_CONFIG
    commands = {"run": _cmd_run, "check": _cmd_check, "status": _cmd_status}
    return commands[args.command](config, args)
