# Spool

Crash-safe store-and-forward buffering for edge sensor data, with no runtime dependencies.

The naive approach, SQLite plus an immediate HTTP POST, silently duplicated **13.1%** of
readings across 100 SIGKILL cycles (see [`docs/motivation.md`](docs/motivation.md)). Spool
keeps readings in a SQLite WAL buffer and ships them with at-least-once delivery. Every record
carries a `(buffer_id, seq)` key, so downstream deduplication is exact.

- A SIGKILL loses at most one uncommitted batch (default: 50 readings or 1 s). This is verified
  by a 1,000-cycle crash test.
- Readings taken before NTP sync are re-timestamped once the clock is synced.
- A full buffer never fails silently. Discarded readings become gap records with exact counts,
  and the gap records ship like data.

Status: pre-release (v0.1 in progress, see `spool_implementation-plan.md`). The only sink so far
is `jsonl`; MQTT and HTTP come next.

## Use it

As a daemon, reading JSON lines on stdin:

```sh
my-sensor-reader | spool run spool.toml     # {"sensor_id": "t1", "value": 21.5, "unit": "C"}
spool check spool.toml
spool status spool.toml
```

As a library, with your own sampling loop:

```python
from spool import Spool

with Spool.from_config("spool.toml") as spool:
    while True:
        spool.write("t1", read_temperature(), "C")   # None records a failed read
```

See [`examples/spool.toml`](examples/spool.toml) and
[`contrib/systemd/spool.service`](contrib/systemd/spool.service). `retention.policy` has no
default: you decide what happens when the uplink is down long enough to fill the buffer.

## Development

```sh
uv venv && source .venv/bin/activate
uv pip install -e ".[dev]"
pytest              # fast suite
pytest -m slow      # crash suites: 1,000 / 100 / 300 SIGKILL cycles (~7 min)
```

Raspberry Pi OS Bookworm and Trixie enforce PEP 668, so install into a venv rather than with a
bare `pip install`.
