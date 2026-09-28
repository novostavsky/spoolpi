# Spool

Crash-safe store-and-forward buffering for edge sensor data, with no runtime dependencies.

Your device samples sensors and writes readings to Spool. Spool keeps them on disk until an
uplink (MQTT, HTTP or a local file) confirms it has them. It survives crashes, reboots, outages
and clock jumps, and when it has to discard data, it says exactly how much.

We killed a sensor-logging process with SIGKILL 300 times (3 seeds × 100) while it wrote and
shipped readings:

| | Lost | Duplicates the receiver can't detect |
|---|---|---|
| The usual approach (SQLite + immediate HTTP POST) | 0 | **11.5%** of readings |
| Spool | **0** | **0** |

The 0.23% of records Spool re-sent after a crash all carried the same `(buffer_id, seq)` key,
so a receiver removes them mechanically. Method and caveats:
[`docs/motivation.md`](docs/motivation.md).

## What it guarantees

- **Crash (SIGKILL, OOM kill, panic):** at most the one uncommitted batch is lost (default: 50
  readings or 1 s of them, whichever comes first). A 1,000-cycle SIGKILL test checks this on
  every CI run.
- **Delivery:** at-least-once. Every record carries a `(buffer_id, seq)` key, so deduplication
  downstream is exact.
- **Full buffer:** never silent. Discarded readings become gap records with exact counts, and
  the gap records ship like data.
- **Clock:** readings taken before NTP sync are re-timestamped once the clock is synced, and
  marked as such.
- **Power cut:** by design at most ~1,000 of the most recent records, but **not yet measured on
  real hardware.**

The details, including the power-cut reasoning, are in
[`docs/guarantees.md`](docs/guarantees.md).

## Install

Needs Linux and Python 3.11+. Install into a virtual environment: Raspberry Pi OS (Bookworm,
Trixie) and Debian enforce PEP 668, so a bare `pip install` is refused.

```sh
uv venv /opt/spool/.venv
uv pip install --python /opt/spool/.venv/bin/python 'spool[mqtt]'   # or [http]; none for jsonl
```

Without uv: `python3 -m venv /opt/spool/.venv && /opt/spool/.venv/bin/pip install 'spool[mqtt]'`.

> **Status:** v0.1 pre-release. It isn't on PyPI yet (and `spool` is taken there, so the
> published name will differ). For now, install from a built wheel:
> `uv pip install dist/spool-*.whl`.

## Use it

**As a daemon**, reading JSON lines on stdin:

```sh
spool check /etc/spool/spool.toml    # validate the config; is the broker reachable?
my-sensor-reader | spool run /etc/spool/spool.toml
spool status /etc/spool/spool.toml   # what's buffered, what was discarded
```

Each input line is `{"sensor_id": "t1", "value": 21.5, "unit": "C"}`. `"value": null` records a
failed read.

**As a library**, with your own sampling loop:

```python
from spool import Spool

with Spool.from_config("/etc/spool/spool.toml") as spool:
    while running:
        spool.write("t1", read_temperature(), "C")   # None records a failed read
        spool.tick()                                  # commit an aged batch when idle
```

A minimal config:

```toml
[buffer]
path = "/var/lib/spool/buffer.db"

[retention]
policy = "drop_oldest"   # required: what to do when the buffer is full ("halt_and_alarm" also works)
max_rows = 1000000       # ~120 MB of disk at ~121 bytes per reading

[sink]
type = "mqtt"
host = "broker.example.org"
```

## Documentation

| | |
|---|---|
| [Guarantees](docs/guarantees.md) | exactly what can be lost, duplicated or delayed, and when |
| [Configuration](docs/configuration.md) | every setting, with defaults |
| [Operations](docs/operations.md) | deploying on a Pi, systemd, sizing, monitoring, alarms, upgrades |
| [Library](docs/library.md) | the Python API: `Spool`, `write`, `tick`, `drain`, errors, threads |
| [Receiving data](#receiving-data) | MQTT topics, the HTTP contract, the Postgres consumer |
| [Motivation](docs/motivation.md) | the crash experiments behind the headline numbers |
| [CI](docs/ci.md) | how Spool itself is tested |
| [Changelog](CHANGELOG.md) | |

## Receiving data

Every record carries `buffer_id` and `seq`. Delivery is at-least-once, so store records with
`UNIQUE (buffer_id, seq)` and ignore duplicates. Records with `"type": "gap"` say how many
readings were discarded and why (`retention:drop_oldest`, `backpressure`, `rejected:sink`).

**MQTT:** one message per record on `spool/{device_id}/{type}/{sensor_id}`, QoS 1.

**HTTP:** one POST per batch, with body `{"device_id": "...", "records": [...]}`
(`Content-Encoding: gzip` if enabled). The response tells Spool what happened:

| Response | Meaning |
|---|---|
| `2xx`, empty body or no `accepted`/`rejected` keys | every record stored |
| `2xx` / `400` / `422` with `{"accepted": [seq...], "rejected": [seq...]}` | per record; unlisted records are retried |
| anything else (`401`, `403`, `413`, `429`, `5xx`, a redirect, a timeout) | the whole batch is retried later |

Put a record in `rejected` only if it can never succeed, for example because it fails
validation. Spool quarantines it and reports it as a gap. Anything systemic should be a non-2xx
error without per-record detail. Rejecting every record of a batch is treated as an outage
anyway, not as poison.

### Reference consumer: MQTT → Postgres

```sh
uv pip install 'spool[consumer]'
export SPOOL_CONSUMER_DSN=postgresql://spool@db/telemetry
python -m spool.consumer --broker broker.example.org:1883 --init-schema
```

It creates the tables in
[`src/spool/consumer/schema.sql`](src/spool/consumer/schema.sql): `spool_readings`, `spool_gaps`
and `spool_dead_letters`, each keyed by `(buffer_id, seq)`. It stores each batch in one
transaction and acknowledges messages to the broker only after the commit, so a crash means
redelivery, never loss, and the key turns redelivery into a no-op.

- **Persistent session:** the consumer subscribes with one, so the broker holds messages while
  the consumer is down. Keep `--client-id` stable.
- **Bad messages:** a message that can never be stored goes to `spool_dead_letters` instead of
  blocking the stream.
- **Broker in-flight limit:** manual acknowledgement means the broker's limit caps the batch
  size. For mosquitto, raise `max_inflight_messages` (default 20) to around `--batch-size`.
- **Querying:** order a device's readings by `seq`, not by time, and treat `ts` as trustworthy
  only where `ts_quality > 0`.

## Development

```sh
uv sync --extra dev                   # pinned by uv.lock
uv run pytest                         # fast suite
uv run pytest -m slow                 # crash suites: 1,000 / 100 / 300 SIGKILL cycles (~7 min)

git config core.hooksPath .githooks   # lint on commit, quick CI on push
bash ci/run.sh                        # full CI: lint, Python 3.11-3.13, package, crash suites
```

See [`docs/ci.md`](docs/ci.md) for the CI stages and the nightly 10,000-cycle run.

## License

Apache-2.0. See [`LICENSE`](LICENSE).
