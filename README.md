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

Status: pre-release (v0.1 in progress, see `spool_implementation-plan.md`). Sinks: `mqtt`
(install `spool[mqtt]`), `http` (install `spool[http]`) and `jsonl`.

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
uv venv && source .venv/bin/activate
uv pip install -e ".[dev]"
pytest              # fast suite
pytest -m slow      # crash suites: 1,000 / 100 / 300 SIGKILL cycles (~7 min)
```

Raspberry Pi OS Bookworm and Trixie enforce PEP 668, so install into a venv rather than with a
bare `pip install`.
