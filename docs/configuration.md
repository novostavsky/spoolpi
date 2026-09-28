# Configuration

Spool reads one TOML file, given on the command line (`spool run /etc/spool/spool.toml`) or to
`Spool.from_config(...)`. Relative paths in it are resolved against the file's own directory.
`spool check <file>` validates it without starting anything.

Every error names the file, the line and a fix, for example:

```
/etc/spool/spool.toml:4: retention.policy is required and has no default: decide what happens
when the buffer is full (drop the oldest readings, or stop and alarm)
  fix: add under [retention]: policy = "drop_oldest"   # or "halt_and_alarm"
```

Unknown sections and keys are errors too (with a "did you mean"), so a typo can't silently
fall back to a default. [`examples/spool.toml`](../examples/spool.toml) is a commented, valid
starting point.

Only `[buffer] path`, `[retention] policy` and `max_rows`, and `[sink] type` (plus that sink's
required key) are required. Everything else has a default.

## [buffer]

| Key | Default | Meaning |
|---|---|---|
| `path` | *required* | The SQLite buffer file. Its directory must exist and be writable. Keep it on local storage: WAL mode doesn't work on network filesystems. |
| `wal_autocheckpoint` | `1000` | Pages of write-ahead log before SQLite checkpoints it into the main file (1,000 pages ≈ 4 MB). |

Deleting the buffer file starts a new buffer with a new `buffer_id`. That's safe for
deduplication downstream, but whatever wasn't shipped is gone.

## [retention]

What happens when the uplink is down long enough to fill the buffer. There's deliberately no
default: it's your decision.

| Key | Default | Meaning |
|---|---|---|
| `policy` | *required* | `"drop_oldest"` discards the oldest unsent readings to make room. `"halt_and_alarm"` refuses new readings until the uplink catches up. Either way, every discarded reading is counted in a gap record that ships like data. |
| `max_rows` | *required* | The cap, in unsent readings (pending + in flight). At ~121 bytes per reading, 1,000,000 ≈ 120 MB of disk. |

With `drop_oldest`, readings already being sent are never evicted: the sink may already have
them. With `halt_and_alarm`, `write()` raises `BufferFull`, and the CLI counts refused readings
and carries on.

## [batch]

How readings are grouped into commits. These two keys also set the crash-loss window: a SIGKILL
loses at most the uncommitted batch.

| Key | Default | Meaning |
|---|---|---|
| `max_rows` | `50` | Commit once the batch holds this many readings. |
| `max_delay_s` | `1.0` | Commit once this long has passed since the last commit. A sparse writer therefore commits every reading immediately. |

These defaults are provisional until SD-card fsync latency has been measured on a Pi.

## [shipper]

| Key | Default | Meaning |
|---|---|---|
| `batch_size` | `100` | Records per send. |
| `send_timeout_s` | `10` | A send that takes longer is abandoned and its records retried. Must exceed the sink's own timeouts (checked). |
| `poll_interval_s` | `1` | How often an idle shipper looks for new rows. New commits wake it anyway. |
| `purge_interval_s` | `60` | How often acknowledged rows are deleted from the buffer. |
| `immediate_retries` | `2` | Failures in a row retried with no delay before backing off. At 25% send failure this took throughput from 14% to 63% of normal (`bench/retry_policy.py`). |
| `backoff_initial_s` | `0.5` | First backoff delay after the immediate retries; it doubles on each failure. |
| `backoff_max_s` | `60` | Longest backoff delay. Must be ≥ `backoff_initial_s`. |
| `stop_timeout_s` | `10` | On SIGTERM, how long the shipper gets to finish the batch in flight. Keep it below systemd's `TimeoutStopSec`. |

## [sink]

`type` chooses the sink, and only that sink's keys are accepted: a key belonging to another
sink type is an error.

| Key | Default | Meaning |
|---|---|---|
| `type` | *required* | `"mqtt"`, `"http"` or `"jsonl"`. |

### type = "jsonl"

Appends one JSON line per record to a local file, fsyncing before it acknowledges. Useful for
handing data to another local process, and for testing.

| Key | Default | Meaning |
|---|---|---|
| `path` | *required* | The output file. Resent records appear as repeated lines, so readers deduplicate on `(buffer_id, seq)`. |

### type = "mqtt"

One JSON message per record, QoS 1. A record counts as delivered when the broker's PUBACK
arrives. Needs `spool[mqtt]`.

| Key | Default | Meaning |
|---|---|---|
| `host` | *required* | Broker host name. |
| `port` | `1883` | Broker port (usually 8883 with TLS). |
| `topic` | `"spool/{device_id}/{type}/{sensor_id}"` | Topic template. Fields: `{device_id}`, `{buffer_id}`, `{type}` (`reading` or `gap`), `{sensor_id}` (`_all` for gaps covering all sensors). Values are percent-escaped, so `/`, `+` and `#` in a sensor name can't add levels or wildcards. |
| `client_id` | `"spool-<device id>"` | MQTT client id. |
| `protocol` | `"5"` | `"5"` or `"3.1.1"`. Only MQTT 5 lets the broker reject individual records (not authorized, bad topic, bad payload), which Spool then quarantines instead of retrying forever. |
| `keepalive_s` | `60` | MQTT keepalive. |
| `connect_timeout_s` | `3` | How long a send waits for a connection before failing. |
| `ack_timeout_s` | `5` | How long a send waits for PUBACKs. `connect_timeout_s + ack_timeout_s` must be less than `[shipper] send_timeout_s` (checked). |
| `tls` | `false` | Use TLS. |
| `ca_file` | system CAs | CA certificate for a private broker certificate. |
| `username` | none | Username. |
| `password_file` | none | File containing the password, so the password stays out of the config file. |

### type = "http"

One POST per batch; the receiver's response contract is in the
[README](../README.md#receiving-data). Needs `spool[http]`.

| Key | Default | Meaning |
|---|---|---|
| `url` | *required* | `http://` or `https://` endpoint. Redirects are never followed. |
| `timeout_s` | `5` | Request timeout. Must be less than `[shipper] send_timeout_s` (checked). |
| `token_file` | none | File containing a bearer token, sent as `Authorization: Bearer <token>`. `spool check` warns if it would travel over plain `http://`. |
| `ca_file` | system CAs | CA certificate for a private server certificate. |
| `verify` | `true` | Verify the server's TLS certificate. Turning this off is warned about by `spool check`. |
| `gzip` | `false` | Gzip request bodies (`Content-Encoding: gzip`). Worth it on metered or cellular links. |

## [device]

| Key | Default | Meaning |
|---|---|---|
| `id` | derived from `/etc/machine-id` | Names the device in MQTT topics and payloads. The default is a hash of the machine id (the raw id stays on the device), falling back to the hostname. |

## [source]

Where `spool run` gets readings from. Not used by the library.

| Key | Default | Meaning |
|---|---|---|
| `type` | `"stdin"` | `"stdin"`: JSON lines such as `{"sensor_id": "t1", "value": 21.5, "unit": "C"}`; at end of input Spool delivers everything and exits. `"fake"`: a counting stream, for trying Spool out. Bad input lines are counted and skipped, never fatal. |
| `sensor_id` | `"fake"` | Sensor id for the fake source. |
| `rate_hz` | `10` | Readings per second for the fake source. |
