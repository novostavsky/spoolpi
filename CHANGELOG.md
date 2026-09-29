# Changelog

## 0.1.0 (unreleased)

First release. It works end to end on a device, from the sensor to Postgres, and is tested
against real SIGKILLs, a real MQTT broker and a real database. It hasn't yet been measured on
Raspberry Pi hardware: SD-card fsync latency, power cuts, memory use.

### Buffer and delivery
- SQLite WAL buffer. A SIGKILL loses at most the uncommitted batch (default 50 readings or 1 s),
  checked across 1,000 kills on every CI run.
- At-least-once delivery. Every record is keyed by `(buffer_id, seq)`: `seq` is never reissued,
  and `buffer_id` is new for every buffer file.
- Batching commits on size or age; sparse writers commit immediately.
- A shipper thread with a timeout on every send, 2 immediate retries, then capped exponential
  backoff with jitter; SIGTERM finishes the batch in flight.
- Retention policies `drop_oldest` and `halt_and_alarm` (no default: you choose), with exact
  gap records that ship like data.
- Poison records: a sink can reject records for good. They're quarantined and reported as gaps,
  and a sink rejecting everything is treated as an outage, not as poison.
- Clock handling: `CLOCK_BOOTTIME` + boot id + wall clock on every reading. NTP sync is detected
  via `adjtimex` (with a `timedatectl` fallback), and pre-sync readings are corrected at ship
  time.

### Sinks
- `mqtt` (`spoolpi[mqtt]`): QoS 1, acked on PUBACK, MQTT 5 per-record rejections, TLS,
  password file, topic templates.
- `http` (`spoolpi[http]`): one POST per batch with a per-record accepted/rejected response
  contract, gzip, bearer token, private CA.
- `jsonl`: an fsynced local file.

### Tools
- `spoolpi run | check | status`, TOML config with file:line:fix errors, `spoolpi --version`.
- A systemd unit that survives `systemctl restart` without loss.
- Reference consumer `python -m spoolpi.consumer` (`spoolpi[consumer]`): MQTT → Postgres, exactly
  once via `UNIQUE (buffer_id, seq)`, with a dead-letter table.

### Known limitations
- The power-cut loss bound (~1,000 records) is reasoned, not measured.
- No schema migrations: an upgrade that changes the buffer schema needs an empty buffer.
- Quarantined records can't be re-sent.
- One writer process per buffer.
