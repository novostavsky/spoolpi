# Guarantees

What SpoolPi promises about each reading, what can go wrong, and how each claim is checked.
"Record" means a reading or a gap record; both carry a `(buffer_id, seq)` key.

## The life of a reading

1. **`write()`**: the reading is stamped (boot-time clock, wall clock, boot id, clock quality)
   and added to the in-memory batch.
2. **Commit**: the batch is written to the SQLite buffer in one transaction. A commit happens
   when the batch holds `batch.max_rows` readings (default 50), or when `batch.max_delay_s`
   (default 1 s) has passed since the last commit. Sparse writes therefore commit immediately.
3. **Ship**: the shipper thread claims committed rows, sends them to the sink, and marks each
   one acknowledged, rejected, or pending again.
4. **Purge**: acknowledged rows are deleted on a slow cadence.

## What can be lost

| Event | What's lost | Checked by |
|---|---|---|
| Process crash (SIGKILL, OOM kill, Python crash) | Readings in the uncommitted batch: at most `batch.max_rows`, or `batch.max_delay_s` worth | 1,000-cycle SIGKILL test (`tests/test_buffer_crash.py`), on every CI run and at 10,000 cycles nightly |
| `systemctl stop` / SIGTERM | Nothing. Pending readings are committed and the batch in flight is finished (within `shipper.stop_timeout_s`) | `tests/test_cli.py`, `bench/systemd_restart_check.py` |
| Power cut, kernel panic | Recently committed records too, typically the last ~30 s; see the next section | 10 simulated power cuts on a Pi Zero 2 W (`bench/pi/powercut.py`); real plug pulls still to do |
| Buffer full | Nothing silently. The retention policy discards readings and records each discard in a gap record | Hypothesis state machine + 300-cycle SIGKILL test (`tests/test_retention.py`) |
| Sink permanently rejects a record | The record is quarantined in the buffer, not shipped, and reported as a `rejected:sink` gap | `tests/test_poison.py` |

### Power cuts

The buffer runs SQLite in WAL mode with `synchronous=NORMAL`. A commit is durable against a
**process** crash as soon as it returns. It becomes durable against a **power** cut only when
the write-ahead log is next fsynced, and SQLite doesn't fsync on every commit in this mode. The
file itself is never corrupted: on restart SQLite discards the unsynced tail of the log.

Two things fsync the log, and together they bound the loss:

- Every `seq` block reservation, one per 1,000 records by default, is committed with
  `synchronous=FULL`. That fsyncs everything written before it. The reason is that a power cut
  must never roll back a reservation whose numbers already shipped, since they would then be
  reissued.
- Every WAL checkpoint (about every 4 MB of log) fsyncs too.

Separately, the kernel writes dirty pages out within about 30 seconds on its own.

**Design bound:** a power cut loses at most the records committed since the last reservation
(up to ~1,000), plus the uncommitted batch, and usually only the last ~30 seconds. Readings
that roll back are simply gone. Records that had shipped but whose acknowledgement rolled back
are sent again under the same key.

**Measured** (2026-09-29, [`hardware.md`](hardware.md#power-cuts-benchpipowercutpy)): 10
simulated power cuts on a Raspberry Pi Zero 2 W writing 10 readings/s to an SD card. Each cut
reset the Pi without syncing, so everything in the page cache was lost.
- Every cut lost committed readings: 44 to 352, median 253. That's the last 4–35 s.
- What survived was what the kernel had written back on its 30 s timer, or everything up to the
  last seq reservation, whichever was later. No cut came near the ~1,000-record bound.
- The database passed `PRAGMA integrity_check` after every cut.

Two limits on that result: the simulated cut doesn't drop the SD card's own write cache, as a
real plug pull can; and the run stopped after 10 cuts, when the Pi didn't come back from the
11th reset.

## What can be duplicated

Delivery is **at-least-once**. A record is sent again when the process dies (or a power cut
hits) after the sink stored it but before the acknowledgement was recorded, or when a send
times out and the sink stored the data anyway.

- A re-sent record always carries its original `(buffer_id, seq)`, so a receiver that stores
  records under `UNIQUE (buffer_id, seq)` deduplicates exactly.
- A reading is **never** sent under two different keys. That's the kind of duplicate no
  receiver could detect.
- In the Week 0 rerun (`docs/motivation.md`), 0.23% of records were re-sent across 300 kills,
  and **0** duplicates remained after deduplicating by key.

Keys are unique per buffer file:
- `seq` comes from a persisted high-water mark, reserved in blocks, and is never reissued (checked
  across 100 kill/restart cycles in `tests/test_identity_crash.py`).
- `buffer_id` is a fresh UUID whenever a buffer file is created, so deleting and recreating the
  buffer can't collide with keys already shipped.

Gap records are keyed the same way. A gap's count can grow while it's unsent. Once it has a
`seq` it never changes, so a resent gap always carries the same count.

## Order

Within one buffer, `seq` increases with write order, across restarts too. Use it, not
timestamps, to order a device's records: clocks jump and `seq` doesn't. Records are shipped
roughly in `seq` order, but retries and partial acknowledgements can reorder delivery.

## Time

Every reading carries three clocks:

- **`mono_ns`:** `CLOCK_BOOTTIME`. Accurate within one boot and meaningless across boots.
- **`boot_id`:** identifies the boot, so `mono_ns` values can be compared within it.
- **`wall_ns`:** the wall clock, which may be wrong.

`ts_quality` says how far to trust `wall_ns`:

| `ts_quality` | Meaning |
|---|---|
| 2 (synced) | the clock was NTP-synchronised when the reading was taken |
| 1 (corrected) | sampled before sync; `wall_ns` was recomputed at ship time from `mono_ns` and the offset measured once the clock synced (same boot only) |
| 0 (unsynced) | sampled before sync and shipped uncorrected: `wall_ns` may be 1970 or simply wrong |

Correction happens when a reading is shipped, so a reading ships as 0 when:
- it was **shipped before the clock synced** (the uplink came up before NTP did, as on a LAN
  with a local broker); or
- the device **rebooted** before syncing, because the correction needs the offset measured in
  the same boot.

A receiver can still fix quality-0 readings itself. `mono_ns` and `boot_id` are exact, so any
synced reading from the same boot gives the offset: `wall ≈ mono_ns + (wall_ns − mono_ns)` of
that synced reading.

Sync is read from the kernel (`adjtimex`), with `timedatectl` as a fallback. The correction was
verified against an injected clock offset in a real Linux time namespace, accurate to within
26 ns. It has not yet been checked against a real NTP step on a Pi (`bench/pi/clock_coldboot.py`
is ready to run).

## What SpoolPi does not promise

- **Durability at the sink.** An acknowledgement means the sink said it has the data: a PUBACK
  from the MQTT broker, a 2xx from the HTTP server, an fsync for the jsonl sink. An MQTT broker
  that loses messages after PUBACK loses them. The reference consumer only acknowledges after
  its Postgres commit.
- **Multiple writers.** One process writes to a buffer. Two processes writing the same buffer
  file aren't supported.
- **Encryption at rest.** The buffer is a plain SQLite file.
