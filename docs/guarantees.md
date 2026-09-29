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
| Power cut, kernel panic | With `durability = "power"` (the default): the uncommitted batch, as for a crash. With `"process"`: also the commits of roughly the last 30 s. See the next section | 25 simulated power cuts and 5 real plug pulls on a Pi Zero 2 W with `"power"`, and 10 simulated cuts with `"process"` (`bench/pi/powercut.py`) |
| Buffer full | Nothing silently. The retention policy discards readings and records each discard in a gap record | Hypothesis state machine + 300-cycle SIGKILL test (`tests/test_retention.py`) |
| Sink permanently rejects a record | The record is quarantined in the buffer, not shipped, and reported as a `rejected:sink` gap | `tests/test_poison.py` |

### Power cuts

The buffer runs SQLite in WAL mode. `[buffer] durability` decides when a commit is safe from a
power cut. In both modes, the file itself is never corrupted: on restart SQLite discards any
unsynced tail of the log.

**`durability = "power"` (the default)** uses `synchronous=FULL`. Every commit fsyncs the log
before it returns, so a committed reading survives a power cut exactly as it survives a crash.
The same holds for the acknowledgements and gap records written in those commits. **What a
power cut loses:** the uncommitted batch, at most `batch.max_rows` readings or
`batch.max_delay_s` worth.

**`durability = "process"`** uses `synchronous=NORMAL`. A commit is durable against a **process**
crash as soon as it returns. It becomes durable against a **power** cut only when the log is
next fsynced, and in this mode SQLite doesn't fsync on every commit. Two things fsync the log,
and together they bound the loss:

- Every `seq` block reservation, one per 1,000 records by default, is committed with
  `synchronous=FULL`. That fsyncs everything written before it. The reason is that a power cut
  must never roll back a reservation whose numbers already shipped, since they would then be
  reissued.
- Every WAL checkpoint (about every 4 MB of log) fsyncs too.

Separately, the kernel writes dirty pages out within about 30 seconds on its own.

**Design bound for `"process"`:** a power cut loses at most the records committed since the
last reservation (up to ~1,000), plus the uncommitted batch, and usually only the last ~30
seconds. Readings that roll back are simply gone. Records that had shipped but whose
acknowledgement rolled back are sent again under the same key.

**Measured** (2026-09-29, [`hardware.md`](hardware.md#power-cuts-benchpipowercutpy)) on a
Raspberry Pi Zero 2 W writing 10 readings/s to an SD card. Each simulated cut reset the Pi
without syncing, so everything in the page cache was lost.

| | `"power"` (25 cuts) | `"process"` (10 cuts) |
|---|---|---|
| Committed readings lost per cut | **0** | 44–352 (median 253): the last 4–35 s |
| Integrity check after the cut | ok 25/25 | ok 10/10 |

Under `"process"`, what survived was what the kernel had written back on its 30 s timer, or
everything up to the last seq reservation, whichever was later. No cut came near the
~1,000-record bound.

**Real plug pulls:** a simulated cut keeps the SD card powered. So the last check was 5 real
plug pulls under `"power"`:
- 0 committed readings lost;
- integrity ok every time;
- every pull's readings stored gap-free.

One limit remains: that's one card. A card that acknowledges writes before they're durable
would lose recent commits even under `"power"`. Run `bench/pi/powercut.py --manual` on your own
hardware to check.

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

Correction happens in two places.

**1. On the device, when a reading ships.** The correction needs the offset measured once the
clock synced, in the same boot. So a shipper that starts with the clock unsynced **holds
readings** for up to `[shipper] hold_unsynced_s` (default 120 s). Gap records still ship. As
soon as the clock syncs, the held readings ship as `ts_quality = 1`. A reading still ships as 0
when:
- the clock didn't sync within the hold (no NTP, no network), or the hold is set to 0; or
- the device **rebooted** before its clock synced.

**2. At the receiver, afterwards.** `mono_ns` and `boot_id` are exact, so any trusted reading of
the same boot gives the offset: `offset = wall_ns − mono_ns`. The reference consumer does this
automatically:
- it keeps each boot's offset in `spoolpi_boot_clocks`, from the earliest trusted reading;
- when the offset is known, whether the quality-0 readings came earlier or later, it sets their
  `wall_ns = mono_ns + offset` and `ts_quality = 1`;
- it keeps the device's original value in `wall_ns_device`.

The same offset gives a gap its wall time:

```sql
SELECT g.*, to_timestamp((g.from_mono_ns + c.offset_ns) / 1e9) AS from_ts,
            to_timestamp((g.to_mono_ns   + c.offset_ns) / 1e9) AS to_ts
FROM spoolpi_gaps g JOIN spoolpi_boot_clocks c USING (boot_id);
```

A reading stays at 0 only if its whole boot produced no trusted reading at all.

Sync is read from the kernel (`adjtimex`), with `timedatectl` as a fallback. The correction was
verified against an injected clock offset in a real Linux time namespace, accurate to within
26 ns. On a Raspberry Pi Zero 2 W, it corrected readings to within 3 µs across a real 30-day
step by `systemd-timesyncd` ([`hardware.md`](hardware.md#clock-without-an-rtc-benchpiclock_coldbootpy)).
Before the hold existed, the same test showed why it's needed: with the uplink up before sync,
640 of 650 pre-sync readings shipped 30 days wrong. The hold is covered by
`tests/test_shipper.py`, and the consumer's correction by `tests/test_consumer.py` against a
real PostgreSQL. On the Pi, with the hold at its default, the same scenario shipped all 650
corrected.

## What SpoolPi does not promise

- **Durability at the sink.** An acknowledgement means the sink said it has the data: a PUBACK
  from the MQTT broker, a 2xx from the HTTP server, an fsync for the jsonl sink. An MQTT broker
  that loses messages after PUBACK loses them. The reference consumer only acknowledges after
  its Postgres commit.
- **Multiple writers.** One process writes to a buffer. Two processes writing the same buffer
  file aren't supported.
- **Encryption at rest.** The buffer is a plain SQLite file.
