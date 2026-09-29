# Roadmap

What v0.1 doesn't do, and what may come next. Nothing below is a promise or a date.

## Known limitations of v0.1

| Limitation | Workaround |
|---|---|
| **No buffer schema migrations.** An upgrade that changes the buffer schema needs an empty buffer. | Before upgrading, let the old version ship everything (`spoolpi status` shows `pending` at 0). See [`operations.md`](operations.md#upgrades). |
| **Quarantined records can't be re-sent.** Records a sink rejected for good stay in the buffer, counted in a `rejected:sink` gap. | Inspect them with `sqlite3` ([`operations.md`](operations.md#quarantine)). Don't re-queue them by hand: that would double-count them against their gap. |
| **One writer process per buffer.** | Give each process its own buffer file. |
| **`spoolpi status` counts only discards still in the buffer.** Shipped gap records are purged. | The lifetime total lives at the receiver: sum `count` in `spoolpi_gaps`. |
| **The reference consumer's throughput is unbenchmarked**, and is capped by the broker's in-flight window (mosquitto: 20). | Raise mosquitto's `max_inflight_messages` to about `--batch-size`. |
| **The fake source restarts at 0 on each run.** | It's for demos. Real readings come from stdin or the library. |
| **Power-cut safety is measured on one Pi and one SD card** (25 simulated cuts, 5 real plug pulls, 0 committed readings lost). | Check your own card with `bench/pi/powercut.py --manual` ([`hardware.md`](hardware.md)). |

## Likely next

- **Re-sending quarantined records**, via a "recovered" record type so gap counts stay exact.
- **Buffer schema migrations**, so upgrades don't need an empty buffer.
- **A consumer throughput benchmark**, and batching tuned to it.

## Considered, not planned

Data quality checks, downsampling, multi-process buffers, encryption at rest, Sparkplug B, a
Grafana dashboard, a hardware-abstraction layer for sensors, drift detection, a LoRa sink, a web
UI.
