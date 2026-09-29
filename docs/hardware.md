# Measurements on real hardware

Measured on a **Raspberry Pi Zero 2 W** (Rev 1.0, 4 × Cortex-A53, 415 MB RAM usable) running
Raspberry Pi OS on Debian 13 (trixie), 64-bit kernel, Python 3.13.5, with a 256 GB microSD card.
The scripts are in `bench/pi/`. `bash bench/pi/deploy.sh` pushes the working tree to the Pi,
and `bash bench/pi/run.sh <command>` runs a command there.

## Test suite on ARM64 (2026-09-29)

The fast suite runs on the Pi too: **216 passed, 20 skipped** (the mosquitto and PostgreSQL
integration tests; those tools aren't installed on the Pi). It passes with no changes for ARM,
including:
- the `adjtimex` struct-layout canaries;
- the injected-offset clock test in an unprivileged time namespace;
- the throughput-under-failure acceptance test.

The first run found a harness bug. On the Zero, starting Python and importing SpoolPi takes
longer than the crash tests' 0.05–0.5 s kill window. So many children died before opening the
buffer, and the checker then created an empty database file itself. Two fixes:
- The harness starts the kill timer when the child reports `ready`. One cycle in ten still
  kills during start-up.
- The checker never creates the file.

## Commit latency on the SD card (`bench/pi/fsync_latency.py`)

150 commits per row, readings from 10 sensors.

| | p50 | p90 | p99 | max | Capacity |
|---|---|---|---|---|---|
| raw 4 KiB write + fsync | 5.4 ms | 7.4 ms | 9.6 ms | 70 ms | |
| commit, `NORMAL`, batch 1 | 0.35 ms | 0.43 ms | 0.52 ms | 11 ms | ~2,800 readings/s |
| commit, `NORMAL`, batch 10 | 1.0 ms | 1.2 ms | 8.9 ms | 109 ms | ~10,000 readings/s |
| **commit, `NORMAL`, batch 50 (default)** | **3.8 ms** | 5.5 ms | 66 ms | 146 ms | ~13,000 readings/s |
| commit, `NORMAL`, batch 200 | 14 ms | 35 ms | 139 ms | 188 ms | ~14,000 readings/s |
| commit, `FULL`, batch 1 | 8.3 ms | 10 ms | 108 ms | 112 ms | ~120 readings/s |
| commit, `FULL`, batch 10 | 9.6 ms | 13 ms | 19 ms | 114 ms | ~1,000 readings/s |
| **commit, `FULL`, batch 50** | **13 ms** | 18 ms | 115 ms | 119 ms | ~3,800 readings/s |
| commit, `FULL`, batch 200 | 23 ms | 30 ms | 123 ms | 145 ms | ~8,900 readings/s |

- **The batch defaults (50 readings / 1 s) are confirmed.** A default commit takes ~4 ms, three
  orders of magnitude of headroom over the target load (10 sensors × 1 Hz = 10 readings/s).
- **A power-safe mode is affordable at that load.** `FULL` fsyncs every commit. At about one
  commit per second, that's ~13 ms of mostly I/O wait per second (~1%).
- **Its costs:**
  - `write()` commits synchronously, so the caller can occasionally stall for ~115 ms (p99).
  - Flushing small writes immediately means more program operations on the card.

## Memory (`bench/pi/rss.py`)

`spoolpi run` with the jsonl sink, fed 10 sensors × 1 Hz on stdin for 5 minutes:

| | RSS |
|---|---|
| after 60 s | 23.8 MB |
| after 5 min | 24.0 MB |
| **peak (VmHWM)** | **24.0 MB** (target: < 30 MB) |

- It ran 3 threads (main, shipper, clock anchor) and delivered all 3,000 readings.
- Memory grew +0.18 MB after warm-up, consistent with SQLite's 2 MB page cache filling. The
  seven-day run will confirm there's no leak.
- The MQTT sink (which adds paho) isn't measured yet.
