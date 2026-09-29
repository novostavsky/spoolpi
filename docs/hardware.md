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

`spoolpi run` fed 10 sensors × 1 Hz on stdin for 5 minutes (`bench/pi/rss.py 300 10 1 <sink>`):

| | jsonl sink | MQTT sink |
|---|---|---|
| after 60 s | 23.8 MB | 28.0 MB |
| after 5 min | 24.0 MB | 28.2 MB |
| **peak (VmHWM)** | **24.0 MB** | **28.2 MB** (target: < 30 MB) |
| threads | 3 | 4 (+ paho's network loop) |
| delivered | 3,000 / 3,000 | 3,000 / 3,000 |

- **Both meet the target. MQTT is close:** 1.8 MB of headroom. paho and its network thread add
  ~4 MB.
- **Memory grew ~0.2 MB after warm-up in both,** consistent with SQLite's 2 MB page cache
  filling. The seven-day run will confirm there's no leak.
- The MQTT run published to a mosquitto on the Pi itself. `bash bench/pi/install_test_tools.sh`
  unpacks one without root, and the MQTT integration tests pass against it on ARM (14/14).

## Clock without an RTC (`bench/pi/clock_coldboot.py`)

The Zero has no RTC. It boots at the last saved clock and steps when NTP syncs. The test
simulates that with the real `systemd-timesyncd`:
1. NTP off, and the clock set 30 days back (the kernel then reports it unsynced).
2. SpoolPi runs for 60 s at 10 sensors × 1 Hz.
3. NTP on. It stepped the clock within 0.2 s here (on a LAN with internet).
4. Another 60 s of readings.

The wall-clock error of each delivered reading is measured against the post-sync offset
(`wall_ns − mono_ns`):

| Scenario | ts 0 (unsynced) | ts 1 (corrected) | ts 2 (synced) |
|---|---|---|---|
| **online**: uplink up before sync (jsonl) | **640, all 30 days wrong** | 10, error 3 µs | 590, error ≤ 5 µs |
| **offline**: uplink only after sync (MQTT, broker started after the step) | **0** | 650, error ≤ 3 µs | 590, error ≤ 8 µs |

- **Correction is exact:** 3 µs against a real 30-day NTP step. It applies whenever the readings
  are still buffered at sync time.
- **Readings shipped before sync stay wrong.** With the uplink up first, every pre-sync reading
  except the last unsent batch shipped 30 days off, marked `ts_quality = 0`. On an RTC-less Pi on
  a LAN, that's every boot until timesyncd syncs. See the time section in
  [`guarantees.md`](guarantees.md#time).

## Power cuts (`bench/pi/powercut.py`)

The controller runs on the dev machine and drives the Pi over SSH. Each cycle:
1. It starts a writer on the Pi (`bench/pi/powercut_child.py`) at 10 readings/s, with the
   default batching. The writer reports every commit.
2. After a random 5–120 s, it resets the Pi with sysrq `b`: an immediate reboot with no sync
   and no unmount, so the page cache is lost as in a power cut.
3. It checks the reboot really happened (by `boot_id`), then compares the last commit the Pi
   reported with what the buffer actually holds.

It needs this sudo rule on the Pi, narrow on purpose:

```
pi ALL=(root) NOPASSWD: /usr/bin/tee /proc/sysrq-trigger, /usr/bin/systemctl * systemd-timesyncd, /usr/bin/timedatectl set-ntp *, /usr/bin/date -s *, /usr/sbin/reboot
```

### `synchronous=NORMAL` (the current default), seed 11

| Cut | Ran | Committed (reported) | Survived | Lost | Integrity |
|---|---|---|---|---|---|
| 0 | 57 s | 573 | 331 | 242 | ok |
| 1 | 69 s | 694 | 342 | 352 | ok |
| 2 | 111 s | 1,112 | 991 | 121 | ok |
| 3 | 59 s | 584 | 320 | 264 | ok |
| 4 | 63 s | 639 | 309 | 330 | ok |
| 5 | 73 s | 727 | 650 | 77 | ok |
| 6 | 26 s | 265 | 0 | 265 | ok |
| 7 | 64 s | 639 | 342 | 297 | ok |
| 8 | 77 s | 782 | 628 | 154 | ok |
| 9 | 96 s | 969 | 925 | 44 | ok |

- **Every cut lost committed readings:** 44–352, median 253, mean 215. At 10 readings/s that's
  the last 4–35 s.
- **What survives is what the kernel wrote back on its own.** Linux writes dirty pages out once
  they're ~30 s old (`vm.dirty_expire_centisecs = 3000`). Cuts at 57–69 s kept ~310–340
  readings, i.e. the first ~32 s. The 26 s cut kept nothing.
- **The seq reservation works as designed.** Its `FULL` commit at about the 1,000th record fsyncs
  everything before it. Both cuts that ran past it kept exactly 991 readings.
- **No corruption.** `PRAGMA integrity_check` passed after every cut, and SQLite discarded the
  unsynced WAL tail cleanly.
- The design bound (~1,000 records) held with room to spare. The typical loss is the ~30 s
  writeback window, not the bound.

### `synchronous=FULL`, seed 12

| Cut | Ran | Committed (reported) | Survived | Lost | Integrity |
|---|---|---|---|---|---|
| 0 | 60 s | 600 | 600 | 0 | ok |
| 1 | 81 s | 805 | 805 | 0 | ok |
| 2 | 82 s | 816 | 816 | 0 | ok |
| 3 | 21 s | 211 | 211 | 0 | ok |

- **Every committed reading survived every cut.** That includes the 21 s cut, a length that
  lost everything under `NORMAL`.
- Only the uncommitted batch can be lost. At 10 readings/s that's up to 1 s, as for a process
  crash.
- It's only 4 cuts, but they agree with how `FULL` works: every commit fsyncs the WAL before it
  returns.

### The Pi sometimes doesn't come back from a reset

It happened twice: after the 11th reset in the `NORMAL` run, and after the 5th in the `FULL` run.
Both times:
- the Pi dropped off the network (no ping, no ARP or DHCP entry);
- the green LED flickered at boot, then stopped;
- only a manual power cycle brought it back.

The first time, the card was reflashed without a diagnosis. The boot partition was intact, with
`fsck.repair=yes` already set. The cause is not known yet; see the plan's open questions.

The power-cut controller now stops when the Pi doesn't return within 5 minutes, and keeps the
cuts completed so far.

Still to do: real plug pulls. A sysrq reset keeps the SD card powered, so the card's own write
cache survives; a plug pull doesn't.
