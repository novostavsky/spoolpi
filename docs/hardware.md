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
- The MQTT sink (which adds paho) isn't measured on the Pi yet. On x86 it adds 3.5 MB over jsonl
  (26.6 vs 23.1 MB), so expect ~27.5 MB on the Zero. `bench/pi/rss.py 300 10 1 mqtt` measures
  it against a local mosquitto (`bash bench/pi/install_test_tools.sh` unpacks one without root).

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

**The Pi didn't come back from the 11th reset.** It fell off the network (no ping, no ARP entry)
and needed a manual power cycle. The cause is still unknown, and it may be specific to sysrq
resets. Either way, it ended the run after 10 cuts.

The `FULL` comparison run hasn't happened yet. Following the latency measurements above, `FULL`
should lose at most the uncommitted batch (~1 s).

Still to do: real plug pulls. A sysrq reset keeps the SD card powered, so the card's own write
cache survives; a plug pull doesn't.
