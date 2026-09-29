# Measurements on real hardware

Measured on a **Raspberry Pi Zero 2 W** (Rev 1.0, 4 × Cortex-A53, 415 MB RAM usable) running
Raspberry Pi OS on Debian 13 (trixie), 64-bit kernel, Python 3.13.5, with a 256 GB microSD card.
The scripts are in `bench/pi/`. `bash bench/pi/deploy.sh` pushes the working tree to the Pi,
and `bash bench/pi/run.sh <command>` runs a command there.

## Known issues when testing on a Pi

These affect the test setup, not SpoolPi, but each one cost a test run. They are not reported
upstream.

1. **A power cut 25–60 s after boot can erase the Pi's network configuration** (Raspberry Pi OS
   trixie).
   - **What happens:** NetworkManager rewrites its profiles in `/etc/netplan/90-NM-*.yaml` on
     every boot, about 25–30 s after power-on. A cut before the kernel writes them back leaves
     them 0 bytes, and from then on the Pi boots without network. Power cycles don't help.
   - **Seen:** twice in 16 simulated cuts, before the harness waited for boot. The account is
     under [Power cuts](#a-power-cut-after-boot-left-the-pi-without-wi-fi-for-good).
   - **Avoid it:** before a cut, wait for `systemctl is-system-running --wait`, then run `sync`.
     `bench/pi/powercut.py` does both before every run. For hand plug pulls, wait at least a
     minute after the Pi comes up.
   - **Recover:** restore the profile from Imager's `network-config` on the boot partition
     (steps in [`operations.md`](operations.md#power-cuts)).

2. **Anything written in the last ~30 s is at risk, including your own setup.** A reset seconds
   after `uv sync` left an empty `METADATA` file and a broken venv. `bench/pi/deploy.sh` ends
   with `sync`; do the same after any manual change on the Pi.

3. **The system journal doesn't survive a reboot.** This image logs to RAM only
   (`Storage=volatile`), so a boot that fails leaves no journal behind.
   `/var/log/cloud-init.log`, which is written on every boot, was the useful timeline. For
   long test campaigns, consider `Storage=persistent` in `/etc/systemd/journald.conf`. That
   means more writes, but also evidence.

4. **Diagnosing a card on Windows needs `usbipd`.** `wsl --mount` refuses USB card readers.
   `usbipd bind` + `usbipd attach --wsl` passes the reader through. Then, to keep evidence
   intact:
   - lock the device with `blockdev --setro`;
   - check with `e2fsck -fn`;
   - mount with `-o ro,noload`.

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
3. NTP on. The clock read as synced at the first check after `timedatectl set-ntp true`
   returned (on a LAN with internet), so this doesn't measure a realistic time-to-sync.
4. Another 60 s of readings.

The wall-clock error of each delivered reading is measured against the post-sync offset
(`wall_ns − mono_ns`):

| Scenario | ts 0 (unsynced) | ts 1 (corrected) | ts 2 (synced) |
|---|---|---|---|
| **online**: uplink up before sync (jsonl) | **640, all 30 days wrong** | 10, error 3 µs | 590, error ≤ 5 µs |
| **offline**: uplink only after sync (MQTT, broker started after the step) | **0** | 650, error ≤ 3 µs | 590, error ≤ 8 µs |
| **online, with `hold_unsynced_s = 120`** (the default since 09-29) | **0** | 650, error 3 µs | 590, error ≤ 89 µs |

- **Correction is exact:** 3 µs against a real 30-day NTP step. It applies whenever the readings
  are still buffered at sync time.
- **Readings shipped before sync stay wrong.** Without the hold, with the uplink up first,
  every pre-sync reading except the last unsent batch shipped 30 days off, marked
  `ts_quality = 0`. On an RTC-less Pi on a LAN, that's every boot until timesyncd syncs.
- **The hold fixes it.** With `[shipper] hold_unsynced_s` at its default, the same scenario
  shipped all 650 pre-sync readings corrected, to 3 µs. See the time section in
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

That run stopped at the 5th reset, when the Pi lost its network (next section). After the repair,
the run resumed with the harness waiting for each boot to finish and syncing first.

### `synchronous=FULL`, seed 13 (16 more cuts)

| Cut | Ran | Committed = survived | Lost | Cut | Ran | Committed = survived | Lost |
|---|---|---|---|---|---|---|---|
| 0 | 35 s | 353 | 0 | 8 | 89 s | 892 | 0 |
| 1 | 84 s | 837 | 0 | 9 | 20 s | 199 | 0 |
| 2 | 84 s | 837 | 0 | 10 | 66 s | 661 | 0 |
| 3 | 103 s | 1,024 | 0 | 11 | 30 s | 298 | 0 |
| 4 | 26 s | 265 | 0 | 12 | 39 s | 386 | 0 |
| 5 | 32 s | 320 | 0 | 13 | 55 s | 551 | 0 |
| 6 | 22 s | 221 | 0 | 14 | 101 s | 1,013 | 0 |
| 7 | 31 s | 309 | 0 | 15 | 75 s | 749 | 0 |

Integrity was ok after all 16. The Pi booted back in 40–58 s every time.

### `durability = "power"`, the setting itself, seed 21 (5 more cuts)

The earlier runs set `synchronous=FULL` directly. This one went through the new
`[buffer] durability = "power"` path. Cuts at 24, 84, 78, 60 and 30 s kept all 243, 848, 782,
606 and 298 committed readings. Integrity ok 5/5.

**`NORMAL` vs `FULL`, summed up:**

| | `"process"` = `NORMAL` (10 cuts) | `"power"` = `FULL` (25 cuts) |
|---|---|---|
| Committed readings lost per cut | 44–352 (median 253) | **0** |
| Cuts that lost anything | 10 / 10 | **0 / 25** |
| Integrity failures | 0 | 0 |
| Commit latency, batch 50 (p50 / p99) | 3.8 / 66 ms | 13 / 115 ms |

- **Under `FULL`, every committed reading survived every cut.** That includes cuts of 20–26 s,
  a length that lost everything under `NORMAL`.
- **Only the uncommitted batch can be lost.** At 10 readings/s that's up to 1 s, the same as a
  process crash.
- That matches how `FULL` works: every commit fsyncs the WAL before it returns.

### A power cut after boot left the Pi without Wi-Fi for good

**What happened.** Twice during the power-cut runs, the Pi stopped coming back: after the 11th
reset of the `NORMAL` run, and after the 5th of the `FULL` run on a freshly flashed card. Both
times:
- it never reappeared on the network (no ping, no ARP entry, no DHCP lease);
- the green LED flickered at boot, then went quiet;
- repeated power cycles didn't help, so the damage was stored on the card.

The first card was reflashed without a diagnosis. The second was examined.

**How it was examined.** The card was attached to WSL through `usbipd` and locked read-only at
the block level (`blockdev --setro`), so that nothing would change it during the investigation.
Then:
- `e2fsck -fn` (check only) found a healthy filesystem: a wrong free-block count and an orphan
  flag, both normal after an unclean power-off, and nothing else.
- The partition was mounted `ro,noload`, so the journal wasn't replayed; replaying it would have
  written to the card.
- The system journal had nothing to offer: this image keeps it in memory only
  (`Storage=volatile`).
- `/var/log/cloud-init.log`, which is written on every boot, gave the timeline instead:

| Boot (UTC) | What it was | Wait for the network in cloud-init's `init` stage |
|---|---|---|
| 10:05, 10:07, 10:10, 10:11 | after `FULL` cuts 0–3; came back | ~21 s |
| **10:13** | after the reset for cut 4; didn't come back | **~7 s** |
| 11:13, 11:28 | manual power cycles; didn't come back | ~7 s |

cloud-init's own work was identical in good and bad boots, and `wlan0` existed in both, so the
Wi-Fi driver loaded. The bad boots simply had no connection to wait for.

**The cause.** Both of NetworkManager's connection profiles were empty files:

```
/etc/netplan/90-NM-5098e2cc-….yaml   0 bytes   modified 10:12:01 UTC   (Wi-Fi)
/etc/netplan/90-NM-75a1216a-….yaml   0 bytes   modified 10:11:57 UTC   (Ethernet)
/etc/NetworkManager/system-connections/   empty
```

On Raspberry Pi OS trixie, NetworkManager stores its profiles as netplan YAML in `/etc/netplan/`.
In the boot after cut 3, about 26–30 s after power-on, it rewrote both files. The reset for cut 4
landed seconds later, inside the kernel's ~30 s writeback window. The files' truncation reached
the card and their new contents didn't. Every boot after that started with no network
configuration at all, which is why power cycles didn't help. This is the same writeback window
that costs SpoolPi readings under `synchronous=NORMAL`, hitting an OS file instead.

The first failure had the same symptoms and was almost certainly the same. That card was
reflashed, though, so it's unconfirmed.

**The repair.** Imager's copy of the network settings survives on the boot partition as
`network-config`, already in netplan format. The fix:
1. Move the two empty files to `/var/backups/netplan-broken-2026-09-29/` as evidence.
2. Install `network-config` as `/etc/netplan/50-cloud-init.yaml`, mode 600, since it contains
   the Wi-Fi password.
3. `sync`, and check the card again with `e2fsck -fn`: clean.

**After the repair: the files are rewritten on every boot.**
- **First boot after the repair:** NetworkManager took over `50-cloud-init.yaml`. It removed that
  file and wrote the two `90-NM-*.yaml` profiles again, now 591 and 275 bytes.
- **After a clean `sudo reboot`:** both profiles were rewritten again, about 25–30 s after
  power-on, with identical sizes and no configuration change.

So this isn't a one-off: **every boot of this image has a window, roughly 25–60 s after
power-on, in which a power cut can erase the network configuration.**

**What it means.**
- **For SpoolPi:** nothing changes. Its buffer passed `integrity_check` after every cut, including
  the one that broke the network.
- **For devices in the field:** on this OS, a power cut in the first ~minute after boot can
  disable networking until someone repairs or reflashes the card. A supply that drops out twice
  within a minute is enough. [`operations.md`](operations.md#power-cuts) says how to avoid it and
  how to recover.
- **For the test:** the controller now waits for the boot to finish
  (`systemctl is-system-running --wait`) and runs `sync` before each run. That way a cut measures
  SpoolPi's writes, not the OS's boot-time writes. It also stops when the Pi doesn't return within
  5 minutes, keeping the cuts completed so far.

Still to do: real plug pulls. A sysrq reset keeps the SD card powered, so the card's own write
cache survives; a plug pull doesn't.
