# Operations

Running SpoolPi on a device: installing, sizing, monitoring, and what to do when something goes
wrong.

## Deploying on a Raspberry Pi (or any systemd Linux)

1. **Install** into a venv; Raspberry Pi OS enforces PEP 668:

   ```sh
   sudo mkdir -p /opt/spoolpi && sudo uv venv /opt/spoolpi/.venv
   sudo uv pip install --python /opt/spoolpi/.venv/bin/python 'spoolpi[mqtt]'   # from a wheel for now
   ```

2. **Configure** `/etc/spoolpi/spoolpi.toml`, starting from
   [`examples/spoolpi.toml`](../examples/spoolpi.toml). Put the buffer under `/var/lib/spoolpi/`, and
   run `/opt/spoolpi/.venv/bin/spoolpi check /etc/spoolpi/spoolpi.toml`.

3. **Run it as a service** with [`contrib/systemd/spoolpi.service`](../contrib/systemd/spoolpi.service):

   ```sh
   sudo cp contrib/systemd/spoolpi.service /etc/systemd/system/
   sudo systemctl daemon-reload && sudo systemctl enable --now spoolpi
   ```

   The unit pipes a producer (whatever prints your readings as JSON lines) into `spoolpi run`.
   Replace `/usr/local/bin/read-sensors` with yours. If you use SpoolPi as a library instead,
   run your own program the same way.

   Details that matter:
   - **No network ordering.** The unit has no `After=network-online.target`, on purpose: SpoolPi
     must start and buffer while offline.
   - **Shutdown.** SIGTERM makes SpoolPi commit what's pending and finish the batch in flight.
     Keep `[shipper] stop_timeout_s` below the unit's `TimeoutStopSec`.
   - **Service user.** It runs as a throwaway user (`DynamicUser=yes`), and `StateDirectory=spoolpi`
     gives it a private, writable `/var/lib/spoolpi`.
   - **Secrets.** A password or token file is passed in with `LoadCredential=`: systemd reads the
     root-only file and gives the service a private copy. Point the config at that copy:

     ```ini
     # in spoolpi.service
     LoadCredential=mqtt-password:/etc/spoolpi/mqtt-password
     ```
     ```toml
     # in spoolpi.toml
     password_file = "/run/credentials/spoolpi.service/mqtt-password"
     ```

4. **Check it** with `journalctl -u spoolpi` and `spoolpi status /etc/spoolpi/spoolpi.toml`.

5. **Run `sync`** after installing, upgrading or editing the config. A power cut loses whatever
   the kernel hasn't written back yet, usually the last ~30 s of writes, and that includes your
   installation. On the test Pi, a reset a few seconds after `uv sync` left an empty package
   `METADATA` file and a broken venv.

### Power cuts

A power cut loses the readings committed in roughly the last 30 seconds (measured on a Pi Zero 2 W:
4–35 s, [`guarantees.md`](guarantees.md#power-cuts)). The buffer itself stays intact. If your
device loses power often, size your expectations to that. Where possible, give it a clean
shutdown path (a UPS HAT, or a supercapacitor with a shutdown signal), because
`systemctl stop` loses nothing.

**The operating system is more fragile than the buffer.** In testing on Raspberry Pi OS trixie, a
power cut shortly after boot left the Pi permanently without Wi-Fi. NetworkManager rewrites its
connection files in `/etc/netplan/` on every boot, about 25–30 s after power-on. The cut landed
before the new contents reached the card, leaving them empty, and every later boot came up with
no network. The window is roughly 25–60 s after power-on, on every boot. SpoolPi's buffer was
intact, but a device that can't connect ships nothing. The full account is in
[`hardware.md`](hardware.md#a-power-cut-after-boot-left-the-pi-without-wi-fi-for-good).

For devices that can lose power:
- **Make the root filesystem read-only** (`sudo raspi-config` → Performance → Overlay File
  System). Writes to `/` then go to RAM and disappear at reboot, so a cut can't damage the system.
  Keep the buffer on a separate writable partition, and point `[buffer] path` at it, or SpoolPi
  loses everything at every reboot.
- **Otherwise, avoid cuts in the first minute after boot** where you can, e.g. with a
  supercapacitor that rides out short drop-outs.
- **Keep `fsck.repair=yes`** in `/boot/firmware/cmdline.txt`; it's on by default. It repairs
  filesystem damage, but can't bring back a file that was saved empty.

**Recovering a Pi that lost its network this way.** Symptoms: it boots (the green LED flickers,
then stops) but never joins the network, and power cycles don't help.
1. Put the card in a Linux machine (a WSL distro works, with `usbipd` for a USB reader) and
   mount the root partition.
2. Check `/etc/netplan/`. If the `90-NM-*.yaml` files are 0 bytes, move them aside.
3. If the Pi was set up with Raspberry Pi Imager, the boot partition has the original settings
   as `network-config`:

   ```sh
   sudo install -m 600 /path/to/bootfs/network-config /path/to/rootfs/etc/netplan/50-cloud-init.yaml
   sync
   ```

   Otherwise, write a netplan file with your Wi-Fi settings.

### Clock

SpoolPi doesn't need the clock to be right. It needs to know *whether* it's right. Keep
`systemd-timesyncd` (or chrony) enabled. Readings taken before the first sync are corrected
after it (`ts_quality = 1`), provided they're still buffered when the clock syncs and the
device hasn't rebooted in between. If the uplink is up before NTP syncs, pre-sync readings ship
at once as `ts_quality = 0`. A Pi without an RTC does this on every boot. The receiver can
correct them from `mono_ns` and `boot_id` ([`guarantees.md`](guarantees.md#time)). `spoolpi check`
shows whether the clock is synced, and how SpoolPi knows (`adjtimex`, or the slower `timedatectl`
fallback).

## Sizing the buffer

A buffered reading takes about **121 bytes** on disk (measured with 10 sensors and a unit
string). On top of the cap, the file holds:
- the write-ahead log, up to ~4 MB with the default `wal_autocheckpoint`;
- acknowledged rows waiting for the next purge (`purge_interval_s` worth).

| Load | Per day | `max_rows` for 1 day offline | Disk |
|---|---|---|---|
| 10 sensors × 1 Hz | 864,000 | 1,000,000 | ~125 MB |
| 10 sensors × 10 Hz | 8,640,000 | 9,000,000 | ~1.1 GB |
| 50 sensors × 1/min | 72,000 | 100,000 | ~16 MB |

The file grows to its high-water mark and stays there. Freed space is reused, and SpoolPi never
runs `VACUUM`, which on an SD card would rewrite the whole file.

## Monitoring

`spoolpi status /etc/spoolpi/spoolpi.toml` (add `--json` for scripts) reports:

| Field | Meaning |
|---|---|
| `pending` | committed, waiting to be sent |
| `inflight` | being sent right now |
| `acked_not_purged` | delivered, waiting for the next purge |
| `unacked` / `cap` | how full the buffer is |
| `unshipped_gaps` | gap records waiting to be sent |
| `discarded_in_buffered_gaps` | readings counted in gap records still in the buffer (shipped gaps are purged, so this isn't a lifetime total; that lives at the receiver) |
| `file_bytes` | buffer file size |

A steadily growing `pending` means the uplink is slower than your sensors, or down.

### Log messages

SpoolPi logs state changes, not individual readings. On a Pi Zero, per-reading logs would cost
more writes than the data itself. So each message below appears once per event, and repeats are
suppressed for 60 seconds.

| Message | What it means | What to do |
|---|---|---|
| `send failed, backing off` | the sink is failing (unreachable, 5xx, timeouts) | nothing if it's brief; check the network and the sink if it persists |
| `buffer reached its cap; drop_oldest is discarding readings` | the uplink has been down long enough to fill the buffer | the discards are counted in gap records; fix the uplink, or raise `max_rows` |
| `buffer reached its cap; halt_and_alarm is discarding readings` | new readings are being refused | same; your program gets `BufferFull` |
| `buffer is below its cap again` | recovered | nothing |
| `sink permanently rejected N readings; quarantined in the buffer` | the receiver refused specific records for good (e.g. validation) | look at the receiver's reason; see *Quarantine* below |
| `sink rejected all N records of a batch; treating it as a failed send` | the receiver refuses everything, which looks systemic (schema, auth, wrong endpoint), so nothing is dropped | fix the receiver; delivery resumes on its own |
| `N hung sends still running; not sending more` | the sink hangs past `send_timeout_s` repeatedly | check the sink |
| `adjtimex unusable on this system, falling back to timedatectl` | the kernel interface didn't pass its checks | harmless; sync checks just become slower |
| `shipper iteration failed` (with a traceback) | an unexpected error; the shipper retries | please report it |

### Quarantine

Records a sink rejected for good stay in the buffer: `state = 3` in the `readings` table. They
are never sent, don't count toward the cap, and only the newest 10,000 are kept. Inspect them
with:

```sh
sqlite3 /var/lib/spoolpi/buffer.db \
  "SELECT seq, sensor_id, value, wall_ns FROM readings WHERE state = 3 ORDER BY id DESC LIMIT 20"
```

There's no supported way to re-send them in v0.1. A manual `UPDATE` would deliver readings the
receiver has already counted as lost in a `rejected:sink` gap, and would skew the cap
accounting.

## Upgrades

The buffer file has a schema version, and SpoolPi refuses to open a version it doesn't know, with
a message naming the file. v0.1 has no migrations. Before upgrading across a schema change, let
the old version ship everything (`spoolpi status` shows `pending` at 0), stop it, and remove the
buffer file.

## The receiving side

See the README's [Receiving data](../README.md#receiving-data) section for the MQTT and HTTP
contracts and the reference consumer. To run the consumer as a service, use
[`contrib/systemd/spoolpi-consumer.service`](../contrib/systemd/spoolpi-consumer.service). Keep its
`--client-id` stable, because the broker holds messages for that session while the consumer is
down.
