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

### Clock

SpoolPi doesn't need the clock to be right. It needs to know *whether* it's right. Keep
`systemd-timesyncd` (or chrony) enabled. Readings taken before the first sync are corrected
after it (`ts_quality = 1`), as long as the device hasn't rebooted in between. `spoolpi check`
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
