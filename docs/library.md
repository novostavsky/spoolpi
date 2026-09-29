# Using SpoolPi as a library

You keep your sampling loop: the hardware-specific part stays yours. SpoolPi gives you `write()`.

```python
from spoolpi import BufferFull, SpoolPi

with SpoolPi.from_config("/etc/spoolpi/spoolpi.toml") as spoolpi:
    while running:
        for sensor in sensors:
            try:
                spoolpi.write(sensor.id, sensor.read(), sensor.unit)
            except BufferFull:
                pass  # halt_and_alarm only: already counted in a gap record and logged
        spoolpi.tick()
        time.sleep(1)
```

## `SpoolPi`

| | |
|---|---|
| `SpoolPi.from_config(path)` | Load a [config file](configuration.md) and start. |
| `SpoolPi(config, sink=None)` | Start from a loaded config (`spoolpi.load_config(path)`). Passing `sink` replaces the configured sink with any object implementing the sink protocol. |
| `write(sensor_id, value, unit=None)` | Record one sample, stamped now. `value=None` records a failed read, which still ships. |
| `write_reading(reading)` | Record a fully built `spoolpi.Reading` (your own timestamps, QC flags). |
| `tick()` | Commit the pending batch if it's older than `batch.max_delay_s`. Call it when idle between writes, so readings don't wait for the next write. |
| `flush()` | Commit the pending batch now. |
| `drain(timeout_s)` | Commit, then wait until everything is shipped. Returns `True` if it was. |
| `close(timeout_s=None)` | Commit, let the shipper finish its batch in flight (up to `shipper.stop_timeout_s`), and stop. Unshipped rows stay buffered for next time. Also called by `with`. |
| `buffer_id`, `device_id` | The buffer's UUID and the device id in use. |
| `discarded` | Readings refused under `halt_and_alarm` since start (all counted in gap records). |

What starting does:
- It opens the buffer.
- It returns rows the previous run had in flight to pending, since they will be resent under
  the same keys.
- It starts two background threads: the shipper and the clock anchor.

### Threads

Call `write`, `tick`, `flush`, `drain` and `close` from **the thread that created the SpoolPi**.
The buffer's SQLite connection belongs to that thread and refuses others. To sample from several
threads, send the readings through a `queue.Queue` to that one thread. SpoolPi's own threads never
block `write()`.

### Errors

| Raised by | Exception | Meaning |
|---|---|---|
| `write`, `tick`, `flush` | `spoolpi.BufferFull` | `halt_and_alarm` refused a batch. `.discarded` is how many readings, and they're already counted in a gap record. Don't retry them. `drain` and `close` absorb this instead of raising. |
| `write`, `write_reading` | `ValueError` | Text SpoolPi can't store, e.g. a sensor id containing a lone UTF-16 surrogate. Refused at once, so it can't poison the batch. |
| `SpoolPi.from_config`, `load_config` | `spoolpi.ConfigError` | Names the file, line and fix. |

A non-finite value (`nan`, `inf`) is stored as `None`: a failed read.

## Writing your own sink

A sink is any object with `send` and `close` (`spoolpi.sinks.base.Sink`):

```python
from collections.abc import Sequence

from spoolpi.sinks.base import AckSet, Envelope, SinkError, to_wire


class MySink:
    def send(self, batch: Sequence[Envelope]) -> AckSet:
        try:
            response = post_somewhere([to_wire(e) for e in batch])
        except OSError as e:
            raise SinkError(str(e)) from e  # the whole batch is retried later
        return AckSet.of(
            accepted=[e.seq for e in batch if response.stored(e.seq)],
            rejected=[e.seq for e in batch if response.invalid(e.seq)],
        )

    def close(self) -> None:
        ...


spoolpi = SpoolPi(load_config("spoolpi.toml"), sink=MySink())
```

The contract:
- **`send` receives envelopes** `(buffer_id, seq, payload)`, where `payload` is a `Reading` or a
  `GapRecord`. `to_wire(envelope)` gives the plain-JSON form used by the built-in sinks.
- **Return an `AckSet`:**
  - `accepted` means stored durably;
  - `rejected` means it can never succeed: the record is quarantined and counted in a gap;
  - anything in neither set is retried.
- **Raise for anything systemic** (network, auth, server down) rather than rejecting.
  Rejecting a whole batch of 2+ records is treated as an outage anyway, and nothing is dropped.
- **Delivery is at-least-once:** the same `(buffer_id, seq)` can arrive again, so store it
  idempotently.
- **`send` must tolerate being abandoned.** It runs on its own thread and can be abandoned
  after `shipper.send_timeout_s`, and `close` may then run while it's still going. Don't free
  resources a running `send` still uses; the built-in jsonl and HTTP sinks wait for it.

## Stability

v0.1 supports the names exported from `spoolpi` (`SpoolPi`, `Reading`, `GapRecord`, `BufferFull`,
`ConfigError`, `Policy`, `Retention`, `load_config`) plus the sink protocol in
`spoolpi.sinks.base`. Everything under `spoolpi.core` works, and is how SpoolPi is built and tested,
but may change before 1.0.
