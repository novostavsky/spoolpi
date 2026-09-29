"""Memory of the `spoolpi run` daemon at a steady load (Definition of Done: < 30 MB RSS
on a Zero 2 W at 10 sensors x 1 Hz, measured).

Starts `spoolpi run` with the jsonl or MQTT sink, feeds it SENSORS readings per
second on stdin for DURATION seconds, and samples VmRSS / VmHWM / threads from
/proc every few seconds. The MQTT sink publishes to a private local mosquitto
(see bench/pi/install_test_tools.sh), and a subscriber counts what arrives.

Usage: python bench/pi/rss.py [duration_s] [sensors] [hz] [jsonl|mqtt]
       default: 300 10 1 jsonl
"""

from __future__ import annotations

import contextlib
import json
import subprocess
import sys
import tempfile
import threading
import time
from collections.abc import Callable, Iterator
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))  # for tests.harness

CONFIG = """\
[buffer]
path = "buffer.db"
[retention]
policy = "drop_oldest"
max_rows = 1000000
"""
JSONL_SINK = """\
[sink]
type = "jsonl"
path = "out.jsonl"
"""
MQTT_SINK = """\
[sink]
type = "mqtt"
host = "127.0.0.1"
port = {port}
"""


@contextlib.contextmanager
def jsonl_sink(tmp: Path) -> Iterator[tuple[str, Callable[[], int]]]:
    out = tmp / "out.jsonl"
    yield JSONL_SINK, lambda: sum(1 for _ in out.open())


@contextlib.contextmanager
def mqtt_sink(tmp: Path) -> Iterator[tuple[str, Callable[[], int]]]:
    import paho.mqtt.client as mqtt

    from tests.harness.broker import Broker

    with Broker(tmp) as broker:
        received = 0
        lock = threading.Lock()

        def on_message(*_: object) -> None:
            nonlocal received
            with lock:
                received += 1

        sub = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="rss-counter")
        sub.on_message = on_message
        sub.connect("127.0.0.1", broker.port)
        sub.subscribe("spoolpi/#", qos=1)
        sub.loop_start()
        try:
            yield MQTT_SINK.format(port=broker.port), lambda: received
        finally:
            sub.loop_stop()
            sub.disconnect()


def status(pid: int) -> dict[str, int]:
    fields = {}
    for line in Path(f"/proc/{pid}/status").read_text().splitlines():
        key, _, value = line.partition(":")
        if key in ("VmRSS", "VmHWM", "Threads"):
            fields[key] = int(value.split()[0])
    return fields


def main() -> None:
    duration = float(sys.argv[1]) if len(sys.argv) > 1 else 300
    sensors = int(sys.argv[2]) if len(sys.argv) > 2 else 10
    hz = float(sys.argv[3]) if len(sys.argv) > 3 else 1
    sink_type = sys.argv[4] if len(sys.argv) > 4 else "jsonl"
    sink = {"jsonl": jsonl_sink, "mqtt": mqtt_sink}[sink_type]
    with tempfile.TemporaryDirectory() as tmp, sink(Path(tmp)) as (sink_config, delivered_count):
        cfg = Path(tmp) / "spoolpi.toml"
        cfg.write_text(CONFIG + sink_config)
        exe = Path(sys.executable).with_name("spoolpi")
        proc = subprocess.Popen(
            [str(exe), "run", str(cfg)],
            stdin=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True,
            cwd=tmp,
        )
        assert proc.stdin is not None
        samples: list[dict[str, int]] = []
        start = time.monotonic()
        n = 0
        next_sample = start
        while time.monotonic() - start < duration:
            for s in range(sensors):
                proc.stdin.write(
                    json.dumps({"sensor_id": f"s{s}", "value": n * 0.1, "unit": "C"}) + "\n"
                )
            proc.stdin.flush()
            n += 1
            if time.monotonic() >= next_sample:
                samples.append(status(proc.pid))
                next_sample += 5
            time.sleep(1 / hz)
        final = status(proc.pid)
        proc.stdin.close()
        proc.wait(timeout=60)
        time.sleep(2)  # let the last messages reach the subscriber
        delivered = delivered_count()

    rss = [s["VmRSS"] for s in samples]
    print(
        f"sink: {sink_type}; load: {sensors} sensors x {hz:g} Hz for {duration:.0f} s -> "
        f"{n * sensors} readings, {delivered} delivered"
    )
    print(
        f"RSS (MB): start {rss[0] / 1024:.1f}  after 60 s {rss[min(12, len(rss) - 1)] / 1024:.1f}  "
        f"end {rss[-1] / 1024:.1f}  peak (VmHWM) {final['VmHWM'] / 1024:.1f}"
    )
    print(f"threads: {final['Threads']}")
    growth = (rss[-1] - rss[min(12, len(rss) - 1)]) / 1024
    print(f"growth after warm-up: {growth:+.2f} MB over {duration - 60:.0f} s")


if __name__ == "__main__":
    main()
