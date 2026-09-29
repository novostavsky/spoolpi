"""Cold boot without an RTC, on the Pi: readings taken before NTP sync, and what they ship as.

A Pi without an RTC boots at the last saved clock and steps when NTP syncs. This simulates that
with real timesyncd: NTP off, the clock set 30 days back (the kernel marks it unsynced), SpoolPi
running at SENSORS x 1 Hz, then NTP back on and the real step.

Scenarios:
  online   jsonl sink, so the uplink is up before sync: every pre-sync reading ships at once.
  offline  MQTT sink whose broker only starts once the clock has synced (uplink and NTP come
           back together, as on a cellular link); needs bench/pi/install_test_tools.sh.

Checks, from the delivered records: how many shipped as ts_quality 0 / 1 / 2, the wall-clock
error of each group (against the post-sync offset wall - mono), and time to sync.

Needs passwordless sudo for `timedatectl set-ntp *` and `date -s *` (docs/hardware.md).
Always turns NTP back on at the end.

Usage: python bench/pi/clock_coldboot.py online|offline [pre_sync_s] [post_sync_s] [sensors]
       default: 60 60 10
"""

from __future__ import annotations

import json
import statistics
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))  # for tests.harness

from spoolpi.core import clock

BACK_S = 30 * 86400
CONFIG = """\
[buffer]
path = "buffer.db"
[retention]
policy = "drop_oldest"
max_rows = 1000000
"""


def sudo(*args: str) -> None:
    subprocess.run(["sudo", "-n", *args], check=True, capture_output=True)


def wait_for(cond: object, timeout: float, what: str) -> float:
    t0 = time.monotonic()
    while not cond():  # type: ignore[operator]
        if time.monotonic() - t0 > timeout:
            raise TimeoutError(what)
        time.sleep(0.2)
    return time.monotonic() - t0


class Feeder:
    """Writes SENSORS readings a second to `spoolpi run` on stdin."""

    def __init__(self, proc: subprocess.Popen[str], sensors: int) -> None:
        self.proc, self.sensors, self.n = proc, sensors, 0
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def _run(self) -> None:
        assert self.proc.stdin is not None
        while not self._stop.is_set():
            for s in range(self.sensors):
                self.proc.stdin.write(json.dumps({"sensor_id": f"s{s}", "value": self.n}) + "\n")
            self.proc.stdin.flush()
            self.n += 1
            self._stop.wait(1)

    def stop(self) -> None:
        self._stop.set()
        self._thread.join()


def report(records: list[dict[str, object]], sync_s: float, written: int) -> None:
    readings = {(r["buffer_id"], r["seq"]): r for r in records if r["type"] == "reading"}
    rows = list(readings.values())
    synced = [r for r in rows if r["ts_quality"] == 2]
    if not synced:
        print("no synced readings delivered; can't compute the true offset")
        return
    # After sync, wall - mono is the true offset (to within timesyncd's slew).
    offset = statistics.median(int(r["wall_ns"]) - int(r["mono_ns"]) for r in synced)  # type: ignore[call-overload]
    print(
        f"written {written}, delivered {len(rows)} distinct readings; NTP synced {sync_s:.1f} s after enabling"
    )
    for q, name in ((0, "unsynced"), (1, "corrected"), (2, "synced")):
        group = [r for r in rows if r["ts_quality"] == q]
        if not group:
            print(f"  ts_quality {q} ({name}): 0")
            continue
        err = [abs(int(r["wall_ns"]) - int(r["mono_ns"]) - offset) for r in group]  # type: ignore[call-overload]
        print(
            f"  ts_quality {q} ({name}): {len(group)}; wall error median {statistics.median(err) / 1e6:.3f} ms, "
            f"max {max(err) / 1e6:.3f} ms ({max(err) / 86400e9:.2f} days)"
        )


def main() -> None:
    scenario = sys.argv[1]
    pre = float(sys.argv[2]) if len(sys.argv) > 2 else 60
    post = float(sys.argv[3]) if len(sys.argv) > 3 else 60
    sensors = int(sys.argv[4]) if len(sys.argv) > 4 else 10
    exe = Path(sys.executable).with_name("spoolpi")
    try:
        sudo("timedatectl", "set-ntp", "false")
        sudo("date", "-s", f"@{int(time.time()) - BACK_S}")
        wait_for(lambda: not clock.clock_synced(), 30, "the clock still reads as synced")
        print(f"clock set {BACK_S // 86400} days back, unsynced; scenario {scenario}", flush=True)
        with tempfile.TemporaryDirectory() as tmp_s:
            tmp = Path(tmp_s)
            records: list[dict[str, object]] = []
            broker = sub = None
            if scenario == "online":
                sink = f'[sink]\ntype = "jsonl"\npath = "{tmp}/out.jsonl"\n'
            else:
                import paho.mqtt.client as mqtt

                from tests.harness.broker import Broker

                broker = Broker(tmp, persistent=True)
                sink = f'[sink]\ntype = "mqtt"\nhost = "127.0.0.1"\nport = {broker.port}\n'
            (tmp / "spoolpi.toml").write_text(CONFIG + sink)
            proc = subprocess.Popen(
                [str(exe), "run", str(tmp / "spoolpi.toml")],
                stdin=subprocess.PIPE,
                stderr=open(tmp / "spoolpi.log", "w"),  # noqa: SIM115
                text=True,
                cwd=tmp,
            )
            feeder = Feeder(proc, sensors)
            time.sleep(pre)
            sudo("timedatectl", "set-ntp", "true")
            sync_s = wait_for(clock.clock_synced, 300, "NTP didn't sync within 300 s")
            print(f"NTP synced after {sync_s:.1f} s", flush=True)
            if broker is not None:
                broker.start()
                sub = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="coldboot-counter")
                sub.on_message = lambda _c, _u, m: records.append(json.loads(m.payload))
                sub.connect("127.0.0.1", broker.port)
                sub.subscribe("spoolpi/#", qos=1)
                sub.loop_start()
            time.sleep(post)
            feeder.stop()
            assert proc.stdin is not None
            proc.stdin.close()
            proc.wait(timeout=60)
            if sub is not None and broker is not None:
                time.sleep(2)
                sub.loop_stop()
                sub.disconnect()
                broker.stop()
            else:
                records = [json.loads(line) for line in (tmp / "out.jsonl").open()]
            report(records, sync_s, feeder.n * sensors)
    finally:
        sudo("timedatectl", "set-ntp", "true")


if __name__ == "__main__":
    main()
