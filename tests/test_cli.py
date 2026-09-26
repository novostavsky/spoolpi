from __future__ import annotations

import json
import signal
import sqlite3
import subprocess
import sys
import time
from pathlib import Path

CONFIG = """\
[buffer]
path = "buf.db"

[retention]
policy = "drop_oldest"
max_rows = 100000

[batch]
max_rows = 10
max_delay_s = 0.1

[shipper]
poll_interval_s = 0.05
backoff_initial_s = 0.01
backoff_max_s = 0.1
stop_timeout_s = 5

[sink]
type = "jsonl"
path = "out.jsonl"

[source]
type = "{source}"
rate_hz = 200
"""


def config(tmp_path: Path, source: str = "stdin") -> Path:
    p = tmp_path / "spool.toml"
    p.write_text(CONFIG.format(source=source))
    return p


def spool(*args: str, **kw: object) -> subprocess.CompletedProcess[str]:
    return subprocess.run(  # type: ignore[call-overload, no-any-return]
        [sys.executable, "-m", "spool", *args],
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
        **kw,
    )


def delivered(tmp_path: Path) -> list[dict[str, object]]:
    out = tmp_path / "out.jsonl"
    return [json.loads(line) for line in out.read_text().splitlines()] if out.exists() else []


def test_check_ok(tmp_path: Path) -> None:
    result = spool("check", str(config(tmp_path)))
    assert result.returncode == 0, result.stderr
    assert "config    ok" in result.stdout
    assert "drop_oldest, cap 100,000" in result.stdout


def test_check_reports_config_error_with_file_line_and_fix(tmp_path: Path) -> None:
    cfg = config(tmp_path)
    cfg.write_text(cfg.read_text().replace('policy = "drop_oldest"\n', ""))
    result = spool("check", str(cfg))
    assert result.returncode == 2
    assert f"{cfg}:4: retention.policy is required" in result.stderr
    assert "fix: add under [retention]" in result.stderr


def test_run_stdin_delivers_everything_at_eof(tmp_path: Path) -> None:
    lines = [
        {"sensor_id": "t1", "value": 21.5, "unit": "C"},
        {"sensor_id": "t1", "value": None},  # a failed read still ships
        "this is not json",
        '{"sensor_id": "t1", "value": 1' + "0" * 400 + "}",  # too big for a float
        '{"sensor_id": "\\ud800", "value": 1}',  # lone surrogate: can't be stored as text
        {"sensor_id": "t2", "value": 3},
    ]
    stdin = "\n".join(x if isinstance(x, str) else json.dumps(x) for x in lines)  # no final newline
    result = spool("run", str(config(tmp_path)), input=stdin)
    assert result.returncode == 0, result.stderr
    got = [(r["sensor_id"], r["value"]) for r in delivered(tmp_path)]
    assert got == [("t1", 21.5), ("t1", None), ("t2", 3.0)]
    assert "3 readings in, 0 discarded" in result.stderr
    assert "3 bad input lines" in result.stderr

    status = spool("status", "--json", str(config(tmp_path)))
    assert json.loads(status.stdout)["pending"] == 0


def test_halt_and_alarm_refusals_are_counted_not_fatal(tmp_path: Path) -> None:
    cfg = config(tmp_path)
    text = cfg.read_text().replace('"drop_oldest"', '"halt_and_alarm"')
    text = text.replace("max_rows = 100000", "max_rows = 5").replace(
        "max_rows = 10", "max_rows = 20"
    )
    text = text.replace("max_delay_s = 0.1", "max_delay_s = 100")
    cfg.write_text(text)
    stdin = "".join(json.dumps({"sensor_id": "t", "value": i}) + "\n" for i in range(50))
    result = spool("run", str(cfg), input=stdin)
    assert result.returncode == 0, result.stderr
    assert "Traceback" not in result.stderr

    records = delivered(tmp_path)
    readings = {r["seq"]: r["value"] for r in records if r["type"] == "reading"}
    gaps = {r["seq"]: r["count"] for r in records if r["type"] == "gap"}
    assert gaps, "refused batches must ship as gap records"
    assert len(readings) + sum(gaps.values()) == 50  # type: ignore[arg-type]
    assert f"50 readings in, {sum(gaps.values())} discarded" in result.stderr  # type: ignore[arg-type]


def test_sigterm_loses_nothing(tmp_path: Path) -> None:
    cfg = config(tmp_path, source="fake")
    proc = subprocess.Popen(
        [sys.executable, "-m", "spool", "run", str(cfg)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    time.sleep(1.5)
    t0 = time.monotonic()
    proc.send_signal(signal.SIGTERM)
    _, stderr = proc.communicate(timeout=15)
    assert proc.returncode == 0, stderr
    assert time.monotonic() - t0 < 5
    assert "received SIGTERM" in stderr

    shipped = {int(r["value"]) for r in delivered(tmp_path)}  # type: ignore[call-overload]
    conn = sqlite3.connect(tmp_path / "buf.db")
    kept = {int(v) for (v,) in conn.execute("SELECT value FROM readings WHERE state != 2")}
    conn.close()
    everything = shipped | kept
    assert shipped, "nothing shipped in 1.5 s"
    # Every value the fake source produced is either delivered or still buffered.
    assert everything == set(range(max(everything) + 1))


def test_status_without_a_buffer(tmp_path: Path) -> None:
    result = spool("status", str(config(tmp_path)))
    assert result.returncode == 0
    assert "no buffer yet" in result.stdout
