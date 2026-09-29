from __future__ import annotations

import json
import shutil
import ssl
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Any

import pytest

pytest.importorskip("httpx")

from spoolpi.core.buffer import REJECTED, Buffer
from spoolpi.core.reading import Reading
from spoolpi.core.retention import REASON_REJECTED
from spoolpi.core.shipper import Shipper
from spoolpi.sinks.base import AckSet, Envelope, SinkError
from spoolpi.sinks.http import HttpSink
from tests.harness.http_server import IngestServer, Reply
from tests.shipping import FAST_BACKOFF, drain, reading


def batch(*seqs: int) -> list[Envelope]:
    return [Envelope("buf", s, Reading("t1", float(s), "C", s, s, "boot")) for s in seqs]


def sink_for(server: IngestServer, **kw: Any) -> HttpSink:
    opts: dict[str, Any] = {"url": server.url, "device_id": "dev1", "timeout_s": 1.0}
    opts.update(kw)
    return HttpSink(**opts)


# --- response contract -----------------------------------------------------------------


def test_plain_2xx_accepts_everything_and_the_request_is_well_formed() -> None:
    with IngestServer() as server:
        sink = sink_for(server, token="s3cret")
        assert sink.send(batch(1, 2)) == AckSet.of([1, 2])
        sink.close()
    [req] = server.requests
    assert req.path == "/ingest"
    assert req.headers["Content-Type"] == "application/json"
    assert req.headers["Authorization"] == "Bearer s3cret"
    assert req.body["device_id"] == "dev1"
    assert [(r["seq"], r["type"], r["value"]) for r in req.body["records"]] == [
        (1, "reading", 1.0),
        (2, "reading", 2.0),
    ]


@pytest.mark.parametrize("status", [200, 207, 400, 422])
def test_per_record_detail_is_honoured(status: int) -> None:
    detail = {"accepted": [1, 99], "rejected": [2]}  # 99 wasn't sent: ignored
    with IngestServer([Reply(status, detail)]) as server:
        sink = sink_for(server)
        assert sink.send(batch(1, 2, 3)) == AckSet.of([1], rejected=[2])  # 3 is retried
        sink.close()


def test_a_2xx_body_without_detail_keys_means_all_accepted() -> None:
    with IngestServer([Reply(200, {"ok": True}), Reply(200, "thanks")]) as server:
        sink = sink_for(server)
        assert sink.send(batch(1)) == AckSet.of([1])
        assert sink.send(batch(2)) == AckSet.of([2])
        sink.close()


@pytest.mark.parametrize(
    ("reply", "match"),
    [
        (Reply(400, "bad request"), "400"),  # no per-record detail: systemic, not poison
        (Reply(401), "check the token"),
        (Reply(403), "check the token"),
        (Reply(413), "lower \\[shipper\\] batch_size"),
        (Reply(429), "429"),
        (Reply(503), "503"),
        (Reply(302, headers={"Location": "http://elsewhere/"}), "302"),  # never followed
        (Reply(200, {"accepted": "all"}), "unreadable"),
        (Reply(200, {"accepted": [1], "rejected": [1]}), "both accepted and rejected"),
    ],
)
def test_everything_else_fails_the_whole_batch(reply: Reply, match: str) -> None:
    with IngestServer([reply]) as server:
        sink = sink_for(server)
        with pytest.raises((SinkError, ValueError), match=match):
            sink.send(batch(1, 2))
        sink.close()


def test_timeout_and_connection_refused_raise() -> None:
    with IngestServer([Reply(delay_s=2)]) as server:
        sink = sink_for(server, timeout_s=0.3)
        with pytest.raises(SinkError, match="Timeout"):
            sink.send(batch(1))
        sink.close()
    dead = HttpSink(url=server.url, device_id="dev1", timeout_s=0.5)
    with pytest.raises(SinkError, match="ConnectError"):
        dead.send(batch(1))
    dead.close()


def test_gzip_bodies_are_decoded_by_the_server() -> None:
    with IngestServer() as server:
        sink = sink_for(server, gzip=True)
        sink.send(batch(1, 2, 3))
        sink.close()
    [req] = server.requests
    assert req.headers["Content-Encoding"] == "gzip"
    assert [r["seq"] for r in req.body["records"]] == [1, 2, 3]


def test_close_waits_for_a_request_in_flight() -> None:
    with IngestServer([Reply(delay_s=0.5)]) as server:
        sink = sink_for(server, timeout_s=2)
        results: list[AckSet] = []
        t = threading.Thread(target=lambda: results.append(sink.send(batch(1))))
        t.start()
        time.sleep(0.1)
        sink.close()  # must not tear the connection out from under the request
        t.join(3)
        assert results == [AckSet.of([1])]
        with pytest.raises(SinkError, match="closed"):
            sink.send(batch(2))


# --- TLS ---------------------------------------------------------------------------------


@pytest.fixture
def self_signed(tmp_path: Path) -> tuple[Path, ssl.SSLContext]:
    if shutil.which("openssl") is None:
        pytest.skip("openssl not available")
    cert, key = tmp_path / "cert.pem", tmp_path / "key.pem"
    subprocess.run(
        ["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "1"]
        + ["-keyout", str(key), "-out", str(cert), "-subj", "/CN=localhost"]
        + ["-addext", "subjectAltName=DNS:localhost"],
        check=True,
        capture_output=True,
    )
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ctx.load_cert_chain(cert, key)
    return cert, ctx


def test_https_with_a_private_ca(self_signed: tuple[Path, ssl.SSLContext]) -> None:
    cert, ctx = self_signed
    with IngestServer(tls=ctx) as server:
        trusted = sink_for(server, ca_file=cert)
        assert trusted.send(batch(1)) == AckSet.of([1])
        trusted.close()
        untrusted = sink_for(server)
        with pytest.raises(SinkError, match="ConnectError"):  # certificate verify failed
            untrusted.send(batch(2))
        untrusted.close()


# --- end to end ---------------------------------------------------------------------------


def test_end_to_end_with_poison_records_and_a_flaky_server(tmp_path: Path) -> None:
    accepted: dict[int, float] = {}
    gaps: list[dict[str, Any]] = []

    def validating(body: dict[str, Any]) -> Reply:
        ok, bad = [], []
        for r in body["records"]:
            if r["type"] == "reading" and r["value"] % 50 == 7:
                bad.append(r["seq"])  # fails server-side validation, forever
            else:
                ok.append(r["seq"])
                if r["type"] == "reading":
                    accepted[r["seq"]] = r["value"]
                else:
                    gaps.append(r)
        return Reply(422 if bad else 200, {"accepted": ok, "rejected": bad})

    flaky = [Reply(503), Reply(500), validating, Reply(429), validating]
    db = tmp_path / "b.db"
    with Buffer(db) as b:
        b.append([reading(i) for i in range(300)])
    with IngestServer(flaky, default=validating) as server:
        sink = sink_for(server)
        shipper = Shipper(db, sink, batch_size=40, backoff=FAST_BACKOFF, poll_interval_s=0.02)
        shipper.start()
        try:
            drain(db, timeout_s=30)
        finally:
            shipper.stop(timeout_s=5)
    poison = [i for i in range(300) if i % 50 == 7]
    assert shipper.stats.rejected == len(poison)
    assert sorted(accepted.values()) == [float(i) for i in range(300) if i not in poison]
    assert {g["reason"] for g in gaps} == {REASON_REJECTED}
    assert sum(g["count"] for g in gaps) == len(poison)
    with Buffer(db) as b:
        assert b.counts()[REJECTED] == len(poison)


def test_cli_run_over_http(tmp_path: Path) -> None:
    (tmp_path / "token").write_text("tok\n")
    with IngestServer() as server:
        cfg = tmp_path / "spoolpi.toml"
        cfg.write_text(
            '[buffer]\npath = "b.db"\n[retention]\npolicy = "drop_oldest"\nmax_rows = 1000\n'
            f'[sink]\ntype = "http"\nurl = "{server.url}"\ntoken_file = "token"\ngzip = true\n'
            '[device]\nid = "cli-dev"\n'
        )
        lines = "".join(json.dumps({"sensor_id": "t", "value": i}) + "\n" for i in range(25))
        run = subprocess.run(
            [sys.executable, "-m", "spoolpi", "run", str(cfg)],
            input=lines,
            capture_output=True,
            text=True,
            timeout=60,
            check=False,
        )
        assert run.returncode == 0, run.stderr
    assert sorted(r["value"] for r in server.records()) == [float(i) for i in range(25)]
    assert {req.body["device_id"] for req in server.requests} == {"cli-dev"}
    assert {req.headers["Authorization"] for req in server.requests} == {"Bearer tok"}
