"""The naive spike -- the implementation everyone writes first.

Reads a fake sensor in a loop, appends to SQLite with default pragmas, and
posts each reading to a local HTTP endpoint immediately -- before the row is
committed, because fsync-per-row felt too slow in early testing, so commits
are batched every COMMIT_BATCH rows instead. That's the realistic bug: on
restart it resumes from MAX(step) in the committed DB, which means every row
that was already POSTed but rolled back by the crash gets read again and
POSTed a second time. Decoupling "durable" from "delivered" without tracking
both is exactly the gap this project exists to close.

Usage: python naive_spike.py <db_path> <events_path> <http_port>
"""

from __future__ import annotations

import http.server
import json
import sqlite3
import sys
import threading
import time
import urllib.request
from pathlib import Path

COMMIT_BATCH = 15


def start_http_server(port: int, events_path: Path) -> http.server.HTTPServer:
    class Handler(http.server.BaseHTTPRequestHandler):
        def do_POST(self) -> None:
            length = int(self.headers.get("Content-Length", 0))
            body = self.rfile.read(length)
            with open(events_path, "a") as f:
                f.write(body.decode() + "\n")
            self.send_response(200)
            self.end_headers()

        def log_message(self, *args: object) -> None:
            pass

    server = http.server.HTTPServer(("127.0.0.1", port), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server


def main() -> None:
    db_path, events_path, port = sys.argv[1], Path(sys.argv[2]), int(sys.argv[3])
    start_http_server(port, events_path)

    conn = sqlite3.connect(db_path)
    conn.execute("CREATE TABLE IF NOT EXISTS readings (step INTEGER PRIMARY KEY, value REAL)")
    conn.commit()

    row = conn.execute("SELECT MAX(step) FROM readings").fetchone()
    n = 0 if row[0] is None else row[0] + 1

    while True:
        value = float(n)
        conn.execute("INSERT INTO readings (step, value) VALUES (?, ?)", (n, value))
        try:
            urllib.request.urlopen(
                f"http://127.0.0.1:{port}",
                data=json.dumps({"step": n, "value": value}).encode(),
                timeout=1,
            )
        except OSError:
            pass
        if n % COMMIT_BATCH == 0:
            conn.commit()
        n += 1
        time.sleep(0.01)


if __name__ == "__main__":
    main()
