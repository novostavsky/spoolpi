"""A scripted HTTP(S) ingest endpoint for sink tests.

Each POST gets the next reply in the script, then ``default``. A reply may be a
function of the decoded request body, for servers that decide per record.
"""

from __future__ import annotations

import gzip
import json
import ssl
import threading
import time
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Self


@dataclass
class Reply:
    status: int = 200
    body: Any = None  # dict/list -> JSON; str -> raw text
    delay_s: float = 0.0
    headers: dict[str, str] = field(default_factory=dict)


ReplyFor = Reply | Callable[[dict[str, Any]], Reply]


@dataclass
class Request:
    path: str
    headers: dict[str, str]
    body: dict[str, Any]


class IngestServer:
    def __init__(
        self,
        script: Iterable[ReplyFor] = (),
        *,
        default: ReplyFor | None = None,
        tls: ssl.SSLContext | None = None,
    ) -> None:
        self.script = list(script)
        self.default: ReplyFor = default if default is not None else Reply()
        self.requests: list[Request] = []
        self._lock = threading.Lock()
        server = self

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self) -> None:
                raw = self.rfile.read(int(self.headers.get("Content-Length", 0)))
                if self.headers.get("Content-Encoding") == "gzip":
                    raw = gzip.decompress(raw)
                body = json.loads(raw)
                with server._lock:
                    server.requests.append(Request(self.path, dict(self.headers), body))
                    reply = server.script.pop(0) if server.script else server.default
                if callable(reply):
                    reply = reply(body)
                time.sleep(reply.delay_s)
                payload = b""
                if isinstance(reply.body, str):
                    payload = reply.body.encode()
                elif reply.body is not None:
                    payload = json.dumps(reply.body).encode()
                try:
                    self.send_response(reply.status)
                    for k, v in reply.headers.items():
                        self.send_header(k, v)
                    self.send_header("Content-Length", str(len(payload)))
                    self.end_headers()
                    self.wfile.write(payload)
                except (BrokenPipeError, ConnectionResetError):
                    pass  # the client timed out and hung up

            def log_message(self, *_args: object) -> None:
                pass

        self._httpd = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self._httpd.daemon_threads = True
        if tls is not None:
            self._httpd.socket = tls.wrap_socket(self._httpd.socket, server_side=True)
        scheme = "https" if tls is not None else "http"
        host = "localhost" if tls is not None else "127.0.0.1"
        self.url = f"{scheme}://{host}:{self._httpd.server_address[1]}/ingest"
        self._thread = threading.Thread(target=self._httpd.serve_forever, daemon=True)

    def records(self) -> list[dict[str, Any]]:
        with self._lock:
            return [r for req in self.requests for r in req.body["records"]]

    def __enter__(self) -> Self:
        self._thread.start()
        return self

    def __exit__(self, *_exc: object) -> None:
        self._httpd.shutdown()
        self._httpd.server_close()
