"""POST each batch as JSON to an HTTP(S) endpoint.

Needs the ``http`` extra (httpx >= 0.27).

Request: one POST per batch, ``Content-Type: application/json``, optionally
gzip-encoded::

    {"device_id": "...", "records": [<wire record>, ...]}

Response contract:

- ``2xx`` with no body, or a body without the keys below: every record accepted.
- ``2xx``, ``400`` or ``422`` with ``{"accepted": [seq, ...], "rejected": [seq, ...]}``
  (either list may be omitted): per-record results. ``rejected`` means the
  record can never succeed; it is quarantined and reported as a gap. Records in
  neither list are retried.
- Anything else (auth, other 4xx, 5xx, timeouts, redirects, which are never
  followed) fails the whole batch, which is retried with backoff. A 400 without
  per-record detail counts as systemic, never as poison.

Receivers must deduplicate on ``(buffer_id, seq)``: delivery is at-least-once.
"""

from __future__ import annotations

import gzip as gzip_module
import json
import ssl
import threading
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Final

from spoolpi.sinks.base import AckSet, Envelope, SinkError, to_wire

_DETAIL_STATUSES: Final = (400, 422)
_HINTS: Final = {
    401: "check the token (sink.token_file)",
    403: "check the token (sink.token_file)",
    413: "the batch is too large for the server; lower [shipper] batch_size",
}


class HttpSink:
    def __init__(
        self,
        *,
        url: str,
        device_id: str,
        timeout_s: float = 5.0,
        token: str | None = None,
        ca_file: Path | None = None,
        verify: bool = True,
        gzip: bool = False,
        close_wait_s: float = 5.0,
        transport: Any = None,
    ) -> None:
        try:  # an optional extra, so imported only when this sink is used
            import httpx
        except ImportError as e:
            raise ImportError(
                "the http sink needs httpx: install with `uv pip install 'spoolpi[http]'`"
            ) from e
        self._httpx = httpx
        self.url = url
        self.device_id = device_id
        self._gzip = gzip
        self._close_wait_s = close_wait_s
        verify_arg: ssl.SSLContext | bool = verify
        if verify and ca_file is not None:
            verify_arg = ssl.create_default_context(cafile=str(ca_file))
        headers = {"Content-Type": "application/json", "User-Agent": "spoolpi"}
        if token is not None:
            headers["Authorization"] = f"Bearer {token}"
        self._client = httpx.Client(
            timeout=timeout_s,
            verify=verify_arg,
            headers=headers,
            follow_redirects=False,
            transport=transport,
        )
        self._cond = threading.Condition()
        self._inflight = 0
        self._closed = False

    @contextmanager
    def _request_slot(self) -> Iterator[None]:
        with self._cond:
            if self._closed:
                raise SinkError("sink is closed")
            self._inflight += 1
        try:
            yield
        finally:
            with self._cond:
                self._inflight -= 1
                self._cond.notify_all()

    def send(self, batch: Sequence[Envelope]) -> AckSet:
        body = json.dumps(
            {"device_id": self.device_id, "records": [to_wire(e) for e in batch]},
            separators=(",", ":"),
        ).encode()
        headers = {}
        if self._gzip:
            body = gzip_module.compress(body, compresslevel=6)
            headers["Content-Encoding"] = "gzip"
        with self._request_slot():
            try:
                response = self._client.post(self.url, content=body, headers=headers)
            except self._httpx.HTTPError as e:
                raise SinkError(f"POST {self.url} failed: {type(e).__name__}: {e}") from e
        return self._interpret(response, {e.seq for e in batch})

    def _interpret(self, response: Any, sent: set[int]) -> AckSet:
        status = int(response.status_code)
        ok = 200 <= status < 300
        if ok or status in _DETAIL_STATUSES:
            detail = self._detail(response)
            if detail is not None:
                accepted, rejected = detail
                return AckSet.of(accepted & sent, rejected & sent)
            if ok:
                return AckSet.of(sent)
        hint = _HINTS.get(status, "")
        raise SinkError(
            f"POST {self.url} returned {status} {response.reason_phrase}"
            + (f": {hint}" if hint else "")
        )

    def _detail(self, response: Any) -> tuple[set[int], set[int]] | None:
        """Per-record results from the body, or None if the body doesn't give any."""
        if not response.content:
            return None
        try:
            body = response.json()
        except ValueError:
            return None
        if not isinstance(body, dict) or ("accepted" not in body and "rejected" not in body):
            return None
        lists = [body.get("accepted", []), body.get("rejected", [])]
        if not all(isinstance(xs, list) and all(_is_seq(x) for x in xs) for xs in lists):
            # The server tried to say something per record and we can't read it:
            # retrying is safe, guessing isn't.
            raise SinkError(f"POST {self.url}: unreadable accepted/rejected lists in response")
        return set(lists[0]), set(lists[1])

    def close(self) -> None:
        with self._cond:
            self._closed = True
            idle = self._cond.wait_for(lambda: self._inflight == 0, timeout=self._close_wait_s)
        # A request the shipper gave up on may still be using a pooled socket.
        # Closing its fd under it could let the OS reuse the number elsewhere.
        if idle:
            self._client.close()


def _is_seq(x: object) -> bool:
    return isinstance(x, int) and not isinstance(x, bool)
