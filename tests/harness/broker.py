"""A private mosquitto broker for integration tests.

Uses `mosquitto` from PATH, or a copy unpacked without root into
~/.local/mosquitto (apt-get download + dpkg -x). Tests skip when neither exists.
"""

from __future__ import annotations

import os
import shutil
import socket
import subprocess
import time
from pathlib import Path
from typing import Self

_LOCAL = Path.home() / ".local" / "mosquitto" / "root"


def find_mosquitto() -> tuple[str, dict[str, str]] | None:
    if path := shutil.which("mosquitto"):
        return path, dict(os.environ)
    local = _LOCAL / "usr" / "sbin" / "mosquitto"
    if local.exists():
        libs = f"{_LOCAL}/usr/lib/x86_64-linux-gnu:{_LOCAL}/lib/x86_64-linux-gnu"
        return str(local), dict(os.environ, LD_LIBRARY_PATH=libs)
    return None


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


class Broker:
    def __init__(self, workdir: Path, *, acl: str | None = None, persistent: bool = False) -> None:
        """``persistent``: sessions and queued messages survive a stop/start."""
        found = find_mosquitto()
        if found is None:
            raise RuntimeError("mosquitto not available")
        self._bin, self._env = found
        self.port = free_port()
        self._log = workdir / "mosquitto.log"
        conf = [
            f"listener {self.port} 127.0.0.1",
            "allow_anonymous true",
            f"log_dest file {self._log}",
        ]
        if persistent:
            conf += ["persistence true", f"persistence_location {workdir}/"]
        else:
            conf.append("persistence false")
        if acl is not None:
            acl_file = workdir / "acl"
            acl_file.write_text(acl)
            conf.append(f"acl_file {acl_file}")
        self._conf = workdir / "mosquitto.conf"
        self._conf.write_text("\n".join(conf) + "\n")
        self._proc: subprocess.Popen[bytes] | None = None

    def start(self) -> None:
        self._proc = subprocess.Popen(
            [self._bin, "-c", str(self._conf)],
            env=self._env,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            try:
                socket.create_connection(("127.0.0.1", self.port), timeout=0.2).close()
                return
            except OSError:
                time.sleep(0.05)
        raise RuntimeError(f"mosquitto didn't start; see {self._log}")

    def stop(self) -> None:
        if self._proc is not None:
            self._proc.terminate()
            self._proc.wait(timeout=5)
            self._proc = None

    def __enter__(self) -> Self:
        self.start()
        return self

    def __exit__(self, *_exc: object) -> None:
        self.stop()
