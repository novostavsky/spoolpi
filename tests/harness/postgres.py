"""A private PostgreSQL server for integration tests.

Uses `pg_ctl` from PATH, or binaries unpacked without root into
~/.local/postgres (apt-get download + dpkg -x). Tests skip when neither exists.
"""

from __future__ import annotations

import glob
import os
import shutil
import subprocess
from pathlib import Path
from typing import Self

from tests.harness.broker import free_port

_LOCAL = Path.home() / ".local" / "postgres" / "root"


def find_postgres() -> tuple[Path, dict[str, str]] | None:
    if pg_ctl := shutil.which("pg_ctl"):
        return Path(pg_ctl).parent, dict(os.environ)
    # Debian/Ubuntu apt packages install pg_ctl here, off PATH (e.g. on CI runners).
    if system := sorted(glob.glob("/usr/lib/postgresql/*/bin/pg_ctl")):
        return Path(system[-1]).parent, dict(os.environ)
    found = sorted(glob.glob(str(_LOCAL / "usr/lib/postgresql/*/bin")))
    if found:
        return Path(found[-1]), dict(
            os.environ, LD_LIBRARY_PATH=str(_LOCAL / "usr/lib/x86_64-linux-gnu")
        )
    return None


class Postgres:
    def __init__(self, workdir: Path) -> None:
        found = find_postgres()
        if found is None:
            raise RuntimeError("postgres not available")
        self._bin, self._env = found
        self._data = workdir / "pgdata"
        self._log = workdir / "postgres.log"
        self.port = free_port()
        self.dsn = f"postgresql://postgres@127.0.0.1:{self.port}/postgres"
        subprocess.run(
            [str(self._bin / "initdb"), "-D", str(self._data), "-U", "postgres"]
            + ["--auth=trust", "-E", "UTF8", "--no-locale"],
            env=self._env,
            check=True,
            capture_output=True,
        )

    def start(self) -> None:
        opts = f"-p {self.port} -c listen_addresses=127.0.0.1 -k {self._data} -c fsync=off"
        subprocess.run(
            [str(self._bin / "pg_ctl"), "-D", str(self._data), "-l", str(self._log)]
            + ["-o", opts, "-w", "start"],
            env=self._env,
            check=True,
            capture_output=True,
        )

    def stop(self) -> None:
        subprocess.run(
            [str(self._bin / "pg_ctl"), "-D", str(self._data), "-m", "fast", "-w", "stop"],
            env=self._env,
            check=False,
            capture_output=True,
        )

    def __enter__(self) -> Self:
        self.start()
        return self

    def __exit__(self, *_exc: object) -> None:
        self.stop()
