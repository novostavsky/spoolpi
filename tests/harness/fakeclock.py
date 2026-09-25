"""Clock manipulation for tests.

``FakeClock`` is an in-process ``ClockSource`` whose wall clock and sync flag
are fully scripted. ``run_in_time_namespace`` runs a real child process in a
Linux time namespace with CLOCK_BOOTTIME shifted, so the offset arithmetic can
be checked against the kernel rather than against our own fake.
"""

from __future__ import annotations

import shutil
import subprocess

NS_PER_S = 1_000_000_000


class FakeClock:
    def __init__(
        self,
        *,
        boot_id: str = "fake-boot-1",
        mono_start_ns: int = 5 * NS_PER_S,
        wall_offset_ns: int = 0,
        synced: bool = False,
    ) -> None:
        self._boot_id = boot_id
        self._mono = mono_start_ns
        self.wall_offset_ns = wall_offset_ns  # wall = mono + this
        self.is_synced = synced

    @property
    def boot_id(self) -> str:
        return self._boot_id

    def mono_ns(self) -> int:
        return self._mono

    def wall_ns(self) -> int:
        return self._mono + self.wall_offset_ns

    def synced(self) -> bool:
        return self.is_synced

    def advance(self, ns: int) -> None:
        self._mono += ns

    def ntp_step(self, true_offset_ns: int) -> None:
        """Simulate NTP stepping the wall clock to the correct time and declaring sync."""
        self.wall_offset_ns = true_offset_ns
        self.is_synced = True

    def reboot(self, boot_id: str) -> None:
        self._boot_id = boot_id
        self._mono = 5 * NS_PER_S
        self.is_synced = False


def _unshare_argv(boottime_offset_s: int) -> list[str]:
    return [
        "unshare",
        "--user",
        "--map-root-user",
        "--time",
        "--boottime",
        str(boottime_offset_s),
    ]


def time_namespaces_available() -> bool:
    if shutil.which("unshare") is None:
        return False
    probe = subprocess.run([*_unshare_argv(1), "true"], capture_output=True, check=False)
    return probe.returncode == 0


def run_in_time_namespace(
    argv: list[str], boottime_offset_s: int, **kwargs: object
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(  # type: ignore[call-overload, no-any-return]
        [*_unshare_argv(boottime_offset_s), *argv],
        capture_output=True,
        text=True,
        check=True,
        **kwargs,
    )
