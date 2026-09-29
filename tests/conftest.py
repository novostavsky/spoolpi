"""With SPOOLPI_REQUIRE_INTEGRATION=1 (set by ci/run.sh), a test skipped because an
integration tool or dependency is missing fails instead, so CI can't go green by
quietly skipping the broker, database, TLS and time-namespace tests. Without the
variable, those tests skip as usual on machines that lack the tools."""

from __future__ import annotations

import os
from collections.abc import Generator
from typing import Any

import pytest

REQUIRED = os.environ.get("SPOOLPI_REQUIRE_INTEGRATION") == "1"

# Substrings of the skip reasons used for missing tools (see tests/harness and the
# skipif marks), plus pytest.importorskip's "could not import".
_MISSING_TOOL = ("not available", "unavailable", "could not import")


def _missing_tool_reason(report: Any) -> str | None:
    if not (REQUIRED and report.skipped):
        return None
    longrepr = report.longrepr
    reason = str(longrepr[-1]) if isinstance(longrepr, tuple) else str(longrepr)
    return reason if any(m in reason for m in _MISSING_TOOL) else None


def _fail(report: Any, reason: str) -> None:
    report.outcome = "failed"
    report.longrepr = f"SPOOLPI_REQUIRE_INTEGRATION=1, but a required tool is missing: {reason}"


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item: pytest.Item, call: pytest.CallInfo[Any]) -> Generator[None]:
    outcome = yield
    report = outcome.get_result()
    if (reason := _missing_tool_reason(report)) is not None:
        _fail(report, reason)


@pytest.hookimpl(hookwrapper=True)
def pytest_make_collect_report(collector: pytest.Collector) -> Generator[None]:
    # A module-level importorskip skips the whole module at collection time.
    outcome = yield
    report = outcome.get_result()
    if (reason := _missing_tool_reason(report)) is not None:
        _fail(report, reason)
