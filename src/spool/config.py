"""Config loading and validation.

Config errors are most users' first contact with Spool, so every error names
the file, the line, and a concrete fix. ``retention.policy`` deliberately has
no default: deciding what happens when the buffer is full is the user's call.
"""

from __future__ import annotations

import difflib
import re
import tomllib
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Final

from spool.core.retention import Policy, Retention

SINK_TYPES: Final = ("jsonl",)
PLANNED_SINKS: Final = ("mqtt", "http")
SOURCE_TYPES: Final = ("stdin", "fake")


class ConfigError(Exception):
    def __init__(self, file: Path, line: int | None, problem: str, fix: str) -> None:
        where = f"{file}:{line}" if line is not None else str(file)
        super().__init__(f"{where}: {problem}\n  fix: {fix}")
        self.file = file
        self.line = line
        self.problem = problem
        self.fix = fix


@dataclass(frozen=True, slots=True)
class ShipperConfig:
    batch_size: int
    send_timeout_s: float
    poll_interval_s: float
    purge_interval_s: float
    backoff_initial_s: float
    backoff_max_s: float
    stop_timeout_s: float


@dataclass(frozen=True, slots=True)
class SinkConfig:
    type: str
    path: Path | None


@dataclass(frozen=True, slots=True)
class SourceConfig:
    type: str
    sensor_id: str
    rate_hz: float


@dataclass(frozen=True, slots=True)
class Config:
    file: Path
    buffer_path: Path
    wal_autocheckpoint: int
    retention: Retention
    batch_max_rows: int
    batch_max_delay_s: float
    shipper: ShipperConfig
    sink: SinkConfig
    source: SourceConfig


# --- schema ---------------------------------------------------------------------

_REQUIRED: Final = object()
Check = Callable[[Any], str | None]


def _positive(v: Any) -> str | None:
    return None if v > 0 else "must be greater than 0"


def _one_of(options: tuple[str, ...]) -> Check:
    def check(v: Any) -> str | None:
        return None if v in options else f"must be one of {', '.join(map(repr, options))}"

    return check


def _sink_type(v: Any) -> str | None:
    if v in PLANNED_SINKS:
        return "names a sink that isn't available in this version yet (available: jsonl)"
    return _one_of(SINK_TYPES)(v)


@dataclass(frozen=True, slots=True)
class _Key:
    kind: type | tuple[type, ...]
    default: object = _REQUIRED
    check: Check | None = None
    example: str = ""


_NUMBER: Final = (int, float)

_SCHEMA: Final[dict[str, dict[str, _Key]]] = {
    "buffer": {
        "path": _Key(str, example='path = "/var/lib/spool/buffer.db"'),
        "wal_autocheckpoint": _Key(int, 1000, _positive, "wal_autocheckpoint = 1000"),
    },
    "retention": {
        "policy": _Key(
            str,
            check=_one_of(tuple(p.value for p in Policy)),
            example='policy = "drop_oldest"   # or "halt_and_alarm"',
        ),
        "max_rows": _Key(int, check=_positive, example="max_rows = 1000000"),
    },
    "batch": {
        "max_rows": _Key(int, 50, _positive, "max_rows = 50"),
        "max_delay_s": _Key(_NUMBER, 1.0, _positive, "max_delay_s = 1.0"),
    },
    "shipper": {
        "batch_size": _Key(int, 100, _positive, "batch_size = 100"),
        "send_timeout_s": _Key(_NUMBER, 10.0, _positive, "send_timeout_s = 10"),
        "poll_interval_s": _Key(_NUMBER, 1.0, _positive, "poll_interval_s = 1"),
        "purge_interval_s": _Key(_NUMBER, 60.0, _positive, "purge_interval_s = 60"),
        "backoff_initial_s": _Key(_NUMBER, 0.5, _positive, "backoff_initial_s = 0.5"),
        "backoff_max_s": _Key(_NUMBER, 60.0, _positive, "backoff_max_s = 60"),
        "stop_timeout_s": _Key(_NUMBER, 10.0, _positive, "stop_timeout_s = 10"),
    },
    "sink": {
        "type": _Key(str, check=_sink_type, example='type = "jsonl"'),
        "path": _Key(str, None, example='path = "/var/lib/spool/out.jsonl"'),
    },
    "source": {
        "type": _Key(str, "stdin", _one_of(SOURCE_TYPES), 'type = "stdin"'),
        "sensor_id": _Key(str, "fake", example='sensor_id = "fake"'),
        "rate_hz": _Key(_NUMBER, 10.0, _positive, "rate_hz = 10"),
    },
}

_HEADER = re.compile(r"^\s*\[\s*([A-Za-z0-9_.-]+)\s*\]")
_ASSIGN = re.compile(r"^\s*([A-Za-z0-9_-]+)\s*=")


def _locate(text: str, section: str, key: str | None = None) -> int | None:
    """Line of ``key`` in ``[section]``, or of the section header if key is None."""
    current = ""
    for number, line in enumerate(text.splitlines(), start=1):
        if m := _HEADER.match(line):
            current = m.group(1)
            if key is None and current == section:
                return number
        elif key is not None and current == section and (m := _ASSIGN.match(line)):
            if m.group(1) == key:
                return number
    return None


def _type_name(kind: type | tuple[type, ...]) -> str:
    kinds = kind if isinstance(kind, tuple) else (kind,)
    names = {int: "an integer", float: "a number", str: "a string"}
    return " or ".join(names.get(k, k.__name__) for k in kinds)


class _Loader:
    def __init__(self, file: Path, text: str) -> None:
        self.file = file
        self.text = text

    def error(self, section: str, key: str | None, problem: str, fix: str) -> ConfigError:
        line = _locate(self.text, section, key)
        if line is None and key is not None:
            line = _locate(self.text, section)
        return ConfigError(self.file, line, problem, fix)

    def sections(self, data: dict[str, Any]) -> dict[str, dict[str, Any]]:
        out: dict[str, dict[str, Any]] = {}
        for name, value in data.items():
            if name not in _SCHEMA:
                hint = difflib.get_close_matches(name, _SCHEMA, n=1)
                fix = (
                    f"did you mean [{hint[0]}]?"
                    if hint
                    else f"valid sections: {', '.join(_SCHEMA)}"
                )
                raise self.error(name, None, f"unknown section [{name}]", fix)
            if not isinstance(value, dict):
                raise self.error(
                    name, None, f"{name} must be a [{name}] table", f"write it as [{name}]"
                )
            out[name] = value
        return out

    def section(self, name: str, raw: dict[str, Any]) -> dict[str, Any]:
        schema = _SCHEMA[name]
        for key in raw:
            if key not in schema:
                hint = difflib.get_close_matches(key, schema, n=1)
                fix = f"did you mean {hint[0]!r}?" if hint else f"valid keys: {', '.join(schema)}"
                raise self.error(name, key, f"unknown key {name}.{key}", fix)
        values: dict[str, Any] = {}
        for key, spec in schema.items():
            if key not in raw:
                if spec.default is _REQUIRED:
                    raise self.missing(name, key, spec)
                values[key] = spec.default
                continue
            v = raw[key]
            # bool is an int subclass in Python; `max_rows = true` is a mistake, not 1.
            if isinstance(v, bool) or not isinstance(v, spec.kind):
                raise self.error(
                    name,
                    key,
                    f"{name}.{key} must be {_type_name(spec.kind)}, got {type(v).__name__} {v!r}",
                    spec.example,
                )
            if spec.check is not None and (problem := spec.check(v)) is not None:
                raise self.error(name, key, f"{name}.{key} {problem}, got {v!r}", spec.example)
            values[key] = v
        return values

    def missing(self, section: str, key: str, spec: _Key) -> ConfigError:
        if section == "retention" and key == "policy":
            problem = (
                "retention.policy is required and has no default: decide what happens "
                "when the buffer is full (drop the oldest readings, or stop and alarm)"
            )
        else:
            problem = f"{section}.{key} is required"
        return self.error(section, None, problem, f"add under [{section}]: {spec.example}")


def load(file: str | Path) -> Config:
    path = Path(file)
    try:
        text = path.read_text()
    except OSError as e:
        raise ConfigError(path, None, f"cannot read config: {e.strerror}", "check the path") from e
    try:
        data = tomllib.loads(text)
    except tomllib.TOMLDecodeError as e:
        m = re.search(r"line (\d+)", str(e))
        raise ConfigError(
            path, int(m.group(1)) if m else None, f"not valid TOML: {e}", "fix the syntax"
        ) from e

    loader = _Loader(path, text)
    raw = loader.sections(data)
    s = {name: loader.section(name, raw.get(name, {})) for name in _SCHEMA}

    sink_path = s["sink"]["path"]
    if s["sink"]["type"] == "jsonl" and sink_path is None:
        raise loader.error(
            "sink", "type", "the jsonl sink needs a path", _SCHEMA["sink"]["path"].example
        )
    shipper = s["shipper"]
    if shipper["backoff_max_s"] < shipper["backoff_initial_s"]:
        raise loader.error(
            "shipper",
            "backoff_max_s",
            "shipper.backoff_max_s is smaller than backoff_initial_s",
            f"backoff_max_s = {shipper['backoff_initial_s']}   # or larger",
        )

    base = path.parent
    return Config(
        file=path,
        buffer_path=base / s["buffer"]["path"],
        wal_autocheckpoint=s["buffer"]["wal_autocheckpoint"],
        retention=Retention(Policy(s["retention"]["policy"]), s["retention"]["max_rows"]),
        batch_max_rows=s["batch"]["max_rows"],
        batch_max_delay_s=float(s["batch"]["max_delay_s"]),
        shipper=ShipperConfig(
            batch_size=shipper["batch_size"],
            send_timeout_s=float(shipper["send_timeout_s"]),
            poll_interval_s=float(shipper["poll_interval_s"]),
            purge_interval_s=float(shipper["purge_interval_s"]),
            backoff_initial_s=float(shipper["backoff_initial_s"]),
            backoff_max_s=float(shipper["backoff_max_s"]),
            stop_timeout_s=float(shipper["stop_timeout_s"]),
        ),
        sink=SinkConfig(s["sink"]["type"], base / sink_path if sink_path is not None else None),
        source=SourceConfig(
            s["source"]["type"], s["source"]["sensor_id"], float(s["source"]["rate_hz"])
        ),
    )
