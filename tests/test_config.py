from __future__ import annotations

from pathlib import Path

import pytest

from spool.config import ConfigError, load
from spool.core.retention import Policy

ROOT = Path(__file__).resolve().parent.parent

VALID = """\
[buffer]
path = "buf.db"

[retention]
policy = "drop_oldest"
max_rows = 100

[sink]
type = "jsonl"
path = "out/data.jsonl"
"""


def write(tmp_path: Path, text: str) -> Path:
    p = tmp_path / "spool.toml"
    p.write_text(text)
    return p


def error_for(tmp_path: Path, text: str) -> ConfigError:
    with pytest.raises(ConfigError) as info:
        load(write(tmp_path, text))
    return info.value


def line_of(text: str, needle: str) -> int:
    return next(i for i, line in enumerate(text.splitlines(), 1) if needle in line)


def test_minimal_config_loads_with_defaults(tmp_path: Path) -> None:
    cfg = load(write(tmp_path, VALID))
    assert cfg.buffer_path == tmp_path / "buf.db"
    assert cfg.sink.path == tmp_path / "out/data.jsonl"
    assert cfg.retention.policy is Policy.DROP_OLDEST and cfg.retention.max_rows == 100
    assert (cfg.batch_max_rows, cfg.batch_max_delay_s) == (50, 1.0)
    assert cfg.shipper.batch_size == 100 and cfg.shipper.stop_timeout_s == 10.0
    assert cfg.source.type == "stdin"


def test_example_config_is_valid() -> None:
    load(ROOT / "examples" / "spool.toml")


# --- acceptance: no default policy; errors name file, line and fix ---------------


def test_missing_policy_is_refused_with_file_line_and_fix(tmp_path: Path) -> None:
    text = VALID.replace('policy = "drop_oldest"\n', "")
    e = error_for(tmp_path, text)
    assert e.line == line_of(text, "[retention]")
    assert str(e).startswith(f"{tmp_path / 'spool.toml'}:{e.line}: ")
    assert "no default" in e.problem
    assert 'policy = "drop_oldest"' in e.fix and "halt_and_alarm" in e.fix


def test_missing_retention_section_is_refused(tmp_path: Path) -> None:
    text = VALID.replace('[retention]\npolicy = "drop_oldest"\nmax_rows = 100\n', "")
    e = error_for(tmp_path, text)
    assert e.line is None
    assert "retention.policy is required" in e.problem
    assert "[retention]" in e.fix


def test_wrong_type_points_at_its_line(tmp_path: Path) -> None:
    text = VALID.replace("max_rows = 100", 'max_rows = "lots"')
    e = error_for(tmp_path, text)
    assert e.line == line_of(text, "max_rows")
    assert "must be an integer, got str 'lots'" in e.problem
    assert e.fix == "max_rows = 1000000"


def test_bool_is_not_accepted_as_a_number(tmp_path: Path) -> None:
    e = error_for(tmp_path, VALID.replace("max_rows = 100", "max_rows = true"))
    assert "got bool True" in e.problem


def test_same_key_in_another_section_gets_the_right_line(tmp_path: Path) -> None:
    text = VALID + "\n[batch]\nmax_rows = 0\n"
    e = error_for(tmp_path, text)
    assert e.line == len(text.splitlines())
    assert "batch.max_rows must be greater than 0" in e.problem


def test_unknown_key_suggests_the_close_one(tmp_path: Path) -> None:
    text = VALID + "\n[batch]\nmax_row = 5\n"
    e = error_for(tmp_path, text)
    assert e.line == len(text.splitlines())
    assert "unknown key batch.max_row" in e.problem
    assert e.fix == "did you mean 'max_rows'?"


def test_unknown_section_suggests_the_close_one(tmp_path: Path) -> None:
    text = VALID.replace("[retention]", "[retension]")
    e = error_for(tmp_path, text)
    assert e.line == line_of(text, "[retension]")
    assert e.fix == "did you mean [retention]?"


def test_invalid_policy_lists_the_options(tmp_path: Path) -> None:
    e = error_for(tmp_path, VALID.replace('"drop_oldest"', '"keep_everything"'))
    assert "must be one of 'drop_oldest', 'halt_and_alarm'" in e.problem


def test_toml_syntax_error_has_a_line(tmp_path: Path) -> None:
    text = VALID.replace("max_rows = 100", "max_rows = = 100")
    e = error_for(tmp_path, text)
    assert e.line == line_of(text, "= = 100")
    assert "not valid TOML" in e.problem


def test_planned_sink_says_it_is_not_available_yet(tmp_path: Path) -> None:
    e = error_for(tmp_path, VALID.replace('type = "jsonl"', 'type = "mqtt"'))
    assert "isn't available in this version yet" in e.problem


def test_jsonl_sink_needs_a_path(tmp_path: Path) -> None:
    text = VALID.replace('path = "out/data.jsonl"\n', "")
    e = error_for(tmp_path, text)
    assert e.line == line_of(text, 'type = "jsonl"')
    assert "path =" in e.fix


def test_backoff_bounds_must_be_ordered(tmp_path: Path) -> None:
    text = VALID + "\n[shipper]\nbackoff_initial_s = 5\nbackoff_max_s = 1\n"
    e = error_for(tmp_path, text)
    assert e.line == line_of(text, "backoff_max_s")


def test_unreadable_file(tmp_path: Path) -> None:
    with pytest.raises(ConfigError, match="cannot read config"):
        load(tmp_path / "missing.toml")
