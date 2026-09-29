from __future__ import annotations

from pathlib import Path

import pytest

from spoolpi.config import ConfigError, load
from spoolpi.core.retention import Policy

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
    p = tmp_path / "spoolpi.toml"
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
    load(ROOT / "examples" / "spoolpi.toml")


# --- acceptance: no default policy; errors name file, line and fix ---------------


def test_missing_policy_is_refused_with_file_line_and_fix(tmp_path: Path) -> None:
    text = VALID.replace('policy = "drop_oldest"\n', "")
    e = error_for(tmp_path, text)
    assert e.line == line_of(text, "[retention]")
    assert str(e).startswith(f"{tmp_path / 'spoolpi.toml'}:{e.line}: ")
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


def test_unknown_sink_type_lists_the_available_ones(tmp_path: Path) -> None:
    e = error_for(tmp_path, VALID.replace('type = "jsonl"', 'type = "kafka"'))
    assert "must be one of 'jsonl', 'mqtt', 'http'" in e.problem


def test_jsonl_sink_needs_a_path(tmp_path: Path) -> None:
    text = VALID.replace('path = "out/data.jsonl"\n', "")
    e = error_for(tmp_path, text)
    assert e.line == line_of(text, 'type = "jsonl"')
    assert "path =" in e.fix


def test_immediate_retries_default_zero_ok_negative_refused(tmp_path: Path) -> None:
    assert load(write(tmp_path, VALID)).shipper.immediate_retries == 2
    text = VALID + "\n[shipper]\nimmediate_retries = 0\n"
    assert load(write(tmp_path, text)).shipper.immediate_retries == 0
    e = error_for(tmp_path, VALID + "\n[shipper]\nimmediate_retries = -1\n")
    assert "must not be negative" in e.problem


def test_durability_defaults_to_power_and_takes_process(tmp_path: Path) -> None:
    assert load(write(tmp_path, VALID)).durability == "power"
    text = VALID.replace('path = "buf.db"\n', 'path = "buf.db"\ndurability = "process"\n')
    assert load(write(tmp_path, text)).durability == "process"
    text = VALID.replace('path = "buf.db"\n', 'path = "buf.db"\ndurability = "always"\n')
    e = error_for(tmp_path, text)
    assert e.line == line_of(text, "durability")
    assert "'power', 'process'" in e.problem and 'durability = "power"' in e.fix


def test_hold_unsynced_default_zero_ok_negative_refused(tmp_path: Path) -> None:
    assert load(write(tmp_path, VALID)).shipper.hold_unsynced_s == 120.0
    text = VALID + "\n[shipper]\nhold_unsynced_s = 0\n"
    assert load(write(tmp_path, text)).shipper.hold_unsynced_s == 0.0
    e = error_for(tmp_path, VALID + "\n[shipper]\nhold_unsynced_s = -5\n")
    assert "must not be negative" in e.problem


def test_backoff_bounds_must_be_ordered(tmp_path: Path) -> None:
    text = VALID + "\n[shipper]\nbackoff_initial_s = 5\nbackoff_max_s = 1\n"
    e = error_for(tmp_path, text)
    assert e.line == line_of(text, "backoff_max_s")


# --- mqtt sink ---------------------------------------------------------------------

MQTT = VALID.replace(
    'type = "jsonl"\npath = "out/data.jsonl"\n', 'type = "mqtt"\nhost = "broker"\n'
)


def test_mqtt_config_loads_with_defaults(tmp_path: Path) -> None:
    text = MQTT.replace('host = "broker"', 'host = "broker"\ntls = true\npassword_file = "pw"')
    cfg = load(write(tmp_path, text + '\n[device]\nid = "greenhouse-1"\n'))
    m = cfg.sink.mqtt
    assert m is not None and cfg.sink.path is None
    assert (m.host, m.port, m.protocol, m.tls) == ("broker", 1883, "5", True)
    assert m.topic == "spoolpi/{device_id}/{type}/{sensor_id}"
    assert m.password_file == tmp_path / "pw"
    assert cfg.device_id == "greenhouse-1"


def test_mqtt_needs_a_host(tmp_path: Path) -> None:
    e = error_for(tmp_path, MQTT.replace('host = "broker"\n', ""))
    assert "the mqtt sink needs sink.host" in e.problem


def test_keys_of_another_sink_type_are_refused(tmp_path: Path) -> None:
    text = MQTT.replace('host = "broker"', 'host = "broker"\npath = "x.jsonl"')
    e = error_for(tmp_path, text)
    assert e.line == line_of(text, 'path = "x.jsonl"')
    assert "sink.path doesn't apply to the mqtt sink" in e.problem


def test_topic_template_is_checked(tmp_path: Path) -> None:
    text = MQTT.replace('host = "broker"', 'host = "broker"\ntopic = "x/{sensor}"')
    e = error_for(tmp_path, text)
    assert "unknown field {sensor}" in e.problem
    text = MQTT.replace('host = "broker"', 'host = "broker"\ntopic = "x/+/{sensor_id}"')
    assert "wildcard" in error_for(tmp_path, text).problem


def test_mqtt_timeouts_must_fit_inside_the_send_timeout(tmp_path: Path) -> None:
    text = MQTT.replace('host = "broker"', 'host = "broker"\nack_timeout_s = 9')
    e = error_for(tmp_path, text)
    assert e.line == line_of(text, "ack_timeout_s")
    assert "must be less than shipper.send_timeout_s" in e.problem


def test_bool_keys_take_booleans_only(tmp_path: Path) -> None:
    text = MQTT.replace('host = "broker"', 'host = "broker"\ntls = "yes"')
    assert "must be true or false" in error_for(tmp_path, text).problem


# --- http sink ---------------------------------------------------------------------

HTTP = VALID.replace(
    'type = "jsonl"\npath = "out/data.jsonl"\n', 'type = "http"\nurl = "https://ingest.example/x"\n'
)


def test_http_config_loads_with_defaults(tmp_path: Path) -> None:
    text = HTTP.replace('/x"', '/x"\ntoken_file = "tok"\ngzip = true')
    cfg = load(write(tmp_path, text))
    h = cfg.sink.http
    assert h is not None and cfg.sink.mqtt is None
    assert (h.url, h.timeout_s, h.verify, h.gzip) == ("https://ingest.example/x", 5.0, True, True)
    assert h.token_file == tmp_path / "tok"


def test_http_url_is_required_and_checked(tmp_path: Path) -> None:
    e = error_for(tmp_path, HTTP.replace('url = "https://ingest.example/x"\n', ""))
    assert "the http sink needs sink.url" in e.problem
    e = error_for(tmp_path, HTTP.replace("https://", "ftp://"))
    assert "must be an http:// or https:// URL" in e.problem


def test_http_timeout_must_fit_inside_the_send_timeout(tmp_path: Path) -> None:
    text = HTTP.replace('/x"', '/x"\ntimeout_s = 10')
    e = error_for(tmp_path, text)
    assert e.line == line_of(text, "timeout_s")
    assert "sink.timeout_s (10 s) must be less than shipper.send_timeout_s" in e.problem


def test_mqtt_keys_are_refused_for_http(tmp_path: Path) -> None:
    text = HTTP.replace('/x"', '/x"\nhost = "b"')
    assert "sink.host doesn't apply to the http sink" in error_for(tmp_path, text).problem


def test_unreadable_file(tmp_path: Path) -> None:
    with pytest.raises(ConfigError, match="cannot read config"):
        load(tmp_path / "missing.toml")
