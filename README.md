# Spool

Crash-safe store-and-forward buffering for edge sensor data. A SIGKILL-driven crash test of the
naive approach (SQLite + immediate HTTP POST) found a **13.1% silent duplication rate** across
100 kill/restart cycles — see [`docs/motivation.md`](docs/motivation.md) for the full writeup.

Status: early build, following `spool_implementation-plan.md`. Not ready for use.

## Development

```sh
uv venv
source .venv/bin/activate
uv pip install -e ".[dev]"
pytest
```

Core (`src/spool/core`) has zero runtime dependencies by design — see the implementation plan's
"Read This First" section for why.
