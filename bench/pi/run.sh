#!/usr/bin/env bash
# Run a command on the Pi inside ~/spoolpi with its venv on PATH.
#   bash bench/pi/run.sh <command...>        e.g. bash bench/pi/run.sh pytest -q
set -euo pipefail
host="${SPOOLPI_PI_HOST:-spoolpi-zero}"
# %q-quote each argument so it survives the remote shell unchanged.
ssh "$host" "cd ~/spoolpi && export PATH=\"\$HOME/spoolpi/.venv/bin:\$HOME/.local/bin:\$PATH\" && $(printf '%q ' "$@")"
