#!/usr/bin/env bash
# Push the committed tree to the Pi and set up its environment (no root needed).
#   bash bench/pi/deploy.sh [ssh-host]      default host: spoolpi-zero (see ~/.ssh/config)
set -euo pipefail
host="${1:-spoolpi-zero}"
repo="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

echo "== pushing the working tree ($(git -C "$repo" rev-parse --short HEAD) + local changes) to $host:~/spoolpi"
# Tracked and new-but-not-ignored files, so work in progress can be measured before it's committed.
(cd "$repo" && git ls-files -z --cached --others --exclude-standard | tar -c --null -T -) \
  | ssh "$host" 'rm -rf ~/spoolpi/src ~/spoolpi/tests ~/spoolpi/bench && mkdir -p ~/spoolpi && tar -x -C ~/spoolpi'

ssh "$host" bash -s <<'EOF'
set -euo pipefail
export PATH="$HOME/.local/bin:$PATH"
if ! command -v uv >/dev/null; then
  echo "== installing uv"
  curl -LsSf https://astral.sh/uv/install.sh | sh >/dev/null
fi
echo "== $(uv --version)"
cd ~/spoolpi
echo "== syncing the locked environment (first run downloads wheels for aarch64)"
uv sync --locked --extra dev --quiet
.venv/bin/spoolpi --version
# The power-cut test resets the Pi without syncing; make sure the deploy itself is on disk.
sync
EOF
