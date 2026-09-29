#!/usr/bin/env bash
# Unpack mosquitto on the Pi without root (apt-get download + dpkg -x into ~/.local/mosquitto),
# where tests/harness/broker.py finds it. Needed for the MQTT-sink RSS measurement and the
# MQTT integration tests on ARM.
#   bash bench/pi/install_test_tools.sh [ssh-host]
set -euo pipefail
host="${1:-spoolpi-zero}"
ssh "$host" bash -s <<'EOF'
set -euo pipefail
dest=~/.local/mosquitto
if [ -x "$dest/root/usr/sbin/mosquitto" ]; then
  echo "== mosquitto already unpacked in $dest"
  exit 0
fi
mkdir -p "$dest/debs" "$dest/root"
cd "$dest/debs"
# mosquitto's library dependencies that a stock Pi OS Lite lacks (libc, libssl, libsystemd are there).
apt-get download mosquitto libcjson1 libdlt2 libwebsockets19t64 libwrap0 libev4t64 libuv1t64
for deb in *.deb; do dpkg -x "$deb" "$dest/root"; done
sync
libs=$(ls -d "$dest"/root/usr/lib/*-linux-gnu "$dest"/root/lib/*-linux-gnu 2>/dev/null | paste -sd:)
LD_LIBRARY_PATH="$libs" "$dest/root/usr/sbin/mosquitto" -h | head -1
EOF
