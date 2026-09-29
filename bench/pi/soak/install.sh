#!/bin/bash
# Seven-day soak: install and start everything. Run once on the Pi, as root:
#   sudo bash ~/spoolpi/bench/pi/soak/install.sh
# Needs the repo deployed to ~/spoolpi (bench/pi/deploy.sh) and a wheel built in
# ~/spoolpi/dist (uv build). Undo with uninstall.sh; the data in ~/soak stays.
set -euo pipefail
[ "$(id -u)" = 0 ] || { echo "run with sudo"; exit 1; }
src=/home/pi/spoolpi
soak=$src/bench/pi/soak
uv=/home/pi/.local/bin/uv
wheel=$(ls -t "$src"/dist/spoolpi-*.whl | head -1)
echo "== wheel: $wheel"

echo "== packages: mosquitto (the broker), libpq5 (plain psycopg on 32-bit ARM), sqlite3"
apt-get update -qq
apt-get install -y -qq mosquitto libpq5 sqlite3 >/dev/null

echo "== SpoolPi into /opt/spoolpi/.venv, as operations.md describes"
"$uv" venv --quiet --allow-existing --python /usr/bin/python3 /opt/spoolpi/.venv
"$uv" pip install --quiet --python /opt/spoolpi/.venv/bin/python --reinstall-package spoolpi \
  "spoolpi[mqtt,consumer] @ file://$wheel"
/opt/spoolpi/.venv/bin/spoolpi --version

echo "== producer, config, broker settings"
install -m 755 "$soak/read-sensors" /usr/local/bin/read-sensors
install -d /etc/spoolpi
install -m 644 "$soak/spoolpi.toml" /etc/spoolpi/spoolpi.toml
install -m 644 "$soak/mosquitto-soak.conf" /etc/mosquitto/conf.d/spoolpi-soak.conf

echo "== soak scripts (root-owned) and data dir (pi-owned)"
install -d /opt/spoolpi-soak/bin
install -m 644 "$soak/receiver.py" "$soak/metrics.py" "$soak/outage.sh" /opt/spoolpi-soak/bin/
install -d -o pi -g pi /home/pi/soak

echo "== a persistent journal for the week, so restarts leave evidence"
install -d /etc/systemd/journald.conf.d
printf '[Journal]\nStorage=persistent\nSystemMaxUse=200M\n' > /etc/systemd/journald.conf.d/spoolpi-soak.conf
systemctl restart systemd-journald

echo "== units: the shipped spoolpi.service, unchanged, plus the soak units"
install -m 644 "$src/contrib/systemd/spoolpi.service" /etc/systemd/system/spoolpi.service
install -m 644 "$soak"/systemd/* /etc/systemd/system/
systemctl daemon-reload
systemctl restart mosquitto.service
systemctl enable --now spoolpi-soak-receiver.service
# The broker drops messages nobody is subscribed to (and still PUBACKs them). Until the
# receiver's first connection creates its persistent session, SpoolPi's first readings
# would vanish, so wait for the subscription before starting SpoolPi.
for _ in $(seq 60); do
  journalctl -u spoolpi-soak-receiver.service --since "-2min" --no-pager | grep -q "subscribed" && break
  sleep 1
done
journalctl -u spoolpi-soak-receiver.service --since "-2min" --no-pager | grep -q "subscribed" \
  || { echo "the receiver never subscribed; not starting SpoolPi"; exit 1; }
sleep 2
systemctl enable --now spoolpi.service
systemctl enable --now spoolpi-soak-metrics.timer spoolpi-soak-outage.timer

echo "$(date +%s) soak start $(/opt/spoolpi/.venv/bin/spoolpi --version)" >> /home/pi/soak/soak.log
chown pi:pi /home/pi/soak/soak.log
sync
sleep 5
systemctl --no-pager --lines=0 status spoolpi.service spoolpi-soak-receiver.service mosquitto.service | grep -E '●|Active'
echo "== started. Check with: python3 ~/spoolpi/bench/pi/soak/check.py"
