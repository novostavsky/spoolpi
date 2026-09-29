#!/bin/bash
# Seven-day soak: stop and remove the services. The data in ~/soak and the
# buffer in /var/lib/private/spoolpi stay, for the final check.
#   sudo bash ~/spoolpi/bench/pi/soak/uninstall.sh
set -u
[ "$(id -u)" = 0 ] || { echo "run with sudo"; exit 1; }
systemctl disable --now spoolpi-soak-outage.timer spoolpi-soak-metrics.timer
systemctl disable --now spoolpi.service spoolpi-soak-receiver.service
systemctl start mosquitto.service  # in case it was stopped mid-outage
rm -f /etc/systemd/system/spoolpi-soak-* /etc/systemd/system/spoolpi.service
rm -f /etc/mosquitto/conf.d/spoolpi-soak.conf /etc/systemd/journald.conf.d/spoolpi-soak.conf
systemctl daemon-reload
echo "$(date +%s) soak stop" >> /home/pi/soak/soak.log
sync
echo "== stopped; data kept in /home/pi/soak"
