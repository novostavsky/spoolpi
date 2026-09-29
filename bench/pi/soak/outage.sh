#!/bin/bash
# Seven-day soak: one deliberate uplink outage. Stops the MQTT broker for a random
# 10-60 minutes, then starts it again. Run as root by spoolpi-soak-outage.timer.
set -u
out=/home/pi/soak
minutes=$(( 10 + RANDOM % 51 ))
echo "$(date +%s) outage start ${minutes}min" >> "$out/outages.log"
systemctl stop mosquitto.service
sleep $(( minutes * 60 ))
systemctl start mosquitto.service
echo "$(date +%s) outage end" >> "$out/outages.log"
chown pi:pi "$out/outages.log"
