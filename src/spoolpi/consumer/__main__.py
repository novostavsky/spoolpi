"""python -m spoolpi.consumer --broker HOST[:PORT] [--dsn DSN] [--init-schema]"""

from __future__ import annotations

import argparse
import logging
import os
import signal
import sys
import threading
from pathlib import Path

import psycopg

from spoolpi.consumer.mqtt import MqttConsumer
from spoolpi.consumer.postgres import apply_schema


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(
        prog="python -m spoolpi.consumer",
        description="Store SpoolPi records from MQTT in Postgres, deduplicated on (buffer_id, seq).",
    )
    p.add_argument("--broker", required=True, help="MQTT broker, HOST or HOST:PORT")
    p.add_argument(
        "--dsn",
        default=os.environ.get("SPOOLPI_CONSUMER_DSN"),
        help="Postgres connection string (default: $SPOOLPI_CONSUMER_DSN, which keeps passwords off the command line)",
    )
    p.add_argument("--topic", default="spoolpi/#")
    p.add_argument(
        "--client-id", default="spoolpi-consumer", help="stable id: the broker keeps this session"
    )
    p.add_argument("--batch-size", type=int, default=500)
    p.add_argument("--tls", action="store_true")
    p.add_argument("--ca-file")
    p.add_argument("--username")
    p.add_argument("--password-file", type=Path)
    p.add_argument(
        "--init-schema", action="store_true", help="create the tables first (idempotent)"
    )
    args = p.parse_args(argv)
    if not args.dsn:
        p.error("--dsn or $SPOOLPI_CONSUMER_DSN is required")

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    host, _, port = args.broker.partition(":")
    if args.init_schema:
        with psycopg.connect(args.dsn) as conn:
            apply_schema(conn)

    password = args.password_file.read_text().rstrip("\r\n") if args.password_file else None
    consumer = MqttConsumer(
        host=host,
        port=int(port or 1883),
        dsn=args.dsn,
        topic=args.topic,
        client_id=args.client_id,
        batch_size=args.batch_size,
        tls=args.tls,
        ca_file=args.ca_file,
        username=args.username,
        password=password,
    )
    stop = threading.Event()
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, lambda *_: stop.set())
    consumer.run(stop)
    t = consumer.totals
    logging.getLogger("spoolpi.consumer").info(
        "stopped: %d readings, %d gaps stored; %d duplicates ignored; %d dead letters",
        t.readings,
        t.gaps,
        t.duplicates,
        t.dead_letters,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
