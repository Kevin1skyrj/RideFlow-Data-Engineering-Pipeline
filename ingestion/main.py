"""Ingestion consumer CLI.

python -m ingestion.main                     # run until interrupted
python -m ingestion.main --max-messages 5000 # bounded run, for tests
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

from ingestion.config import ConsumerConfig
from ingestion.consumer import IngestionConsumer


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="ingestion",
        description="Consume RideFlow events from Kafka into the Parquet landing zone",
    )
    parser.add_argument("--bootstrap-servers", default=None)
    parser.add_argument("--group-id", default=None)
    parser.add_argument("--landing-zone", type=Path, default=None)
    parser.add_argument(
        "--batch-max-events",
        type=int,
        default=None,
        help="Flush after this many events (default 10000)",
    )
    parser.add_argument(
        "--batch-max-seconds",
        type=float,
        default=None,
        help="Flush after this long, whichever comes first (default 60)",
    )
    parser.add_argument(
        "--max-messages",
        type=int,
        default=None,
        help="Stop after N messages. Bounded runs make reconciliation testable.",
    )
    parser.add_argument(
        "--idle-timeout",
        type=float,
        default=None,
        help="Stop after this many seconds with no messages",
    )
    parser.add_argument("--verbose", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    logging.basicConfig(
        level=logging.INFO if args.verbose else logging.WARNING,
        format="%(asctime)s %(levelname)-7s %(name)s | %(message)s",
    )

    config = ConsumerConfig.from_env()
    if args.bootstrap_servers:
        config.bootstrap_servers = args.bootstrap_servers
    if args.group_id:
        config.group_id = args.group_id
    if args.landing_zone:
        config.landing_zone = args.landing_zone
    if args.batch_max_events:
        config.batch_max_events = args.batch_max_events
    if args.batch_max_seconds:
        config.batch_max_seconds = args.batch_max_seconds

    from event_generator.kafka_sink import broker_available

    if not broker_available(config.bootstrap_servers, timeout=5.0):
        print(
            f"\nKafka unavailable: no broker at {config.bootstrap_servers}\n"
            "Start it with: docker compose -f docker/docker-compose.yml up -d",
            file=sys.stderr,
        )
        return 2

    consumer = IngestionConsumer(config)
    consumer.install_signal_handlers()
    stats = consumer.run(max_messages=args.max_messages, idle_timeout=args.idle_timeout)

    summary = {
        "group_id": config.group_id,
        "landing_zone": str(config.landing_zone),
        **stats.as_dict(),
        "dlq": consumer.dlq.report.as_dict(),
    }
    print(json.dumps(summary, indent=2), file=sys.stderr)

    # Non-zero when the reconciliation identity fails: every polled message must
    # be accounted for as landed, rejected, or a dropped duplicate.
    return 0 if stats.reconciles() else 1


if __name__ == "__main__":
    raise SystemExit(main())
