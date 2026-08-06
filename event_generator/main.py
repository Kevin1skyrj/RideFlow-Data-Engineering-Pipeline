"""CLI entry point.

python -m event_generator.main --trips-per-hour 600 --duration 3600 --seed 42
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime
from pathlib import Path

from event_generator.config import (
    DEFAULT_OUTPUT_DIR,
    ROOT,
    AnomalyConfig,
    GeneratorConfig,
)
from event_generator.generator import generate
from event_generator.sinks import JsonlFileSink, PartitionedJsonlSink, StdoutSink

DEFAULT_OUTPUT = ROOT / "data" / "generated" / "events.jsonl"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="event_generator",
        description="Generate synthetic RideFlow events conforming to event_contract.md v1.0.0",
    )
    parser.add_argument(
        "--start",
        type=str,
        default=None,
        help="ISO-8601 window start in UTC (default: today 02:30Z, i.e. 08:00 IST)",
    )
    parser.add_argument("--duration", type=int, default=3600, help="Window length in seconds")
    parser.add_argument("--trips-per-hour", type=float, default=600.0)
    parser.add_argument(
        "--drivers",
        type=int,
        default=0,
        help="Fleet size (default: derived from --trips-per-hour)",
    )
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--city", type=str, default="BLR")
    parser.add_argument(
        "--environment",
        type=str,
        default="local",
        choices=["local", "dev", "staging", "prod"],
    )
    parser.add_argument(
        "--sink",
        choices=["file", "partitioned", "stdout", "kafka"],
        default="file",
        help="Where to send events (default: file)",
    )
    parser.add_argument("--out", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument(
        "--bootstrap-servers",
        type=str,
        default=None,
        help="Kafka bootstrap servers (default: KAFKA_BOOTSTRAP_SERVERS or localhost:29092)",
    )
    parser.add_argument(
        "--partitioned",
        action="store_true",
        help="Deprecated alias for --sink partitioned",
    )
    parser.add_argument("--stdout", action="store_true", help="Deprecated alias for --sink stdout")
    parser.add_argument(
        "--no-anomalies",
        action="store_true",
        help="Disable anomaly injection (clean stream for baseline tests)",
    )
    parser.add_argument(
        "--summary-only",
        action="store_true",
        help="Print the run summary without writing events",
    )
    return parser


def default_start() -> datetime:
    """Today at 02:30 UTC - 08:00 IST, inside the Bengaluru morning peak."""
    today = datetime.now(UTC).date()
    return datetime(today.year, today.month, today.day, 2, 30, tzinfo=UTC)


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)

    start = GeneratorConfig.parse_start(args.start) if args.start else default_start()
    config = GeneratorConfig(
        start_time=start,
        duration_sec=args.duration,
        trips_per_hour=args.trips_per_hour,
        driver_count=args.drivers,
        seed=args.seed,
        city_code=args.city,
        environment=args.environment,
        anomalies=AnomalyConfig.disabled() if args.no_anomalies else AnomalyConfig(),
    )

    result = generate(config)

    # Legacy flags win if given, so existing invocations keep working.
    target = args.sink
    if args.stdout:
        target = "stdout"
    elif args.partitioned:
        target = "partitioned"

    written = 0
    delivery: dict[str, object] | None = None

    if not args.summary_only:
        if target == "stdout":
            written = StdoutSink().write(result.events)
        elif target == "partitioned":
            written = PartitionedJsonlSink(
                DEFAULT_OUTPUT_DIR.parent / "generated" / "partitioned"
            ).write(result.events)
        elif target == "kafka":
            from event_generator.kafka_sink import (
                KafkaSink,
                KafkaUnavailableError,
                ProducerConfig,
                broker_available,
            )

            producer_config = ProducerConfig.from_env()
            if args.bootstrap_servers:
                producer_config.bootstrap_servers = args.bootstrap_servers

            # Probe before producing. Without this, librdkafka spends the full
            # delivery.timeout.ms retrying and emitting connection errors before
            # anything actionable is printed - 30 seconds of noise to say
            # "Docker isn't running".
            if not broker_available(producer_config.bootstrap_servers, timeout=5.0):
                print(
                    f"\nKafka unavailable: no broker at "
                    f"{producer_config.bootstrap_servers}\n"
                    "Start it with: docker compose -f docker/docker-compose.yml up -d",
                    file=sys.stderr,
                )
                return 2
            try:
                sink = KafkaSink(producer_config)
                written = sink.write(result.events)
                delivery = sink.report.as_dict()
            except KafkaUnavailableError as exc:
                # A stopped broker is an operational state, not a crash. Report
                # it as one so the message is actionable instead of a traceback.
                print(f"\nKafka unavailable: {exc}", file=sys.stderr)
                return 2
        else:
            written = JsonlFileSink(args.out).write(result.events)

    summary = {
        "seed": config.seed,
        "city": config.city_code,
        "sink": target,
        "window_start": start.isoformat().replace("+00:00", "Z"),
        "window_end": config.end_time.isoformat().replace("+00:00", "Z"),
        "events_written": written,
        **result.summary(),
    }
    if delivery is not None:
        summary["kafka_delivery"] = delivery
    print(json.dumps(summary, indent=2), file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
