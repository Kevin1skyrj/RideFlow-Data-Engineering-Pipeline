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
    parser.add_argument("--out", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument(
        "--partitioned",
        action="store_true",
        help="Write Hive-style dt=/hour= partitions instead of one file",
    )
    parser.add_argument("--stdout", action="store_true", help="Write events to stdout")
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

    if not args.summary_only:
        if args.stdout:
            sink = StdoutSink()
        elif args.partitioned:
            sink = PartitionedJsonlSink(DEFAULT_OUTPUT_DIR.parent / "generated" / "partitioned")
        else:
            sink = JsonlFileSink(args.out)
        written = sink.write(result.events)
    else:
        written = 0

    summary = {
        "seed": config.seed,
        "city": config.city_code,
        "window_start": start.isoformat().replace("+00:00", "Z"),
        "window_end": config.end_time.isoformat().replace("+00:00", "Z"),
        "events_written": written,
        **result.summary(),
    }
    print(json.dumps(summary, indent=2), file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
