"""Consumer configuration.

Defaults come from docs/kafka_design.md section 3.2. Two of them prevent silent
data loss and are worth understanding rather than copying.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]

TOPIC_TRIPS = "rideflow.trips.events.v1"
TOPIC_PRESENCE = "rideflow.drivers.presence.v1"
DLQ_TRIPS = "rideflow.trips.events.dlq.v1"
DLQ_PRESENCE = "rideflow.drivers.presence.dlq.v1"

DLQ_FOR = {TOPIC_TRIPS: DLQ_TRIPS, TOPIC_PRESENCE: DLQ_PRESENCE}
FAMILY_FOR = {TOPIC_TRIPS: "trips", TOPIC_PRESENCE: "presence"}


@dataclass
class ConsumerConfig:
    bootstrap_servers: str = "localhost:29092"
    group_id: str = "rideflow-trip-ingestor"
    topics: tuple[str, ...] = (TOPIC_TRIPS, TOPIC_PRESENCE)

    enable_auto_commit: bool = False
    """The most important setting here.

    Auto-commit commits on a timer regardless of whether processing succeeded.
    A crash between an auto-commit and the durable write loses those messages
    silently - no error, no gap in any log, just missing data.
    """

    auto_offset_reset: str = "earliest"
    """On a new group or a lost offset, start from the beginning. `latest` would
    silently skip unprocessed history, which looks like success."""

    max_poll_records: int = 500
    session_timeout_ms: int = 45_000
    max_poll_interval_ms: int = 300_000
    partition_assignment_strategy: str = "cooperative-sticky"
    """Incremental rebalancing: only affected partitions move, instead of
    stopping every consumer in the group."""

    batch_max_events: int = 10_000
    batch_max_seconds: float = 60.0
    """Flush on whichever fires first.

    Both are needed. Size alone stalls forever in low traffic - an event could
    sit unflushed until morning. Time alone produces tiny files under load. The
    pair bounds both latency and file size.
    """

    landing_zone: Path = ROOT / "data" / "raw"
    compression: str = "snappy"

    @classmethod
    def from_env(cls, env: dict[str, str] | None = None) -> ConsumerConfig:
        source = env if env is not None else dict(os.environ)

        def flag(key: str, default: bool) -> bool:
            raw = source.get(key)
            return default if raw is None else raw.strip().lower() in {"1", "true", "yes"}

        return cls(
            bootstrap_servers=source.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092"),
            group_id=source.get("KAFKA_CONSUMER_GROUP_TRIPS", "rideflow-trip-ingestor"),
            enable_auto_commit=flag("KAFKA_ENABLE_AUTO_COMMIT", False),
            auto_offset_reset=source.get("KAFKA_AUTO_OFFSET_RESET", "earliest"),
            max_poll_records=int(source.get("KAFKA_MAX_POLL_RECORDS", 500)),
            session_timeout_ms=int(source.get("KAFKA_SESSION_TIMEOUT_MS", 45_000)),
            landing_zone=Path(source.get("RIDEFLOW_LANDING_ZONE", ROOT / "data" / "raw")),
        )

    def to_librdkafka(self) -> dict[str, Any]:
        if self.enable_auto_commit:
            raise ValueError(
                "enable.auto.commit must be False. Auto-commit commits on a timer "
                "regardless of processing success, which permits silent data loss "
                "on crash (kafka_design.md 3.2)."
            )
        return {
            "bootstrap.servers": self.bootstrap_servers,
            "group.id": self.group_id,
            "enable.auto.commit": False,
            "auto.offset.reset": self.auto_offset_reset,
            "session.timeout.ms": self.session_timeout_ms,
            "max.poll.interval.ms": self.max_poll_interval_ms,
            "partition.assignment.strategy": self.partition_assignment_strategy,
            # Bounds how much the client buffers per partition, which keeps
            # memory predictable when the consumer falls behind.
            "queued.max.messages.kbytes": 65536,
        }
