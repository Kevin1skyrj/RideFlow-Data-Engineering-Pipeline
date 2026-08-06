"""Kafka producer sink.

Replaces the file sinks from M2 without the generator knowing: both satisfy the
`Sink` protocol, so `generate()` is untouched by the change.

Producer configuration follows docs/kafka_design.md section 5.3. The two
settings that matter most:

  acks=all              Wait for every in-sync replica. acks=1 risks silent loss
                        if the leader dies after acknowledging.
  enable.idempotence    Eliminates PRODUCER-side duplicates from internal
                        retries - a different duplicate source from the
                        at-least-once redelivery the consumer must handle.

Delivery is at-least-once by design. Kafka's exactly-once covers Kafka-to-Kafka
transactions; RideFlow's sink is the filesystem, which is not a transactional
participant, so exactly-once cannot span that boundary (kafka_design.md 5.2).
Duplicates are removable downstream; loss is not.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from event_generator.anomalies import TAG_MALFORMED_JSON
from event_generator.envelope import PRESENCE_EVENT_TYPES, Event

if TYPE_CHECKING:  # pragma: no cover
    from collections.abc import Iterable

DEFAULT_BOOTSTRAP = "localhost:29092"
TOPIC_TRIPS = "rideflow.trips.events.v1"
TOPIC_PRESENCE = "rideflow.drivers.presence.v1"


class KafkaUnavailableError(RuntimeError):
    """Raised when no broker can be reached.

    Deliberately distinct from a delivery failure: 'the broker is not running'
    and 'the broker rejected this message' need different responses, and
    collapsing them into one error makes the first look like a code bug.
    """


@dataclass
class ProducerConfig:
    bootstrap_servers: str = DEFAULT_BOOTSTRAP
    acks: str = "all"
    enable_idempotence: bool = True
    retries: int = 10
    max_in_flight: int = 5
    linger_ms: int = 10
    delivery_timeout_ms: int = 120_000
    compression_type: str = "snappy"
    client_id: str = "rideflow-event-generator"

    @classmethod
    def from_env(cls, env: dict[str, str] | None = None) -> ProducerConfig:
        source = env if env is not None else dict(os.environ)

        def flag(key: str, default: bool) -> bool:
            raw = source.get(key)
            return default if raw is None else raw.strip().lower() in {"1", "true", "yes"}

        return cls(
            bootstrap_servers=source.get("KAFKA_BOOTSTRAP_SERVERS", DEFAULT_BOOTSTRAP),
            acks=source.get("KAFKA_ACKS", "all"),
            enable_idempotence=flag("KAFKA_ENABLE_IDEMPOTENCE", True),
            retries=int(source.get("KAFKA_RETRIES", 10)),
            max_in_flight=int(source.get("KAFKA_MAX_IN_FLIGHT", 5)),
            linger_ms=int(source.get("KAFKA_LINGER_MS", 10)),
            delivery_timeout_ms=int(source.get("KAFKA_DELIVERY_TIMEOUT_MS", 120_000)),
            compression_type=source.get("KAFKA_COMPRESSION_TYPE", "snappy"),
        )

    def to_librdkafka(self) -> dict[str, Any]:
        """Translate to librdkafka property names."""
        if self.enable_idempotence and self.max_in_flight > 5:
            raise ValueError(
                "max.in.flight.requests.per.connection must be <= 5 when "
                "enable.idempotence is true, otherwise librdkafka rejects the config"
            )
        if self.enable_idempotence and self.acks != "all":
            raise ValueError("enable.idempotence requires acks=all")

        return {
            "bootstrap.servers": self.bootstrap_servers,
            "client.id": self.client_id,
            "acks": self.acks,
            "enable.idempotence": self.enable_idempotence,
            "retries": self.retries,
            "max.in.flight.requests.per.connection": self.max_in_flight,
            "linger.ms": self.linger_ms,
            "delivery.timeout.ms": self.delivery_timeout_ms,
            "compression.type": self.compression_type,
        }


@dataclass
class DeliveryReport:
    delivered: int = 0
    failed: int = 0
    errors: list[str] = field(default_factory=list)

    def record_error(self, message: str) -> None:
        self.failed += 1
        # Bounded: a total broker outage would otherwise accumulate one string
        # per message and exhaust memory before the timeout fires.
        if len(self.errors) < 20:
            self.errors.append(message)

    def as_dict(self) -> dict[str, Any]:
        return {
            "delivered": self.delivered,
            "failed": self.failed,
            "sample_errors": self.errors[:5],
        }


def topic_for(event: Event) -> str:
    """Route by event family.

    Trip and presence events cannot share a topic: trip events are keyed by
    trip_id for per-trip ordering, presence events by driver_id for per-driver
    ordering, and a topic has exactly one partitioning strategy
    (event_contract.md 3.3).
    """
    return TOPIC_PRESENCE if event.event_type in PRESENCE_EVENT_TYPES else TOPIC_TRIPS


def headers_for(event: Event) -> list[tuple[str, bytes]]:
    """Headers duplicate envelope fields on purpose.

    A consumer can route or filter on them WITHOUT paying deserialisation cost,
    which matters when most messages are being skipped (kafka_design.md 4.3).
    """
    return [
        ("event_type", event.event_type.encode()),
        ("event_version", event.event_version.encode()),
        ("content_type", b"application/json"),
        ("producer_service", event.producer_service.encode()),
    ]


def serialise(event: Event) -> bytes:
    """Encode for the wire, preserving injected malformed-JSON anomalies.

    A truncated payload must reach the broker intact so the consumer's
    MALFORMED_JSON rejection path is exercised end to end. Repairing it here
    would quietly delete the test case.
    """
    line = json.dumps(event.to_dict(), separators=(",", ":"), ensure_ascii=False)
    if TAG_MALFORMED_JSON in event.anomaly_tags:
        line = line[: max(len(line) // 2, 12)]
    return line.encode("utf-8")


class KafkaSink:
    """Publishes events to Kafka. Satisfies the `Sink` protocol."""

    def __init__(
        self,
        config: ProducerConfig | None = None,
        *,
        flush_every: int = 10_000,
        producer: Any = None,
    ) -> None:
        self.config = config or ProducerConfig.from_env()
        self.flush_every = flush_every
        self.report = DeliveryReport()
        self._producer = producer or self._build_producer()

    def _build_producer(self) -> Any:
        try:
            from confluent_kafka import Producer
        except ImportError as exc:  # pragma: no cover
            raise KafkaUnavailableError(
                "confluent-kafka is not installed. pip install -r requirements.txt"
            ) from exc
        return Producer(self.config.to_librdkafka())

    def _on_delivery(self, err: Any, msg: Any) -> None:
        if err is not None:
            self.report.record_error(str(err))
        else:
            self.report.delivered += 1

    def write(self, events: Iterable[Event]) -> int:
        produced = 0
        for event in events:
            payload = serialise(event)
            while True:
                try:
                    self._producer.produce(
                        topic=topic_for(event),
                        key=event.partition_key.encode(),
                        value=payload,
                        headers=headers_for(event),
                        on_delivery=self._on_delivery,
                    )
                    break
                except BufferError:
                    # The local queue is full: the producer is outrunning the
                    # broker. Serving delivery callbacks drains it. Dropping the
                    # message instead would be silent data loss at the source.
                    self._producer.poll(0.5)

            produced += 1
            if produced % self.flush_every == 0:
                self._producer.flush(30)
            else:
                self._producer.poll(0)

        remaining = self._producer.flush(60)
        if remaining:
            self.report.record_error(f"{remaining} message(s) still queued after flush")

        if self.report.delivered == 0 and produced > 0:
            raise KafkaUnavailableError(
                f"No messages were delivered to {self.config.bootstrap_servers}. "
                "Is the broker running?  docker compose -f docker/docker-compose.yml up -d"
            )
        return produced

    def close(self) -> None:
        self._producer.flush(30)


def broker_available(bootstrap_servers: str = DEFAULT_BOOTSTRAP, timeout: float = 5.0) -> bool:
    """Cheap reachability probe, used to skip integration tests cleanly.

    A skipped test that says why is far more useful than a failing test that
    only means Docker is not running.
    """
    try:
        from confluent_kafka.admin import AdminClient
    except ImportError:  # pragma: no cover
        return False
    try:
        metadata = AdminClient({"bootstrap.servers": bootstrap_servers}).list_topics(
            timeout=timeout
        )
        return metadata is not None
    except Exception:
        # Any failure at all means "not reachable" - a bad host, a refused
        # connection and a timeout are all the same answer to the caller.
        return False
