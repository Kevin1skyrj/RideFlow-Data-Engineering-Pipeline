"""Event envelope construction and serialisation.

Envelope shape is fixed by docs/event_contract.md section 3.1. The generator
carries an `Event` object rather than a plain dict so the anomaly layer can
manipulate timing and identity *after* the event is fully formed - which is how
a duplicate or a late arrival is produced without touching business logic.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from typing import Any

CONTRACT_EVENT_TYPES = (
    "RideRequested",
    "RideAccepted",
    "DriverArrived",
    "RideStarted",
    "RideCompleted",
    "RideCancelled",
    "PaymentCompleted",
    "DriverOnline",
    "DriverOffline",
)

TRIP_EVENT_TYPES = frozenset(CONTRACT_EVENT_TYPES[:7])
PRESENCE_EVENT_TYPES = frozenset(CONTRACT_EVENT_TYPES[7:])


def iso_utc(moment: datetime) -> str:
    """ISO-8601 with millisecond precision and a literal Z.

    The contract requires the Z suffix (section 3.1). datetime.isoformat()
    renders UTC as '+00:00', which would fail the validation regex, so the
    format is built explicitly rather than relying on isoformat().
    """
    return f"{moment.strftime('%Y-%m-%dT%H:%M:%S')}" f".{moment.microsecond // 1000:03d}Z"


def _encode(value: Any) -> Any:
    """JSON-safe encoding that preserves monetary precision.

    Decimals become floats only at the serialisation boundary, and only after
    quantisation to 2dp in pricing.py. Money is never a float during arithmetic.
    """
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, datetime):
        return iso_utc(value)
    if isinstance(value, dict):
        return {k: _encode(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_encode(v) for v in value]
    return value


@dataclass
class Event:
    event_id: str
    event_type: str
    event_timestamp: datetime
    ingested_at: datetime
    partition_key: str
    correlation_id: str
    causation_id: str | None
    payload: dict[str, Any]
    producer_service: str
    producer_version: str
    environment: str
    event_version: str = "1.0.0"

    anomaly_tags: list[str] = field(default_factory=list)
    """Generator-internal labels. Never serialised into the event.

    Labelled anomalies are what let a test assert the pipeline *handled* a
    duplicate, rather than merely that it did not crash.
    """

    def tag(self, label: str) -> None:
        if label not in self.anomaly_tags:
            self.anomaly_tags.append(label)

    @property
    def lateness_sec(self) -> float:
        return (self.ingested_at - self.event_timestamp).total_seconds()

    def to_dict(self) -> dict[str, Any]:
        """Contract-shaped dict. Key order matches the contract for readability
        in the landing zone; JSON object order is not semantically meaningful."""
        return {
            "event_id": self.event_id,
            "event_type": self.event_type,
            "event_version": self.event_version,
            "event_timestamp": iso_utc(self.event_timestamp),
            "ingested_at": iso_utc(self.ingested_at),
            "partition_key": self.partition_key,
            "correlation_id": self.correlation_id,
            "causation_id": self.causation_id,
            "producer_service": self.producer_service,
            "producer_version": self.producer_version,
            "environment": self.environment,
            "payload": _encode(self.payload),
        }

    def copy_with(self, **overrides: Any) -> Event:
        import copy

        clone = Event(
            event_id=self.event_id,
            event_type=self.event_type,
            event_timestamp=self.event_timestamp,
            ingested_at=self.ingested_at,
            partition_key=self.partition_key,
            correlation_id=self.correlation_id,
            causation_id=self.causation_id,
            payload=copy.deepcopy(self.payload),
            producer_service=self.producer_service,
            producer_version=self.producer_version,
            environment=self.environment,
            event_version=self.event_version,
            anomaly_tags=list(self.anomaly_tags),
        )
        for key, value in overrides.items():
            setattr(clone, key, value)
        return clone
