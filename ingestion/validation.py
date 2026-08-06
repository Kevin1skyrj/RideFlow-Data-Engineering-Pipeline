"""Message validation and DLQ rejection reasons.

Implements the nine ordered checks in docs/etl_design.md section 2.1. Order
matters: each check assumes the previous one passed, so a malformed byte string
never reaches schema validation and produces a confusing error there instead of
an accurate MALFORMED_JSON.

What this deliberately does NOT check
-------------------------------------
Cross-event consistency. The consumer never verifies that a RideStarted has a
preceding RideAccepted, because it sees a *stream*, not a *trip*. A consumer
that rejected RideStarted for arriving after RideCompleted would discard
perfectly valid data - exactly the out-of-order case the generator produces on
purpose. Sequence rules S1-S8 are validated in the dbt intermediate layer,
against the fully assembled trip.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import StrEnum
from typing import Any

from jsonschema import Draft202012Validator

from ingestion.contract import (
    REQUIRED_ENVELOPE_FIELDS,
    SEMVER,
    known_event_types,
    load_schemas,
)

CLOCK_SKEW_TOLERANCE = timedelta(minutes=5)
"""Invariant T1/T3. Device clocks drift; 5 minutes absorbs that without
accepting data that is genuinely from the wrong time."""

MAX_LATENESS = timedelta(days=7)
"""Invariant T4. Beyond this, lateness is treated as corruption rather than
late data - and it is deliberately matched to Kafka retention, so an event that
is still recoverable is still considered valid."""


class RejectionReason(StrEnum):
    """DLQ reasons. Each maps to a distinct failure mode so the reason
    distribution is itself a diagnostic (kafka_design.md 6.3)."""

    MALFORMED_JSON = "MALFORMED_JSON"
    NOT_AN_OBJECT = "NOT_AN_OBJECT"
    MISSING_ENVELOPE_FIELD = "MISSING_ENVELOPE_FIELD"
    UNKNOWN_EVENT_TYPE = "UNKNOWN_EVENT_TYPE"
    INVALID_VERSION = "INVALID_VERSION"
    INVALID_TIMESTAMP = "INVALID_TIMESTAMP"
    TIMESTAMP_OUT_OF_BOUNDS = "TIMESTAMP_OUT_OF_BOUNDS"
    SCHEMA_VIOLATION = "SCHEMA_VIOLATION"


@dataclass(frozen=True)
class ValidEvent:
    """A message that passed every check, ready for the landing zone."""

    event_id: str
    event_type: str
    event_version: str
    event_timestamp: datetime
    ingested_at: datetime
    partition_key: str
    correlation_id: str
    causation_id: str | None
    producer_service: str
    producer_version: str
    environment: str
    payload_json: str

    @property
    def lateness_sec(self) -> float:
        return (self.ingested_at - self.event_timestamp).total_seconds()


@dataclass(frozen=True)
class Rejection:
    reason: RejectionReason
    detail: str


def parse_timestamp(value: Any) -> datetime | None:
    """Parse an ISO-8601 UTC timestamp, or None if unparseable."""
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo is not None else None


class Validator:
    """Validates raw Kafka message bytes against the event contract."""

    def __init__(self) -> None:
        schemas = load_schemas()
        self._envelope = Draft202012Validator(schemas["envelope"])
        self._payloads = {
            name: Draft202012Validator(schema)
            for name, schema in schemas.items()
            if name != "envelope"
        }
        self._known_types = known_event_types()

    def validate(self, raw: bytes) -> tuple[ValidEvent | None, Rejection | None]:
        """Returns (event, None) on success or (None, rejection) on failure."""

        # 1. Parseable JSON
        try:
            document = json.loads(raw)
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            return None, Rejection(RejectionReason.MALFORMED_JSON, str(exc)[:200])

        if not isinstance(document, dict):
            return None, Rejection(
                RejectionReason.NOT_AN_OBJECT, f"top level is {type(document).__name__}"
            )

        # 2. Every required envelope field present
        missing = [f for f in REQUIRED_ENVELOPE_FIELDS if f not in document]
        if missing:
            return None, Rejection(
                RejectionReason.MISSING_ENVELOPE_FIELD, f"missing: {', '.join(missing)}"
            )
        if "causation_id" not in document:
            return None, Rejection(
                RejectionReason.MISSING_ENVELOPE_FIELD,
                "missing: causation_id (nullable, but must be present)",
            )

        # 3. Known event type - the ONE enum where unknown means DLQ
        event_type = document["event_type"]
        if event_type not in self._known_types:
            return None, Rejection(
                RejectionReason.UNKNOWN_EVENT_TYPE,
                f"{event_type!r} is not one of the nine contract types",
            )

        # 4. Parseable version
        version = document["event_version"]
        if not isinstance(version, str) or not SEMVER.match(version):
            return None, Rejection(RejectionReason.INVALID_VERSION, f"{version!r}")

        # 5. Parseable timestamps
        event_timestamp = parse_timestamp(document["event_timestamp"])
        ingested_at = parse_timestamp(document["ingested_at"])
        if event_timestamp is None or ingested_at is None:
            return None, Rejection(
                RejectionReason.INVALID_TIMESTAMP,
                f"event_timestamp={document['event_timestamp']!r} "
                f"ingested_at={document['ingested_at']!r}",
            )

        # 6. Timestamps within T3/T4 bounds
        drift = ingested_at - event_timestamp
        if drift < -CLOCK_SKEW_TOLERANCE:
            return None, Rejection(
                RejectionReason.TIMESTAMP_OUT_OF_BOUNDS,
                f"event_timestamp is {-drift} ahead of ingested_at (T3)",
            )
        if drift > MAX_LATENESS:
            return None, Rejection(
                RejectionReason.TIMESTAMP_OUT_OF_BOUNDS,
                f"event is {drift} late, beyond the {MAX_LATENESS} tolerance (T4)",
            )

        # 7. Envelope shape
        envelope_errors = list(self._envelope.iter_errors(document))
        if envelope_errors:
            first = envelope_errors[0]
            return None, Rejection(
                RejectionReason.SCHEMA_VIOLATION,
                f"envelope{list(first.path)}: {first.message}"[:300],
            )

        # 8. Payload shape for this event type
        payload = document["payload"]
        payload_errors = list(self._payloads[event_type].iter_errors(payload))
        if payload_errors:
            first = payload_errors[0]
            return None, Rejection(
                RejectionReason.SCHEMA_VIOLATION,
                f"payload{list(first.path)}: {first.message}"[:300],
            )

        # 9. Unknown ENUM VALUES (other than event_type) and unknown extra
        #    fields are NOT checked here on purpose. They are new information,
        #    not corruption, and rejecting them would force a coordinated
        #    producer/consumer release for every new enum value.

        return (
            ValidEvent(
                event_id=document["event_id"],
                event_type=event_type,
                event_version=version,
                event_timestamp=event_timestamp,
                ingested_at=ingested_at,
                partition_key=document["partition_key"],
                correlation_id=document["correlation_id"],
                causation_id=document["causation_id"],
                producer_service=document["producer_service"],
                producer_version=document["producer_version"],
                environment=document["environment"],
                payload_json=json.dumps(payload, separators=(",", ":"), sort_keys=True),
            ),
            None,
        )
