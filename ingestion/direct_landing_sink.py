"""Broker-free adapter for deterministic warehouse fixtures.

This sink deliberately reuses the production serializer, validator, and
Parquet writer.  It exists for tests that need to prove the warehouse can be
built from a clean checkout without also making Kafka availability part of the
same failure signal.  Kafka delivery and consumption are tested separately.
"""

from __future__ import annotations

from collections.abc import Iterable
from pathlib import Path

from event_generator.envelope import PRESENCE_EVENT_TYPES, Event
from event_generator.kafka_sink import serialise, topic_for
from ingestion.validation import Validator
from ingestion.writer import ParquetLandingZoneWriter


class DirectLandingSink:
    """Validate generated events and write production-shaped landing Parquet."""

    def __init__(self, root: Path) -> None:
        self.validator = Validator()
        self.writer = ParquetLandingZoneWriter(root)
        self.rejected = 0

    def write(self, events: Iterable[Event]) -> int:
        records = []
        for offset, generated_event in enumerate(events):
            event, rejection = self.validator.validate(serialise(generated_event))
            if rejection is not None or event is None:
                # Production routes these messages to a DLQ. This broker-free
                # fixture has no DLQ, so omit only the invalid messages while
                # retaining valid anomalies for warehouse quarantine tests.
                self.rejected += 1
                continue

            family = "presence" if generated_event.event_type in PRESENCE_EVENT_TYPES else "trips"
            # There is no broker in this mode. Stable synthetic coordinates
            # retain the landing schema and make each fixture row traceable.
            records.append((event, family, topic_for(generated_event), 0, offset))

        return self.writer.write(records).rows_written
