"""Parquet writer for the immutable landing zone.

Layout (etl_design.md section 4.1):

    data/raw/topic=<family>/dt=YYYY-MM-DD/hour=HH/part-<uuid>.parquet

Partitioned on `ingested_at`, NOT `event_timestamp`. Arrival time is monotonic,
so partitions are strictly append-only. Partitioning on event time would require
writing a late event into an already-closed partition, which is exactly the
mutation the landing zone forbids.

Every file gets a unique name. Files are never appended to, never rewritten and
never deleted - the landing zone is the system of record, and it is the one
component the platform cannot rebuild from something else.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from ingestion.validation import ValidEvent

LANDING_SCHEMA = pa.schema(
    [
        pa.field("event_id", pa.string(), nullable=False),
        pa.field("event_type", pa.string(), nullable=False),
        pa.field("event_version", pa.string(), nullable=False),
        pa.field("event_timestamp", pa.timestamp("ms", tz="UTC"), nullable=False),
        pa.field("ingested_at", pa.timestamp("ms", tz="UTC"), nullable=False),
        pa.field("partition_key", pa.string(), nullable=False),
        pa.field("correlation_id", pa.string(), nullable=False),
        # Nullable on purpose: null means "this event starts a causal chain",
        # which is information, not missing data.
        pa.field("causation_id", pa.string(), nullable=True),
        pa.field("producer_service", pa.string(), nullable=False),
        pa.field("producer_version", pa.string(), nullable=False),
        pa.field("environment", pa.string(), nullable=False),
        # The payload stays as JSON rather than being flattened into typed
        # columns. Nine event types have nine different shapes; flattening would
        # produce a very wide, very sparse table. dbt staging extracts typed
        # columns per event type, where the shape is known.
        pa.field("payload_json", pa.string(), nullable=False),
        # Kafka coordinates: what makes any warehouse row traceable back to the
        # exact message that produced it.
        pa.field("kafka_topic", pa.string(), nullable=False),
        pa.field("kafka_partition", pa.int32(), nullable=False),
        pa.field("kafka_offset", pa.int64(), nullable=False),
        # ── PHYSICAL load time, distinct from ingested_at ───────────────────
        # Wall clock when this row was written to the landing zone.
        #
        # `ingested_at` is a BUSINESS timestamp - it says when the event was
        # received in the world being modelled, and the generator sets it so
        # that late arrivals and clock skew are simulable. It is therefore NOT
        # monotonic with respect to the physical load: re-consuming a topic
        # re-lands old events today while they keep an `ingested_at` from
        # months ago.
        #
        # Incremental windows need a clock that only ever moves forward, or a
        # replay silently falls outside the lookback and those rows never reach
        # the marts. That failure is invisible: dbt succeeds, the marts look
        # internally consistent, and only a cross-layer reconciliation catches
        # it. This column is that clock.
        pa.field("landed_at", pa.timestamp("ms", tz="UTC"), nullable=False),
    ]
)
"""Explicit Arrow schema, not inferred.

Inference reads types from the first batch, so a batch where every causation_id
happened to be null would infer a null-typed column and the next batch would
fail to append. An explicit schema makes every file structurally identical.
"""


@dataclass(frozen=True)
class WriteResult:
    files_written: int
    rows_written: int
    paths: list[Path]


class ParquetLandingZoneWriter:
    def __init__(self, root: Path, *, compression: str = "snappy") -> None:
        self.root = Path(root)
        self.compression = compression

    def _partition_dir(self, topic_family: str, event: ValidEvent) -> Path:
        stamp = event.ingested_at
        return (
            self.root
            / f"topic={topic_family}"
            / f"dt={stamp.strftime('%Y-%m-%d')}"
            / f"hour={stamp.strftime('%H')}"
        )

    def write(self, records: list[tuple[ValidEvent, str, str, int, int]]) -> WriteResult:
        """Write a batch as Parquet.

        `records` is (event, topic_family, kafka_topic, partition, offset).

        A batch can straddle an hour boundary, so it is grouped by target
        partition directory and written as one file per directory. Writing one
        file per record would multiply the small-file problem that is already
        the first expected bottleneck.
        """
        if not records:
            return WriteResult(0, 0, [])

        # One timestamp for the whole batch: every row in a batch really was
        # written at the same moment, and per-row clock reads would only add
        # meaningless microsecond variation.
        landed_at = datetime.now(UTC)

        grouped: dict[Path, list[tuple[ValidEvent, str, int, int]]] = {}
        for event, family, kafka_topic, partition, offset in records:
            directory = self._partition_dir(family, event)
            grouped.setdefault(directory, []).append((event, kafka_topic, partition, offset))

        paths: list[Path] = []
        rows = 0
        for directory in sorted(grouped):
            entries = grouped[directory]
            directory.mkdir(parents=True, exist_ok=True)
            path = directory / f"part-{uuid.uuid4().hex[:16]}.parquet"

            table = pa.Table.from_pydict(
                {
                    "event_id": [e.event_id for e, _, _, _ in entries],
                    "event_type": [e.event_type for e, _, _, _ in entries],
                    "event_version": [e.event_version for e, _, _, _ in entries],
                    "event_timestamp": [e.event_timestamp for e, _, _, _ in entries],
                    "ingested_at": [e.ingested_at for e, _, _, _ in entries],
                    "partition_key": [e.partition_key for e, _, _, _ in entries],
                    "correlation_id": [e.correlation_id for e, _, _, _ in entries],
                    "causation_id": [e.causation_id for e, _, _, _ in entries],
                    "producer_service": [e.producer_service for e, _, _, _ in entries],
                    "producer_version": [e.producer_version for e, _, _, _ in entries],
                    "environment": [e.environment for e, _, _, _ in entries],
                    "payload_json": [e.payload_json for e, _, _, _ in entries],
                    "kafka_topic": [t for _, t, _, _ in entries],
                    "kafka_partition": [p for _, _, p, _ in entries],
                    "kafka_offset": [o for _, _, _, o in entries],
                    "landed_at": [landed_at] * len(entries),
                },
                schema=LANDING_SCHEMA,
            )

            # Write to a temporary name first, then rename. A crash mid-write
            # would otherwise leave a truncated .parquet file in the landing
            # zone, and a corrupt file in the system of record is far worse than
            # a missing one - the missing one gets redelivered.
            temporary = path.with_suffix(".parquet.tmp")
            pq.write_table(table, temporary, compression=self.compression)
            temporary.replace(path)

            paths.append(path)
            rows += len(entries)

        return WriteResult(files_written=len(paths), rows_written=rows, paths=paths)
