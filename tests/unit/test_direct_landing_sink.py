from __future__ import annotations

from datetime import UTC, datetime

import pyarrow.parquet as pq

from event_generator.config import AnomalyConfig, GeneratorConfig
from event_generator.envelope import Event
from event_generator.generator import generate
from ingestion.direct_landing_sink import DirectLandingSink


def test_direct_landing_sink_writes_valid_parquet_for_both_topics(tmp_path):
    generated = generate(
        GeneratorConfig(
            start_time=datetime(2026, 1, 1, 2, 30, tzinfo=UTC),
            duration_sec=60,
            trips_per_hour=600,
            driver_count=10,
            seed=42,
            anomalies=AnomalyConfig.disabled(),
        )
    )

    written = DirectLandingSink(tmp_path).write(generated.events)

    paths = list(tmp_path.glob("topic=*/dt=*/hour=*/*.parquet"))
    families = {path.parts[-4] for path in paths}
    rows = sum(pq.read_metadata(path).num_rows for path in paths)
    assert written == len(generated.events)
    assert rows == written
    assert families == {"topic=presence", "topic=trips"}


def test_direct_landing_sink_skips_messages_the_consumer_would_dlq(tmp_path):
    invalid = Event(
        event_id="evt-bad",
        event_type="RideRequested",
        event_timestamp=datetime(2026, 1, 1, 2, 30, tzinfo=UTC),
        ingested_at=datetime(2026, 1, 1, 2, 30, tzinfo=UTC),
        partition_key="trip-bad",
        correlation_id="trip-bad",
        causation_id=None,
        payload={},
        producer_service="test",
        producer_version="1.0.0",
        environment="local",
    )

    sink = DirectLandingSink(tmp_path)
    assert sink.write([invalid]) == 0
    assert sink.rejected == 1
    assert list(tmp_path.rglob("*.parquet")) == []
