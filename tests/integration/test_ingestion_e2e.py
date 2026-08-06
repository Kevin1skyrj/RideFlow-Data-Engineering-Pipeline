"""End-to-end ingestion: Kafka -> validation -> Parquet landing zone.

Requires a running broker. Skips with an actionable reason otherwise.
"""

from __future__ import annotations

import json
import uuid
from datetime import UTC, datetime

import pyarrow.parquet as pq
import pytest

from event_generator.config import AnomalyConfig, GeneratorConfig
from event_generator.generator import generate
from event_generator.kafka_sink import KafkaSink, ProducerConfig, broker_available
from ingestion.config import ConsumerConfig
from ingestion.consumer import IngestionConsumer

BOOTSTRAP = "localhost:29092"

pytestmark = pytest.mark.skipif(
    not broker_available(BOOTSTRAP),
    reason=(
        f"No Kafka broker at {BOOTSTRAP}. "
        "Start it with: docker compose -f docker/docker-compose.yml up -d"
    ),
)


def _publish(events) -> int:
    return KafkaSink(ProducerConfig(bootstrap_servers=BOOTSTRAP)).write(events)


def _consume(tmp_path, group: str, *, idle: float = 12.0) -> IngestionConsumer:
    config = ConsumerConfig(
        bootstrap_servers=BOOTSTRAP,
        group_id=group,
        landing_zone=tmp_path,
        batch_max_events=500,
    )
    consumer = IngestionConsumer(config)
    consumer.run(idle_timeout=idle)
    return consumer


def _landed_ids(tmp_path) -> set[str]:
    ids: set[str] = set()
    for path in tmp_path.rglob("*.parquet"):
        ids.update(pq.read_table(path, columns=["event_id"]).column("event_id").to_pylist())
    return ids


@pytest.fixture(scope="module")
def published():
    """A uniquely-seeded batch so it cannot collide with other runs."""
    config = GeneratorConfig(
        start_time=datetime(2026, 3, 17, 2, 30, tzinfo=UTC),
        duration_sec=900,
        trips_per_hour=200,
        seed=int(uuid.uuid4().int % 100_000),
        anomalies=AnomalyConfig.disabled(),
    )
    events = generate(config).events
    _publish(events)
    return events


class TestEndToEnd:
    def test_published_events_reach_the_landing_zone(self, tmp_path, published):
        """The M4 exit criterion, minus the crash."""
        consumer = _consume(tmp_path, f"e2e-{uuid.uuid4().hex[:8]}")
        stats = consumer.stats

        assert stats.polled > 0, "consumed nothing"
        assert stats.reconciles(), (
            f"N1 broken: polled={stats.polled} landed={stats.landed} "
            f"rejected={stats.rejected} dupes={stats.duplicates_dropped}"
        )

        landed = _landed_ids(tmp_path)
        missing = {e.event_id for e in published} - landed
        assert not missing, f"{len(missing)} published events never landed"

    def test_landing_zone_is_hive_partitioned(self, tmp_path, published):
        _consume(tmp_path, f"e2e-part-{uuid.uuid4().hex[:8]}")
        files = list(tmp_path.rglob("*.parquet"))
        assert files, "no parquet written"
        for path in files:
            parts = {p.split("=")[0] for p in path.parts if "=" in p}
            assert {"topic", "dt", "hour"} <= parts, f"bad layout: {path}"

    def test_kafka_coordinates_are_recorded(self, tmp_path, published):
        """Topic/partition/offset are what make a warehouse row traceable back
        to the exact message that produced it."""
        _consume(tmp_path, f"e2e-coord-{uuid.uuid4().hex[:8]}")
        path = next(tmp_path.rglob("*.parquet"))
        row = pq.read_table(path).to_pylist()[0]
        assert row["kafka_topic"].startswith("rideflow.")
        assert row["kafka_partition"] >= 0
        assert row["kafka_offset"] >= 0

    def test_payload_round_trips_unchanged(self, tmp_path, published):
        _consume(tmp_path, f"e2e-payload-{uuid.uuid4().hex[:8]}")
        by_id = {e.event_id: e for e in published}
        checked = 0
        for path in tmp_path.rglob("*.parquet"):
            for row in pq.read_table(path).to_pylist():
                original = by_id.get(row["event_id"])
                if original is None:
                    continue  # from another test's batch
                assert json.loads(row["payload_json"]) == original.to_dict()["payload"]
                checked += 1
                if checked >= 50:
                    return
        assert checked, "no payloads verified"


class TestMalformedHandling:
    def test_malformed_events_are_rejected_with_reasons(self, tmp_path):
        """Injected bad data must land in the DLQ, and valid events around it
        must be unaffected - one bad message may not stall a partition."""
        config = GeneratorConfig(
            start_time=datetime(2026, 3, 17, 2, 30, tzinfo=UTC),
            duration_sec=600,
            trips_per_hour=200,
            seed=int(uuid.uuid4().int % 100_000),
            anomalies=AnomalyConfig(
                duplicate=0.0,
                late_arrival=0.0,
                out_of_order=0.0,
                malformed=0.10,
                orphan=0.0,
                clock_skew=0.0,
            ),
        )
        events = generate(config).events
        _publish(events)

        consumer = _consume(tmp_path, f"e2e-dlq-{uuid.uuid4().hex[:8]}")
        stats = consumer.stats

        assert stats.rejected > 0, "injected malformed events were all accepted"
        assert stats.landed > 0, "valid events did not survive alongside bad ones"
        assert stats.reconciles()
        for reason in stats.rejections_by_reason:
            assert reason in {
                "MALFORMED_JSON",
                "NOT_AN_OBJECT",
                "MISSING_ENVELOPE_FIELD",
                "UNKNOWN_EVENT_TYPE",
                "INVALID_VERSION",
                "INVALID_TIMESTAMP",
                "TIMESTAMP_OUT_OF_BOUNDS",
                "SCHEMA_VIOLATION",
            }


class TestThroughput:
    @pytest.mark.slow
    def test_sustains_the_target_rate(self, tmp_path):
        """PROJECT_PLAN.md 6.2 target: >= 1,000 events/sec on one consumer.

        Measured over the first-to-last-message window, NOT wall clock - wall
        clock includes idle polling and shutdown, which understates the rate.
        """
        config = GeneratorConfig(
            start_time=datetime(2026, 3, 17, 2, 30, tzinfo=UTC),
            duration_sec=3600,
            trips_per_hour=600,
            seed=int(uuid.uuid4().int % 100_000),
            anomalies=AnomalyConfig.disabled(),
        )
        _publish(generate(config).events)

        consumer = _consume(tmp_path, f"e2e-perf-{uuid.uuid4().hex[:8]}", idle=10)
        stats = consumer.stats

        assert stats.polled >= 1000, f"only {stats.polled} messages to measure"
        assert stats.events_per_sec >= 1000, (
            f"{stats.events_per_sec:.0f} ev/s is below the 1,000 ev/s target "
            f"({stats.polled} messages in {stats.processing_sec:.2f}s)"
        )
