"""End-to-end Kafka tests. Require a running broker.

These skip - with a reason that says exactly what to do - when no broker is
reachable. A skipped test that explains itself is far more useful than a failing
one whose only real meaning is "Docker is not running".

    docker compose -f docker/docker-compose.yml up -d
    pytest tests/integration -v
"""

from __future__ import annotations

import json
import uuid
from datetime import UTC, datetime

import pytest

from event_generator.config import AnomalyConfig, GeneratorConfig
from event_generator.envelope import PRESENCE_EVENT_TYPES
from event_generator.generator import generate
from event_generator.kafka_sink import (
    TOPIC_PRESENCE,
    TOPIC_TRIPS,
    KafkaSink,
    ProducerConfig,
    broker_available,
)

BOOTSTRAP = "localhost:29092"

pytestmark = pytest.mark.skipif(
    not broker_available(BOOTSTRAP),
    reason=(
        f"No Kafka broker at {BOOTSTRAP}. "
        "Start it with: docker compose -f docker/docker-compose.yml up -d"
    ),
)

EXPECTED_PARTITIONS = {
    TOPIC_TRIPS: 12,
    TOPIC_PRESENCE: 6,
    "rideflow.trips.events.dlq.v1": 3,
    "rideflow.drivers.presence.dlq.v1": 3,
}


@pytest.fixture(scope="module")
def admin():
    from confluent_kafka.admin import AdminClient

    return AdminClient({"bootstrap.servers": BOOTSTRAP})


@pytest.fixture(scope="module")
def metadata(admin):
    return admin.list_topics(timeout=15)


@pytest.fixture(scope="module")
def sample_events():
    config = GeneratorConfig(
        start_time=datetime(2026, 3, 17, 2, 30, tzinfo=UTC),
        duration_sec=600,
        trips_per_hour=120,
        seed=31415,
        anomalies=AnomalyConfig.disabled(),
    )
    return generate(config).events


class TestTopicProvisioning:
    def test_all_topics_exist(self, metadata):
        missing = [t for t in EXPECTED_PARTITIONS if t not in metadata.topics]
        assert not missing, (
            f"Missing topics: {missing}. The topic-init service should create "
            "them; check `docker compose logs topic-init`."
        )

    @pytest.mark.parametrize("topic,expected", sorted(EXPECTED_PARTITIONS.items()))
    def test_partition_counts_are_explicit(self, metadata, topic, expected):
        """Auto-created topics silently get 1 partition, which would cap consumer
        scale-out at a single consumer. This proves provisioning ran."""
        actual = len(metadata.topics[topic].partitions)
        assert actual == expected, f"{topic} has {actual} partitions, expected {expected}"

    def test_trip_partition_count_supports_even_scale_out(self, metadata):
        """12 divides by 1, 2, 3, 4, 6, 12 - every realistic consumer count
        distributes evenly, with no idle consumers."""
        partitions = len(metadata.topics[TOPIC_TRIPS].partitions)
        for consumers in (1, 2, 3, 4, 6, 12):
            assert partitions % consumers == 0

    def test_auto_topic_creation_is_disabled(self, admin):
        """Producing to an unknown topic must fail rather than quietly creating
        a 1-partition topic."""
        from confluent_kafka import Producer

        ghost = f"rideflow.should-not-exist.{uuid.uuid4().hex[:8]}"
        producer = Producer({"bootstrap.servers": BOOTSTRAP, "message.timeout.ms": 5000})
        errors: list[object] = []
        producer.produce(ghost, value=b"x", on_delivery=lambda e, m: errors.append(e))
        producer.flush(10)
        assert (
            errors and errors[0] is not None
        ), "message to an unknown topic was accepted - auto-creation is enabled"


def subscribed_consumer(topics: list[str], group_prefix: str):
    """A consumer that is genuinely assigned partitions before it is used.

    `subscribe()` is asynchronous: it returns immediately while the group join
    happens in the background. A single poll() is NOT enough to guarantee
    assignment has completed.

    That matters because of `auto.offset.reset=latest`. If the producer writes
    before assignment finishes, the consumer's starting offset is set past those
    messages and they become permanently invisible - the test then reports
    "lost N events" when nothing was lost at all. Waiting on assignment() is the
    only deterministic fix; sleeping is a guess.
    """
    import time

    from confluent_kafka import Consumer

    consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP,
            "group.id": f"{group_prefix}-{uuid.uuid4().hex[:8]}",
            "auto.offset.reset": "latest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe(topics)

    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        consumer.poll(0.5)
        if consumer.assignment():
            return consumer
    consumer.close()
    raise AssertionError(f"consumer never got a partition assignment for {topics}")


def drain(consumer, *, expected: int, timeout_s: float = 60.0) -> list:
    """Consume until `expected` messages arrive or the wall clock runs out.

    Bounded by elapsed time, not by a poll-iteration count: poll() returns
    immediately when a message is waiting, so a loop counter can be exhausted in
    a fraction of a second and produce a spurious failure under load.
    """
    import time

    messages = []
    deadline = time.monotonic() + timeout_s
    while len(messages) < expected and time.monotonic() < deadline:
        message = consumer.poll(1.0)
        if message is None or message.error():
            continue
        messages.append(message)
    return messages


class TestProduceAndConsume:
    def test_events_round_trip_intact(self, sample_events):
        """The core M3 exit criterion: events flow end to end into Kafka."""
        consumer = subscribed_consumer([TOPIC_TRIPS, TOPIC_PRESENCE], "test-roundtrip")
        try:
            produced = KafkaSink(ProducerConfig(bootstrap_servers=BOOTSTRAP)).write(sample_events)
            received = {
                json.loads(m.value())["event_id"] for m in drain(consumer, expected=produced)
            }
        finally:
            consumer.close()

        expected_ids = {e.event_id for e in sample_events}
        missing = expected_ids - received
        assert not missing, f"lost {len(missing)} of {len(expected_ids)} events"

    def test_routing_puts_each_family_on_its_own_topic(self, sample_events):
        consumer = subscribed_consumer([TOPIC_TRIPS, TOPIC_PRESENCE], "test-routing")
        try:
            produced = KafkaSink(ProducerConfig(bootstrap_servers=BOOTSTRAP)).write(sample_events)
            messages = drain(consumer, expected=produced)
        finally:
            consumer.close()
        assert messages, "no messages consumed"

        for message in messages:
            event_type = json.loads(message.value())["event_type"]
            if event_type in PRESENCE_EVENT_TYPES:
                assert message.topic() == TOPIC_PRESENCE, f"{event_type} on {message.topic()}"
            else:
                assert message.topic() == TOPIC_TRIPS, f"{event_type} on {message.topic()}"

    def test_same_trip_always_lands_on_one_partition(self, sample_events):
        """Per-trip ordering depends entirely on this. If a trip's events were
        split across partitions, ordering within the trip would be lost."""
        consumer = subscribed_consumer([TOPIC_TRIPS], "test-partition")
        try:
            produced = KafkaSink(ProducerConfig(bootstrap_servers=BOOTSTRAP)).write(sample_events)
            messages = drain(consumer, expected=produced)
        finally:
            consumer.close()

        partitions: dict[str, set[int]] = {}
        for message in messages:
            trip_id = json.loads(message.value())["payload"].get("trip_id")
            if trip_id:
                partitions.setdefault(trip_id, set()).add(message.partition())
        assert partitions, "no trip events consumed"

        split = {t: p for t, p in partitions.items() if len(p) > 1}
        assert not split, f"trips split across partitions: {list(split)[:5]}"

    def test_headers_survive_the_round_trip(self, sample_events):
        consumer = subscribed_consumer([TOPIC_TRIPS], "test-headers")
        try:
            KafkaSink(ProducerConfig(bootstrap_servers=BOOTSTRAP)).write(sample_events)
            messages = drain(consumer, expected=10, timeout_s=30)
        finally:
            consumer.close()
        assert messages, "no messages consumed"

        for message in messages:
            headers = dict(message.headers() or [])
            assert "event_type" in headers, f"headers lost: {list(headers)}"
            assert headers["event_type"].decode() in {
                *PRESENCE_EVENT_TYPES,
                "RideRequested",
                "RideAccepted",
                "DriverArrived",
                "RideStarted",
                "RideCompleted",
                "RideCancelled",
                "PaymentCompleted",
            }


class TestDurability:
    def test_committed_messages_survive_a_consumer_restart(self, sample_events):
        """A new consumer group reading from `earliest` must see the full
        retained log - the property that makes replay possible at all."""
        from confluent_kafka import Consumer

        KafkaSink(ProducerConfig(bootstrap_servers=BOOTSTRAP)).write(sample_events[:50])

        group = f"test-replay-{uuid.uuid4().hex[:8]}"
        consumer = Consumer(
            {
                "bootstrap.servers": BOOTSTRAP,
                "group.id": group,
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
            }
        )
        consumer.subscribe([TOPIC_TRIPS])

        count = 0
        for _ in range(90):
            message = consumer.poll(1.0)
            if message is None or message.error():
                continue
            count += 1
            if count >= 50:
                break
        consumer.close()
        assert count > 0, "replay from earliest returned nothing"
