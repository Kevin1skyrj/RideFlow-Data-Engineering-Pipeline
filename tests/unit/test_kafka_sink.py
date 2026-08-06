"""Kafka producer: configuration, routing, headers, serialisation.

These run WITHOUT a broker, using a fake producer that records what would have
been sent. The settings asserted here are the ones whose defaults cause silent
data loss - so they are worth pinning in a test rather than trusting to a config
file nobody re-reads.
"""

from __future__ import annotations

import json

import pytest

from event_generator.anomalies import TAG_MALFORMED_JSON
from event_generator.envelope import PRESENCE_EVENT_TYPES, TRIP_EVENT_TYPES
from event_generator.kafka_sink import (
    TOPIC_PRESENCE,
    TOPIC_TRIPS,
    DeliveryReport,
    KafkaSink,
    KafkaUnavailableError,
    ProducerConfig,
    headers_for,
    serialise,
    topic_for,
)


class FakeProducer:
    """Records produce() calls and fires delivery callbacks immediately."""

    def __init__(self, *, fail: bool = False, buffer_error_once: bool = False) -> None:
        self.messages: list[dict] = []
        self.flushes = 0
        self._fail = fail
        self._buffer_error_once = buffer_error_once

    def produce(self, *, topic, key, value, headers, on_delivery):
        if self._buffer_error_once:
            self._buffer_error_once = False
            raise BufferError("queue full")
        self.messages.append({"topic": topic, "key": key, "value": value, "headers": dict(headers)})
        on_delivery("broker down" if self._fail else None, None)

    def poll(self, _timeout):
        return 0

    def flush(self, _timeout=None):
        self.flushes += 1
        return 0


@pytest.fixture
def events(clean_result):
    return clean_result.events[:400]


class TestProducerConfig:
    def test_defaults_match_the_design_document(self):
        config = ProducerConfig()
        assert config.acks == "all", "acks=1 risks silent loss on leader failure"
        assert config.enable_idempotence is True
        assert config.max_in_flight == 5
        assert config.compression_type == "snappy"

    def test_translates_to_librdkafka_property_names(self):
        rendered = ProducerConfig().to_librdkafka()
        assert rendered["bootstrap.servers"]
        assert rendered["enable.idempotence"] is True
        assert rendered["max.in.flight.requests.per.connection"] == 5
        assert rendered["acks"] == "all"

    def test_rejects_in_flight_above_five_with_idempotence(self):
        """librdkafka refuses this combination. Failing here with an explanation
        beats failing at connect time with a librdkafka error string."""
        with pytest.raises(ValueError, match=r"max\.in\.flight"):
            ProducerConfig(max_in_flight=10).to_librdkafka()

    def test_rejects_idempotence_without_acks_all(self):
        with pytest.raises(ValueError, match="acks=all"):
            ProducerConfig(acks="1").to_librdkafka()

    def test_reads_environment_overrides(self):
        config = ProducerConfig.from_env(
            {
                "KAFKA_BOOTSTRAP_SERVERS": "broker:9092",
                "KAFKA_RETRIES": "3",
                "KAFKA_ENABLE_IDEMPOTENCE": "false",
                "KAFKA_COMPRESSION_TYPE": "gzip",
            }
        )
        assert config.bootstrap_servers == "broker:9092"
        assert config.retries == 3
        assert config.enable_idempotence is False
        assert config.compression_type == "gzip"

    def test_env_defaults_apply_when_unset(self):
        config = ProducerConfig.from_env({})
        assert config.bootstrap_servers == "localhost:29092"
        assert config.enable_idempotence is True


class TestRouting:
    def test_trip_events_go_to_the_trips_topic(self):
        for event_type in sorted(TRIP_EVENT_TYPES):
            event = _stub(event_type)
            assert topic_for(event) == TOPIC_TRIPS

    def test_presence_events_go_to_their_own_topic(self):
        for event_type in sorted(PRESENCE_EVENT_TYPES):
            event = _stub(event_type)
            assert topic_for(event) == TOPIC_PRESENCE

    def test_the_two_families_never_share_a_topic(self):
        """Trip events key on trip_id, presence on driver_id. One topic has one
        partitioning strategy, so sharing would lose one of the two orderings."""
        assert TOPIC_TRIPS != TOPIC_PRESENCE

    def test_message_key_is_the_partition_key(self, events):
        producer = FakeProducer()
        KafkaSink(producer=producer).write(events)
        for event, message in zip(events, producer.messages, strict=True):
            assert message["key"] == event.partition_key.encode()


class TestHeaders:
    def test_headers_carry_routing_metadata(self):
        headers = dict(headers_for(_stub("RideRequested")))
        assert headers["event_type"] == b"RideRequested"
        assert headers["event_version"] == b"1.0.0"
        assert headers["content_type"] == b"application/json"
        assert headers["producer_service"]

    def test_headers_let_a_consumer_filter_without_deserialising(self, events):
        """That is the entire reason they duplicate envelope fields."""
        producer = FakeProducer()
        KafkaSink(producer=producer).write(events)
        for message in producer.messages:
            assert message["headers"]["event_type"].decode() in {
                *TRIP_EVENT_TYPES,
                *PRESENCE_EVENT_TYPES,
            }


class TestSerialisation:
    def test_normal_events_serialise_to_valid_json(self, events):
        for event in events:
            if TAG_MALFORMED_JSON in event.anomaly_tags:
                continue
            assert json.loads(serialise(event))["event_id"] == event.event_id

    def test_malformed_json_reaches_the_broker_still_broken(self, result):
        """Repairing it here would silently delete the DLQ test case: the
        MALFORMED_JSON rejection path needs genuinely unparseable bytes."""
        broken = [e for e in result.events if TAG_MALFORMED_JSON in e.anomaly_tags]
        if not broken:
            pytest.skip("no malformed-JSON anomalies in this run")
        for event in broken:
            with pytest.raises(json.JSONDecodeError):
                json.loads(serialise(event))


class TestSinkBehaviour:
    def test_every_event_is_produced(self, events):
        producer = FakeProducer()
        written = KafkaSink(producer=producer).write(events)
        assert written == len(events) == len(producer.messages)

    def test_delivery_callbacks_are_counted(self, events):
        producer = FakeProducer()
        sink = KafkaSink(producer=producer)
        sink.write(events)
        assert sink.report.delivered == len(events)
        assert sink.report.failed == 0

    def test_buffer_error_retries_instead_of_dropping(self, events):
        """A full local queue means the producer is outrunning the broker.
        Dropping the message would be silent data loss at the source."""
        producer = FakeProducer(buffer_error_once=True)
        written = KafkaSink(producer=producer).write(events)
        assert written == len(events)
        assert len(producer.messages) == len(events)

    def test_total_delivery_failure_raises_rather_than_reporting_success(self, events):
        """Returning a count with zero deliveries would look like success."""
        producer = FakeProducer(fail=True)
        with pytest.raises(KafkaUnavailableError, match="Is the broker running"):
            KafkaSink(producer=producer).write(events)

    def test_flush_is_called_before_returning(self, events):
        producer = FakeProducer()
        KafkaSink(producer=producer).write(events)
        assert producer.flushes >= 1, "unflushed messages may never reach the broker"


class TestDeliveryReport:
    def test_error_list_is_bounded(self):
        """A total outage must not accumulate one string per message."""
        report = DeliveryReport()
        for index in range(5000):
            report.record_error(f"error {index}")
        assert report.failed == 5000
        assert len(report.errors) <= 20


def _stub(event_type: str):
    from datetime import UTC, datetime

    from event_generator.envelope import Event

    moment = datetime(2026, 3, 17, 2, 42, tzinfo=UTC)
    return Event(
        event_id="9f2b1c7e-4a3d-4e18-b6c2-77a1e9d40c31",
        event_type=event_type,
        event_timestamp=moment,
        ingested_at=moment,
        partition_key="1a4f8c22-9e5b-4c07-8d3a-2f6b1e904d77",
        correlation_id="1a4f8c22-9e5b-4c07-8d3a-2f6b1e904d77",
        causation_id=None,
        payload={},
        producer_service="rideflow-event-generator",
        producer_version="0.1.0",
        environment="local",
    )
