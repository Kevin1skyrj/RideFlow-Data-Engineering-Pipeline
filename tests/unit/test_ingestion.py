"""Ingestion consumer: validation, deduplication, batching, Parquet, offsets.

Runs without a broker using a fake consumer. The behaviours asserted here are
the ones whose failure modes are SILENT - lost messages, uncommitted offsets,
duplicates that survive - so they are worth pinning explicitly.
"""

from __future__ import annotations

import base64
import json
from datetime import datetime, timedelta

import pyarrow.parquet as pq
import pytest

from ingestion.config import DLQ_TRIPS, TOPIC_TRIPS, ConsumerConfig
from ingestion.consumer import IngestionConsumer
from ingestion.dlq import DlqProducer, build_dlq_message
from ingestion.validation import Rejection, RejectionReason, Validator
from ingestion.writer import LANDING_SCHEMA, ParquetLandingZoneWriter


# --------------------------------------------------------------------------
# Fakes
# --------------------------------------------------------------------------
class FakeMessage:
    def __init__(self, value, *, topic=TOPIC_TRIPS, partition=0, offset=0, key=b"k"):
        self._value, self._topic = value, topic
        self._partition, self._offset, self._key = partition, offset, key

    def value(self):
        return self._value

    def topic(self):
        return self._topic

    def partition(self):
        return self._partition

    def offset(self):
        return self._offset

    def key(self):
        return self._key

    def headers(self):
        return [("event_type", b"RideRequested")]

    def error(self):
        return None


class FakeKafkaConsumer:
    def __init__(self, messages):
        self._messages = list(messages)
        self.commits = 0
        self.closed = False

    def poll(self, _timeout):
        return self._messages.pop(0) if self._messages else None

    def commit(self, asynchronous=False):
        self.commits += 1

    def close(self):
        self.closed = True


class FakeDlqProducer(DlqProducer):
    def __init__(self):
        self.sent: list[tuple[str, bytes]] = []
        from ingestion.dlq import DlqReport

        self.report = DlqReport()

    def send(self, topic, *, key, value):
        self.sent.append((topic, value))
        self.report.produced += 1

    def flush(self, timeout=30.0):
        return 0


@pytest.fixture(scope="module")
def raw_events(clean_result):
    return [json.dumps(e.to_dict()).encode() for e in clean_result.events[:600]]


@pytest.fixture
def validator():
    return Validator()


# --------------------------------------------------------------------------
class TestValidation:
    def test_clean_generator_output_is_fully_accepted(self, validator, raw_events):
        """The generator and the consumer share one contract. Any rejection here
        means they have drifted apart."""
        failures = []
        for raw in raw_events:
            _, rejection = validator.validate(raw)
            if rejection:
                failures.append(f"{rejection.reason}: {rejection.detail[:120]}")
        assert not failures, f"{len(failures)} clean events rejected:\n" + "\n".join(failures[:5])

    @pytest.mark.parametrize(
        "raw,expected",
        [
            (b"{not json", RejectionReason.MALFORMED_JSON),
            (b"", RejectionReason.MALFORMED_JSON),
            (b"[1,2,3]", RejectionReason.NOT_AN_OBJECT),
            (b'"a string"', RejectionReason.NOT_AN_OBJECT),
            (b"{}", RejectionReason.MISSING_ENVELOPE_FIELD),
        ],
    )
    def test_structural_rejections(self, validator, raw, expected):
        event, rejection = validator.validate(raw)
        assert event is None
        assert rejection.reason == expected

    def test_unknown_event_type_is_rejected(self, validator, raw_events):
        """The one enum where unknown means DLQ: the consumer cannot infer the
        shape of a payload it has never seen."""
        document = json.loads(raw_events[0])
        document["event_type"] = "RideTeleported"
        _, rejection = validator.validate(json.dumps(document).encode())
        assert rejection.reason == RejectionReason.UNKNOWN_EVENT_TYPE

    def test_unknown_enum_VALUE_is_accepted(self, validator, raw_events):
        """A new cancellation reason must NOT go to the DLQ. Rejecting it would
        force a coordinated producer/consumer release for every new enum value
        (event_contract.md 2.3)."""
        for raw in raw_events:
            document = json.loads(raw)
            if document["event_type"] != "RideCancelled":
                continue
            document["payload"]["cancellation_reason_code"] = "DRIVER_ABDUCTED_BY_ALIENS"
            event, rejection = validator.validate(json.dumps(document).encode())
            assert rejection is None, f"new enum value rejected: {rejection}"
            assert event is not None
            return
        pytest.skip("no RideCancelled event in sample")

    def test_missing_causation_id_is_rejected_even_though_it_is_nullable(
        self, validator, raw_events
    ):
        """Null means 'starts a causal chain'. Absent means the producer is
        broken. The two are different and must not be conflated."""
        document = json.loads(raw_events[0])
        del document["causation_id"]
        _, rejection = validator.validate(json.dumps(document).encode())
        assert rejection.reason == RejectionReason.MISSING_ENVELOPE_FIELD
        assert "causation_id" in rejection.detail

    def test_null_causation_id_is_accepted(self, validator, raw_events):
        document = json.loads(raw_events[0])
        document["causation_id"] = None
        event, rejection = validator.validate(json.dumps(document).encode())
        assert rejection is None
        assert event.causation_id is None

    def test_invalid_semver_is_rejected(self, validator, raw_events):
        document = json.loads(raw_events[0])
        document["event_version"] = "v1"
        _, rejection = validator.validate(json.dumps(document).encode())
        assert rejection.reason == RejectionReason.INVALID_VERSION

    def test_unparseable_timestamp_is_rejected(self, validator, raw_events):
        document = json.loads(raw_events[0])
        document["event_timestamp"] = "last tuesday"
        _, rejection = validator.validate(json.dumps(document).encode())
        assert rejection.reason == RejectionReason.INVALID_TIMESTAMP

    def test_excessive_clock_skew_is_rejected(self, validator, raw_events):
        """Invariant T3: an event more than 5 minutes ahead of its own arrival."""
        document = json.loads(raw_events[0])
        ingested = datetime.fromisoformat(document["ingested_at"].replace("Z", "+00:00"))
        ahead = ingested + timedelta(minutes=20)
        document["event_timestamp"] = ahead.isoformat().replace("+00:00", "Z")
        _, rejection = validator.validate(json.dumps(document).encode())
        assert rejection.reason == RejectionReason.TIMESTAMP_OUT_OF_BOUNDS

    def test_small_clock_skew_is_tolerated(self, validator, raw_events):
        """Device clocks drift. Rejecting a 2-minute skew would discard valid
        data from every slightly-wrong phone."""
        document = json.loads(raw_events[0])
        ingested = datetime.fromisoformat(document["ingested_at"].replace("Z", "+00:00"))
        document["event_timestamp"] = (
            (ingested + timedelta(minutes=2)).isoformat().replace("+00:00", "Z")
        )
        _, rejection = validator.validate(json.dumps(document).encode())
        assert rejection is None

    def test_events_beyond_the_lateness_horizon_are_rejected(self, validator, raw_events):
        """Invariant T4, matched to Kafka retention: anything older is
        unrecoverable anyway, so it is treated as corruption."""
        document = json.loads(raw_events[0])
        ingested = datetime.fromisoformat(document["ingested_at"].replace("Z", "+00:00"))
        document["event_timestamp"] = (
            (ingested - timedelta(days=9)).isoformat().replace("+00:00", "Z")
        )
        _, rejection = validator.validate(json.dumps(document).encode())
        assert rejection.reason == RejectionReason.TIMESTAMP_OUT_OF_BOUNDS

    def test_negative_fare_is_a_schema_violation(self, validator, raw_events):
        for raw in raw_events:
            document = json.loads(raw)
            if document["event_type"] != "RideCompleted":
                continue
            document["payload"]["total_fare"] = -100.0
            _, rejection = validator.validate(json.dumps(document).encode())
            assert rejection.reason == RejectionReason.SCHEMA_VIOLATION
            return
        pytest.skip("no RideCompleted event in sample")

    def test_surge_below_the_contract_floor_is_rejected(self, validator, raw_events):
        for raw in raw_events:
            document = json.loads(raw)
            if document["event_type"] != "RideRequested":
                continue
            document["payload"]["surge_multiplier"] = 0.4
            _, rejection = validator.validate(json.dumps(document).encode())
            assert rejection.reason == RejectionReason.SCHEMA_VIOLATION
            return
        pytest.skip("no RideRequested event in sample")


# --------------------------------------------------------------------------
class TestParquetWriter:
    def test_writes_hive_partitions_on_ingested_at(self, tmp_path, validator, raw_events):
        writer = ParquetLandingZoneWriter(tmp_path)
        records = []
        for offset, raw in enumerate(raw_events[:200]):
            event, _ = validator.validate(raw)
            records.append((event, "trips", TOPIC_TRIPS, 0, offset))

        result = writer.write(records)
        assert result.rows_written == 200
        assert result.files_written >= 1

        for path in result.paths:
            assert "topic=trips" in str(path)
            assert "dt=" in str(path) and "hour=" in str(path)

    def test_written_schema_is_explicit_and_stable(self, tmp_path, validator, raw_events):
        """Inferred schemas break when a batch happens to have an all-null
        column. An explicit schema makes every file structurally identical."""
        writer = ParquetLandingZoneWriter(tmp_path)
        event, _ = validator.validate(raw_events[0])
        result = writer.write([(event, "trips", TOPIC_TRIPS, 0, 0)])
        table = pq.read_table(result.paths[0])
        assert table.schema.names == LANDING_SCHEMA.names

    def test_kafka_coordinates_are_preserved(self, tmp_path, validator, raw_events):
        """Topic/partition/offset are what make a warehouse row traceable back
        to the exact message that produced it."""
        writer = ParquetLandingZoneWriter(tmp_path)
        event, _ = validator.validate(raw_events[0])
        result = writer.write([(event, "trips", TOPIC_TRIPS, 7, 12345)])
        table = pq.read_table(result.paths[0]).to_pylist()
        assert table[0]["kafka_partition"] == 7
        assert table[0]["kafka_offset"] == 12345

    def test_payload_survives_as_json(self, tmp_path, validator, raw_events):
        writer = ParquetLandingZoneWriter(tmp_path)
        event, _ = validator.validate(raw_events[0])
        result = writer.write([(event, "trips", TOPIC_TRIPS, 0, 0)])
        stored = pq.read_table(result.paths[0]).to_pylist()[0]["payload_json"]
        assert json.loads(stored) == json.loads(event.payload_json)

    def test_no_temporary_files_are_left_behind(self, tmp_path, validator, raw_events):
        """A .tmp file in the landing zone would look like a corrupt part file
        to every downstream reader."""
        writer = ParquetLandingZoneWriter(tmp_path)
        records = [
            (validator.validate(raw)[0], "trips", TOPIC_TRIPS, 0, i)
            for i, raw in enumerate(raw_events[:50])
        ]
        writer.write(records)
        assert not list(tmp_path.rglob("*.tmp"))

    def test_empty_batch_writes_nothing(self, tmp_path):
        result = ParquetLandingZoneWriter(tmp_path).write([])
        assert result.rows_written == 0
        assert not list(tmp_path.rglob("*.parquet"))


# --------------------------------------------------------------------------
class TestDlqMessage:
    def test_preserves_raw_bytes_verbatim(self):
        """Malformed JSON has no parsed form. Re-serialising it would destroy
        the evidence needed to find the producer bug."""
        raw = b'{"broken": tru'
        message = build_dlq_message(
            rejection=Rejection(RejectionReason.MALFORMED_JSON, "boom"),
            raw=raw,
            consumer_group="g",
            source_topic=TOPIC_TRIPS,
            source_partition=3,
            source_offset=99,
            key=b"key",
            headers=[("event_type", b"RideRequested")],
        )
        document = json.loads(message)
        assert base64.b64decode(document["original_value_base64"]) == raw

    def test_carries_replay_coordinates(self):
        document = json.loads(
            build_dlq_message(
                rejection=Rejection(RejectionReason.SCHEMA_VIOLATION, "bad"),
                raw=b"{}",
                consumer_group="rideflow-trip-ingestor",
                source_topic=TOPIC_TRIPS,
                source_partition=5,
                source_offset=4242,
                key=b"k",
                headers=None,
            )
        )
        assert document["source_partition"] == 5
        assert document["source_offset"] == 4242
        assert document["rejection_reason"] == "SCHEMA_VIOLATION"
        assert document["consumer_group"] == "rideflow-trip-ingestor"


# --------------------------------------------------------------------------
def _consumer(tmp_path, messages, **overrides):
    config = ConsumerConfig(landing_zone=tmp_path, **overrides)
    fake = FakeKafkaConsumer(messages)
    return (
        IngestionConsumer(
            config,
            consumer=fake,
            dlq=FakeDlqProducer(),
            writer=ParquetLandingZoneWriter(tmp_path),
        ),
        fake,
    )


class TestConsumerLoop:
    def test_valid_messages_land_and_reconcile(self, tmp_path, raw_events):
        messages = [FakeMessage(raw, offset=i) for i, raw in enumerate(raw_events[:300])]
        consumer, _ = _consumer(tmp_path, messages)
        stats = consumer.run(idle_timeout=0.1)

        assert stats.polled == 300
        assert stats.landed == 300
        assert stats.rejected == 0
        assert stats.reconciles(), "N1 broken: messages vanished"

    def test_duplicates_within_a_batch_are_dropped(self, tmp_path, raw_events):
        """Pass 1 of two. This is an optimisation - it cannot see duplicates in
        a later batch. Staging is the authoritative pass."""
        messages = [FakeMessage(raw_events[0], offset=i) for i in range(5)]
        consumer, _ = _consumer(tmp_path, messages)
        stats = consumer.run(idle_timeout=0.1)

        assert stats.polled == 5
        assert stats.landed == 1
        assert stats.duplicates_dropped == 4
        assert stats.reconciles()

    def test_malformed_messages_go_to_the_dlq_with_a_reason(self, tmp_path, raw_events):
        messages = [
            FakeMessage(raw_events[0], offset=0),
            FakeMessage(b"{corrupt", offset=1),
            FakeMessage(raw_events[1], offset=2),
        ]
        consumer, _ = _consumer(tmp_path, messages)
        stats = consumer.run(idle_timeout=0.1)

        assert stats.landed == 2
        assert stats.rejected == 1
        assert stats.rejections_by_reason == {"MALFORMED_JSON": 1}
        assert consumer.dlq.sent[0][0] == DLQ_TRIPS

    def test_a_poison_message_does_not_stall_the_stream(self, tmp_path, raw_events):
        """The offset is committed for rejected messages too. Otherwise one bad
        byte halts a partition forever in a retry loop."""
        messages = [FakeMessage(b"{poison", offset=i) for i in range(10)]
        messages += [FakeMessage(raw_events[0], offset=10)]
        consumer, fake = _consumer(tmp_path, messages)
        stats = consumer.run(idle_timeout=0.1)

        assert stats.rejected == 10
        assert stats.landed == 1, "valid message after poison was never processed"
        assert fake.commits >= 1

    def test_offsets_are_committed_only_after_the_write(self, tmp_path, raw_events):
        """The safety property of the whole consumer. Committing first would
        make a crash lose data silently."""
        write_order: list[str] = []

        class RecordingWriter(ParquetLandingZoneWriter):
            def write(self, records):
                write_order.append("write")
                return super().write(records)

        class RecordingConsumer(FakeKafkaConsumer):
            def commit(self, asynchronous=False):
                write_order.append("commit")
                super().commit(asynchronous)

        messages = [FakeMessage(raw, offset=i) for i, raw in enumerate(raw_events[:20])]
        config = ConsumerConfig(landing_zone=tmp_path)
        fake = RecordingConsumer(messages)
        consumer = IngestionConsumer(
            config,
            consumer=fake,
            dlq=FakeDlqProducer(),
            writer=RecordingWriter(tmp_path),
        )
        consumer.run(idle_timeout=0.1)

        assert write_order, "nothing happened"
        assert write_order[0] == "write"
        for i in range(1, len(write_order)):
            if write_order[i] == "commit":
                assert write_order[i - 1] == "write", "commit preceded its write"

    def test_batch_flushes_on_size_trigger(self, tmp_path, raw_events):
        messages = [FakeMessage(raw, offset=i) for i, raw in enumerate(raw_events[:250])]
        consumer, _ = _consumer(tmp_path, messages, batch_max_events=50)
        stats = consumer.run(idle_timeout=0.1)
        assert stats.batches_flushed >= 5, "size trigger did not fire"

    def test_partial_batch_is_flushed_on_shutdown(self, tmp_path, raw_events):
        """Without this, the last partial batch would sit unwritten forever and
        those messages would be silently lost on a clean stop."""
        messages = [FakeMessage(raw, offset=i) for i, raw in enumerate(raw_events[:7])]
        consumer, fake = _consumer(tmp_path, messages, batch_max_events=10_000)
        stats = consumer.run(idle_timeout=0.1)
        assert stats.landed == 7
        assert fake.closed

    def test_consumer_is_always_closed(self, tmp_path, raw_events):
        messages = [FakeMessage(raw_events[0], offset=0)]
        consumer, fake = _consumer(tmp_path, messages)
        consumer.run(idle_timeout=0.1)
        assert fake.closed, "an unclosed consumer leaves its partitions assigned"


class TestConsumerConfig:
    def test_auto_commit_is_refused(self):
        """Config that permits silent data loss must not be constructible by
        accident."""
        with pytest.raises(ValueError, match=r"auto\.commit"):
            ConsumerConfig(enable_auto_commit=True).to_librdkafka()

    def test_defaults_match_the_design_document(self):
        config = ConsumerConfig()
        assert config.enable_auto_commit is False
        assert config.auto_offset_reset == "earliest"
        rendered = config.to_librdkafka()
        assert rendered["enable.auto.commit"] is False
        assert rendered["partition.assignment.strategy"] == "cooperative-sticky"
