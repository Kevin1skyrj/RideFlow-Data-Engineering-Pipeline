"""Anomaly injection: rates, labelling, and shape.

Anomalies are the whole reason the pipeline exists. These tests assert they are
present, labelled, and of the right kind - not merely that generation survived.
"""

from __future__ import annotations

from collections import Counter
from datetime import UTC, datetime

from event_generator.anomalies import (
    TAG_CLOCK_SKEW,
    TAG_DUPLICATE,
    TAG_LATE,
    TAG_MALFORMED,
    TAG_MALFORMED_JSON,
)
from event_generator.config import AnomalyConfig, GeneratorConfig
from event_generator.generator import generate

START = datetime(2026, 3, 17, 2, 30, tzinfo=UTC)


def test_clean_stream_has_no_anomalies(clean_result):
    report = clean_result.anomalies
    assert report.duplicates == 0
    assert report.late_arrivals == 0
    assert report.malformed == 0
    assert report.orphaned_trips == 0
    assert not any(e.anomaly_tags for e in clean_result.events)


def test_every_anomaly_type_is_produced(reference, calibration):
    """Rare anomalies can vanish in a short window, which would leave the
    pipeline's most important paths untested. Volume is raised here so all six
    types are guaranteed to appear."""
    config = GeneratorConfig(
        start_time=START,
        duration_sec=3600,
        trips_per_hour=1500,
        seed=99,
        anomalies=AnomalyConfig(
            duplicate=0.02,
            late_arrival=0.02,
            out_of_order=0.02,
            malformed=0.02,
            orphan=0.02,
            clock_skew=0.02,
        ),
    )
    report = generate(config, reference=reference, calibration=calibration).anomalies

    assert report.duplicates > 0
    assert report.late_arrivals > 0
    assert report.out_of_order > 0
    assert report.malformed > 0
    assert report.malformed_json > 0
    assert report.orphaned_trips > 0
    assert report.clock_skewed > 0


def test_injected_rates_land_near_their_configuration(reference, calibration):
    """Loose bounds deliberately: this catches an anomaly type wired to the
    wrong config field or dropped entirely, not sampling noise."""
    rate = 0.05
    config = GeneratorConfig(
        start_time=START,
        duration_sec=3600,
        trips_per_hour=1200,
        seed=7,
        anomalies=AnomalyConfig(
            duplicate=rate,
            late_arrival=rate,
            out_of_order=0.0,
            malformed=0.0,
            orphan=0.0,
            clock_skew=0.0,
        ),
    )
    result = generate(config, reference=reference, calibration=calibration)
    base = len(result.events) - result.anomalies.duplicates

    observed_duplicate = result.anomalies.duplicates / base
    observed_late = result.anomalies.late_arrivals / base
    assert rate * 0.5 <= observed_duplicate <= rate * 1.5
    assert rate * 0.5 <= observed_late <= rate * 1.5


def test_duplicates_reuse_the_event_id(result):
    """A duplicate is the SAME event redelivered. A fresh event_id would make it
    a distinct event and defeat deduplication entirely."""
    duplicates = [e for e in result.events if TAG_DUPLICATE in e.anomaly_tags]
    if not duplicates:
        return
    counts = Counter(e.event_id for e in result.events)
    for event in duplicates:
        assert counts[event.event_id] >= 2, f"{event.event_id} is not actually duplicated"


def test_duplicates_arrive_later_than_their_original(result):
    originals = {}
    for event in result.events:
        if TAG_DUPLICATE not in event.anomaly_tags:
            originals.setdefault(event.event_id, event)
    for event in result.events:
        if TAG_DUPLICATE in event.anomaly_tags and event.event_id in originals:
            assert event.ingested_at > originals[event.event_id].ingested_at


def test_duplicates_carry_an_identical_payload(result):
    """Deduplication keeps the earliest arrival on the basis that both copies
    are the same event. That only holds if the payloads really are identical."""
    by_id: dict[str, list] = {}
    for event in result.events:
        by_id.setdefault(event.event_id, []).append(event)
    for copies in by_id.values():
        if len(copies) < 2:
            continue
        first = copies[0].to_dict()["payload"]
        for other in copies[1:]:
            assert other.to_dict()["payload"] == first


def test_late_arrivals_exceed_the_lateness_threshold(result):
    """The consumer flags anything over 300s. An injected 'late' event that
    landed inside that window would not exercise the path at all."""
    late = [e for e in result.events if TAG_LATE in e.anomaly_tags]
    for event in late:
        assert event.lateness_sec > 300, f"only {event.lateness_sec}s late"


def test_clock_skew_produces_negative_lateness(result):
    skewed = [e for e in result.events if TAG_CLOCK_SKEW in e.anomaly_tags]
    for event in skewed:
        assert event.lateness_sec < 0, "clock skew must invert the normal ordering"


def test_orphaned_trips_lost_their_ride_requested(result):
    """The successors must survive: quarantine, never discard. A discarded
    orphan can never be recovered when its missing anchor arrives late."""
    orphaned = {
        e.correlation_id for e in result.events if "orphan_source_dropped" in e.anomaly_tags
    }
    for correlation_id in orphaned:
        chain = [e for e in result.events if e.correlation_id == correlation_id]
        assert chain, "orphaned trip lost every event, not just its anchor"
        assert not any(e.event_type == "RideRequested" for e in chain)


def test_malformed_events_are_labelled(result):
    for event in result.events:
        if TAG_MALFORMED_JSON in event.anomaly_tags:
            assert TAG_MALFORMED in event.anomaly_tags


def test_malformed_json_serialises_to_unparseable_bytes(reference, calibration):
    """A schema-violating event is still valid JSON. This variant must produce
    genuinely unparseable bytes so the MALFORMED_JSON rejection path - where
    there is no parsed form at all - is exercised."""
    import json

    from event_generator.sinks import _serialise

    config = GeneratorConfig(
        start_time=START,
        duration_sec=1800,
        trips_per_hour=900,
        seed=11,
        anomalies=AnomalyConfig(
            duplicate=0.0,
            late_arrival=0.0,
            out_of_order=0.0,
            malformed=0.08,
            orphan=0.0,
            clock_skew=0.0,
        ),
    )
    result = generate(config, reference=reference, calibration=calibration)
    broken = [e for e in result.events if TAG_MALFORMED_JSON in e.anomaly_tags]
    assert broken, "no malformed-JSON events generated"

    for event in broken:
        try:
            json.loads(_serialise(event))
        except json.JSONDecodeError:
            continue
        raise AssertionError("malformed_json event still parsed as valid JSON")


def test_out_of_order_events_break_arrival_ordering(reference, calibration):
    """The point of the anomaly is that arrival order stops matching causal
    order. If every 'out of order' event still arrived in sequence, the
    downstream assembly logic would never be tested."""
    config = GeneratorConfig(
        start_time=START,
        duration_sec=3600,
        trips_per_hour=800,
        seed=5150,
        anomalies=AnomalyConfig(
            duplicate=0.0,
            late_arrival=0.0,
            out_of_order=0.15,
            malformed=0.0,
            orphan=0.0,
            clock_skew=0.0,
        ),
    )
    result = generate(config, reference=reference, calibration=calibration)

    inversions = 0
    by_trip: dict[str, list] = {}
    for event in result.events:
        if "trip_id" in event.payload:
            by_trip.setdefault(event.payload["trip_id"], []).append(event)

    for chain in by_trip.values():
        causal = sorted(chain, key=lambda e: e.event_timestamp)
        arrival = sorted(chain, key=lambda e: e.ingested_at)
        if [e.event_id for e in causal] != [e.event_id for e in arrival]:
            inversions += 1

    assert inversions > 0, "no trip had its arrival order inverted"
