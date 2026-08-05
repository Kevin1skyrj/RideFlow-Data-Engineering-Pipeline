"""Deliberate anomaly injection.

A pipeline that has never been shown bad data has not been tested. Every
anomaly here is *labelled* on the Event via `anomaly_tags`, so a test can assert
that the pipeline handled a duplicate correctly rather than merely that it did
not crash. The tags are generator-internal and never serialised.

Anomalies are applied to a known-good chain produced by lifecycle.py, so a
malformed event is always a deliberate mutation - never a bug in the business
logic that happened to produce invalid data.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta

from event_generator.calibration import Sampler
from event_generator.config import AnomalyConfig
from event_generator.envelope import Event

TAG_DUPLICATE = "duplicate"
TAG_LATE = "late_arrival"
TAG_OUT_OF_ORDER = "out_of_order"
TAG_MALFORMED = "malformed"
TAG_MALFORMED_JSON = "malformed_json"
TAG_ORPHAN_SOURCE = "orphan_source_dropped"
TAG_CLOCK_SKEW = "clock_skew"


@dataclass
class AnomalyReport:
    """Counts for the run summary and for the anomaly-rate test."""

    total_events: int = 0
    duplicates: int = 0
    late_arrivals: int = 0
    out_of_order: int = 0
    malformed: int = 0
    malformed_json: int = 0
    orphaned_trips: int = 0
    clock_skewed: int = 0

    def as_dict(self) -> dict[str, int]:
        return {
            "total_events": self.total_events,
            "duplicates": self.duplicates,
            "late_arrivals": self.late_arrivals,
            "out_of_order": self.out_of_order,
            "malformed": self.malformed,
            "malformed_json": self.malformed_json,
            "orphaned_trips": self.orphaned_trips,
            "clock_skewed": self.clock_skewed,
        }


def _corrupt_payload(event: Event, sampler: Sampler) -> None:
    """Produce a schema violation the consumer must route to the DLQ.

    Each variant maps to a distinct DLQ rejection reason in kafka_design.md
    section 6.3, so the DLQ reason distribution is itself testable.
    """
    payload = event.payload
    variants = ["drop_required", "negative_money", "bad_enum", "out_of_range"]
    variant = sampler.choice(variants)

    if variant == "drop_required":
        keys = sorted(k for k in payload if k not in {"trip_id", "driver_id"})
        if keys:
            del payload[sampler.choice(keys)]
    elif variant == "negative_money":
        money_keys = sorted(
            k
            for k in payload
            if k.endswith(("_fare", "_amount", "_fee", "_payout")) and payload[k] is not None
        )
        if money_keys:
            key = sampler.choice(money_keys)
            payload[key] = -abs(float(payload[key])) - 1.0
    elif variant == "bad_enum":
        if "vehicle_type" in payload:
            payload["vehicle_type"] = "TELEPORTER"
        elif "payment_method" in payload:
            payload["payment_method"] = "BARTER"
        else:
            payload["city_id"] = -999
    else:  # out_of_range
        if "surge_multiplier" in payload:
            payload["surge_multiplier"] = 0.4  # contract floor is 1.00
        elif "pickup_lat" in payload:
            payload["pickup_lat"] = 191.7
        else:
            payload["city_id"] = 0


def inject(
    events: list[Event], *, sampler: Sampler, config: AnomalyConfig
) -> tuple[list[Event], AnomalyReport]:
    """Apply anomalies to a good event stream.

    Order matters. Orphaning removes events, so it runs first; duplication adds
    events, so it runs last - otherwise a duplicate could be produced from an
    event that is subsequently dropped, leaving a duplicate with no original.
    """
    report = AnomalyReport()
    if config.total_rate() == 0.0:
        report.total_events = len(events)
        return events, report

    # --- orphaning: drop the RideRequested that anchors a trip --------------
    orphan_targets: set[str] = set()
    if config.orphan > 0:
        correlations = sorted({e.correlation_id for e in events if e.event_type != "DriverOnline"})
        for correlation_id in correlations:
            if sampler.chance(config.orphan):
                orphan_targets.add(correlation_id)

    surviving: list[Event] = []
    for event in events:
        if event.correlation_id in orphan_targets and event.event_type == "RideRequested":
            report.orphaned_trips += 1
            continue
        if event.correlation_id in orphan_targets:
            event.tag(TAG_ORPHAN_SOURCE)
        surviving.append(event)

    # --- per-event mutations ------------------------------------------------
    for event in surviving:
        if sampler.chance(config.late_arrival):
            delay = sampler.randint(*config.late_arrival_delay_sec)
            event.ingested_at = event.ingested_at + timedelta(seconds=delay)
            event.tag(TAG_LATE)
            report.late_arrivals += 1

        elif sampler.chance(config.out_of_order):
            delay = sampler.randint(*config.out_of_order_delay_sec)
            event.ingested_at = event.ingested_at + timedelta(seconds=delay)
            event.tag(TAG_OUT_OF_ORDER)
            report.out_of_order += 1

        if sampler.chance(config.clock_skew):
            # Producer clock ahead of the consumer: lateness goes negative.
            # Clamping it to zero would hide systematic skew, which is a real
            # producer defect worth surfacing.
            skew = sampler.randint(*config.clock_skew_sec)
            event.event_timestamp = event.ingested_at + timedelta(seconds=skew)
            event.tag(TAG_CLOCK_SKEW)
            report.clock_skewed += 1

        if sampler.chance(config.malformed):
            if sampler.chance(0.25):
                event.tag(TAG_MALFORMED_JSON)
                report.malformed_json += 1
            else:
                _corrupt_payload(event, sampler)
                report.malformed += 1
            event.tag(TAG_MALFORMED)

    # --- duplication --------------------------------------------------------
    duplicates: list[Event] = []
    for event in surviving:
        if sampler.chance(config.duplicate):
            delay = sampler.randint(*config.duplicate_delay_sec)
            clone = event.copy_with(ingested_at=event.ingested_at + timedelta(seconds=delay))
            clone.tag(TAG_DUPLICATE)
            duplicates.append(clone)
            report.duplicates += 1

    combined = surviving + duplicates
    report.total_events = len(combined)
    return combined, report
