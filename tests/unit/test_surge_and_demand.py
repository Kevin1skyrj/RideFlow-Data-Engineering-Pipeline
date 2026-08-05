"""Surge and demand behaviour.

These are the tests that make the *pricing effectiveness* and *marketplace
health* questions in PROJECT_PLAN.md section 2.1 answerable. If surge were
sampled at random rather than derived from imbalance, every one of these would
fail - which is exactly the point of writing them.
"""

from __future__ import annotations

from collections import defaultdict
from datetime import UTC, datetime
from decimal import Decimal
from zoneinfo import ZoneInfo

import pytest

from event_generator.config import AnomalyConfig, GeneratorConfig
from event_generator.generator import generate

IST = ZoneInfo("Asia/Kolkata")
MIDNIGHT_IST = datetime(2026, 3, 16, 18, 30, tzinfo=UTC)  # = 2026-03-17 00:00 IST


@pytest.fixture(scope="module")
def full_day(reference, calibration):
    """A full local day, so hour-of-day behaviour is observable."""
    config = GeneratorConfig(
        start_time=MIDNIGHT_IST,
        duration_sec=86400,
        trips_per_hour=300,
        seed=2026,
        anomalies=AnomalyConfig.disabled(),
    )
    return generate(config, reference=reference, calibration=calibration)


@pytest.fixture(scope="module")
def completed(full_day):
    return [e for e in full_day.events if e.event_type == "RideCompleted"]


@pytest.fixture(scope="module")
def surge_by_hour(completed):
    grouped: dict[int, list[float]] = defaultdict(list)
    for event in completed:
        hour = event.payload["completed_at"].astimezone(IST).hour
        grouped[hour].append(float(event.payload["surge_multiplier"]))
    return {h: sum(v) / len(v) for h, v in grouped.items() if v}


class TestSurge:
    def test_surge_never_falls_below_one(self, completed):
        """Contract floor. A multiplier below 1.00 would make surge_amount
        negative and break invariant F4."""
        for event in completed:
            assert event.payload["surge_multiplier"] >= Decimal("1.0")

    def test_surge_respects_the_city_cap(self, completed, city):
        for event in completed:
            assert event.payload["surge_multiplier"] <= city.max_surge_multiplier

    def test_surge_actually_varies(self, completed):
        """A generator with broken surge produces a single constant value, and
        every pricing analysis downstream becomes meaningless."""
        distinct = {event.payload["surge_multiplier"] for event in completed}
        assert len(distinct) >= 5, f"surge took only {len(distinct)} distinct values"

    def test_a_plausible_minority_of_trips_surge(self, completed):
        """Surge on nearly every trip means the threshold is too low; surge on
        almost none means it never fires. Real platforms sit well inside these
        bounds."""
        surged = sum(1 for e in completed if e.payload["surge_multiplier"] > Decimal("1.0"))
        share = surged / len(completed)
        assert 0.10 <= share <= 0.55, f"{share:.1%} of trips surged"

    def test_evening_peak_surges_more_than_the_quiet_small_hours(self, surge_by_hour):
        """The load-bearing assertion: surge must correlate with demand.

        If surge were random this fails, which is what distinguishes derived
        surge from sampled surge.
        """
        evening = max(surge_by_hour.get(h, 1.0) for h in (18, 19, 20))
        quiet = max(surge_by_hour.get(h, 1.0) for h in (2, 3, 4))
        assert evening > quiet * 1.10, f"evening={evening:.2f} quiet={quiet:.2f}"

    def test_the_small_hours_are_essentially_unsurged(self, surge_by_hour):
        """Surging at 3 a.m. is small-number noise, not scarcity."""
        for hour in (2, 3, 4):
            assert surge_by_hour.get(hour, 1.0) < 1.10, f"hour {hour} surged"

    def test_surge_is_quantised_for_display(self, completed):
        """Platforms show 1.2x, not 1.23847x."""
        for event in completed:
            multiplier = event.payload["surge_multiplier"]
            assert (multiplier * 10) % 1 == 0, f"unquantised surge {multiplier}"


class TestDemandCurve:
    def test_demand_follows_local_time_not_utc(self, full_day):
        """Events store UTC; 08:00 IST is 02:30 UTC. Driving demand off UTC
        would put the Bengaluru morning peak at 2 a.m."""
        requests = [e for e in full_day.events if e.event_type == "RideRequested"]
        by_hour: dict[int, int] = defaultdict(int)
        for event in requests:
            by_hour[event.payload["requested_at"].astimezone(IST).hour] += 1

        morning = max(by_hour[h] for h in (8, 9, 10))
        evening = max(by_hour[h] for h in (18, 19, 20))
        small_hours = max(by_hour[h] for h in (2, 3, 4))

        assert morning > small_hours * 3, "no morning peak in local time"
        assert evening > small_hours * 3, "no evening peak in local time"

    def test_commercial_zones_pull_inbound_in_the_morning(self, full_day, reference):
        """Origin-destination flow must have direction. Uniform OD choice makes
        every zone-level metric in the warehouse meaningless."""
        zones = {z.zone_id: z for z in reference.active_zones(1)}
        morning_commercial = evening_commercial = 0
        morning_total = evening_total = 0

        for event in full_day.events:
            if event.event_type != "RideRequested":
                continue
            hour = event.payload["requested_at"].astimezone(IST).hour
            zone = zones.get(event.payload["dropoff_zone_id"])
            if zone is None:
                continue
            if hour in (8, 9, 10):
                morning_total += 1
                morning_commercial += zone.zone_type == "COMMERCIAL"
            elif hour in (18, 19, 20):
                evening_total += 1
                evening_commercial += zone.zone_type == "COMMERCIAL"

        morning_share = morning_commercial / morning_total
        evening_share = evening_commercial / evening_total
        assert morning_share > evening_share, (
            f"commercial dropoff share: morning {morning_share:.1%} "
            f"vs evening {evening_share:.1%} - flow has no direction"
        )

    def test_trip_distances_are_plausible(self, completed):
        distances = sorted(float(e.payload["distance_km"]) for e in completed)
        median = distances[len(distances) // 2]
        assert 3.0 <= median <= 20.0, f"median trip {median:.1f} km"
        assert distances[0] > 0

    def test_every_vehicle_tier_is_used(self, full_day):
        """AUTO and BIKE carry a large real share in Bengaluru. A generator that
        never emitted them would misrepresent the market badly."""
        seen = {
            e.payload["vehicle_type"] for e in full_day.events if e.event_type == "RideRequested"
        }
        assert {"ECONOMY", "PREMIUM", "XL", "POOL", "AUTO", "BIKE"} <= seen
