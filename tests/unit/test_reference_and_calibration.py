"""Reference data and calibration provenance.

The provenance test is not bureaucracy. It is what stops "calibrated against
real data" from silently becoming a claim that covers parameters which were
invented (docs/data_strategy.md sections 3.3 and 7).
"""

from __future__ import annotations

from decimal import Decimal

import pytest

VALID_SOURCES = {"tlc_calibrated", "hand_tuned"}

NO_NYC_ANALOGUE = {"AUTO", "BIKE"}
"""New York has no auto-rickshaws and no bike taxis, so these tiers can never
be honestly labelled tlc_calibrated."""


class TestCalibrationProvenance:
    def test_every_block_declares_a_source(self, calibration):
        sources = calibration.sources()
        assert sources, "no calibration block carries a source label"
        for key, source in sorted(sources.items()):
            assert source in VALID_SOURCES, f"{key} has invalid source {source!r}"

    def test_tiers_without_a_nyc_analogue_are_not_claimed_as_calibrated(self, calibration):
        by_type = calibration["trip_distance_km"]["by_vehicle_type"]
        for tier in NO_NYC_ANALOGUE:
            assert (
                by_type[tier]["source"] == "hand_tuned"
            ), f"{tier} cannot be tlc_calibrated - NYC has no equivalent vehicle class"

    def test_supply_side_blocks_are_hand_tuned(self, calibration):
        """TLC has no driver-session data at all, so anything describing supply
        must be hand_tuned."""
        assert calibration["driver_sessions"]["source"] == "hand_tuned"
        assert calibration["funnel_rates"]["source"] == "hand_tuned"

    def test_hourly_curves_cover_a_full_day(self, calibration):
        for key in ("hourly_demand_weekday", "hourly_demand_weekend"):
            values = calibration[key]["values"]
            assert len(values) == 24, f"{key} has {len(values)} hours"
            assert all(v > 0 for v in values)

    def test_weekday_curve_is_bimodal(self, calibration):
        """Commute peaks are the defining shape of weekday demand. A flat curve
        would make every peak-hour metric meaningless."""
        values = calibration["hourly_demand_weekday"]["values"]
        morning = max(values[7:11])
        evening = max(values[17:21])
        midday = min(values[12:16])
        assert morning > midday * 1.3
        assert evening > midday * 1.3

    def test_probability_weights_are_normalised(self, calibration):
        for block in (
            "vehicle_type_mix",
            "customer_tier_mix",
            "payment_method_mix",
            "weather_distribution",
        ):
            weights = calibration[block]["weights"]
            assert abs(sum(weights.values()) - 1.0) < 0.001, f"{block} does not sum to 1"


class TestReferenceData:
    def test_every_dimension_has_an_unknown_row(self):
        """Surrogate key -1 keeps unresolved lookups countable instead of
        silently dropping fact rows from every aggregate."""
        import csv

        from event_generator.config import SEEDS_DIR

        for path in sorted(SEEDS_DIR.glob("dim_*.csv")):
            with path.open(newline="", encoding="utf-8") as fh:
                rows = list(csv.DictReader(fh))
            key_column = next(c for c in rows[0] if c.endswith("_id"))
            keys = {int(r[key_column]) for r in rows}
            assert -1 in keys, f"{path.name} has no UNKNOWN row at -1"

    def test_zone_coordinates_are_real_and_inside_the_city(self, reference):
        """Coordinates are geocoded from OpenStreetMap, not invented. Anything
        outside the Bengaluru bounding box is a geocoding failure."""
        for zone in reference.active_zones(1):
            assert 12.70 <= zone.centroid_lat <= 13.25, f"{zone.zone_code} lat"
            assert 77.35 <= zone.centroid_lon <= 77.85, f"{zone.zone_code} lon"

    def test_the_m1_sample_fixture_zone_ids_still_exist(self, reference):
        """docs/samples/sample_events.json references these. Renumbering zones
        would silently invalidate the M1 fixture."""
        required = {87, 118, 205, 260, 331, 412}
        assert required <= {z.zone_id for z in reference.active_zones(1)}

    def test_at_least_one_airport_zone_exists(self, reference):
        """Invariant F6 gates airport_fee on this flag. With no airport zone the
        fee path would never execute and the rule would be untested."""
        airports = [z for z in reference.active_zones(1) if z.is_airport_zone]
        assert airports

    def test_city_pricing_is_positive(self, city):
        for field in ("base_fare", "per_km_rate", "per_min_rate", "commission_pct", "tax_pct"):
            assert getattr(city, field) > Decimal("0"), field
        assert city.max_surge_multiplier >= Decimal("1")
        assert city.currency_code == "INR"

    def test_vehicle_tiers_are_ordered_by_price(self, reference):
        economy = reference.vehicle_types["ECONOMY"]
        premium = reference.vehicle_types["PREMIUM"]
        bike = reference.vehicle_types["BIKE"]
        assert premium.base_fare_multiplier > economy.base_fare_multiplier
        assert bike.base_fare_multiplier < economy.base_fare_multiplier

    @pytest.mark.parametrize("code", ["CASH", "CARD", "UPI", "WALLET"])
    def test_expected_payment_methods_are_active(self, reference, code):
        assert code in reference.payment_methods
