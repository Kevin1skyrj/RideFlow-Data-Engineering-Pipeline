"""Fare arithmetic: the financial invariants F1-F7.

These use property-based testing rather than hand-picked examples. Rounding
defects in money code appear at specific distance/duration combinations that a
human will not think to write down - the classic case being components that sum
to x.xx5 where half-up rounding at the total drifts a paisa from the sum of the
rounded parts (testing_strategy.md section 2.4).
"""

from __future__ import annotations

from decimal import Decimal

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from event_generator.pricing import ZERO, cancellation_fee, compute_fare, money

DISTANCES = st.decimals(min_value=Decimal("0.10"), max_value=Decimal("120.00"), places=2)
DURATIONS = st.integers(min_value=60, max_value=14400)
SURGES = st.decimals(min_value=Decimal("1.0"), max_value=Decimal("4.0"), places=1)
TOLLS = st.decimals(min_value=Decimal("0.00"), max_value=Decimal("400.00"), places=2)


@pytest.fixture(scope="module")
def economy(reference):
    return reference.vehicle_types["ECONOMY"]


@given(distance=DISTANCES, duration=DURATIONS, surge=SURGES, toll=TOLLS)
@settings(max_examples=400, deadline=None)
def test_fare_invariants_hold_for_arbitrary_inputs(city, economy, distance, duration, surge, toll):
    """F1, F2, F4, F5 for randomised valid inputs.

    check_invariants() raises on violation, so reaching the end is the assertion.
    """
    fare = compute_fare(
        city=city,
        vehicle_type=economy,
        distance_km=distance,
        duration_sec=duration,
        surge_multiplier=surge,
        is_airport_trip=False,
        toll_amount=toll,
    )
    fare.check_invariants()


@given(distance=DISTANCES, duration=DURATIONS, surge=SURGES)
@settings(max_examples=200, deadline=None)
def test_components_sum_exactly_to_total(city, economy, distance, duration, surge):
    """F1 with zero tolerance, not the +/-0.01 the contract permits.

    The contract allows a paisa of slack to absorb rounding. This asserts the
    stronger property that the implementation does not need it - rounding each
    component first and summing means the total is exact by construction.
    """
    fare = compute_fare(
        city=city,
        vehicle_type=economy,
        distance_km=distance,
        duration_sec=duration,
        surge_multiplier=surge,
        is_airport_trip=False,
    )
    total = (
        fare.base_fare
        + fare.distance_fare
        + fare.time_fare
        + fare.surge_amount
        + fare.airport_fee
        + fare.toll_amount
        + fare.booking_fee
        + fare.tax_amount
    )
    assert total == fare.total_fare


@given(distance=DISTANCES, duration=DURATIONS)
@settings(max_examples=100, deadline=None)
def test_no_surge_means_no_surge_amount(city, economy, distance, duration):
    fare = compute_fare(
        city=city,
        vehicle_type=economy,
        distance_km=distance,
        duration_sec=duration,
        surge_multiplier=Decimal("1.0"),
        is_airport_trip=False,
    )
    assert fare.surge_amount == ZERO


def test_airport_fee_only_on_airport_trips(city, economy):
    """Invariant F6."""
    common = dict(
        city=city,
        vehicle_type=economy,
        distance_km=Decimal("12.00"),
        duration_sec=1800,
        surge_multiplier=Decimal("1.0"),
    )
    assert compute_fare(**common, is_airport_trip=False).airport_fee == ZERO
    assert compute_fare(**common, is_airport_trip=True).airport_fee > ZERO


def test_surge_below_one_is_rejected(city, economy):
    """The contract floor is 1.00. A multiplier below it would make
    surge_amount negative and silently break F4."""
    with pytest.raises(ValueError, match="surge_multiplier"):
        compute_fare(
            city=city,
            vehicle_type=economy,
            distance_km=Decimal("5.00"),
            duration_sec=600,
            surge_multiplier=Decimal("0.8"),
            is_airport_trip=False,
        )


@pytest.mark.parametrize("distance,duration", [(Decimal("0"), 600), (Decimal("5"), 0)])
def test_degenerate_trips_are_rejected(city, economy, distance, duration):
    with pytest.raises(ValueError):
        compute_fare(
            city=city,
            vehicle_type=economy,
            distance_km=distance,
            duration_sec=duration,
            surge_multiplier=Decimal("1.0"),
            is_airport_trip=False,
        )


def test_commission_is_taken_on_the_pre_tax_subtotal(city, economy):
    """Tax is remitted to the government, not earned - so it must not be part of
    the base the platform takes commission on."""
    fare = compute_fare(
        city=city,
        vehicle_type=economy,
        distance_km=Decimal("10.00"),
        duration_sec=1200,
        surge_multiplier=Decimal("1.0"),
        is_airport_trip=False,
    )
    taxable = fare.total_fare - fare.tax_amount
    expected = money(taxable * city.commission_pct / Decimal(100))
    assert fare.platform_commission == expected
    assert fare.driver_payout + fare.platform_commission == taxable


def test_money_rounds_half_up():
    assert money(Decimal("1.005")) == Decimal("1.01")
    assert money(Decimal("2.675")) == Decimal("2.68")
    assert money(Decimal("0.004")) == Decimal("0.00")


def test_money_never_uses_binary_float_semantics():
    """0.1 + 0.2 != 0.3 in IEEE 754. Decimal is why this passes."""
    assert money(Decimal("0.1")) + money(Decimal("0.2")) == money(Decimal("0.3"))


class TestCancellationFee:
    def test_no_fee_inside_the_grace_period(self):
        assert (
            cancellation_fee(stage="MATCHED", is_fee_applicable=True, seconds_since_request=30)
            == ZERO
        )

    def test_no_fee_when_the_reason_does_not_allow_one(self):
        assert (
            cancellation_fee(stage="MATCHED", is_fee_applicable=False, seconds_since_request=600)
            == ZERO
        )

    def test_fee_rises_with_the_stage_reached(self):
        fees = [
            cancellation_fee(stage=stage, is_fee_applicable=True, seconds_since_request=600)
            for stage in ("REQUESTED", "MATCHED", "DRIVER_ARRIVED", "STARTED")
        ]
        assert fees == sorted(fees), "a later cancellation must never cost less"
        assert fees[0] == ZERO, "pre-match cancellation is always free"
