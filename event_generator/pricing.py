"""Fare computation.

This module is the sole authority on money in the generator. It is written so
the financial invariants in event_contract.md section 7.4 hold *exactly*, not
merely within tolerance:

    F1  base + distance + time + surge + airport + toll + booking + tax = total
    F2  driver_payout + platform_commission = total - tax
    F4  every amount >= 0
    F5  surge_amount = (base + distance + time) * (surge_multiplier - 1)

The technique is to round each component to 2dp *first*, then derive the
taxable subtotal as the sum of those already-rounded values. Rounding the total
independently of its parts is what produces the classic one-paisa drift that
fails F1 on a small fraction of rows.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import ROUND_HALF_UP, Decimal

from event_generator.reference import City, VehicleType

CENTS = Decimal("0.01")
ZERO = Decimal("0.00")


def money(value: Decimal | float | int | str) -> Decimal:
    """Quantise to 2dp, half-up. Never use float for currency."""
    if not isinstance(value, Decimal):
        value = Decimal(str(value))
    return value.quantize(CENTS, rounding=ROUND_HALF_UP)


@dataclass(frozen=True)
class FareBreakdown:
    base_fare: Decimal
    distance_fare: Decimal
    time_fare: Decimal
    surge_multiplier: Decimal
    surge_amount: Decimal
    airport_fee: Decimal
    toll_amount: Decimal
    booking_fee: Decimal
    tax_amount: Decimal
    total_fare: Decimal
    driver_payout: Decimal
    platform_commission: Decimal
    currency: str

    def check_invariants(self) -> None:
        """Assert F1, F2, F4, F5. Called in tests and by --self-check."""
        components = (
            self.base_fare
            + self.distance_fare
            + self.time_fare
            + self.surge_amount
            + self.airport_fee
            + self.toll_amount
            + self.booking_fee
            + self.tax_amount
        )
        if abs(components - self.total_fare) > CENTS:
            raise AssertionError(f"F1 violated: components={components} total={self.total_fare}")

        split = self.driver_payout + self.platform_commission
        net = self.total_fare - self.tax_amount
        if abs(split - net) > CENTS:
            raise AssertionError(f"F2 violated: split={split} net={net}")

        for name, value in (
            ("base_fare", self.base_fare),
            ("distance_fare", self.distance_fare),
            ("time_fare", self.time_fare),
            ("surge_amount", self.surge_amount),
            ("airport_fee", self.airport_fee),
            ("toll_amount", self.toll_amount),
            ("booking_fee", self.booking_fee),
            ("tax_amount", self.tax_amount),
            ("total_fare", self.total_fare),
            ("driver_payout", self.driver_payout),
            ("platform_commission", self.platform_commission),
        ):
            if value < ZERO:
                raise AssertionError(f"F4 violated: {name}={value}")

        pre_surge = self.base_fare + self.distance_fare + self.time_fare
        expected = pre_surge * (self.surge_multiplier - Decimal("1"))
        if abs(expected - self.surge_amount) > CENTS:
            raise AssertionError(f"F5 violated: expected={expected} actual={self.surge_amount}")


def compute_fare(
    *,
    city: City,
    vehicle_type: VehicleType,
    distance_km: Decimal,
    duration_sec: int,
    surge_multiplier: Decimal,
    is_airport_trip: bool,
    toll_amount: Decimal = ZERO,
    booking_fee: Decimal = Decimal("25.00"),
    driver_pay_share: Decimal = Decimal("0.80"),
) -> FareBreakdown:
    """Build a fare that satisfies F1, F2, F4, F5 exactly."""
    if surge_multiplier < Decimal("1"):
        raise ValueError(f"surge_multiplier must be >= 1.00, got {surge_multiplier}")
    if distance_km <= 0:
        raise ValueError(f"distance_km must be positive, got {distance_km}")
    if duration_sec <= 0:
        raise ValueError(f"duration_sec must be positive, got {duration_sec}")

    base_fare = money(city.base_fare * vehicle_type.base_fare_multiplier)
    distance_fare = money(distance_km * city.per_km_rate * vehicle_type.per_km_multiplier)
    time_fare = money(Decimal(duration_sec) / Decimal(60) * city.per_min_rate)

    # Sum of already-rounded components: this is what F5 is measured against.
    pre_surge = base_fare + distance_fare + time_fare
    surge_amount = money(pre_surge * (surge_multiplier - Decimal("1")))

    # F6: an airport fee may only exist on an airport trip.
    airport_fee = money(city.airport_fee) if is_airport_trip else ZERO
    toll = money(toll_amount)
    booking = money(booking_fee)

    # Taxable subtotal is the exact sum of rounded parts - never rounded again.
    taxable = pre_surge + surge_amount + airport_fee + toll + booking
    tax_amount = money(taxable * city.tax_pct / Decimal(100))
    total_fare = taxable + tax_amount

    # Commission is taken on the pre-tax subtotal: tax is remitted, not earned.
    platform_commission = money(taxable * city.commission_pct / Decimal(100))
    driver_payout = taxable - platform_commission

    fare = FareBreakdown(
        base_fare=base_fare,
        distance_fare=distance_fare,
        time_fare=time_fare,
        surge_multiplier=surge_multiplier,
        surge_amount=surge_amount,
        airport_fee=airport_fee,
        toll_amount=toll,
        booking_fee=booking,
        tax_amount=tax_amount,
        total_fare=total_fare,
        driver_payout=driver_payout,
        platform_commission=platform_commission,
        currency=city.currency_code,
    )
    fare.check_invariants()
    return fare


def cancellation_fee(*, stage: str, is_fee_applicable: bool, seconds_since_request: int) -> Decimal:
    """Fee for an abandoned trip.

    Charged only when the reason permits it AND the rider had a real grace
    period. Cancelling within 60 seconds is free regardless of reason - the
    fee exists to compensate a driver who has already started moving.
    """
    if not is_fee_applicable or seconds_since_request < 60:
        return ZERO
    return {
        "REQUESTED": Decimal("0.00"),
        "MATCHED": Decimal("30.00"),
        "DRIVER_ARRIVED": Decimal("50.00"),
        "STARTED": Decimal("75.00"),
    }.get(stage, ZERO)
