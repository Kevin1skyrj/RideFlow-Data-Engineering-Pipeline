"""Loads reference data from the dbt seed CSVs.

The seeds in transformation/seeds/ are the single source of truth for every
enum and lookup in the platform (reference_data.md section 11.4). The generator
reads them rather than hard-coding values, so a change to a seed cannot silently
diverge from what the generator emits.
"""

from __future__ import annotations

import csv
from dataclasses import dataclass
from decimal import Decimal
from functools import cached_property
from pathlib import Path

from event_generator.config import SEEDS_DIR

UNKNOWN_KEY = -1


def _bool(value: str) -> bool:
    return value.strip().lower() == "true"


def _read(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        raise FileNotFoundError(
            f"Seed file missing: {path}\n"
            "Reference data must be present before generating events."
        )
    with path.open(newline="", encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


@dataclass(frozen=True)
class City:
    city_id: int
    city_code: str
    city_name: str
    timezone: str
    currency_code: str
    center_lat: float
    center_lon: float
    commission_pct: Decimal
    base_fare: Decimal
    per_km_rate: Decimal
    per_min_rate: Decimal
    airport_fee: Decimal
    tax_pct: Decimal
    max_surge_multiplier: Decimal
    is_active: bool


@dataclass(frozen=True)
class Zone:
    zone_id: int
    zone_code: str
    zone_name: str
    city_id: int
    zone_type: str
    centroid_lat: float
    centroid_lon: float
    is_airport_zone: bool
    is_surge_eligible: bool
    avg_daily_demand: int
    is_active: bool


@dataclass(frozen=True)
class VehicleType:
    vehicle_type_id: int
    vehicle_type_code: str
    seat_capacity: int
    base_fare_multiplier: Decimal
    per_km_multiplier: Decimal
    is_shared: bool
    is_premium: bool
    min_driver_rating: Decimal
    is_active: bool


@dataclass(frozen=True)
class CancellationReason:
    code: str
    cancelled_by_party: str
    reason_category: str
    is_fee_applicable: bool
    is_driver_fault: bool


@dataclass(frozen=True)
class ReferenceData:
    cities: dict[str, City]
    zones: list[Zone]
    vehicle_types: dict[str, VehicleType]
    cancellation_reasons: dict[str, CancellationReason]
    payment_methods: list[str]
    customer_tiers: list[str]
    weather_codes: list[str]
    traffic_levels: list[str]

    @cached_property
    def zones_by_id(self) -> dict[int, Zone]:
        return {z.zone_id: z for z in self.zones}

    def active_zones(self, city_id: int) -> list[Zone]:
        """Deterministically ordered - iteration order must not vary by run."""
        return sorted(
            (z for z in self.zones if z.city_id == city_id and z.is_active),
            key=lambda z: z.zone_id,
        )

    def reasons_for(self, party: str) -> list[CancellationReason]:
        return sorted(
            (r for r in self.cancellation_reasons.values() if r.cancelled_by_party == party),
            key=lambda r: r.code,
        )


def load_reference_data(seeds_dir: Path | None = None) -> ReferenceData:
    seeds = seeds_dir or SEEDS_DIR

    cities = {}
    for row in _read(seeds / "dim_city.csv"):
        if int(row["city_id"]) == UNKNOWN_KEY:
            continue
        cities[row["city_code"]] = City(
            city_id=int(row["city_id"]),
            city_code=row["city_code"],
            city_name=row["city_name"],
            timezone=row["timezone"],
            currency_code=row["currency_code"],
            center_lat=float(row["center_lat"]),
            center_lon=float(row["center_lon"]),
            commission_pct=Decimal(row["commission_pct"]),
            base_fare=Decimal(row["base_fare"]),
            per_km_rate=Decimal(row["per_km_rate"]),
            per_min_rate=Decimal(row["per_min_rate"]),
            airport_fee=Decimal(row["airport_fee"]),
            tax_pct=Decimal(row["tax_pct"]),
            max_surge_multiplier=Decimal(row["max_surge_multiplier"]),
            is_active=_bool(row["is_active"]),
        )

    zones = [
        Zone(
            zone_id=int(row["zone_id"]),
            zone_code=row["zone_code"],
            zone_name=row["zone_name"],
            city_id=int(row["city_id"]),
            zone_type=row["zone_type"],
            centroid_lat=float(row["centroid_lat"]),
            centroid_lon=float(row["centroid_lon"]),
            is_airport_zone=_bool(row["is_airport_zone"]),
            is_surge_eligible=_bool(row["is_surge_eligible"]),
            avg_daily_demand=int(row["avg_daily_demand"]),
            is_active=_bool(row["is_active"]),
        )
        for row in _read(seeds / "dim_zone.csv")
        if int(row["zone_id"]) != UNKNOWN_KEY
    ]

    vehicle_types = {}
    for row in _read(seeds / "dim_vehicle_type.csv"):
        if int(row["vehicle_type_id"]) == UNKNOWN_KEY:
            continue
        vehicle_types[row["vehicle_type_code"]] = VehicleType(
            vehicle_type_id=int(row["vehicle_type_id"]),
            vehicle_type_code=row["vehicle_type_code"],
            seat_capacity=int(row["seat_capacity"]),
            base_fare_multiplier=Decimal(row["base_fare_multiplier"]),
            per_km_multiplier=Decimal(row["per_km_multiplier"]),
            is_shared=_bool(row["is_shared"]),
            is_premium=_bool(row["is_premium"]),
            min_driver_rating=Decimal(row["min_driver_rating"]),
            is_active=_bool(row["is_active"]),
        )

    cancellation_reasons = {}
    for row in _read(seeds / "dim_cancellation_reason.csv"):
        if int(row["cancellation_reason_id"]) == UNKNOWN_KEY:
            continue
        cancellation_reasons[row["cancellation_reason_code"]] = CancellationReason(
            code=row["cancellation_reason_code"],
            cancelled_by_party=row["cancelled_by_party"],
            reason_category=row["reason_category"],
            is_fee_applicable=_bool(row["is_fee_applicable"]),
            is_driver_fault=_bool(row["is_driver_fault"]),
        )

    def _codes(filename: str, id_col: str, code_col: str, active_only: bool = True) -> list[str]:
        out = []
        for row in _read(seeds / filename):
            if int(row[id_col]) == UNKNOWN_KEY:
                continue
            if active_only and "is_active" in row and not _bool(row["is_active"]):
                continue
            out.append(row[code_col])
        return out

    return ReferenceData(
        cities=cities,
        zones=zones,
        vehicle_types=vehicle_types,
        cancellation_reasons=cancellation_reasons,
        payment_methods=_codes(
            "dim_payment_method.csv", "payment_method_id", "payment_method_code"
        ),
        customer_tiers=_codes("dim_customer_tier.csv", "customer_tier_id", "customer_tier_code"),
        weather_codes=_codes("dim_weather.csv", "weather_id", "weather_code", active_only=False),
        traffic_levels=_codes(
            "dim_traffic_level.csv", "traffic_level_id", "traffic_level_code", active_only=False
        ),
    )
