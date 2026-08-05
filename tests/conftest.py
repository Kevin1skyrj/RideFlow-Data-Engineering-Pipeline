"""Shared fixtures.

Generation is session-scoped: producing a realistic window takes a few seconds,
and every test in the suite examines the same deterministic stream. Re-running
it per test would be slow and would prove nothing extra.
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from event_generator.calibration import Calibration
from event_generator.config import AnomalyConfig, GeneratorConfig
from event_generator.generator import generate
from event_generator.reference import load_reference_data

FIXED_START = datetime(2026, 3, 17, 2, 30, tzinfo=UTC)
"""Tuesday 08:00 IST - inside the Bengaluru weekday morning peak.

Fixed rather than 'now' so that demand-curve assertions are stable: a test that
silently ran at 3 a.m. local would see off-peak volumes and fail for reasons
having nothing to do with the code.
"""


@pytest.fixture(scope="session")
def reference():
    return load_reference_data()


@pytest.fixture(scope="session")
def calibration():
    return Calibration.load()


@pytest.fixture(scope="session")
def city(reference):
    return reference.cities["BLR"]


@pytest.fixture(scope="session")
def base_config():
    return GeneratorConfig(
        start_time=FIXED_START,
        duration_sec=3600,
        trips_per_hour=400,
        seed=20260317,
    )


@pytest.fixture(scope="session")
def clean_config():
    """Anomaly-free stream: every event must be contract-valid."""
    return GeneratorConfig(
        start_time=FIXED_START,
        duration_sec=3600,
        trips_per_hour=400,
        seed=20260317,
        anomalies=AnomalyConfig.disabled(),
    )


@pytest.fixture(scope="session")
def result(base_config, reference, calibration):
    return generate(base_config, reference=reference, calibration=calibration)


@pytest.fixture(scope="session")
def clean_result(clean_config, reference, calibration):
    return generate(clean_config, reference=reference, calibration=calibration)


@pytest.fixture(scope="session")
def trips_by_id(clean_result):
    """Event chains grouped by trip, from the anomaly-free stream."""
    grouped: dict[str, list] = {}
    for event in clean_result.events:
        if "trip_id" in event.payload:
            grouped.setdefault(event.payload["trip_id"], []).append(event)
    for chain in grouped.values():
        chain.sort(key=lambda e: e.event_timestamp)
    return grouped
