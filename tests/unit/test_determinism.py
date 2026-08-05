"""Determinism: identical seed must produce byte-identical output.

This is the foundation the rest of the suite stands on. Without it every other
assertion becomes flaky and no failure is reproducible - and the M2 exit
criterion in PROJECT_PLAN.md would be unverifiable.
"""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime

from event_generator.config import GeneratorConfig
from event_generator.generator import generate

START = datetime(2026, 3, 17, 2, 30, tzinfo=UTC)


def _config(seed: int) -> GeneratorConfig:
    return GeneratorConfig(start_time=START, duration_sec=1800, trips_per_hour=120, seed=seed)


def _digest(events) -> str:
    blob = "\n".join(json.dumps(e.to_dict(), separators=(",", ":"), sort_keys=True) for e in events)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


def test_same_seed_produces_byte_identical_output(reference, calibration):
    first = generate(_config(4242), reference=reference, calibration=calibration)
    second = generate(_config(4242), reference=reference, calibration=calibration)

    assert len(first.events) == len(second.events)
    assert _digest(first.events) == _digest(second.events)


def test_different_seeds_produce_different_output(reference, calibration):
    """A generator that ignored its seed would pass the test above trivially."""
    first = generate(_config(1), reference=reference, calibration=calibration)
    second = generate(_config(2), reference=reference, calibration=calibration)
    assert _digest(first.events) != _digest(second.events)


def test_summary_statistics_are_reproducible(reference, calibration):
    first = generate(_config(777), reference=reference, calibration=calibration)
    second = generate(_config(777), reference=reference, calibration=calibration)
    assert first.summary() == second.summary()


def test_no_unseeded_randomness_leaks_in(reference, calibration):
    """Guards specifically against uuid.uuid4(), which draws from os.urandom and
    cannot be seeded. If it were used anywhere, identifiers would differ between
    two runs at the same seed even though everything else matched."""
    first = generate(_config(31337), reference=reference, calibration=calibration)
    second = generate(_config(31337), reference=reference, calibration=calibration)

    assert [e.event_id for e in first.events] == [e.event_id for e in second.events]
    assert [e.correlation_id for e in first.events] == [e.correlation_id for e in second.events]


def test_ordering_is_stable_when_timestamps_collide(result):
    """Sorting on ingested_at alone would leave ties in arbitrary order and
    break byte-identical reproducibility."""
    keys = [(e.ingested_at, e.event_id, e.event_type) for e in result.events]
    assert keys == sorted(keys)
