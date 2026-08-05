"""Configuration and filesystem layout for the event generator."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SEEDS_DIR = ROOT / "transformation" / "seeds"
CALIBRATION_PATH = ROOT / "analytics" / "calibration" / "calibration_params.json"
DEFAULT_OUTPUT_DIR = ROOT / "data" / "raw"

CONTRACT_VERSION = "1.0.0"
PRODUCER_SERVICE = "rideflow-event-generator"

DRIVERS_PER_TRIP_HOUR = 0.95
"""Fleet size per trip/hour of demand, when driver_count is not given.

Derivation: a driver is occupied for roughly pickup (~7 min) + rider wait
(~2 min) + trip (~20 min) + post-trip buffer (3 min), so about 1.9 trips per
online hour. Marketplaces run at roughly 70% utilisation rather than 100%, and
some shifts end mid-window, so:

    1 / (1.9 * 0.70) ~= 0.75, plus ~25% headroom for shift churn  ->  0.95

Undersupplying is not a neutral error: it inflates the cancellation rate and
depresses the completion rate, which makes every funnel metric in the warehouse
wrong in the same direction.
"""


@dataclass(frozen=True)
class AnomalyConfig:
    """Rates for deliberately injected bad data.

    Anomaly injection is a first-class requirement (PROJECT_PLAN.md FR-1), not a
    debugging aid: a pipeline that has never been shown bad data has not been
    tested. Every injected anomaly is *labelled* so tests can assert that the
    pipeline handled it, rather than merely that it did not crash.
    """

    duplicate: float = 0.0040
    """Same event_id redelivered. Kafka is at-least-once; this is expected."""

    late_arrival: float = 0.0060
    """Device buffered the event (tunnel, signal loss) and flushed it later."""

    out_of_order: float = 0.0050
    """Arrival order does not match causal order."""

    malformed: float = 0.0035
    """Unparseable or schema-violating payload. Must land in the DLQ."""

    orphan: float = 0.0025
    """Trip whose RideRequested is dropped, leaving successors unanchored."""

    clock_skew: float = 0.0020
    """Producer clock ahead of the consumer's - yields negative lateness."""

    late_arrival_delay_sec: tuple[int, int] = (600, 5400)
    out_of_order_delay_sec: tuple[int, int] = (20, 240)
    duplicate_delay_sec: tuple[int, int] = (2, 45)
    clock_skew_sec: tuple[int, int] = (30, 280)

    def total_rate(self) -> float:
        return (
            self.duplicate
            + self.late_arrival
            + self.out_of_order
            + self.malformed
            + self.orphan
            + self.clock_skew
        )

    @classmethod
    def disabled(cls) -> AnomalyConfig:
        """All rates zero - for tests that need a clean stream."""
        return cls(
            duplicate=0.0,
            late_arrival=0.0,
            out_of_order=0.0,
            malformed=0.0,
            orphan=0.0,
            clock_skew=0.0,
        )


@dataclass(frozen=True)
class GeneratorConfig:
    """A single generation run.

    `seed` is load-bearing: identical seed must produce byte-identical output
    (testing_strategy.md section 2.1). Every random draw in the generator comes
    from one seeded Random instance threaded through the call graph - there are
    no module-level random calls and no uuid4(), which is not seedable.
    """

    start_time: datetime
    duration_sec: int = 3600
    trips_per_hour: float = 600.0
    driver_count: int = 0
    """Fleet size. Zero means derive it from `trips_per_hour` - see
    DRIVERS_PER_TRIP_HOUR for the capacity arithmetic behind the ratio."""
    seed: int = 42
    city_code: str = "BLR"
    environment: str = "local"
    producer_version: str = "0.1.0"
    anomalies: AnomalyConfig = field(default_factory=AnomalyConfig)

    def __post_init__(self) -> None:
        if self.start_time.tzinfo is None:
            raise ValueError("start_time must be timezone-aware (use UTC)")
        if self.start_time.utcoffset() != timedelta(0):
            raise ValueError("start_time must be UTC")
        if self.duration_sec <= 0:
            raise ValueError("duration_sec must be positive")
        if self.trips_per_hour <= 0:
            raise ValueError("trips_per_hour must be positive")
        if self.driver_count < 0:
            raise ValueError("driver_count must not be negative")
        if self.driver_count == 0:
            object.__setattr__(
                self,
                "driver_count",
                max(int(self.trips_per_hour * DRIVERS_PER_TRIP_HOUR), 12),
            )
        if self.environment not in {"local", "dev", "staging", "prod"}:
            raise ValueError(f"invalid environment: {self.environment}")

    @property
    def end_time(self) -> datetime:
        return self.start_time + timedelta(seconds=self.duration_sec)

    @property
    def expected_trips(self) -> int:
        return int(self.trips_per_hour * self.duration_sec / 3600)

    @staticmethod
    def parse_start(value: str) -> datetime:
        """Parse an ISO-8601 start time, normalising to UTC."""
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=UTC)
        return dt.astimezone(UTC)
