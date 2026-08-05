"""Supply side: drivers, duty sessions, dispatch, and surge.

Surge here is *derived from simulated supply/demand imbalance*, never sampled at
random (PROJECT_PLAN.md FR-1). That distinction matters: random surge produces
data where surge correlates with nothing, so any analysis of pricing
effectiveness is measuring noise. Derived surge means the marketplace-health and
pricing questions in PROJECT_PLAN.md section 2.1 have real answers in the data.
"""

from __future__ import annotations

import math
from collections import defaultdict
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from zoneinfo import ZoneInfo

from event_generator.calibration import Calibration, Sampler
from event_generator.config import GeneratorConfig
from event_generator.geo import jitter_point
from event_generator.reference import City, ReferenceData, Zone

SURGE_BUCKET_SEC = 600
"""10-minute surge recalculation window."""

SURGE_STEP = Decimal("0.1")
"""Real platforms quantise surge for display; continuous values are not shown."""

WARMUP_SEC = 4 * 3600
"""Drivers may already be on duty when the observation window opens.

Without a warm-up, every session would start at T0 and the marketplace would
have zero supply at the exact moment the first trips are requested.
"""

ZONE_MOBILITY = 0.35
"""Extra reachable supply as a MULTIPLE of a zone's own online drivers.

SIMPLIFICATION: a driver's operating zone is fixed to where they went online.
Real drivers drift between adjacent zones, so a zone can draw on a little more
supply than it strictly hosts.

Deliberately a multiplier on *local* supply, not a share of the city-wide fleet.
An earlier version used the city-wide form, which made reachable supply grow
with total fleet size - so doubling the fleet doubled every zone's apparent
supply and surge could never fire anywhere. Local imbalance is what produces
surge; a city-wide term erases exactly the signal being measured.
"""

MIN_DEMAND_FOR_SURGE = 3
"""Requests in a zone-bucket below which surge never fires.

Without this, a 3 a.m. bucket with one request and two idle drivers computes a
ratio above threshold and surges - which is small-number noise, not scarcity.
Real platforms do not raise prices because a single person opened the app.
"""

MIN_EFFECTIVE_SUPPLY = 2.0
"""Floor on the supply denominator, for the same reason: dividing by a fraction
of a driver produces a ratio that means nothing."""

TRIPS_PER_DRIVER_HOUR = 1.9
"""Throughput of one online driver.

Pickup (~7 min) + rider wait (~2 min) + trip (~20 min) + buffer (3 min) is about
32 minutes of occupancy per trip. Used to convert a count of online drivers into
a *rate* of trips they can serve, so surge compares demand flow against supply
capacity rather than a flow against a stock - which is not a like-for-like
comparison and produces a ratio that means nothing.
"""


@dataclass(frozen=True)
class Driver:
    driver_id: str
    vehicle_id: str
    vehicle_type: str
    rating: Decimal
    device_platform: str
    app_version: str


@dataclass
class DriverSession:
    session_id: str
    driver: Driver
    online_at: datetime
    offline_at: datetime | None
    online_zone: Zone
    offline_zone: Zone | None
    online_lat: float
    online_lon: float
    offline_lat: float | None
    offline_lon: float | None
    offline_reason: str | None
    trips_completed: int = 0
    busy_until: datetime | None = None

    @property
    def is_open(self) -> bool:
        """A session with no DriverOffline is legitimate, not missing data."""
        return self.offline_at is None

    @property
    def duration_sec(self) -> int | None:
        if self.offline_at is None:
            return None
        return int((self.offline_at - self.online_at).total_seconds())

    def covers(self, moment: datetime) -> bool:
        if moment < self.online_at:
            return False
        return self.offline_at is None or moment < self.offline_at

    def is_available(self, moment: datetime) -> bool:
        if not self.covers(moment):
            return False
        return self.busy_until is None or self.busy_until <= moment

    def occupy(self, until: datetime) -> None:
        self.busy_until = until


@dataclass
class TripRequest:
    """A demand event, before any driver is involved."""

    requested_at: datetime
    pickup_zone: Zone
    dropoff_zone: Zone
    vehicle_type: str


class Marketplace:
    """Driver supply and the surge grid derived from it."""

    def __init__(
        self,
        sessions: list[DriverSession],
        city: City,
        calibration: Calibration,
    ) -> None:
        self.sessions = sessions
        self.city = city
        self._calibration = calibration
        self._surge: dict[tuple[int, int], Decimal] = {}

    # ---- construction -----------------------------------------------------

    @classmethod
    def build(
        cls,
        *,
        sampler: Sampler,
        reference: ReferenceData,
        calibration: Calibration,
        config: GeneratorConfig,
        city: City,
    ) -> Marketplace:
        zones = reference.active_zones(city.city_id)
        params = calibration["driver_sessions"]
        device = calibration["device"]
        mix = calibration["vehicle_type_mix"]["weights"]

        zone_weights = [float(z.avg_daily_demand) for z in zones]
        sessions: list[DriverSession] = []

        # `driver_count` is the target number of drivers online *at any moment*.
        # Sessions are finite, so a long window needs shift churn: a 24-hour run
        # with only one cohort of drivers runs out of supply once the first
        # shifts end, which silently collapses the completion rate.
        avg_session_sec = math.exp(params["session_duration_sec"]["mu"])
        session_count = max(
            int(config.driver_count * (1.0 + config.duration_sec / avg_session_sec)),
            config.driver_count,
        )

        # Shift starts follow the local-hour distribution across the whole
        # window plus the warm-up, so supply arrives throughout the run.
        tz = ZoneInfo(city.timezone)
        shift_weights = params["shift_start_hour_weights"]
        first_hour = (config.start_time - timedelta(seconds=WARMUP_SEC)).replace(
            minute=0, second=0, microsecond=0
        )
        hour_slots: list[datetime] = []
        slot_weights: list[float] = []
        cursor = first_hour
        while cursor < config.end_time:
            hour_slots.append(cursor)
            slot_weights.append(float(shift_weights[cursor.astimezone(tz).hour]) + 0.005)
            cursor += timedelta(hours=1)

        for _ in range(session_count):
            vehicle_type = sampler.weighted_choice(mix)
            rating = Decimal(str(round(sampler.from_spec(params["driver_rating"]), 2)))
            driver = Driver(
                driver_id=sampler.uuid(),
                vehicle_id=sampler.uuid(),
                vehicle_type=vehicle_type,
                rating=rating,
                device_platform=sampler.weighted_choice(device["driver_platform_weights"]),
                app_version=sampler.choice(device["driver_app_versions"]),
            )

            slot = sampler.weighted_pick(hour_slots, slot_weights)
            online_at = slot + timedelta(seconds=sampler.uniform(0, 3600))
            duration = sampler.int_from_spec(params["session_duration_sec"])
            offline_at: datetime | None = online_at + timedelta(seconds=duration)

            # A shift that would already be over when the window opens supplies
            # nothing. Extend it into the window instead of discarding the
            # driver: the warm-up exists to seed the marketplace with drivers
            # already on duty, not to silently delete a fifth of the fleet.
            if offline_at <= config.start_time:
                offline_at = config.start_time + timedelta(
                    seconds=sampler.uniform(60, max(float(config.duration_sec), 600.0))
                )

            # Sessions still running when the window closes stay open, as do a
            # small share that simply never emit an offline event (app killed,
            # battery died). Both are handled downstream as open sessions.
            if offline_at > config.end_time or sampler.chance(params["p_session_left_open"]):
                offline_at = None

            online_zone = sampler.weighted_pick(zones, zone_weights)
            online_lat, online_lon = jitter_point(
                online_zone.centroid_lat, online_zone.centroid_lon, 1.4, sampler
            )

            if offline_at is None:
                offline_zone = offline_lat = offline_lon = offline_reason = None
            else:
                offline_zone = sampler.weighted_pick(zones, zone_weights)
                offline_lat, offline_lon = jitter_point(
                    offline_zone.centroid_lat, offline_zone.centroid_lon, 1.4, sampler
                )
                offline_reason = sampler.weighted_choice(params["offline_reason_weights"])

            sessions.append(
                DriverSession(
                    session_id=sampler.uuid(),
                    driver=driver,
                    online_at=online_at,
                    offline_at=offline_at,
                    online_zone=online_zone,
                    offline_zone=offline_zone,
                    online_lat=online_lat,
                    online_lon=online_lon,
                    offline_lat=offline_lat,
                    offline_lon=offline_lon,
                    offline_reason=offline_reason,
                )
            )

        sessions.sort(key=lambda s: (s.online_at, s.session_id))
        return cls(sessions, city, calibration)

    # ---- surge ------------------------------------------------------------

    @staticmethod
    def _bucket(moment: datetime) -> int:
        return int(moment.timestamp()) // SURGE_BUCKET_SEC

    def compute_surge_grid(
        self, requests: Iterable[TripRequest], weather_supply_factor: float = 1.0
    ) -> None:
        """Derive a surge multiplier per (zone, 10-minute bucket).

        Demand is the request count in that zone and bucket. Supply is the
        number of drivers online in that zone, plus a mobility allowance, scaled
        by the weather's supply impact - heavy rain reduces supply at the same
        time as it raises demand, which is exactly the imbalance that produces
        real surge (reference_data.md section 9).
        """
        demand: dict[tuple[int, int], int] = defaultdict(int)
        buckets: set[int] = set()
        for request in requests:
            key = (request.pickup_zone.zone_id, self._bucket(request.requested_at))
            demand[key] += 1
            buckets.add(key[1])

        params = self._calibration["surge"]["imbalance_to_multiplier"]
        threshold = float(params["ratio_threshold"])
        sensitivity = float(params["sensitivity"])
        floor = Decimal(str(params["min_multiplier"]))
        alpha = float(params["smoothing_alpha"])
        ceiling = self.city.max_surge_multiplier

        supply: dict[tuple[int, int], float] = defaultdict(float)
        citywide: dict[int, float] = defaultdict(float)
        for bucket in buckets:
            moment = datetime.fromtimestamp(bucket * SURGE_BUCKET_SEC, tz=self.city_tz)
            for session in self.sessions:
                if session.covers(moment):
                    supply[(session.online_zone.zone_id, bucket)] += 1.0
                    citywide[bucket] += 1.0

        previous: dict[int, Decimal] = {}
        for (zone_id, bucket), count in sorted(demand.items()):
            if count < MIN_DEMAND_FOR_SURGE:
                previous[zone_id] = floor
                continue

            local = supply.get((zone_id, bucket), 0.0)
            reachable = local * (1.0 + ZONE_MOBILITY)
            effective = max(reachable * weather_supply_factor, MIN_EFFECTIVE_SUPPLY)

            # Compare like with like: convert the bucket's request count into a
            # per-hour demand rate, and the online driver count into a per-hour
            # capacity. Dividing a 10-minute flow by a stock of drivers is not a
            # ratio of anything, and it leaves surge permanently at 1.0.
            demand_per_hour = count * (3600.0 / SURGE_BUCKET_SEC)
            capacity_per_hour = effective * TRIPS_PER_DRIVER_HOUR

            ratio = demand_per_hour / max(capacity_per_hour, 1.0)
            excess = max(0.0, ratio - threshold)
            raw = Decimal(str(1.0 + sensitivity * excess))

            # Exponential smoothing against the previous bucket for this zone:
            # real surge does not jump from 1.0x to 3.0x between adjacent
            # 10-minute windows.
            prior = previous.get(zone_id)
            if prior is not None:
                raw = prior + Decimal(str(alpha)) * (raw - prior)

            multiplier = min(max(raw, floor), ceiling)
            multiplier = (multiplier / SURGE_STEP).quantize(Decimal("1")) * SURGE_STEP
            multiplier = min(max(multiplier, floor), ceiling)

            self._surge[(zone_id, bucket)] = multiplier
            previous[zone_id] = multiplier

    @property
    def city_tz(self):

        return UTC

    def surge_for(self, zone: Zone, moment: datetime) -> Decimal:
        if not zone.is_surge_eligible:
            return Decimal("1.0")
        return self._surge.get((zone.zone_id, self._bucket(moment)), Decimal("1.0"))

    # ---- dispatch ---------------------------------------------------------

    def find_driver(
        self, *, moment: datetime, vehicle_type: str, min_rating: Decimal
    ) -> DriverSession | None:
        """First eligible driver, or None - which becomes an expired request.

        Eligibility is not just availability: a tier with a rating floor draws
        from a narrower pool, which is why a high-surge premium request can go
        unmatched while economy drivers sit idle nearby (reference_data.md
        section 2).
        """
        for session in self.sessions:
            if session.driver.vehicle_type != vehicle_type:
                continue
            if session.driver.rating < min_rating:
                continue
            if session.is_available(moment):
                return session
        return None

    def online_count(self, moment: datetime) -> int:
        return sum(1 for s in self.sessions if s.covers(moment))
