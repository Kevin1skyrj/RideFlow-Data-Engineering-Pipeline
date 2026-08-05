"""Generation orchestrator.

Runs in four passes, and the ordering is the interesting part:

  1. Demand   - decide when and where trips are requested.
  2. Surge    - derive multipliers from the imbalance between that demand and
                the simulated driver supply.
  3. Lifecycle- simulate each trip against real supply, applying the surge.
  4. Anomalies- mutate the known-good stream.

Surge has to be a separate pass because it is a property of the *aggregate*
imbalance in a zone and time bucket, not of any individual trip. Computing it
per-trip would make it a function of nothing, which is exactly the random surge
that PROJECT_PLAN.md FR-1 rules out.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta

from event_generator.anomalies import AnomalyReport, inject
from event_generator.calibration import Calibration, Sampler
from event_generator.config import GeneratorConfig
from event_generator.demand import DemandModel
from event_generator.envelope import Event
from event_generator.lifecycle import TripSimulator
from event_generator.marketplace import Marketplace, TripRequest
from event_generator.reference import ReferenceData, load_reference_data


@dataclass
class GenerationResult:
    events: list[Event]
    anomalies: AnomalyReport
    trips_requested: int
    trips_completed: int
    trips_cancelled: int
    trips_expired: int
    driver_sessions: int
    open_sessions: int

    def summary(self) -> dict[str, object]:
        completion_rate = (
            self.trips_completed / self.trips_requested if self.trips_requested else 0.0
        )
        return {
            "events": len(self.events),
            "trips_requested": self.trips_requested,
            "trips_completed": self.trips_completed,
            "trips_cancelled": self.trips_cancelled,
            "trips_expired": self.trips_expired,
            "completion_rate": round(completion_rate, 4),
            "driver_sessions": self.driver_sessions,
            "open_sessions": self.open_sessions,
            "anomalies": self.anomalies.as_dict(),
        }


def generate(
    config: GeneratorConfig,
    *,
    reference: ReferenceData | None = None,
    calibration: Calibration | None = None,
) -> GenerationResult:
    reference = reference or load_reference_data()
    calibration = calibration or Calibration.load()
    sampler = Sampler(config.seed)

    city = reference.cities.get(config.city_code)
    if city is None:
        raise ValueError(
            f"Unknown city_code {config.city_code!r}. " f"Available: {sorted(reference.cities)}"
        )

    demand = DemandModel(
        sampler=sampler,
        calibration=calibration,
        reference=reference,
        city=city,
        expected_trips=config.expected_trips,
    )
    marketplace = Marketplace.build(
        sampler=sampler,
        reference=reference,
        calibration=calibration,
        config=config,
        city=city,
    )

    # --- pass 1: demand -----------------------------------------------------
    requests = _plan_requests(config, sampler, demand)

    # --- pass 2: surge ------------------------------------------------------
    dominant_weather = demand.pick_weather()
    marketplace.compute_surge_grid(
        requests,
        weather_supply_factor=demand.weather_supply_factor(dominant_weather, reference),
    )

    # --- pass 3: lifecycle --------------------------------------------------
    simulator = TripSimulator(
        sampler=sampler,
        calibration=calibration,
        reference=reference,
        demand=demand,
        marketplace=marketplace,
        city=city,
        config=config,
    )

    events: list[Event] = []
    completed = cancelled = expired = 0

    for request in requests:
        plan = simulator.plan(request, demand.pick_rider())
        chain = simulator.simulate(plan)
        events.extend(chain)

        types = {event.event_type for event in chain}
        if "RideCompleted" in types:
            completed += 1
        elif "RideCancelled" in types:
            cancelled += 1
        else:
            expired += 1

    for session in marketplace.sessions:
        events.extend(simulator.session_events(session))

    # --- pass 4: anomalies --------------------------------------------------
    events, report = inject(events, sampler=sampler, config=config.anomalies)

    # Emitted in arrival order - what a consumer actually observes. Sorting by
    # (ingested_at, event_id) rather than ingested_at alone keeps the output
    # stable when two events share a timestamp, which determinism requires.
    events.sort(key=lambda e: (e.ingested_at, e.event_id, e.event_type))

    return GenerationResult(
        events=events,
        anomalies=report,
        trips_requested=len(requests),
        trips_completed=completed,
        trips_cancelled=cancelled,
        trips_expired=expired,
        driver_sessions=len(marketplace.sessions),
        open_sessions=sum(1 for s in marketplace.sessions if s.is_open),
    )


def _plan_requests(
    config: GeneratorConfig, sampler: Sampler, demand: DemandModel
) -> list[TripRequest]:
    """Distribute trips across the window, weighted by the local demand curve.

    Trips are allocated by weighted draw rather than a fixed per-minute count so
    that demand varies naturally minute to minute instead of stepping between
    hourly plateaus.
    """
    minutes = max(config.duration_sec // 60, 1)
    starts = [config.start_time + timedelta(minutes=m) for m in range(minutes)]
    weights = [demand.demand_multiplier(moment) for moment in starts]

    requests: list[TripRequest] = []
    for _ in range(config.expected_trips):
        minute_start = sampler.weighted_pick(starts, weights)
        requested_at = minute_start + timedelta(
            seconds=sampler.randint(0, 59), milliseconds=sampler.randint(0, 999)
        )
        pickup = demand.pick_pickup_zone(requested_at)
        dropoff = demand.pick_dropoff_zone(requested_at, pickup)
        requests.append(
            TripRequest(
                requested_at=requested_at,
                pickup_zone=pickup,
                dropoff_zone=dropoff,
                vehicle_type=demand.pick_vehicle_type(),
            )
        )

    requests.sort(key=lambda r: (r.requested_at, r.pickup_zone.zone_id))
    return requests
