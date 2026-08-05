"""Trip lifecycle simulation: one trip in, a causally-linked event chain out.

Every chain produced here satisfies the sequence rules S1-S8 and the temporal
invariants T1-T2 from docs/event_contract.md by construction. Anomalies are
applied afterwards by anomalies.py, so a malformed or out-of-order event is
always a deliberate mutation of a known-good chain rather than an accident of
the business logic.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal

from event_generator.calibration import Calibration, Sampler
from event_generator.config import GeneratorConfig
from event_generator.demand import DemandModel, Rider
from event_generator.envelope import Event
from event_generator.geo import (
    INTRA_ZONE_KM_RANGE,
    ROAD_FACTOR_RANGE,
    haversine_km,
    jitter_point,
    offset_point,
)
from event_generator.marketplace import DriverSession, Marketplace, TripRequest
from event_generator.pricing import ZERO, cancellation_fee, compute_fare, money
from event_generator.reference import City, ReferenceData

ZONE_JITTER_KM = 1.2
ARRIVAL_TOLERANCE_SEC = 180
"""Late-arrival threshold from event_contract.md section 5.3. Encoded once here
so every consumer agrees on what 'late' means."""

DRIVER_BUFFER_SEC = 180
"""Post-trip idle before a driver is dispatchable again."""


@dataclass
class TripPlan:
    """Everything decided about a trip before any event is emitted."""

    trip_id: str
    rider: Rider
    request: TripRequest
    pickup_lat: float
    pickup_lon: float
    dropoff_lat: float
    dropoff_lon: float
    distance_km: Decimal
    duration_sec: int
    estimated_distance_km: Decimal
    estimated_duration_sec: int
    estimated_fare: Decimal
    surge_multiplier: Decimal
    weather_code: str
    traffic_level: str
    payment_method: str
    is_airport_pickup: bool
    is_airport_dropoff: bool
    promo_code: str | None


class TripSimulator:
    def __init__(
        self,
        *,
        sampler: Sampler,
        calibration: Calibration,
        reference: ReferenceData,
        demand: DemandModel,
        marketplace: Marketplace,
        city: City,
        config: GeneratorConfig,
    ) -> None:
        self._s = sampler
        self._cal = calibration
        self._ref = reference
        self._demand = demand
        self._market = marketplace
        self._city = city
        self._config = config

    # ---- helpers ----------------------------------------------------------

    def _event(
        self,
        *,
        event_type: str,
        moment: datetime,
        partition_key: str,
        correlation_id: str,
        causation_id: str | None,
        payload: dict,
    ) -> Event:
        lag = self._s.from_spec(self._cal["ingestion_lag_sec"])
        return Event(
            event_id=self._s.uuid(),
            event_type=event_type,
            event_timestamp=moment,
            ingested_at=moment + timedelta(seconds=lag),
            partition_key=partition_key,
            correlation_id=correlation_id,
            causation_id=causation_id,
            payload=payload,
            producer_service="rideflow-event-generator",
            producer_version=self._config.producer_version,
            environment=self._config.environment,
        )

    def _gateway_reference(self, method: str) -> str:
        """A processor reference. Cash has no gateway, but the contract makes
        this field mandatory, so cash carries an internal receipt id instead -
        which is exactly why reconciliation N3 must exclude cash trips."""
        prefix = {
            "CARD": "pg",
            "NETBANKING": "pg",
            "UPI": "upi",
            "WALLET": "wl",
            "CORPORATE": "crp",
            "CASH": "csh",
        }.get(method, "pg")
        body = "".join(self._s.choice("0123456789abcdef") for _ in range(12))
        return f"{prefix}_{body}"

    # ---- planning ---------------------------------------------------------

    def plan(self, request: TripRequest, rider: Rider) -> TripPlan:
        pickup = request.pickup_zone
        dropoff = request.dropoff_zone

        pickup_lat, pickup_lon = jitter_point(
            pickup.centroid_lat, pickup.centroid_lon, ZONE_JITTER_KM, self._s
        )
        dropoff_lat, dropoff_lon = jitter_point(
            dropoff.centroid_lat, dropoff.centroid_lon, ZONE_JITTER_KM, self._s
        )

        # Distance derives from REAL geocoded zone centroids, scaled by a road
        # factor - not sampled blind from a distribution.
        if pickup.zone_id == dropoff.zone_id:
            straight = self._s.uniform(*INTRA_ZONE_KM_RANGE)
        else:
            straight = haversine_km(pickup_lat, pickup_lon, dropoff_lat, dropoff_lon)
        road_factor = self._s.uniform(*ROAD_FACTOR_RANGE)

        spec = self._cal["trip_distance_km"]["by_vehicle_type"][request.vehicle_type]
        distance = min(max(straight * road_factor, spec["min"]), spec["max"])
        distance_km = money(Decimal(str(distance)))

        traffic_level = self._demand.pick_traffic(request.requested_at)
        weather_code = self._demand.pick_weather()
        speed = self._demand.speed_kmh(traffic_level)
        duration_sec = max(int(float(distance_km) / speed * 3600 * self._s.uniform(0.88, 1.22)), 60)

        # The rider is quoted before the trip happens, so the estimate is a
        # noisy forecast. The gap is what estimate_accuracy_pct measures.
        est_distance = money(distance_km * Decimal(str(self._s.uniform(0.92, 1.07))))
        est_duration = max(int(duration_sec * self._s.uniform(0.84, 1.12)), 60)

        surge = self._market.surge_for(pickup, request.requested_at)
        vehicle_type = self._ref.vehicle_types[request.vehicle_type]
        is_airport = pickup.is_airport_zone or dropoff.is_airport_zone

        pricing = self._cal["pricing"]
        estimated = compute_fare(
            city=self._city,
            vehicle_type=vehicle_type,
            distance_km=est_distance,
            duration_sec=est_duration,
            surge_multiplier=surge,
            is_airport_trip=is_airport,
            booking_fee=Decimal(str(pricing["booking_fee_inr"])),
        )

        promo = None
        if self._s.chance(self._cal["payments"]["p_promo_applied"]):
            promo = self._s.choice(self._cal["payments"]["promo_codes"])

        return TripPlan(
            trip_id=self._s.uuid(),
            rider=rider,
            request=request,
            pickup_lat=pickup_lat,
            pickup_lon=pickup_lon,
            dropoff_lat=dropoff_lat,
            dropoff_lon=dropoff_lon,
            distance_km=distance_km,
            duration_sec=duration_sec,
            estimated_distance_km=est_distance,
            estimated_duration_sec=est_duration,
            estimated_fare=estimated.total_fare,
            surge_multiplier=surge,
            weather_code=weather_code,
            traffic_level=traffic_level,
            payment_method=self._demand.pick_payment_method(),
            is_airport_pickup=pickup.is_airport_zone,
            is_airport_dropoff=dropoff.is_airport_zone,
            promo_code=promo,
        )

    # ---- event chain ------------------------------------------------------

    def simulate(self, plan: TripPlan) -> list[Event]:
        events: list[Event] = []
        request = plan.request
        pickup, dropoff = request.pickup_zone, request.dropoff_zone
        trip_id = plan.trip_id

        requested = self._event(
            event_type="RideRequested",
            moment=request.requested_at,
            partition_key=trip_id,
            correlation_id=trip_id,
            causation_id=None,
            payload={
                "trip_id": trip_id,
                "rider_id": plan.rider.rider_id,
                "city_id": self._city.city_id,
                "requested_at": request.requested_at,
                "pickup_lat": plan.pickup_lat,
                "pickup_lon": plan.pickup_lon,
                "pickup_zone_id": pickup.zone_id,
                "dropoff_lat": plan.dropoff_lat,
                "dropoff_lon": plan.dropoff_lon,
                "dropoff_zone_id": dropoff.zone_id,
                "vehicle_type": request.vehicle_type,
                "estimated_fare": plan.estimated_fare,
                "estimated_distance_km": plan.estimated_distance_km,
                "estimated_duration_sec": plan.estimated_duration_sec,
                "surge_multiplier": plan.surge_multiplier,
                "payment_method": plan.payment_method,
                "customer_tier": plan.rider.customer_tier,
                "is_airport_pickup": plan.is_airport_pickup,
                "is_airport_dropoff": plan.is_airport_dropoff,
                "weather_code": plan.weather_code,
                "traffic_level": plan.traffic_level,
                "promo_code": plan.promo_code,
                "device_platform": plan.rider.device_platform,
                "app_version": plan.rider.app_version,
            },
        )
        events.append(requested)

        funnel = self._cal["funnel_rates"]
        vehicle_type = self._ref.vehicle_types[request.vehicle_type]

        # --- pre-match cancellation -----------------------------------------
        if self._s.chance(funnel["p_cancel_at_requested"]):
            events.append(
                self._cancel(
                    plan=plan,
                    causation=requested,
                    stage="REQUESTED",
                    driver_id=None,
                    at=request.requested_at + timedelta(seconds=self._s.randint(20, 420)),
                )
            )
            return events

        session = self._market.find_driver(
            moment=request.requested_at,
            vehicle_type=request.vehicle_type,
            min_rating=vehicle_type.min_driver_rating,
        )

        if session is None:
            # No eligible supply. Most such requests are cancelled by the
            # platform; a minority emit nothing further and become EXPIRED,
            # which is only *inferable* - Gap 1 in event_contract.md 0.1.
            if self._s.chance(0.72):
                events.append(
                    self._cancel(
                        plan=plan,
                        causation=requested,
                        stage="REQUESTED",
                        driver_id=None,
                        at=request.requested_at + timedelta(seconds=self._s.randint(120, 600)),
                        forced_reason="NO_DRIVER_AVAILABLE",
                        forced_party="SYSTEM",
                    )
                )
            return events

        # --- match -----------------------------------------------------------
        matching = self._s.int_from_spec(self._cal["matching"]["matching_duration_sec"])
        accepted_at = request.requested_at + timedelta(seconds=matching)
        eta = self._s.int_from_spec(self._cal["matching"]["eta_to_pickup_sec"])
        pickup_distance = money(
            Decimal(str(self._s.from_spec(self._cal["matching"]["distance_to_pickup_km"])))
        )
        driver_lat, driver_lon = offset_point(
            plan.pickup_lat, plan.pickup_lon, float(pickup_distance), self._s
        )
        attempt = self._s.weighted_index(self._cal["matching"]["dispatch_attempt_weights"]) + 1
        driver = session.driver

        accepted = self._event(
            event_type="RideAccepted",
            moment=accepted_at,
            partition_key=trip_id,
            correlation_id=trip_id,
            causation_id=requested.event_id,
            payload={
                "trip_id": trip_id,
                "rider_id": plan.rider.rider_id,
                "driver_id": driver.driver_id,
                "city_id": self._city.city_id,
                "accepted_at": accepted_at,
                "driver_lat": driver_lat,
                "driver_lon": driver_lon,
                "driver_zone_id": session.online_zone.zone_id,
                "vehicle_id": driver.vehicle_id,
                "vehicle_type": driver.vehicle_type,
                "matching_duration_sec": matching,
                "eta_to_pickup_sec": eta,
                "distance_to_pickup_km": pickup_distance,
                "driver_rating_at_accept": driver.rating,
                "dispatch_attempt_number": attempt,
            },
        )
        events.append(accepted)

        if self._s.chance(funnel["p_cancel_at_matched"]):
            cancel_at = accepted_at + timedelta(seconds=self._s.randint(15, max(eta, 60)))
            events.append(
                self._cancel(
                    plan=plan,
                    causation=accepted,
                    stage="MATCHED",
                    driver_id=driver.driver_id,
                    at=cancel_at,
                )
            )
            session.occupy(cancel_at + timedelta(seconds=60))
            return events

        # --- arrival ----------------------------------------------------------
        delay = self._s.int_from_spec(self._cal["matching"]["arrival_delay_sec"])
        actual_pickup = max(eta + delay, 30)
        delay = actual_pickup - eta  # keep the stored delay consistent with the clock
        arrived_at = accepted_at + timedelta(seconds=actual_pickup)
        arrival_lat, arrival_lon = offset_point(
            plan.pickup_lat, plan.pickup_lon, self._s.uniform(0.0, 0.35), self._s
        )

        arrived = self._event(
            event_type="DriverArrived",
            moment=arrived_at,
            partition_key=trip_id,
            correlation_id=trip_id,
            causation_id=accepted.event_id,
            payload={
                "trip_id": trip_id,
                "rider_id": plan.rider.rider_id,
                "driver_id": driver.driver_id,
                "city_id": self._city.city_id,
                "arrived_at": arrived_at,
                "driver_lat": arrival_lat,
                "driver_lon": arrival_lon,
                "actual_pickup_duration_sec": actual_pickup,
                "arrival_delay_sec": delay,
                "is_late_arrival": delay > ARRIVAL_TOLERANCE_SEC,
            },
        )
        events.append(arrived)

        if self._s.chance(funnel["p_cancel_at_driver_arrived"]):
            cancel_at = arrived_at + timedelta(seconds=self._s.randint(60, 600))
            events.append(
                self._cancel(
                    plan=plan,
                    causation=arrived,
                    stage="DRIVER_ARRIVED",
                    driver_id=driver.driver_id,
                    at=cancel_at,
                )
            )
            session.occupy(cancel_at + timedelta(seconds=DRIVER_BUFFER_SEC))
            return events

        # --- start ------------------------------------------------------------
        wait = self._s.int_from_spec(self._cal["matching"]["rider_wait_duration_sec"])
        started_at = arrived_at + timedelta(seconds=wait)
        started = self._event(
            event_type="RideStarted",
            moment=started_at,
            partition_key=trip_id,
            correlation_id=trip_id,
            causation_id=arrived.event_id,
            payload={
                "trip_id": trip_id,
                "rider_id": plan.rider.rider_id,
                "driver_id": driver.driver_id,
                "city_id": self._city.city_id,
                "started_at": started_at,
                "actual_pickup_lat": arrival_lat,
                "actual_pickup_lon": arrival_lon,
                "rider_wait_duration_sec": wait,
                "pickup_zone_id": pickup.zone_id,
            },
        )
        events.append(started)

        if self._s.chance(funnel["p_cancel_at_started"]):
            cancel_at = started_at + timedelta(seconds=self._s.randint(30, 300))
            events.append(
                self._cancel(
                    plan=plan,
                    causation=started,
                    stage="STARTED",
                    driver_id=driver.driver_id,
                    at=cancel_at,
                )
            )
            session.occupy(cancel_at + timedelta(seconds=DRIVER_BUFFER_SEC))
            return events

        # --- completion -------------------------------------------------------
        completed_at = started_at + timedelta(seconds=plan.duration_sec)
        pricing = self._cal["pricing"]
        toll = ZERO
        if self._s.chance(pricing["p_toll"]):
            toll = money(
                Decimal(
                    str(
                        self._s.uniform(
                            pricing["toll_amount_inr"]["min"], pricing["toll_amount_inr"]["max"]
                        )
                    )
                )
            )

        fare = compute_fare(
            city=self._city,
            vehicle_type=vehicle_type,
            distance_km=plan.distance_km,
            duration_sec=plan.duration_sec,
            surge_multiplier=plan.surge_multiplier,
            is_airport_trip=plan.is_airport_pickup or plan.is_airport_dropoff,
            toll_amount=toll,
            booking_fee=Decimal(str(pricing["booking_fee_inr"])),
        )

        completed = self._event(
            event_type="RideCompleted",
            moment=completed_at,
            partition_key=trip_id,
            correlation_id=trip_id,
            causation_id=started.event_id,
            payload={
                "trip_id": trip_id,
                "rider_id": plan.rider.rider_id,
                "driver_id": driver.driver_id,
                "city_id": self._city.city_id,
                "completed_at": completed_at,
                "dropoff_lat": plan.dropoff_lat,
                "dropoff_lon": plan.dropoff_lon,
                "dropoff_zone_id": dropoff.zone_id,
                "distance_km": plan.distance_km,
                "duration_sec": plan.duration_sec,
                "base_fare": fare.base_fare,
                "distance_fare": fare.distance_fare,
                "time_fare": fare.time_fare,
                "surge_multiplier": fare.surge_multiplier,
                "surge_amount": fare.surge_amount,
                "airport_fee": fare.airport_fee,
                "toll_amount": fare.toll_amount,
                "booking_fee": fare.booking_fee,
                "tax_amount": fare.tax_amount,
                "total_fare": fare.total_fare,
                "driver_payout": fare.driver_payout,
                "platform_commission": fare.platform_commission,
                "currency": fare.currency,
                "traffic_level": plan.traffic_level,
                "weather_code": plan.weather_code,
                "payment_method": plan.payment_method,
            },
        )
        events.append(completed)

        session.trips_completed += 1
        session.occupy(completed_at + timedelta(seconds=DRIVER_BUFFER_SEC))

        # --- payment ----------------------------------------------------------
        events.append(
            self._payment(
                plan=plan,
                causation=completed,
                fare=fare,
                driver_id=driver.driver_id,
                completed_at=completed_at,
            )
        )
        return events

    # ---- terminal events --------------------------------------------------

    def _cancel(
        self,
        *,
        plan: TripPlan,
        causation: Event,
        stage: str,
        driver_id: str | None,
        at: datetime,
        forced_reason: str | None = None,
        forced_party: str | None = None,
    ) -> Event:
        if forced_reason:
            reason = self._ref.cancellation_reasons[forced_reason]
            party = forced_party or reason.cancelled_by_party
        else:
            party = self._s.weighted_choice(self._cal["funnel_rates"]["cancelled_by_weights"])
            # A pre-match cancellation cannot be blamed on a driver who was
            # never assigned, so driver-party reasons are excluded there.
            if stage == "REQUESTED" and party == "DRIVER":
                party = "RIDER"
            candidates = self._ref.reasons_for(party)
            reason = self._s.choice(candidates)

        seconds_since = max(int((at - plan.request.requested_at).total_seconds()), 0)
        fee = cancellation_fee(
            stage=stage,
            is_fee_applicable=reason.is_fee_applicable,
            seconds_since_request=seconds_since,
        )

        return self._event(
            event_type="RideCancelled",
            moment=at,
            partition_key=plan.trip_id,
            correlation_id=plan.trip_id,
            causation_id=causation.event_id,
            payload={
                "trip_id": plan.trip_id,
                "rider_id": plan.rider.rider_id,
                "driver_id": driver_id,
                "city_id": self._city.city_id,
                "cancelled_at": at,
                "cancelled_by": party,
                "cancellation_reason_code": reason.code,
                "cancelled_at_status": stage,
                "seconds_since_request": seconds_since,
                "cancellation_fee": fee,
                "is_fee_charged": fee > ZERO,
                "is_driver_fault": reason.is_driver_fault,
                "currency": self._city.currency_code,
            },
        )

    def _payment(
        self, *, plan: TripPlan, causation: Event, fare, driver_id: str, completed_at: datetime
    ) -> Event:
        params = self._cal["payments"]
        lag = self._s.randint(
            params["settlement_lag_sec"]["min"], params["settlement_lag_sec"]["max"]
        )
        paid_at = completed_at + timedelta(seconds=lag)

        attempt = self._s.weighted_index(params["attempt_number_weights"]) + 1
        previous_failure = self._s.choice(params["failure_reasons"]) if attempt > 1 else None

        tip = ZERO
        if self._s.chance(params["p_tip"]):
            pct = self._s.uniform(
                params["tip_pct_of_fare"]["min"], params["tip_pct_of_fare"]["max"]
            )
            tip = money(fare.total_fare * Decimal(str(pct)))

        discount = ZERO
        promo = plan.promo_code
        if promo is not None:
            pct = self._s.uniform(
                params["promo_discount_pct"]["min"], params["promo_discount_pct"]["max"]
            )
            discount = money(fare.total_fare * Decimal(str(pct)))
            # F3 requires amount_charged > 0, so a discount can never exceed the
            # fare. Capping at 90% keeps a positive charge on every payment.
            discount = min(discount, money(fare.total_fare * Decimal("0.9")))
        if discount == ZERO:
            promo = None

        amount_charged = fare.total_fare + tip - discount

        return self._event(
            event_type="PaymentCompleted",
            moment=paid_at,
            partition_key=plan.trip_id,
            correlation_id=plan.trip_id,
            causation_id=causation.event_id,
            payload={
                "trip_id": plan.trip_id,
                "rider_id": plan.rider.rider_id,
                "driver_id": driver_id,
                "city_id": self._city.city_id,
                "payment_id": self._s.uuid(),
                "paid_at": paid_at,
                "payment_method": plan.payment_method,
                "payment_status": "SUCCEEDED",
                "trip_fare": fare.total_fare,
                "tip_amount": tip,
                "discount_amount": discount,
                "amount_charged": amount_charged,
                "currency": self._city.currency_code,
                "promo_code": promo,
                "attempt_number": attempt,
                "previous_attempt_failure_reason": previous_failure,
                "gateway_reference": self._gateway_reference(plan.payment_method),
            },
        )

    # ---- presence events --------------------------------------------------

    def session_events(self, session: DriverSession) -> list[Event]:
        driver = session.driver
        online = self._event(
            event_type="DriverOnline",
            moment=session.online_at,
            partition_key=driver.driver_id,
            correlation_id=session.session_id,
            causation_id=None,
            payload={
                "driver_id": driver.driver_id,
                "session_id": session.session_id,
                "city_id": self._city.city_id,
                "online_at": session.online_at,
                "driver_lat": session.online_lat,
                "driver_lon": session.online_lon,
                "zone_id": session.online_zone.zone_id,
                "vehicle_id": driver.vehicle_id,
                "vehicle_type": driver.vehicle_type,
                "driver_status": "AVAILABLE",
                "device_platform": driver.device_platform,
                "app_version": driver.app_version,
            },
        )
        if session.is_open:
            # No DriverOffline: the session is still running when the window
            # closes, or the app died. Either is legitimate - imputing an end
            # time would fabricate supply hours.
            return [online]

        offline = self._event(
            event_type="DriverOffline",
            moment=session.offline_at,
            partition_key=driver.driver_id,
            correlation_id=session.session_id,
            causation_id=online.event_id,
            payload={
                "driver_id": driver.driver_id,
                "session_id": session.session_id,
                "city_id": self._city.city_id,
                "online_at": session.online_at,
                "offline_at": session.offline_at,
                "driver_lat": session.offline_lat,
                "driver_lon": session.offline_lon,
                "zone_id": session.offline_zone.zone_id,
                "session_duration_sec": session.duration_sec,
                "trips_completed_in_session": session.trips_completed,
                "offline_reason": session.offline_reason,
            },
        )
        return [online, offline]
