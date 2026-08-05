"""Demand side: riders, temporal demand curves, and origin-destination choice.

Two things here are deliberate rather than convenient:

1. Demand is shaped by *local* hour, resolved through the city's timezone. All
   events are stored in UTC, but "morning peak" is a local concept - 08:00 IST
   is 02:30 UTC. Driving demand off UTC would put the Bengaluru peak at 2 a.m.

2. Destination choice is biased by zone type and time of day, so commercial
   zones pull inbound in the morning and residential zones pull inbound in the
   evening. Without that, origin-destination flows are uniform noise and every
   zone-level metric in the warehouse is meaningless.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from zoneinfo import ZoneInfo

from event_generator.calibration import Calibration, Sampler
from event_generator.reference import City, ReferenceData, Zone

RIDER_POOL_MULTIPLIER = 2.4
"""Riders per expected trip.

Below 1.0 every trip would be a unique rider and dim_rider could never
accumulate lifetime history; very high values make repeat riders vanishingly
rare. This gives a realistic mix of one-off and returning riders.
"""


@dataclass(frozen=True)
class Rider:
    rider_id: str
    customer_tier: str
    device_platform: str
    app_version: str
    home_zone_id: int


class DemandModel:
    def __init__(
        self,
        *,
        sampler: Sampler,
        calibration: Calibration,
        reference: ReferenceData,
        city: City,
        expected_trips: int,
    ) -> None:
        self._sampler = sampler
        self._calibration = calibration
        self._city = city
        self._tz = ZoneInfo(city.timezone)
        self._zones = reference.active_zones(city.city_id)
        self._zone_weights = [float(z.avg_daily_demand) for z in self._zones]
        self._flow = calibration["zone_flow"]

        pool_size = max(int(expected_trips * RIDER_POOL_MULTIPLIER), 25)
        tier_weights = calibration["customer_tier_mix"]["weights"]
        device = calibration["device"]
        self.riders = [
            Rider(
                rider_id=sampler.uuid(),
                customer_tier=sampler.weighted_choice(tier_weights),
                device_platform=sampler.weighted_choice(device["rider_platform_weights"]),
                app_version=sampler.choice(device["rider_app_versions"]),
                home_zone_id=sampler.weighted_pick(self._zones, self._zone_weights).zone_id,
            )
            for _ in range(pool_size)
        ]
        # Zipf-ish frequency: a minority of riders take a majority of trips.
        self._rider_weights = [1.0 / (i + 1) ** 0.65 for i in range(len(self.riders))]

    # ---- time -------------------------------------------------------------

    def local_hour(self, moment: datetime) -> int:
        return moment.astimezone(self._tz).hour

    def is_weekend(self, moment: datetime) -> bool:
        return moment.astimezone(self._tz).weekday() >= 5

    def demand_multiplier(self, moment: datetime) -> float:
        key = "hourly_demand_weekend" if self.is_weekend(moment) else "hourly_demand_weekday"
        return float(self._calibration[key]["values"][self.local_hour(moment)])

    # ---- selection --------------------------------------------------------

    def pick_rider(self) -> Rider:
        return self._sampler.weighted_pick(self.riders, self._rider_weights)

    def pick_pickup_zone(self, moment: datetime) -> Zone:
        """Origins mirror destinations: residential in the morning, commercial
        in the evening - the reverse of the destination bias."""
        hour = self.local_hour(moment)
        if hour in self._flow["morning_peak_hours"]:
            boost = self._flow["evening_destination_boost"]
        elif hour in self._flow["evening_peak_hours"]:
            boost = self._flow["morning_destination_boost"]
        else:
            boost = self._flow["offpeak_destination_boost"]

        weights = [
            base * float(boost.get(zone.zone_type, 1.0))
            for zone, base in zip(self._zones, self._zone_weights, strict=True)
        ]
        return self._sampler.weighted_pick(self._zones, weights)

    def pick_dropoff_zone(self, moment: datetime, pickup: Zone) -> Zone:
        hour = self.local_hour(moment)
        if hour in self._flow["morning_peak_hours"]:
            boost = self._flow["morning_destination_boost"]
        elif hour in self._flow["evening_peak_hours"]:
            boost = self._flow["evening_destination_boost"]
        else:
            boost = self._flow["offpeak_destination_boost"]

        weights = []
        for zone, base in zip(self._zones, self._zone_weights, strict=True):
            weight = base * float(boost.get(zone.zone_type, 1.0))
            if zone.zone_id == pickup.zone_id:
                weight *= 0.08  # same-zone trips happen, but are uncommon
            weights.append(weight)
        return self._sampler.weighted_pick(self._zones, weights)

    def pick_vehicle_type(self) -> str:
        return self._sampler.weighted_choice(self._calibration["vehicle_type_mix"]["weights"])

    def pick_weather(self) -> str:
        return self._sampler.weighted_choice(self._calibration["weather_distribution"]["weights"])

    def pick_traffic(self, moment: datetime) -> str:
        cfg = self._calibration["traffic_by_hour"]
        hour = self.local_hour(moment)
        if hour in cfg["peak_hours"]:
            weights = cfg["weights_peak"]
        elif hour in cfg["shoulder_hours"]:
            weights = cfg["weights_shoulder"]
        else:
            weights = cfg["weights_offpeak"]
        return self._sampler.weighted_pick(cfg["levels"], weights)

    def pick_payment_method(self) -> str:
        return self._sampler.weighted_choice(self._calibration["payment_method_mix"]["weights"])

    def speed_kmh(self, traffic_level: str) -> float:
        return float(self._calibration["speed_kmh_by_traffic"]["values"][traffic_level])

    def weather_supply_factor(self, weather_code: str, reference: ReferenceData) -> float:
        """Adverse weather cuts supply while raising demand - the imbalance that
        drives real surge."""
        return {
            "CLEAR": 1.00,
            "CLOUDY": 1.00,
            "RAIN_LIGHT": 0.92,
            "RAIN_HEAVY": 0.75,
            "THUNDERSTORM": 0.55,
            "FOG": 0.85,
            "EXTREME_HEAT": 0.88,
        }.get(weather_code, 1.0)
