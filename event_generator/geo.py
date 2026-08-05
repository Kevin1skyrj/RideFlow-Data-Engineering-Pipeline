"""Geographic helpers.

Zone centroids come from real OpenStreetMap geocoding (see
transformation/seeds/dim_zone.csv, column `geocode_source`), so distances
derived here are grounded in actual Bengaluru geography rather than invented.
"""

from __future__ import annotations

import math

EARTH_RADIUS_KM = 6371.0088

ROAD_FACTOR_RANGE = (1.24, 1.52)
"""Road distance / straight-line distance.

Real road networks are never straight. A factor around 1.3-1.4 is the usual
observed range for dense urban grids; sampling within it prevents every trip
between the same two zones having an identical distance.
"""

INTRA_ZONE_KM_RANGE = (0.6, 3.2)
"""Trips that start and end in the same zone still cover real distance."""


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance in kilometres."""
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * EARTH_RADIUS_KM * math.asin(math.sqrt(a))


def jitter_point(lat: float, lon: float, radius_km: float, sampler) -> tuple[float, float]:
    """A random point within `radius_km` of a centroid.

    Riders are not picked up at the geometric centre of a neighbourhood. This
    scatters pickup and dropoff points realistically inside the zone while
    keeping them close enough that zone assignment stays correct.
    """
    distance = radius_km * math.sqrt(sampler.random())
    bearing = sampler.uniform(0, 2 * math.pi)
    dlat = distance / 111.32
    dlon = distance / (111.32 * max(math.cos(math.radians(lat)), 0.01))
    return (
        round(lat + dlat * math.cos(bearing), 6),
        round(lon + dlon * math.sin(bearing), 6),
    )


def offset_point(lat: float, lon: float, km: float, sampler) -> tuple[float, float]:
    """A point exactly `km` away in a random direction."""
    bearing = sampler.uniform(0, 2 * math.pi)
    dlat = km / 111.32
    dlon = km / (111.32 * max(math.cos(math.radians(lat)), 0.01))
    return (
        round(lat + dlat * math.cos(bearing), 6),
        round(lon + dlon * math.sin(bearing), 6),
    )
