import math
from typing import Optional

from geopy.geocoders import Nominatim


def haversine_miles(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Calculate the great-circle distance between two points in miles."""
    R = 3958.8  # Earth radius in miles
    lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = math.sin(dlat / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
    return R * 2 * math.asin(math.sqrt(a))


def geocode(place_name: str) -> Optional[tuple[float, float]]:
    """Geocode a place name to (lat, lon) using Nominatim."""
    geolocator = Nominatim(user_agent="house-hunter")
    location = geolocator.geocode(place_name)
    if location:
        return (location.latitude, location.longitude)
    return None


def compute_distances(
    listing_lat: float,
    listing_lon: float,
    locations: list[dict],
) -> list[dict]:
    """Compute distances from a listing to all priority locations.

    locations: list of {name, latitude, longitude, priority}
    Returns: list of {name, distance_miles, priority}
    """
    results = []
    for loc in locations:
        dist = haversine_miles(listing_lat, listing_lon, loc["latitude"], loc["longitude"])
        results.append({
            "name": loc["name"],
            "distance_miles": round(dist, 1),
            "priority": loc["priority"],
        })
    return results
