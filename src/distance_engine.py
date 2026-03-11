"""
distance_engine.py — Fetches distances and travel times from OSRM API.

If lat/lon coordinates are provided in RouteConfig.location_coords,
this module calls OSRM to fill in or update segment_distances and segment_times.
If OSRM is unavailable or coordinates are missing, manual values from the Excel are kept.

Usage:
    from src.distance_engine import enrich_distances
    enrich_distances(config)  # modifies config in place
"""

from __future__ import annotations

import requests
from src.models import RouteConfig


OSRM_BASE = "http://router.project-osrm.org/route/v1/driving"
TIMEOUT_SEC = 10


def _fetch_osrm(lat1: float, lon1: float, lat2: float, lon2: float) -> tuple[float, int]:
    """
    Call OSRM routing API for a single origin-destination pair.
    Returns (distance_km, travel_time_min).
    Raises RuntimeError on failure.
    """
    url = f"{OSRM_BASE}/{lon1},{lat1};{lon2},{lat2}?overview=false"
    resp = requests.get(url, timeout=TIMEOUT_SEC)
    resp.raise_for_status()

    data = resp.json()
    if data.get("code") != "Ok" or not data.get("routes"):
        raise RuntimeError(f"OSRM returned no route: {data.get('code')}")

    route = data["routes"][0]
    distance_km = round(route["distance"] / 1000, 2)
    travel_time_min = round(route["duration"] / 60)

    return distance_km, travel_time_min


def enrich_distances(config: RouteConfig, overwrite: bool = False) -> dict:
    """
    For each segment in config.segment_distances, check if both locations have
    coordinates in config.location_coords. If so, fetch from OSRM.

    Args:
        config: RouteConfig object (modified in place)
        overwrite: If True, replace existing manual values with OSRM values.
                   If False (default), only fill in segments that have 0 or missing values.

    Returns:
        dict summarising what was fetched/skipped/failed:
        {
            "fetched": ["DEPOT->GANGAJALIA BUS STAND", ...],
            "skipped": ["DMART->DEPOT", ...],
            "failed": {"DEPOT->D MART, TOP 3": "timeout"},
            "no_coords": ["DMART", ...]
        }
    """
    if not config.location_coords:
        return {
            "fetched": [],
            "skipped": list(config.segment_distances.keys()),
            "failed": {},
            "no_coords": [],
            "message": "No coordinates provided. Using manual distances from Excel.",
        }

    # Collect all unique location names from segments
    all_locations = set()
    for key in config.segment_distances:
        parts = key.split("->")
        if len(parts) == 2:
            all_locations.add(parts[0].strip())
            all_locations.add(parts[1].strip())

    # Identify locations missing coordinates
    no_coords = [loc for loc in all_locations if loc not in config.location_coords]

    fetched = []
    skipped = []
    failed = {}

    for key in list(config.segment_distances.keys()):
        parts = key.split("->")
        if len(parts) != 2:
            skipped.append(key)
            continue

        from_loc = parts[0].strip()
        to_loc = parts[1].strip()

        # Skip if either location lacks coordinates
        if from_loc not in config.location_coords or to_loc not in config.location_coords:
            skipped.append(key)
            continue

        # Skip if manual value already exists and overwrite is off
        current_dist = config.segment_distances.get(key, 0)
        if current_dist > 0 and not overwrite:
            skipped.append(key)
            continue

        # Fetch from OSRM
        lat1, lon1 = config.location_coords[from_loc]
        lat2, lon2 = config.location_coords[to_loc]

        try:
            dist_km, time_min = _fetch_osrm(lat1, lon1, lat2, lon2)
            config.segment_distances[key] = dist_km
            config.segment_times[key] = time_min
            fetched.append(key)
        except Exception as e:
            failed[key] = str(e)

    return {
        "fetched": fetched,
        "skipped": skipped,
        "failed": failed,
        "no_coords": no_coords,
    }
