"""
gtfs_exporter.py — Export schedule as GTFS static feed.

Generates a GTFS-compliant ZIP from a completed CitySchedule.
Produces: agency.txt, routes.txt, stops.txt, trips.txt, stop_times.txt, calendar.txt.

Usage:
    from src.gtfs_exporter import export_gtfs
    zip_bytes = export_gtfs(city_schedule, agency_name="AMTS")
    # zip_bytes is a bytes object — write to file or serve for download
"""

from __future__ import annotations
__version__ = "2026-04-17-p3"

import csv
import io
import zipfile
from datetime import datetime


def export_gtfs(
    city_schedule,
    agency_name: str = "AMTS",
    agency_url: str = "https://amts.co.in",
    agency_timezone: str = "Asia/Kolkata",
    service_id: str = "WEEKDAY",
) -> bytes:
    """
    Generate GTFS static feed ZIP from CitySchedule.

    Returns bytes of a ZIP file containing all required GTFS text files.
    """
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("agency.txt", _make_agency(agency_name, agency_url, agency_timezone))
        zf.writestr("calendar.txt", _make_calendar(service_id))

        # Collect all stops across all routes
        all_stops = _collect_stops(city_schedule)
        zf.writestr("stops.txt", _make_stops(all_stops))

        zf.writestr("routes.txt", _make_routes(city_schedule))

        trips_csv, stop_times_csv = _make_trips_and_stop_times(city_schedule, service_id)
        zf.writestr("trips.txt", trips_csv)
        zf.writestr("stop_times.txt", stop_times_csv)

    return buf.getvalue()


def _make_agency(name, url, tz) -> str:
    out = io.StringIO()
    w = csv.writer(out)
    w.writerow(["agency_id", "agency_name", "agency_url", "agency_timezone"])
    w.writerow(["AMTS", name, url, tz])
    return out.getvalue()


def _make_calendar(service_id) -> str:
    out = io.StringIO()
    w = csv.writer(out)
    w.writerow(["service_id", "monday", "tuesday", "wednesday", "thursday",
                "friday", "saturday", "sunday", "start_date", "end_date"])
    w.writerow([service_id, 1, 1, 1, 1, 1, 0, 0, "20260101", "20261231"])
    return out.getvalue()


def _collect_stops(city_schedule) -> dict[str, dict]:
    """
    Collect unique stops from all routes.
    Returns {stop_name: {stop_id, lat, lon}}.
    """
    stops = {}
    for code, r in city_schedule.results.items():
        cfg = r.config
        locations = [cfg.depot, cfg.start_point, cfg.end_point]
        locations += [n.strip() for n in getattr(cfg, "intermediates", []) if n and n.strip()]

        coords = getattr(cfg, "location_coords", {})

        for loc in locations:
            if loc and loc not in stops:
                lat, lon = 0.0, 0.0
                if loc in coords:
                    lat, lon = coords[loc]
                stop_id = _sanitise_id(loc)
                stops[loc] = {"stop_id": stop_id, "lat": lat, "lon": lon}

    return stops


def _make_stops(all_stops: dict[str, dict]) -> str:
    out = io.StringIO()
    w = csv.writer(out)
    w.writerow(["stop_id", "stop_name", "stop_lat", "stop_lon"])
    for name, info in sorted(all_stops.items()):
        w.writerow([info["stop_id"], name, info["lat"], info["lon"]])
    return out.getvalue()


def _make_routes(city_schedule) -> str:
    out = io.StringIO()
    w = csv.writer(out)
    w.writerow(["route_id", "agency_id", "route_short_name", "route_long_name", "route_type"])
    for code in sorted(city_schedule.results):
        r = city_schedule.results[code]
        w.writerow([code, "AMTS", code, r.config.route_name, 3])  # 3 = Bus
    return out.getvalue()


def _make_trips_and_stop_times(city_schedule, service_id) -> tuple[str, str]:
    trips_out = io.StringIO()
    st_out = io.StringIO()
    tw = csv.writer(trips_out)
    sw = csv.writer(st_out)

    tw.writerow(["route_id", "service_id", "trip_id", "direction_id", "trip_headsign"])
    sw.writerow(["trip_id", "arrival_time", "departure_time", "stop_id", "stop_sequence"])

    for code in sorted(city_schedule.results):
        r = city_schedule.results[code]
        trip_seq = {"UP": 0, "DN": 0}

        for bus in r.buses:
            for trip in bus.trips:
                if trip.trip_type != "Revenue":
                    continue
                if trip.actual_departure is None or trip.actual_arrival is None:
                    continue

                direction = trip.direction
                if direction not in trip_seq:
                    direction = "DN"

                trip_seq[direction] += 1
                dir_id = 0 if direction == "UP" else 1
                trip_id = f"{code}_{direction}_{trip_seq[direction]:04d}"

                headsign = trip.end_location

                tw.writerow([code, service_id, trip_id, dir_id, headsign])

                # Stop times: departure from start, arrival at end
                dep_time = _format_gtfs_time(trip.actual_departure)
                arr_time = _format_gtfs_time(trip.actual_arrival)

                start_stop_id = _sanitise_id(trip.start_location)
                end_stop_id = _sanitise_id(trip.end_location)

                sw.writerow([trip_id, dep_time, dep_time, start_stop_id, 1])
                sw.writerow([trip_id, arr_time, arr_time, end_stop_id, 2])

    return trips_out.getvalue(), st_out.getvalue()


def _format_gtfs_time(dt: datetime) -> str:
    """Format as HH:MM:SS. GTFS allows hours > 23 for next-day trips."""
    return f"{dt.hour:02d}:{dt.minute:02d}:{dt.second:02d}"


def _sanitise_id(name: str) -> str:
    """Convert location name to a valid GTFS stop_id."""
    return name.strip().upper().replace(" ", "_").replace(".", "").replace(",", "")
