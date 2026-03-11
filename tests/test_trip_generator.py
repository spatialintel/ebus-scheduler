"""Tests for trip_generator.py"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.config_loader import load_config
from src.trip_generator import generate_trips

EXCEL_PATH = os.path.join(os.path.dirname(__file__), "..", "config", "eBus_Config_Input.xlsx")


def test_generate_trips():
    config, headway_df, travel_time_df = load_config(EXCEL_PATH)
    trips = generate_trips(config, headway_df, travel_time_df)

    print(f"  Total trips generated: {len(trips)}")

    # Count by type
    dead = [t for t in trips if t.trip_type == "Dead"]
    revenue = [t for t in trips if t.trip_type == "Revenue"]
    up = [t for t in trips if t.direction == "UP"]
    dn = [t for t in trips if t.direction == "DN"]
    depot = [t for t in trips if t.direction == "DEPOT"]

    print(f"  Revenue: {len(revenue)} (UP={len(up)}, DN={len(dn)})")
    print(f"  Dead runs: {len(dead)} (DEPOT={len(depot)})")

    # Basic assertions
    assert len(trips) > 0, "Should generate at least some trips"
    assert len(revenue) > 0, "Should have revenue trips"
    assert len(dead) > 0, "Should have dead runs"
    assert len(up) > 0, "Should have UP trips"
    assert len(dn) > 0, "Should have DN trips"

    # Morning dead runs = one per bus
    morning_dead = [t for t in dead if t.start_location == config.depot
                    and t.end_location == config.start_point]
    assert len(morning_dead) == config.fleet_size, \
        f"Morning dead runs: {len(morning_dead)}, expected {config.fleet_size}"
    print(f"  Morning dead runs: {len(morning_dead)} (1 per bus)")

    # Evening dead runs exist
    evening_dead = [t for t in dead if t.end_location == config.depot
                    and t.start_location != config.depot]
    assert len(evening_dead) > 0, "Should have evening dead runs"
    print(f"  Evening dead runs: {len(evening_dead)}")

    # Trips are sorted by earliest_departure
    for i in range(len(trips) - 1):
        assert trips[i].earliest_departure <= trips[i + 1].earliest_departure, \
            f"Trips not sorted at index {i}: {trips[i].earliest_departure} > {trips[i+1].earliest_departure}"
    print("  Sort order: OK")

    # First trip should be a dead run departing before operating_start
    first = trips[0]
    assert first.trip_type == "Dead", f"First trip should be Dead, got {first.trip_type}"
    assert first.start_location == config.depot
    print(f"  First trip: {first.direction} {first.start_location}->{first.end_location} "
          f"@ {first.earliest_departure.strftime('%H:%M')}")

    # First revenue trip should start at operating_start
    first_rev = revenue[0]
    assert first_rev.earliest_departure.hour == config.operating_start.hour
    assert first_rev.earliest_departure.minute == config.operating_start.minute
    print(f"  First revenue: {first_rev.direction} @ {first_rev.earliest_departure.strftime('%H:%M')}")

    # All revenue trips should have positive distance and travel time
    for t in revenue:
        assert t.distance_km > 0, f"Zero distance on {t}"
        assert t.travel_time_min > 0, f"Zero travel time on {t}"

    # UP and DN should alternate in revenue trips
    # (not strictly required — headway might create gaps — but generally true)
    up_count = len(up)
    dn_count = len(dn)
    assert abs(up_count - dn_count) <= 1, \
        f"UP/DN imbalance: {up_count} UP vs {dn_count} DN"
    print(f"  UP/DN balance: {up_count} UP, {dn_count} DN")

    # Print first 10 trips
    print(f"\n  First 10 trips:")
    print(f"  {'Time':>5}  {'Dir':>5}  {'Type':>8}  {'From':>25}  {'To':>25}  {'Dist':>6}  {'TT':>3}")
    print(f"  {'-'*85}")
    for t in trips[:10]:
        print(f"  {t.earliest_departure.strftime('%H:%M'):>5}  {t.direction:>5}  {t.trip_type:>8}  "
              f"{t.start_location:>25}  {t.end_location:>25}  {t.distance_km:>6.1f}  {t.travel_time_min:>3}")

    # Print last 5 trips
    print(f"\n  Last 5 trips:")
    for t in trips[-5:]:
        print(f"  {t.earliest_departure.strftime('%H:%M'):>5}  {t.direction:>5}  {t.trip_type:>8}  "
              f"{t.start_location:>25}  {t.end_location:>25}  {t.distance_km:>6.1f}  {t.travel_time_min:>3}")

    print("\n  All assertions passed.")


if __name__ == "__main__":
    print("Testing trip_generator...\n")
    test_generate_trips()
    print("\nDone.")
