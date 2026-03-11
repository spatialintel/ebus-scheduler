"""Test config_loader.py against the real eBus_Config_Input.xlsx"""

import sys
import os

# Add project root to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from datetime import time
from src.config_loader import load_config

EXCEL_PATH = os.path.join(os.path.dirname(__file__), "..", "config", "eBus_Config_Input.xlsx")


def test_load_config():
    config, headway_df, travel_time_df = load_config(EXCEL_PATH)

    # Route identity
    assert config.route_code == "R1", f"route_code: {config.route_code}"
    assert "Gangajalia" in config.route_name
    print(f"  Route: {config.route_code} - {config.route_name}")

    # Locations
    assert config.depot == "DEPOT"
    assert config.start_point == "GANGAJALIA BUS STAND"
    assert config.end_point == "D MART, TOP 3"
    assert len(config.intermediates) == 2
    print(f"  Locations: {config.depot} | {config.start_point} | {config.end_point}")

    # Fleet
    assert config.fleet_size == 5
    assert config.battery_kwh == 210.0
    assert config.consumption_rate == 1.1
    assert config.initial_soc_percent == 95.0
    print(f"  Fleet: {config.fleet_size} buses, {config.battery_kwh} kWh")

    # Charging
    assert config.depot_flow_rate_kw == 192.0
    assert config.min_soc_percent == 20.0
    print(f"  Charging: {config.depot_flow_rate_kw} kW effective")

    # Operating hours
    assert config.operating_start == time(6, 0)
    assert config.operating_end == time(23, 0)
    assert config.shift_split == time(13, 0)

    # Segments
    print(f"\n  Segments loaded: {len(config.segment_distances)} pairs")
    for key, dist in sorted(config.segment_distances.items()):
        tt = config.segment_times[key]
        print(f"    {key:50s}  {dist:6.2f} km  {tt:3d} min")

    d2s = config.get_distance("DEPOT", "GANGAJALIA BUS STAND")
    assert d2s == 7.25, f"Depot->Start: {d2s}"

    s2e = config.get_distance("GANGAJALIA BUS STAND", "D MART, TOP 3")
    assert s2e == 7.2, f"Start->End: {s2e}"
    print("  Segment lookups OK")

    # Headway
    assert len(headway_df) == 34
    assert headway_df.iloc[0]["headway_min"] == 40
    print(f"\n  Headway: {len(headway_df)} slots")

    # Travel time
    assert len(travel_time_df) == 17
    assert travel_time_df.iloc[0]["up_min"] == 28
    print(f"  TravelTime: {len(travel_time_df)} slots")

    print("\n  All assertions passed.")


if __name__ == "__main__":
    print("Testing config_loader...\n")
    test_load_config()
    print("\nDone.")
