"""Smoke tests for models.py"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from datetime import datetime, time, timedelta
from src.models import Trip, BusState, RouteConfig, ScheduleInfeasibleError


def test_trip():
    t = Trip(
        direction="UP", trip_type="Revenue",
        start_location="GANGAJALIA BUS STAND", end_location="ADHEWADA GAM",
        earliest_departure=datetime(2025, 1, 1, 6, 7),
        latest_departure=datetime(2025, 1, 1, 6, 12),
        travel_time_min=28, distance_km=8.2,
    )
    t.actual_departure = t.earliest_departure
    arrival = t.compute_arrival()
    assert arrival == datetime(2025, 1, 1, 6, 35), f"Got {arrival}"
    print("  Trip OK")


def test_bus_soc():
    bus = BusState(
        bus_id="B01", current_location="GANGAJALIA BUS STAND",
        current_time=datetime(2025, 1, 1, 6, 0),
        soc_percent=95.0, total_km=0.0, shift=1,
        battery_kwh=210.0, consumption_rate=1.1,
    )
    cost = bus._soc_cost(8.2)
    assert abs(cost - 4.295) < 0.01
    print("  BusState SOC OK")


def test_bus_can_serve():
    bus = BusState(
        bus_id="B01", current_location="GANGAJALIA BUS STAND",
        current_time=datetime(2025, 1, 1, 6, 0),
        soc_percent=95.0, total_km=0.0, shift=1,
        battery_kwh=210.0, consumption_rate=1.1,
    )
    trip = Trip(
        direction="UP", trip_type="Revenue",
        start_location="GANGAJALIA BUS STAND", end_location="ADHEWADA GAM",
        earliest_departure=datetime(2025, 1, 1, 6, 7),
        latest_departure=datetime(2025, 1, 1, 6, 12),
        travel_time_min=28, distance_km=8.2,
    )
    assert bus.can_serve(trip) is True
    bus.current_location = "DEPOT"
    assert bus.can_serve(trip) is False
    bus.current_location = "GANGAJALIA BUS STAND"
    bus.current_time = datetime(2025, 1, 1, 6, 5)
    assert bus.can_serve(trip) is False
    bus.current_time = datetime(2025, 1, 1, 6, 0)
    bus.soc_percent = 22.0
    assert bus.can_serve(trip) is False
    print("  BusState.can_serve OK")


def test_bus_assign():
    bus = BusState(
        bus_id="B01", current_location="GANGAJALIA BUS STAND",
        current_time=datetime(2025, 1, 1, 6, 0),
        soc_percent=95.0, total_km=0.0, shift=1,
        battery_kwh=210.0, consumption_rate=1.1,
    )
    trip = Trip(
        direction="UP", trip_type="Revenue",
        start_location="GANGAJALIA BUS STAND", end_location="ADHEWADA GAM",
        earliest_departure=datetime(2025, 1, 1, 6, 7),
        latest_departure=datetime(2025, 1, 1, 6, 12),
        travel_time_min=28, distance_km=8.2,
    )
    bus.assign(trip)
    assert bus.current_location == "ADHEWADA GAM"
    assert bus.current_time == datetime(2025, 1, 1, 6, 35)
    assert abs(bus.soc_percent - 90.705) < 0.01
    assert bus.total_km == 8.2
    assert trip.assigned_bus == "B01"
    print("  BusState.assign OK")


def test_bus_charge():
    bus = BusState(
        bus_id="B01", current_location="DEPOT",
        current_time=datetime(2025, 1, 1, 12, 0),
        soc_percent=25.0, total_km=50.0, shift=1,
        battery_kwh=210.0, consumption_rate=1.1,
    )
    new_soc = bus.charge(duration_min=30, flow_rate_kw=192.0)
    assert abs(new_soc - 70.714) < 0.01
    bus.charge(duration_min=120, flow_rate_kw=192.0)
    assert bus.soc_percent == 100.0
    print("  BusState.charge OK")


def test_route_config():
    cfg = RouteConfig(
        route_code="R1", route_name="Test",
        depot="DEPOT", start_point="GAN", end_point="ADH",
        intermediates=[],
        fleet_size=3, battery_kwh=210.0, consumption_rate=1.1,
        initial_soc_percent=95.0,
        depot_charger_kw=240.0, depot_charger_efficiency=0.80,
        terminal_charger_kw=0, terminal_charger_efficiency=0,
        trigger_soc_percent=30, target_soc_percent=80,
        min_soc_percent=20, min_charge_duration_min=15,
        operating_start=time(6, 0), operating_end=time(23, 0),
        shift_split=time(13, 0),
        min_layover_min=5, preferred_layover_min=10,
        dead_run_buffer_min=25,
        max_headway_deviation_min=5, km_balance_tolerance_pct=10,
        segment_distances={"DEPOT->GAN": 7.2, "GAN->ADH": 8.2},
        segment_times={"DEPOT->GAN": 25, "GAN->ADH": 28},
    )
    assert cfg.depot_flow_rate_kw == 192.0
    assert cfg.min_soc_percent == 20.0
    assert cfg.bus_ids() == ["B01", "B02", "B03"]
    assert cfg.get_distance("DEPOT", "GAN") == 7.2
    assert cfg.segment_key("DEPOT", "GAN") == "DEPOT->GAN"
    print("  RouteConfig OK")


if __name__ == "__main__":
    print("Running models.py tests...")
    test_trip()
    test_bus_soc()
    test_bus_can_serve()
    test_bus_assign()
    test_bus_charge()
    test_route_config()
    print("\nAll tests passed.")
