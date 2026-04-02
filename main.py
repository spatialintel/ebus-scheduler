"""
main.py — eBus Scheduler entry point.

Usage:
    python main.py config/eBus_Config_Input.xlsx
    python main.py config/eBus_Config_Input.xlsx --optimize
    python main.py config/eBus_Config_Input.xlsx --output outputs/schedule.xlsx
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from src.config_loader import load_config
from src.distance_engine import enrich_distances
from src.trip_generator import generate_trips
from src.bus_scheduler import schedule_buses
from src.output_formatter import write_schedule
from src.metrics import compute_metrics


def main(excel_path: str, output_path: str | None = None, optimize: bool = False) -> None:
    print(f"\n{'='*60}")
    print(f"  eBus Scheduler")
    print(f"{'='*60}\n")

    # ── Step 1: Load config ──
    print(f"  Loading config from: {excel_path}")
    config, headway_df, travel_time_df = load_config(excel_path)
    print(f"  Route: {config.route_code} — {config.route_name}")
    print(f"  Fleet: {config.fleet_size} buses, {config.battery_kwh} kWh battery")
    print(f"  Hours: {config.operating_start} – {config.operating_end}")

    # ── Step 2: Enrich distances ──
    result = enrich_distances(config)
    if result["fetched"]:
        print(f"\n  OSRM: fetched {len(result['fetched'])} segments")
    if result["failed"]:
        print(f"  OSRM: {len(result['failed'])} segments failed (using manual values)")
    print(f"  Segments: {len(config.segment_distances)} pairs loaded")

    # ── Step 3: Generate trips ──
    trips = generate_trips(config, headway_df, travel_time_df)
    revenue = [t for t in trips if t.trip_type == "Revenue"]
    dead = [t for t in trips if t.trip_type == "Dead"]
    print(f"\n  Trips generated: {len(trips)} total")
    print(f"    Revenue: {len(revenue)} (UP={len([t for t in revenue if t.direction=='UP'])}, "
          f"DN={len([t for t in revenue if t.direction=='DN'])})")
    print(f"    Dead runs: {len(dead)}")

    if optimize:
        # ── Step 4a: Optimize ──
        print(f"\n  Running optimizer...")
        from src.optimizer import optimize_schedule
        buses, metrics, best_headway = optimize_schedule(
            config, headway_df, travel_time_df, verbose=True
        )
    else:
        # ── Step 4b: Standard schedule ──
        print(f"\n  Scheduling {len(revenue)} revenue trips across {config.fleet_size} buses...")
        buses = schedule_buses(config, trips, headway_df=headway_df, travel_time_df=travel_time_df)
        metrics = compute_metrics(config, buses, total_revenue_trips=len(revenue))

    # ── Step 5: Summary ──
    print(f"\n  {'─'*50}")
    print(f"  SCHEDULE SUMMARY")
    print(f"  {'─'*50}")
    print(f"  {metrics.summary()}")

    print(f"\n  {'Bus':>4}  {'Trips':>5}  {'KM':>7}  {'SOC':>6}  {'Location':>25}")
    print(f"  {'─'*55}")
    for bus in buses:
        print(f"  {bus.bus_id:>4}  {len(bus.trips):>5}  {bus.total_km:>7.1f}  "
              f"{bus.soc_percent:>5.1f}%  {bus.current_location:>25}")

    # ── Step 6: Write output ──
    if output_path is None:
        output_path = f"outputs/{config.route_code}_schedule.xlsx"

    out = write_schedule(config, buses, output_path)
    print(f"\n  Output saved: {out}")
    print(f"\n{'='*60}\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="eBus Scheduler")
    parser.add_argument("config", help="Path to the eBus Config Input Excel file")
    parser.add_argument("--output", "-o", help="Output Excel path (optional)")
    parser.add_argument("--optimize", action="store_true", help="Run headway optimizer")

    args = parser.parse_args()
    main(args.config, args.output, args.optimize)
