"""
city_scheduler.py — Citywide multi-route scheduling orchestrator.

Two modes:

  Optimizer OFF (headway-driven):
    1. Run each route with its config fleet_size
    2. Compute PVR → detect surplus/deficit
    3. Transfer surplus buses to deficit routes
    4. Re-run only affected routes with adjusted fleet_size
    5. Return CitySchedule

  Optimizer ON (KPI-driven):
    1. For each route, binary-search minimum fleet satisfying P1–P6
    2. Sum = citywide minimum fleet
    3. If user fleet > minimum, distribute extras to worst-scoring routes
    4. Return CitySchedule

Usage:
    from src.city_scheduler import schedule_city

    result = schedule_city(city_config, optimize=False)
"""

from __future__ import annotations
__version__ = "2026-04-08-p1"

from copy import deepcopy
from datetime import datetime

from src.city_models import (
    CityConfig, CitySchedule, RouteResult, RouteInput, Transfer,
)
from src.fleet_analyzer import (
    compute_pvr, compute_rebalancing_plan, apply_transfers,
)
from src.trip_generator import generate_trips
from src.bus_scheduler import schedule_buses
from src.metrics import compute_metrics
from src.models import RouteConfig


# ── Single-route runner ──────────────────────────────────────────────────────

def _run_single_route(
    ri: RouteInput,
    fleet_override: int | None = None,
) -> RouteResult:
    """
    Schedule one route. If fleet_override is given, temporarily patch
    config.fleet_size before running (restored after).
    """
    config = ri.config
    original_fleet = config.fleet_size

    if fleet_override is not None and fleet_override != config.fleet_size:
        config.fleet_size = fleet_override

    try:
        trips = generate_trips(config, ri.headway_df, ri.travel_time_df)
        revenue_count = len([t for t in trips if t.trip_type == "Revenue"])
        buses = schedule_buses(
            config, trips,
            headway_df=ri.headway_df,
            travel_time_df=ri.travel_time_df,
        )
        metrics = compute_metrics(
            config, buses,
            total_revenue_trips=revenue_count,
        )

        # Tag buses and trips with route_code
        for bus in buses:
            bus.current_route = config.route_code
            if config.route_code not in bus.route_history:
                bus.route_history.append(config.route_code)
            for trip in bus.trips:
                trip.route_code = config.route_code

        # Relabel bus IDs: R4-B01, R4-B02, ...
        for i, bus in enumerate(buses, 1):
            new_id = f"{config.route_code}-B{i:02d}"
            old_id = bus.bus_id
            bus.bus_id = new_id
            for trip in bus.trips:
                if trip.assigned_bus == old_id:
                    trip.assigned_bus = new_id

        pvr = compute_pvr(ri)

        return RouteResult(
            route_code=config.route_code,
            config=config,
            headway_df=ri.headway_df,
            travel_time_df=ri.travel_time_df,
            buses=buses,
            metrics=metrics,
            pvr=pvr,
            fleet_allocated=config.fleet_size,
            fleet_original=original_fleet,
            surplus=max(0, config.fleet_size - pvr),
            deficit=max(0, pvr - config.fleet_size),
        )
    finally:
        # Always restore original fleet_size
        config.fleet_size = original_fleet


# ── Optimizer OFF: Headway-Driven with Rebalancing ──────────────────────────

def _schedule_headway_driven(city: CityConfig) -> CitySchedule:
    """
    Mode: Optimizer OFF

    Step 1: Run every route with its configured fleet_size
    Step 2: Compute rebalancing plan (surplus → deficit transfers)
    Step 3: Re-run only routes whose fleet changed
    Step 4: Assemble CitySchedule
    """

    # ── Step 1: Initial run ──────────────────────────────────────────────────
    results: dict[str, RouteResult] = {}
    for code, ri in city.routes.items():
        results[code] = _run_single_route(ri)

    # ── Step 2: Rebalancing plan ─────────────────────────────────────────────
    transfers = compute_rebalancing_plan(city)

    if not transfers:
        return CitySchedule(
            city_config=city,
            results=results,
            transfers=[],
        )

    # ── Step 3: Re-run affected routes with adjusted fleet ───────────────────
    adjusted_fleet = apply_transfers(city, transfers)

    for code, new_fleet in adjusted_fleet.items():
        if new_fleet != city.routes[code].config.fleet_size:
            ri = city.routes[code]
            results[code] = _run_single_route(ri, fleet_override=new_fleet)
            results[code].fleet_allocated = new_fleet

    return CitySchedule(
        city_config=city,
        results=results,
        transfers=transfers,
    )


# ── Optimizer ON: KPI-Driven Minimum Fleet ──────────────────────────────────

def _find_min_fleet(ri: RouteInput, max_fleet: int = 20) -> int:
    """
    Binary search for minimum fleet_size that produces a valid schedule
    (all P1–P6 satisfied, SOC floor respected).

    "Valid" = metrics.negative_breaks == 0 and min_soc_seen >= min_soc.
    """
    lo, hi = 1, max_fleet
    best = max_fleet

    while lo <= hi:
        mid = (lo + hi) // 2
        try:
            result = _run_single_route(ri, fleet_override=mid)
            # Check feasibility: SOC floor, no negative breaks, AND trips covered
            soc_ok = result.metrics.min_soc_seen >= ri.config.min_soc_percent
            breaks_ok = result.metrics.negative_breaks == 0
            # Must serve at least 80% of revenue trips (PVR guarantees ~100%)
            coverage = (result.metrics.revenue_trips_assigned /
                        max(1, result.metrics.revenue_trips_total))
            trips_ok = coverage >= 0.80

            if soc_ok and breaks_ok and trips_ok:
                best = mid
                hi = mid - 1
            else:
                lo = mid + 1
        except Exception:
            lo = mid + 1

    return best


def _schedule_kpi_driven(city: CityConfig) -> CitySchedule:
    """
    Mode: Optimizer ON

    Step 1: Binary-search minimum fleet per route
    Step 2: If total_fleet > sum(minimums), distribute extras
    Step 3: Run all routes with final fleet assignments
    """

    # ── Step 1: Find minimum fleet per route ─────────────────────────────────
    min_fleets: dict[str, int] = {}
    for code, ri in city.routes.items():
        min_fleets[code] = _find_min_fleet(ri)

    total_min = sum(min_fleets.values())
    user_total = city.total_fleet

    # ── Step 2: Distribute extras (if user has more than minimum) ────────────
    adjusted = dict(min_fleets)
    transfers: list[Transfer] = []

    if user_total > total_min:
        extras = user_total - total_min

        # Score routes: run at minimum fleet, rank by worst weighted_score
        scores: dict[str, float] = {}
        for code, ri in city.routes.items():
            try:
                result = _run_single_route(ri, fleet_override=min_fleets[code])
                scores[code] = result.metrics.weighted_score()
            except Exception:
                scores[code] = float('inf')

        # Give extras to worst-scoring routes first
        ranked = sorted(scores, key=lambda c: -scores[c])

        bus_counter = 0
        while extras > 0 and ranked:
            for code in ranked:
                if extras <= 0:
                    break
                adjusted[code] += 1
                extras -= 1
                bus_counter += 1
                transfers.append(Transfer(
                    bus_id=f"EXTRA-{bus_counter:03d}",
                    from_route="POOL",
                    to_route=code,
                    reason="kpi_improvement",
                ))

    elif user_total > 0 and user_total < total_min:
        # User has fewer buses than minimum — allocate proportionally
        # Each route gets at least 1, then remaining distributed by PVR weight
        remaining = user_total - len(city.routes)  # 1 per route guaranteed
        if remaining < 0:
            remaining = 0
            adjusted = {code: 1 for code in city.routes}
        else:
            total_pvr_weight = sum(min_fleets.values())
            adjusted = {}
            allocated_so_far = 0
            codes = sorted(min_fleets, key=lambda c: -min_fleets[c])
            for code in codes:
                share = 1 + round(remaining * min_fleets[code] / total_pvr_weight)
                adjusted[code] = max(1, share)
                allocated_so_far += adjusted[code]

            # Correct rounding drift
            while allocated_so_far > user_total and allocated_so_far > len(city.routes):
                for code in reversed(codes):
                    if adjusted[code] > 1 and allocated_so_far > user_total:
                        adjusted[code] -= 1
                        allocated_so_far -= 1

    # ── Step 3: Final run with adjusted fleet ────────────────────────────────
    results: dict[str, RouteResult] = {}
    for code, ri in city.routes.items():
        fleet = adjusted.get(code, min_fleets.get(code, ri.config.fleet_size))
        results[code] = _run_single_route(ri, fleet_override=fleet)
        results[code].fleet_allocated = fleet
        results[code].fleet_original = ri.config.fleet_size

    return CitySchedule(
        city_config=city,
        results=results,
        transfers=transfers,
    )


# ── Public API ───────────────────────────────────────────────────────────────

def schedule_city(
    city: CityConfig,
    optimize: bool = False,
) -> CitySchedule:
    """
    Main entry point for citywide scheduling.

    Args:
        city: CityConfig with all routes loaded
        optimize: False = headway-driven (Optimizer OFF)
                  True  = KPI-driven minimum fleet (Optimizer ON)

    Returns:
        CitySchedule with per-route results + transfer records
    """
    if optimize:
        return _schedule_kpi_driven(city)
    else:
        return _schedule_headway_driven(city)
