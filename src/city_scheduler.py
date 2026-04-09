"""
city_scheduler.py — Citywide multi-route scheduling orchestrator.

Two modes:

  Optimizer OFF (headway-driven, planning-compliant):
    1. Run each route with its config fleet_size
    2. Compute time-sliced PVR -> detect surplus/deficit
    3. Transfer surplus buses to deficit routes (depot-compatible only)
    4. Re-run only affected routes with adjusted fleet_size
    5. Post-rebalance stability check (PVR drift > 0.5 flagged)
    6. Return CitySchedule

  Optimizer ON (efficiency-maximising, KPI-driven):
    1. For each route, binary-search minimum fleet satisfying P1-P6
    2. Sum = citywide minimum fleet
    3. If user fleet > minimum, distribute extras to worst-scoring routes
    4. Return CitySchedule

Mode labels shown in the UI:
  Optimizer OFF -> "Planning-Compliant" (follows headway profile strictly)
  Optimizer ON  -> "Efficiency-Maximising" (finds minimum feasible fleet)

Usage:
    from src.city_scheduler import schedule_city

    result = schedule_city(city_config, optimize=False)
"""

from __future__ import annotations
__version__ = "2026-04-09-p2"

from src.city_models import (
    CityConfig, CitySchedule, RouteResult, RouteInput, Transfer,
)
from src.fleet_analyzer import (
    compute_pvr, compute_pvr_slices,
    compute_fleet_balance, compute_rebalancing_plan,
    apply_transfers, check_rebalance_stability,
)
from src.trip_generator import generate_trips
from src.bus_scheduler import schedule_buses
from src.metrics import compute_metrics


# ---------------------------------------------------------------------------
# Single-route runner (shared by both modes)
# ---------------------------------------------------------------------------

def _run_single_route(
    ri: RouteInput,
    fleet_override: int | None = None,
) -> RouteResult:
    """
    Schedule one route.  If fleet_override is given, temporarily patches
    config.fleet_size before running (always restored in finally block).
    """
    config         = ri.config
    original_fleet = config.fleet_size

    if fleet_override is not None and fleet_override != config.fleet_size:
        config.fleet_size = fleet_override

    try:
        trips         = generate_trips(config, ri.headway_df, ri.travel_time_df)
        revenue_count = len([t for t in trips if t.trip_type == "Revenue"])
        buses         = schedule_buses(
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

        pvr_slices = compute_pvr_slices(ri)

        return RouteResult(
            route_code=config.route_code,
            config=config,
            headway_df=ri.headway_df,
            travel_time_df=ri.travel_time_df,
            buses=buses,
            metrics=metrics,
            pvr=pvr_slices.pvr_peak,
            fleet_allocated=config.fleet_size,
            fleet_original=original_fleet,
            surplus=max(0, config.fleet_size - pvr_slices.pvr_peak),
            deficit=max(0, pvr_slices.pvr_peak - config.fleet_size),
        )
    finally:
        config.fleet_size = original_fleet


# ---------------------------------------------------------------------------
# Optimizer OFF: Headway-Driven / Planning-Compliant
# ---------------------------------------------------------------------------

def _schedule_headway_driven(city: CityConfig) -> CitySchedule:
    """
    Mode: Optimizer OFF  (Planning-Compliant)

    Step 1: Initial run — every route at its configured fleet_size
    Step 2: Time-sliced PVR balance -> rebalancing plan
    Step 3: Re-run only routes whose fleet changed
    Step 4: Post-rebalance stability check
    Step 5: Assemble CitySchedule
    """

    # Step 1 — initial run
    results: dict[str, RouteResult] = {}
    for code, ri in city.routes.items():
        results[code] = _run_single_route(ri)

    # Step 2 — rebalancing plan (uses time-sliced PVR internally)
    pre_balance = compute_fleet_balance(city)
    transfers   = compute_rebalancing_plan(city)

    if not transfers:
        # No transfers needed — still attach empty stability flags
        post_pvr    = {code: r.pvr for code, r in results.items()}
        post_fleet  = {code: r.fleet_allocated for code, r in results.items()}
        stability   = check_rebalance_stability(city, pre_balance, post_fleet, post_pvr)
        return CitySchedule(
            city_config=city,
            results=results,
            transfers=[],
            stability_flags=stability,
        )

    # Step 3 — re-run affected routes
    adjusted_fleet = apply_transfers(city, transfers)
    for code, new_fleet in adjusted_fleet.items():
        if new_fleet != city.routes[code].config.fleet_size:
            ri = city.routes[code]
            results[code] = _run_single_route(ri, fleet_override=new_fleet)
            results[code].fleet_allocated = new_fleet

    # Step 4 — stability check
    post_pvr   = {code: r.pvr for code, r in results.items()}
    stability  = check_rebalance_stability(city, pre_balance, adjusted_fleet, post_pvr)

    return CitySchedule(
        city_config=city,
        results=results,
        transfers=transfers,
        stability_flags=stability,
    )


# ---------------------------------------------------------------------------
# Optimizer ON: KPI-Driven / Efficiency-Maximising
# ---------------------------------------------------------------------------

def _find_min_fleet(ri: RouteInput, max_fleet: int = 20) -> int:
    """
    Binary search for the minimum fleet_size that produces a valid schedule
    (P1-P6 satisfied, SOC floor respected, >= 80 % trip coverage).
    """
    lo, hi = 1, max_fleet
    best   = max_fleet

    while lo <= hi:
        mid = (lo + hi) // 2
        try:
            result   = _run_single_route(ri, fleet_override=mid)
            soc_ok   = result.metrics.min_soc_seen >= ri.config.min_soc_percent
            breaks_ok = result.metrics.negative_breaks == 0
            coverage = (result.metrics.revenue_trips_assigned /
                        max(1, result.metrics.revenue_trips_total))
            trips_ok = coverage >= 0.80

            if soc_ok and breaks_ok and trips_ok:
                best = mid
                hi   = mid - 1
            else:
                lo   = mid + 1
        except Exception:
            lo = mid + 1

    return best


def _schedule_kpi_driven(city: CityConfig) -> CitySchedule:
    """
    Mode: Optimizer ON  (Efficiency-Maximising)

    Step 1: Binary-search minimum fleet per route
    Step 2: Distribute any surplus citywide fleet to worst-scoring routes
    Step 3: Final run with adjusted fleet
    """

    # Step 1 — minimum fleet per route
    min_fleets: dict[str, int] = {}
    for code, ri in city.routes.items():
        min_fleets[code] = _find_min_fleet(ri)

    total_min  = sum(min_fleets.values())
    user_total = city.total_fleet
    adjusted   = dict(min_fleets)
    transfers: list[Transfer] = []

    # Step 2 — distribute extras (if any)
    if user_total > total_min:
        extras = user_total - total_min

        scores: dict[str, float] = {}
        for code, ri in city.routes.items():
            try:
                result      = _run_single_route(ri, fleet_override=min_fleets[code])
                scores[code] = result.metrics.weighted_score()
            except Exception:
                scores[code] = float("inf")

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
        # Under-provisioned: proportional allocation with floor of 1
        remaining       = max(0, user_total - len(city.routes))
        total_pvr_weight = sum(min_fleets.values())
        codes            = sorted(min_fleets, key=lambda c: -min_fleets[c])
        allocated_so_far = 0
        adjusted         = {}
        for code in codes:
            share          = 1 + round(remaining * min_fleets[code] / total_pvr_weight)
            adjusted[code] = max(1, share)
            allocated_so_far += adjusted[code]
        # Correct rounding drift downward
        while allocated_so_far > user_total and allocated_so_far > len(city.routes):
            for code in reversed(codes):
                if adjusted[code] > 1 and allocated_so_far > user_total:
                    adjusted[code]   -= 1
                    allocated_so_far -= 1

    # Step 3 — final run
    results: dict[str, RouteResult] = {}
    for code, ri in city.routes.items():
        fleet = adjusted.get(code, min_fleets.get(code, ri.config.fleet_size))
        results[code] = _run_single_route(ri, fleet_override=fleet)
        results[code].fleet_allocated = fleet
        results[code].fleet_original  = ri.config.fleet_size

    return CitySchedule(
        city_config=city,
        results=results,
        transfers=transfers,
        stability_flags=[],   # not applicable for KPI mode
    )


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def schedule_city(
    city: CityConfig,
    optimize: bool = False,
) -> CitySchedule:
    """
    Main entry point for citywide scheduling.

    Args:
        city:     CityConfig with all routes loaded
        optimize: False -> Planning-Compliant  (Optimizer OFF)
                  True  -> Efficiency-Maximising (Optimizer ON)

    Returns:
        CitySchedule with per-route results + transfer records + stability flags
    """
    if optimize:
        return _schedule_kpi_driven(city)
    return _schedule_headway_driven(city)
