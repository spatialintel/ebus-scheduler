"""
fleet_analyzer.py — Peak Vehicle Requirement (PVR) and fleet rebalancing.

PVR is the theoretical minimum buses needed to maintain a headway profile
on a route, computed as:

    PVR = ceil(cycle_time / headway)

where cycle_time = round_trip_time + 2 × min_break.

The rebalancer identifies surplus routes (allocated > PVR) and deficit routes
(allocated < PVR), then proposes transfers to balance the citywide fleet.

Usage:
    from src.fleet_analyzer import compute_pvr, compute_rebalancing_plan
"""

from __future__ import annotations
__version__ = "2026-04-08-p1"

import math
from datetime import datetime, timedelta

from src.city_models import CityConfig, RouteInput, Transfer


REF_DATE = datetime(2025, 1, 1)


# ── PVR Computation ──────────────────────────────────────────────────────────

def _avg_headway(ri: RouteInput) -> float:
    """Weighted-average headway across all time bands."""
    df = ri.headway_df
    total_weight = 0.0
    total_hw = 0.0
    for _, row in df.iterrows():
        try:
            t_from = _to_minutes(row["time_from"])
            t_to = _to_minutes(row["time_to"])
            span = t_to - t_from
            if span <= 0:
                continue
            total_weight += span
            total_hw += span * float(row["headway_min"])
        except Exception:
            continue
    return total_hw / total_weight if total_weight > 0 else 30.0


def _peak_headway(ri: RouteInput) -> float:
    """Minimum (tightest) headway across all bands — this drives PVR."""
    try:
        return float(ri.headway_df["headway_min"].min())
    except Exception:
        return 30.0


def _avg_travel_time(ri: RouteInput) -> tuple[float, float]:
    """Average UP and DN travel times across bands."""
    df = ri.travel_time_df
    up = df["up_min"].mean() if "up_min" in df.columns else 45.0
    dn = df["dn_min"].mean() if "dn_min" in df.columns else 45.0
    return float(up), float(dn)


def _to_minutes(val) -> float:
    """Convert HH:MM string or time object to minutes since midnight."""
    from datetime import time as _time
    if isinstance(val, _time):
        return val.hour * 60 + val.minute
    if isinstance(val, datetime):
        return val.hour * 60 + val.minute
    parts = str(val).strip().split(":")
    return int(parts[0]) * 60 + int(parts[1])


def compute_pvr(ri: RouteInput) -> int:
    """
    Peak Vehicle Requirement for a single route.

    PVR = ceil(cycle_time / peak_headway)

    cycle_time = avg_up_travel + avg_dn_travel + 2 × preferred_layover
    peak_headway = minimum headway from headway profile
    """
    up_tt, dn_tt = _avg_travel_time(ri)
    min_break = ri.config.preferred_layover_min
    cycle_time = up_tt + dn_tt + 2 * min_break

    peak_hw = _peak_headway(ri)

    # PVR = how many buses needed so one departs every peak_hw minutes
    pvr = math.ceil(cycle_time / peak_hw) if peak_hw > 0 else 1
    return max(1, pvr)  # at least 1 bus


def compute_pvr_all(city: CityConfig) -> dict[str, int]:
    """Compute PVR for every route. Returns {route_code: pvr}."""
    return {code: compute_pvr(ri) for code, ri in city.routes.items()}


# ── Surplus / Deficit Detection ──────────────────────────────────────────────

def compute_fleet_balance(city: CityConfig) -> dict[str, dict]:
    """
    For each route, compute:
      - pvr: theoretical minimum
      - allocated: fleet_size from config
      - surplus: max(0, allocated - pvr)
      - deficit: max(0, pvr - allocated)
      - headroom_pct: (allocated - pvr) / pvr × 100

    Returns {route_code: {pvr, allocated, surplus, deficit, headroom_pct}}
    """
    pvrs = compute_pvr_all(city)
    result = {}
    for code, ri in city.routes.items():
        pvr = pvrs[code]
        allocated = ri.config.fleet_size
        surplus = max(0, allocated - pvr)
        deficit = max(0, pvr - allocated)
        headroom = ((allocated - pvr) / pvr * 100) if pvr > 0 else 0.0
        result[code] = {
            "pvr": pvr,
            "allocated": allocated,
            "surplus": surplus,
            "deficit": deficit,
            "headroom_pct": round(headroom, 1),
        }
    return result


# ── Rebalancing Plan ─────────────────────────────────────────────────────────

def _deficit_priority_score(ri: RouteInput, pvr: int, allocated: int) -> float:
    """
    Higher score = route needs buses more urgently.

    Factors:
      1. Raw deficit (pvr - allocated)
      2. Peak headway tightness (tighter headway = more critical)
      3. Operating window length (longer = more trips to serve)
    """
    deficit = max(0, pvr - allocated)
    if deficit == 0:
        return 0.0

    peak_hw = _peak_headway(ri)
    # Tighter headway → higher urgency (invert: 1/hw)
    hw_urgency = 60.0 / max(peak_hw, 5.0)

    # Longer operating window → more trips at risk
    try:
        start = _to_minutes(ri.config.operating_start)
        end = _to_minutes(ri.config.operating_end)
        window_hrs = (end - start) / 60
    except Exception:
        window_hrs = 15.0

    return deficit * 10.0 + hw_urgency * 2.0 + window_hrs * 0.5


def compute_rebalancing_plan(city: CityConfig) -> list[Transfer]:
    """
    Phase 1 rebalancing: whole-day bus transfers.

    Algorithm:
      1. Compute PVR and surplus/deficit per route
      2. Pool all surplus buses (from routes where allocated > PVR)
      3. Rank deficit routes by urgency score
      4. Assign pooled buses to deficit routes, highest urgency first
      5. Each transfer moves 1 bus at a time, re-ranking after each

    Returns list of Transfer objects.
    Does NOT modify city.routes — caller applies transfers by adjusting fleet_size.
    """
    balance = compute_fleet_balance(city)
    transfers: list[Transfer] = []

    # Build surplus pool: (route_code, count)
    surplus_pool: list[tuple[str, int]] = []
    for code, b in balance.items():
        if b["surplus"] > 0:
            surplus_pool.append((code, b["surplus"]))

    # Total available surplus
    total_surplus = sum(s for _, s in surplus_pool)
    if total_surplus == 0:
        return []

    # Build deficit list with priority
    deficit_routes: list[tuple[str, float]] = []
    for code, b in balance.items():
        if b["deficit"] > 0:
            ri = city.routes[code]
            score = _deficit_priority_score(ri, b["pvr"], b["allocated"])
            deficit_routes.append((code, score))

    if not deficit_routes:
        return []

    # Sort by priority (highest first)
    deficit_routes.sort(key=lambda x: -x[1])

    # Greedy assignment: give buses one at a time
    # Track running allocations
    running_alloc = {code: b["allocated"] for code, b in balance.items()}
    running_surplus = dict(surplus_pool)

    bus_counter = 0
    changed = True
    while changed:
        changed = False
        for deficit_code, _ in deficit_routes:
            pvr = balance[deficit_code]["pvr"]
            if running_alloc[deficit_code] >= pvr:
                continue  # no longer in deficit

            # Find a donor
            for donor_code in list(running_surplus.keys()):
                if running_surplus[donor_code] <= 0:
                    continue

                bus_counter += 1
                bus_id = f"TRANSFER-{bus_counter:03d}"
                transfers.append(Transfer(
                    bus_id=bus_id,
                    from_route=donor_code,
                    to_route=deficit_code,
                    reason="surplus_rebalance",
                ))

                running_surplus[donor_code] -= 1
                running_alloc[donor_code] -= 1
                running_alloc[deficit_code] += 1
                changed = True
                break

    return transfers


def apply_transfers(city: CityConfig, transfers: list[Transfer]) -> dict[str, int]:
    """
    Compute adjusted fleet sizes after applying transfers.
    Returns {route_code: new_fleet_size}.
    Does NOT mutate config objects.
    """
    adjusted = {
        code: ri.config.fleet_size
        for code, ri in city.routes.items()
    }

    for t in transfers:
        if t.from_route in adjusted:
            adjusted[t.from_route] -= 1
        if t.to_route in adjusted:
            adjusted[t.to_route] += 1

    # Ensure no route drops below 1
    for code in adjusted:
        adjusted[code] = max(1, adjusted[code])

    return adjusted
