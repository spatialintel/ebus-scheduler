"""
fleet_analyzer.py — Peak Vehicle Requirement (PVR) and fleet rebalancing.

PVR is the theoretical minimum buses needed to maintain a headway profile
on a route.  Phase 1 used a single static value (cycle_time / peak_headway).
This version computes THREE time-sliced PVR values that better reflect
real fleet demand across the operating day:

    PVR_peak      = ceil(cycle_time / peak_headway)
                    -> buses needed during the tightest headway band
                    -> drives rebalancing decisions

    PVR_offpeak   = ceil(cycle_time / avg_offpeak_headway)
                    -> buses needed outside peak (11:00-15:00 window)
                    -> identifies off-peak slack available for transfer

    PVR_charging  = ceil(PVR_peak * (1 + CHARGING_FRACTION))
                    -> adds buses temporarily unavailable due to P5 midday
                       charging; conservative upper bound during 12:00-15:00

Rebalancing algorithm:
    1. Compute PVR_peak per route (primary driver)
    2. Identify surplus (allocated > PVR_peak) and deficit routes
    3. Check depot compatibility before any transfer (same depot only)
    4. Greedy assignment ranked by urgency score
    5. Post-rebalance stability check: flag PVR drift > 0.5

Usage:
    from src.fleet_analyzer import compute_pvr_slices, compute_rebalancing_plan
"""

from __future__ import annotations
__version__ = "2026-04-09-p2"

import math
from dataclasses import dataclass
from datetime import datetime, time as _time

from src.city_models import CityConfig, RouteInput, Transfer


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

OFFPEAK_START_MIN: int  = 11 * 60   # 11:00
OFFPEAK_END_MIN: int    = 15 * 60   # 15:00

# Fraction of fleet assumed charging simultaneously in midday window (P5).
# Conservative: ~25 % of buses cycle through depot charging 12:00-15:00.
CHARGING_FRACTION: float = 0.25

# PVR drift threshold for stability check (in PVR units).
STABILITY_THRESHOLD: float = 0.5


# ---------------------------------------------------------------------------
# Time helpers
# ---------------------------------------------------------------------------

def _to_minutes(val) -> float:
    """Convert HH:MM string, datetime.time, or datetime to minutes since midnight."""
    if isinstance(val, _time):
        return val.hour * 60 + val.minute
    if isinstance(val, datetime):
        return val.hour * 60 + val.minute
    parts = str(val).strip().split(":")
    return int(parts[0]) * 60 + int(parts[1])


def _band_overlap(t_from: float, t_to: float, win_start: float, win_end: float) -> float:
    """Minutes of overlap between [t_from, t_to) and [win_start, win_end)."""
    return max(0.0, min(t_to, win_end) - max(t_from, win_start))


# ---------------------------------------------------------------------------
# Headway extraction
# ---------------------------------------------------------------------------

def _peak_headway(ri: RouteInput) -> float:
    """Tightest (minimum) headway across all bands."""
    try:
        return float(ri.headway_df["headway_min"].min())
    except Exception:
        return 30.0


def _offpeak_headway(ri: RouteInput) -> float:
    """
    Weighted-average headway during the off-peak window (11:00-15:00).
    Falls back to overall weighted average if no bands overlap the window.
    """
    df = ri.headway_df
    weight, weighted_hw = 0.0, 0.0
    for _, row in df.iterrows():
        try:
            t_from  = _to_minutes(row["time_from"])
            t_to    = _to_minutes(row["time_to"])
            overlap = _band_overlap(t_from, t_to, OFFPEAK_START_MIN, OFFPEAK_END_MIN)
            if overlap <= 0:
                continue
            weight      += overlap
            weighted_hw += overlap * float(row["headway_min"])
        except Exception:
            continue

    if weight > 0:
        return weighted_hw / weight

    # Fallback: overall weighted average
    weight, weighted_hw = 0.0, 0.0
    for _, row in df.iterrows():
        try:
            t_from = _to_minutes(row["time_from"])
            t_to   = _to_minutes(row["time_to"])
            span   = t_to - t_from
            if span <= 0:
                continue
            weight      += span
            weighted_hw += span * float(row["headway_min"])
        except Exception:
            continue
    return weighted_hw / weight if weight > 0 else 30.0


def _avg_travel_time(ri: RouteInput) -> tuple[float, float]:
    """Average UP and DN travel times across all bands."""
    df = ri.travel_time_df
    up = float(df["up_min"].mean()) if "up_min" in df.columns else 45.0
    dn = float(df["dn_min"].mean()) if "dn_min" in df.columns else 45.0
    return up, dn


# ---------------------------------------------------------------------------
# PVR slice dataclass
# ---------------------------------------------------------------------------

@dataclass
class PVRSlices:
    """Three time-sliced PVR values for a single route."""
    route_code: str
    pvr_peak: int           # minimum fleet during tightest headway band
    pvr_offpeak: int        # minimum fleet during 11:00-15:00 off-peak
    pvr_charging: int       # conservative upper bound during midday charging
    peak_headway_min: float
    offpeak_headway_min: float
    cycle_time_min: float

    @property
    def slack_buses(self) -> int:
        """Buses free during off-peak — potentially shareable."""
        return max(0, self.pvr_peak - self.pvr_offpeak)

    def as_dict(self) -> dict:
        return {
            "Route":              self.route_code,
            "PVR (Peak)":         self.pvr_peak,
            "PVR (Off-Peak)":     self.pvr_offpeak,
            "PVR (Charging)":     self.pvr_charging,
            "Peak HW (min)":      round(self.peak_headway_min, 1),
            "Off-Peak HW (min)":  round(self.offpeak_headway_min, 1),
            "Cycle Time (min)":   round(self.cycle_time_min, 1),
            "Off-Peak Slack":     self.slack_buses,
        }


def compute_pvr_slices(ri: RouteInput) -> PVRSlices:
    """
    Compute all three PVR slices for one route.

    cycle_time   = avg_up + avg_dn + 2 x preferred_layover
    PVR_peak     = ceil(cycle_time / peak_headway)
    PVR_offpeak  = ceil(cycle_time / offpeak_headway)
    PVR_charging = ceil(PVR_peak x (1 + CHARGING_FRACTION))
    """
    up_tt, dn_tt = _avg_travel_time(ri)
    layover      = ri.config.preferred_layover_min
    cycle_time   = up_tt + dn_tt + 2 * layover

    peak_hw    = _peak_headway(ri)
    offpeak_hw = _offpeak_headway(ri)

    pvr_peak     = max(1, math.ceil(cycle_time / peak_hw))    if peak_hw    > 0 else 1
    pvr_offpeak  = max(1, math.ceil(cycle_time / offpeak_hw)) if offpeak_hw > 0 else 1
    pvr_charging = max(pvr_peak, math.ceil(pvr_peak * (1 + CHARGING_FRACTION)))

    return PVRSlices(
        route_code=ri.config.route_code,
        pvr_peak=pvr_peak,
        pvr_offpeak=pvr_offpeak,
        pvr_charging=pvr_charging,
        peak_headway_min=peak_hw,
        offpeak_headway_min=offpeak_hw,
        cycle_time_min=cycle_time,
    )


# Backward-compatible scalar API used throughout the rest of the codebase.

def compute_pvr(ri: RouteInput) -> int:
    """Return PVR_peak — primary scalar used throughout the scheduler."""
    return compute_pvr_slices(ri).pvr_peak


def compute_pvr_all(city: CityConfig) -> dict[str, int]:
    """PVR_peak per route. Returns {route_code: pvr_peak}."""
    return {code: compute_pvr(ri) for code, ri in city.routes.items()}


def compute_pvr_slices_all(city: CityConfig) -> dict[str, PVRSlices]:
    """Full PVR slice objects per route."""
    return {code: compute_pvr_slices(ri) for code, ri in city.routes.items()}


# ---------------------------------------------------------------------------
# Fleet balance
# ---------------------------------------------------------------------------

def compute_fleet_balance(city: CityConfig) -> dict[str, dict]:
    """
    Per-route balance table using PVR_peak as the floor.

    Returns {route_code: {pvr, pvr_offpeak, pvr_charging, allocated,
                           surplus, deficit, headroom_pct}}
    """
    slices = compute_pvr_slices_all(city)
    result = {}
    for code, ri in city.routes.items():
        s          = slices[code]
        allocated  = ri.config.fleet_size
        surplus    = max(0, allocated - s.pvr_peak)
        deficit    = max(0, s.pvr_peak - allocated)
        headroom   = ((allocated - s.pvr_peak) / s.pvr_peak * 100) if s.pvr_peak > 0 else 0.0
        result[code] = {
            "pvr":           s.pvr_peak,
            "pvr_offpeak":   s.pvr_offpeak,
            "pvr_charging":  s.pvr_charging,
            "allocated":     allocated,
            "surplus":       surplus,
            "deficit":       deficit,
            "headroom_pct":  round(headroom, 1),
        }
    return result


# ---------------------------------------------------------------------------
# Depot compatibility (Issue 3 fix)
# ---------------------------------------------------------------------------

def _same_depot(city: CityConfig, code_a: str, code_b: str) -> bool:
    """
    Return True only if both routes share the same depot.

    - Single-depot mode (city.depot_name != 'MULTI_DEPOT'): always True.
    - Multi-depot mode: compare config.depot strings per route.
    """
    if city.depot_name != "MULTI_DEPOT":
        return True
    depot_a = getattr(city.routes[code_a].config, "depot", None)
    depot_b = getattr(city.routes[code_b].config, "depot", None)
    if depot_a is None or depot_b is None:
        return True  # missing data -> optimistic fallback
    return str(depot_a).strip().lower() == str(depot_b).strip().lower()


# ---------------------------------------------------------------------------
# Deficit priority scoring
# ---------------------------------------------------------------------------

def _deficit_priority_score(ri: RouteInput, pvr: int, allocated: int) -> float:
    """
    Higher score -> route needs buses more urgently.

    Factors:
      1. Raw deficit (pvr - allocated) x10
      2. Headway tightness: 60 / peak_hw  (tighter = more critical)
      3. Operating window length (longer = more trips at risk)
    """
    deficit = max(0, pvr - allocated)
    if deficit == 0:
        return 0.0

    peak_hw    = _peak_headway(ri)
    hw_urgency = 60.0 / max(peak_hw, 5.0)

    try:
        start      = _to_minutes(ri.config.operating_start)
        end        = _to_minutes(ri.config.operating_end)
        window_hrs = (end - start) / 60
    except Exception:
        window_hrs = 15.0

    return deficit * 10.0 + hw_urgency * 2.0 + window_hrs * 0.5


# ---------------------------------------------------------------------------
# Post-rebalance stability check (Issue 4 fix)
# ---------------------------------------------------------------------------

@dataclass
class StabilityFlag:
    """Result of the post-rebalance PVR stability check for one route."""
    route_code:   str
    pvr_before:   int
    pvr_after:    int
    fleet_before: int
    fleet_after:  int
    drift:        float   # |pvr_after - pvr_before|
    is_stable:    bool    # True if drift <= STABILITY_THRESHOLD

    def as_dict(self) -> dict:
        return {
            "Route":        self.route_code,
            "PVR Before":   self.pvr_before,
            "PVR After":    self.pvr_after,
            "Fleet Before": self.fleet_before,
            "Fleet After":  self.fleet_after,
            "Drift":        round(self.drift, 2),
            "Status":       "✅ Stable" if self.is_stable else "⚠️ Drifted",
        }


def check_rebalance_stability(
    city: CityConfig,
    pre_balance: dict[str, dict],
    post_fleet:  dict[str, int],
    post_pvr:    dict[str, int],
) -> list[StabilityFlag]:
    """
    Compare pre- and post-rebalance PVR values to detect instability.

    Args:
        pre_balance : output of compute_fleet_balance() BEFORE rebalancing
        post_fleet  : {route_code: new_fleet_size} AFTER rebalancing
        post_pvr    : {route_code: pvr_peak} recomputed AFTER re-run

    Returns:
        List of StabilityFlag for all routes, sorted by descending drift.
        Unstable routes (drift > STABILITY_THRESHOLD) are flagged.
    """
    flags = []
    for code in city.routes:
        pvr_before   = pre_balance[code]["pvr"]
        pvr_after    = post_pvr.get(code, pvr_before)
        fleet_before = pre_balance[code]["allocated"]
        fleet_after  = post_fleet.get(code, fleet_before)
        drift        = abs(pvr_after - pvr_before)
        flags.append(StabilityFlag(
            route_code=code,
            pvr_before=pvr_before,
            pvr_after=pvr_after,
            fleet_before=fleet_before,
            fleet_after=fleet_after,
            drift=drift,
            is_stable=(drift <= STABILITY_THRESHOLD),
        ))
    return sorted(flags, key=lambda f: -f.drift)


# ---------------------------------------------------------------------------
# Rebalancing plan
# ---------------------------------------------------------------------------

def compute_rebalancing_plan(city: CityConfig) -> list[Transfer]:
    """
    Phase 1 rebalancing: whole-day bus transfers driven by PVR_peak.

    Steps:
      1. Compute surplus / deficit using PVR_peak as floor
      2. Rank deficit routes by urgency score
      3. For each deficit route, find a depot-compatible donor
      4. Greedy one-bus-at-a-time until no surplus or no deficit remains

    Returns list of Transfer objects.
    Does NOT modify city.routes — use apply_transfers() to get adjusted sizes.
    """
    balance   = compute_fleet_balance(city)
    transfers: list[Transfer] = []

    surplus_pool: dict[str, int] = {
        code: b["surplus"] for code, b in balance.items() if b["surplus"] > 0
    }
    if not surplus_pool:
        return []

    deficit_routes: list[tuple[str, float]] = []
    for code, b in balance.items():
        if b["deficit"] > 0:
            score = _deficit_priority_score(city.routes[code], b["pvr"], b["allocated"])
            deficit_routes.append((code, score))
    if not deficit_routes:
        return []

    deficit_routes.sort(key=lambda x: -x[1])

    running_alloc   = {code: b["allocated"]  for code, b in balance.items()}
    running_surplus = dict(surplus_pool)

    bus_counter = 0
    changed = True
    while changed:
        changed = False
        for deficit_code, _ in deficit_routes:
            pvr = balance[deficit_code]["pvr"]
            if running_alloc[deficit_code] >= pvr:
                continue

            for donor_code in list(running_surplus.keys()):
                if running_surplus[donor_code] <= 0:
                    continue
                # Depot constraint: only transfer within same depot cluster
                if not _same_depot(city, donor_code, deficit_code):
                    continue

                bus_counter += 1
                transfers.append(Transfer(
                    bus_id=f"TRANSFER-{bus_counter:03d}",
                    from_route=donor_code,
                    to_route=deficit_code,
                    reason="surplus_rebalance",
                ))
                running_surplus[donor_code]  -= 1
                running_alloc[donor_code]    -= 1
                running_alloc[deficit_code]  += 1
                changed = True
                break

    return transfers


def apply_transfers(city: CityConfig, transfers: list[Transfer]) -> dict[str, int]:
    """
    Compute adjusted fleet sizes after applying transfers.
    Returns {route_code: new_fleet_size}.  Does NOT mutate config objects.
    """
    adjusted = {code: ri.config.fleet_size for code, ri in city.routes.items()}
    for t in transfers:
        if t.from_route in adjusted:
            adjusted[t.from_route] -= 1
        if t.to_route in adjusted:
            adjusted[t.to_route] += 1
    for code in adjusted:
        adjusted[code] = max(1, adjusted[code])
    return adjusted
