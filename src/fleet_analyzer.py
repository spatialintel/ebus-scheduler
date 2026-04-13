"""
fleet_analyzer.py — Peak Vehicle Requirement (PVR) and fleet rebalancing.

PVR is the theoretical minimum buses needed to maintain a headway profile
on a route.  This version computes THREE time-sliced PVR values:

    PVR_peak      = ceil(cycle_time_peak / peak_headway)
                    cycle_time_peak uses travel times during the peak headway
                    window — NOT the day-average — for accuracy.

    PVR_offpeak   = ceil(cycle_time_offpeak / avg_offpeak_headway)
                    Uses travel times during 11:00-15:00.

    PVR_charging  = ceil(PVR_peak * (1 + charging_fraction))
                    charging_fraction derived from config physics:
                    avg_charge_time / charging_window_length.
                    No magic constant — linked to battery, SOC trigger,
                    charger capacity, and charging window.

Rebalancing algorithm:
    1. Compute PVR_peak per route (primary driver)
    2. Identify surplus (allocated > PVR_peak) and deficit routes
    3. Check depot compatibility before any transfer (same depot only)
    4. Greedy assignment ranked by urgency score
    5. Cap: each donor donates at most MAX_TRANSFER_FRACTION of its fleet
    6. Post-rebalance stability check: flag PVR drift > 0.5

Usage:
    from src.fleet_analyzer import compute_pvr_slices, compute_rebalancing_plan
"""

from __future__ import annotations
__version__ = "2026-04-09-p3"

import math
from dataclasses import dataclass
from datetime import datetime, time as _time

from src.city_models import CityConfig, RouteInput, Transfer


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

OFFPEAK_START_MIN: int  = 11 * 60   # 11:00
OFFPEAK_END_MIN: int    = 15 * 60   # 15:00
CHARGING_START_MIN: int = 12 * 60   # P5 midday charging window start
CHARGING_END_MIN: int   = 15 * 60   # P5 midday charging window end

# Maximum fraction of a route's fleet that may be donated per rebalancing pass.
# Prevents stripping a route to bare PVR with no operational buffer.
MAX_TRANSFER_FRACTION: float = 0.30

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


def _peak_headway_window(ri: RouteInput) -> tuple[float, float]:
    """
    Return (start_min, end_min) of the tightest headway band.
    Used to look up matching travel time for peak PVR calculation.
    Falls back to (0, 24*60) if headway_df has no time columns.
    """
    df = ri.headway_df
    try:
        min_hw = float(df["headway_min"].min())
        for _, row in df.iterrows():
            if float(row["headway_min"]) == min_hw:
                return _to_minutes(row["time_from"]), _to_minutes(row["time_to"])
    except Exception:
        pass
    return 0.0, 24 * 60


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


# ---------------------------------------------------------------------------
# Travel time extraction — Issue 1 fix: peak-window, not day average
# ---------------------------------------------------------------------------

def _travel_time_for_window(
    ri: RouteInput,
    win_start: float,
    win_end: float,
) -> tuple[float, float]:
    """
    Return (up_min, dn_min) weighted-average for bands overlapping [win_start, win_end).

    Fallback chain:
      1. Time-band overlap (requires time_from/time_to columns in travel_time_df)
      2. Max across all bands (conservative — better than average for PVR_peak)
      3. Simple mean (last resort)

    Replaces the old _avg_travel_time() which used day-level averages regardless
    of when peak demand actually occurs, underestimating PVR on congested routes.
    """
    df = ri.travel_time_df
    has_time_cols = "time_from" in df.columns and "time_to" in df.columns

    if has_time_cols:
        up_w, dn_w, weight = 0.0, 0.0, 0.0
        for _, row in df.iterrows():
            try:
                t_from  = _to_minutes(row["time_from"])
                t_to    = _to_minutes(row["time_to"])
                overlap = _band_overlap(t_from, t_to, win_start, win_end)
                if overlap <= 0:
                    continue
                weight += overlap
                up_w   += overlap * float(row["up_min"])
                dn_w   += overlap * float(row["dn_min"])
            except Exception:
                continue
        if weight > 0:
            return up_w / weight, dn_w / weight

    # Fallback 1: max travel time (conservative for PVR_peak)
    try:
        up_max = float(df["up_min"].max())
        dn_max = float(df["dn_min"].max())
        if up_max > 0 and dn_max > 0:
            return up_max, dn_max
    except Exception:
        pass

    # Fallback 2: simple mean
    up = float(df["up_min"].mean()) if "up_min" in df.columns else 45.0
    dn = float(df["dn_min"].mean()) if "dn_min" in df.columns else 45.0
    return up, dn


def _avg_travel_time(ri: RouteInput) -> tuple[float, float]:
    """Day-average travel times — retained for off-peak PVR fallback."""
    df = ri.travel_time_df
    up = float(df["up_min"].mean()) if "up_min" in df.columns else 45.0
    dn = float(df["dn_min"].mean()) if "dn_min" in df.columns else 45.0
    return up, dn


# ---------------------------------------------------------------------------
# Physics-based charging fraction — Issue 2 fix: replaces CHARGING_FRACTION=0.25
# ---------------------------------------------------------------------------

def _charging_fraction(ri: RouteInput) -> float:
    """
    Estimate fraction of fleet simultaneously unavailable due to P5 charging.

    Formula:
        energy_needed   = (target_soc - trigger_soc) / 100 * battery_kwh
        charge_time_min = energy_needed / (charger_kw * efficiency) * 60
        fraction        = charge_time_min / charging_window_min
                          clamped to [0.05, 0.50]

    Charging window is read from cfg.p5_charging_start / cfg.p5_charging_end
    (set per-route in the Excel config).  Falls back to the module-level
    CHARGING_START_MIN / CHARGING_END_MIN constants (12:00–15:00) if the
    config fields are absent or produce an invalid window.

    All inputs from config — no magic constants.
    Falls back to 0.25 if any required field is missing or zero.
    """
    try:
        cfg             = ri.config
        soc_delta       = max(0.0, cfg.target_soc_percent - cfg.trigger_soc_percent) / 100.0
        battery_kwh     = float(cfg.battery_kwh)
        charger_kw      = float(cfg.depot_charger_kw)
        efficiency      = float(getattr(cfg, "depot_charger_efficiency", 0.92))

        # ── Charging window from config (p5_charging_start / p5_charging_end) ──
        # Fall back to hardcoded constants only if config fields are absent.
        cs = getattr(cfg, 'p5_charging_start', None)
        ce = getattr(cfg, 'p5_charging_end',   None)
        if cs is not None and ce is not None:
            cs_min = cs.hour * 60 + cs.minute
            ce_min = ce.hour * 60 + ce.minute
            window_min = float(ce_min - cs_min)
        else:
            window_min = float(CHARGING_END_MIN - CHARGING_START_MIN)  # fallback: 12:00-15:00 = 180 min

        if charger_kw <= 0 or battery_kwh <= 0 or window_min <= 0:
            return 0.25

        energy_kwh      = soc_delta * battery_kwh
        charge_time_min = (energy_kwh / (charger_kw * efficiency)) * 60.0
        fraction        = charge_time_min / window_min

        return max(0.05, min(0.50, fraction))
    except Exception:
        return 0.25


# ---------------------------------------------------------------------------
# PVR slice dataclass
# ---------------------------------------------------------------------------

@dataclass
class PVRSlices:
    """Three time-sliced PVR values for a single route."""
    route_code: str
    pvr_peak: int
    pvr_offpeak: int
    pvr_charging: int
    peak_headway_min: float
    offpeak_headway_min: float
    cycle_time_peak_min: float      # uses peak-window travel time
    cycle_time_offpeak_min: float   # uses off-peak travel time
    charging_fraction: float        # physics-derived, shown for transparency

    @property
    def slack_buses(self) -> int:
        """Buses free during off-peak — shown to planners, deferred to Phase 2."""
        return max(0, self.pvr_peak - self.pvr_offpeak)

    def as_dict(self) -> dict:
        return {
            "Route":                 self.route_code,
            "PVR (Peak)":            self.pvr_peak,
            "PVR (Off-Peak)":        self.pvr_offpeak,
            "PVR (Charging)":        self.pvr_charging,
            "Peak HW (min)":         round(self.peak_headway_min, 1),
            "Off-Peak HW (min)":     round(self.offpeak_headway_min, 1),
            "Cycle Peak (min)":      round(self.cycle_time_peak_min, 1),
            "Cycle Off-Peak (min)":  round(self.cycle_time_offpeak_min, 1),
            "Charge Fraction":       f"{self.charging_fraction:.0%}",
            "Off-Peak Slack":        self.slack_buses,
        }


def compute_pvr_slices(ri: RouteInput) -> PVRSlices:
    """
    Compute all three PVR slices for one route.

    PVR_peak:
        peak_win          = time window of tightest headway band
        (peak_up, peak_dn) = travel times overlapping peak_win
        cycle_time_peak   = peak_up + peak_dn + 2 x layover
        PVR_peak          = ceil(cycle_time_peak / peak_headway)

    PVR_offpeak:
        (op_up, op_dn)     = travel times overlapping 11:00-15:00
        cycle_time_offpeak = op_up + op_dn + 2 x layover
        PVR_offpeak        = ceil(cycle_time_offpeak / offpeak_headway)

    PVR_charging:
        chg_frac           = physics-based charging fraction
        PVR_charging       = ceil(PVR_peak * (1 + chg_frac))
    """
    layover = ri.config.preferred_layover_min

    # Peak cycle time
    peak_win_start, peak_win_end = _peak_headway_window(ri)
    peak_up, peak_dn  = _travel_time_for_window(ri, peak_win_start, peak_win_end)
    cycle_time_peak   = peak_up + peak_dn + 2 * layover

    # Off-peak cycle time
    op_up, op_dn         = _travel_time_for_window(ri, OFFPEAK_START_MIN, OFFPEAK_END_MIN)
    cycle_time_offpeak   = op_up + op_dn + 2 * layover

    peak_hw    = _peak_headway(ri)
    offpeak_hw = _offpeak_headway(ri)
    chg_frac   = _charging_fraction(ri)

    pvr_peak     = max(1, math.ceil(cycle_time_peak    / peak_hw))    if peak_hw    > 0 else 1
    pvr_offpeak  = max(1, math.ceil(cycle_time_offpeak / offpeak_hw)) if offpeak_hw > 0 else 1
    pvr_charging = max(pvr_peak, math.ceil(pvr_peak * (1 + chg_frac)))

    return PVRSlices(
        route_code=ri.config.route_code,
        pvr_peak=pvr_peak,
        pvr_offpeak=pvr_offpeak,
        pvr_charging=pvr_charging,
        peak_headway_min=peak_hw,
        offpeak_headway_min=offpeak_hw,
        cycle_time_peak_min=cycle_time_peak,
        cycle_time_offpeak_min=cycle_time_offpeak,
        charging_fraction=chg_frac,
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
        s         = slices[code]
        allocated = ri.config.fleet_size
        surplus   = max(0, allocated - s.pvr_peak)
        deficit   = max(0, s.pvr_peak - allocated)
        headroom  = ((allocated - s.pvr_peak) / s.pvr_peak * 100) if s.pvr_peak > 0 else 0.0
        result[code] = {
            "pvr":          s.pvr_peak,
            "pvr_offpeak":  s.pvr_offpeak,
            "pvr_charging": s.pvr_charging,
            "allocated":    allocated,
            "surplus":      surplus,
            "deficit":      deficit,
            "headroom_pct": round(headroom, 1),
        }
    return result


# ---------------------------------------------------------------------------
# Depot compatibility
# ---------------------------------------------------------------------------

def _same_depot(city: CityConfig, code_a: str, code_b: str) -> bool:
    """
    Return True only if both routes share the same depot.
    Single-depot mode always returns True.
    Multi-depot mode compares config.depot strings.
    """
    if city.depot_name != "MULTI_DEPOT":
        return True
    depot_a = getattr(city.routes[code_a].config, "depot", None)
    depot_b = getattr(city.routes[code_b].config, "depot", None)
    if depot_a is None or depot_b is None:
        return True
    return str(depot_a).strip().lower() == str(depot_b).strip().lower()


# ---------------------------------------------------------------------------
# Deficit priority scoring
# ---------------------------------------------------------------------------

def _deficit_priority_score(ri: RouteInput, pvr: int, allocated: int) -> float:
    """
    Higher score -> route needs buses more urgently.
    Factors: raw deficit x10, headway tightness, operating window length.
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
# Post-rebalance stability check
# ---------------------------------------------------------------------------

@dataclass
class StabilityFlag:
    """Result of the post-rebalance PVR stability check for one route."""
    route_code:   str
    pvr_before:   int
    pvr_after:    int
    fleet_before: int
    fleet_after:  int
    drift:        float
    is_stable:    bool

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
    """Compare pre/post-rebalance PVR. Flag drift > STABILITY_THRESHOLD."""
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
# Rebalancing plan — Issue 5 fix: MAX_TRANSFER_FRACTION cap per donor
# ---------------------------------------------------------------------------

def compute_rebalancing_plan(city: CityConfig) -> list[Transfer]:
    """
    Phase 1 rebalancing: whole-day bus transfers driven by PVR_peak.

    Steps:
      1. Compute surplus / deficit using PVR_peak as floor
      2. Cap each donor: max donatable = floor(allocated * MAX_TRANSFER_FRACTION)
         Prevents a route from being stripped to bare PVR with no buffer.
      3. Rank deficit routes by urgency score
      4. For each deficit route, find a depot-compatible donor
      5. Greedy one-bus-at-a-time until no capped surplus or no deficit remains

    Returns list of Transfer objects.
    Does NOT modify city.routes — use apply_transfers() to get adjusted sizes.
    """
    balance   = compute_fleet_balance(city)
    transfers: list[Transfer] = []

    # Surplus pool capped at MAX_TRANSFER_FRACTION of each route's fleet
    surplus_pool: dict[str, int] = {}
    for code, b in balance.items():
        if b["surplus"] > 0:
            cap = max(0, math.floor(b["allocated"] * MAX_TRANSFER_FRACTION))
            surplus_pool[code] = min(b["surplus"], cap)

    if not any(v > 0 for v in surplus_pool.values()):
        return []

    deficit_routes: list[tuple[str, float]] = []
    for code, b in balance.items():
        if b["deficit"] > 0:
            score = _deficit_priority_score(city.routes[code], b["pvr"], b["allocated"])
            deficit_routes.append((code, score))
    if not deficit_routes:
        return []

    deficit_routes.sort(key=lambda x: -x[1])

    running_alloc   = {code: b["allocated"] for code, b in balance.items()}
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
    Returns {route_code: new_fleet_size}. Does NOT mutate config objects.
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
