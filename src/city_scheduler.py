"""
city_scheduler.py — Citywide multi-route scheduling orchestrator.

Three modes:

  Planning-Compliant (mode='planning'):
    1. Run each route with its config fleet_size + headway profile
    2. Time-sliced PVR -> detect surplus/deficit
    3. Transfer surplus buses to deficit routes (depot-compatible only)
    4. Re-run only affected routes with adjusted fleet_size
    5. Post-rebalance stability check
    6. Return CitySchedule

  Efficiency-Maximising (mode='efficiency'):
    1. Binary-search minimum fleet per route satisfying P1-P6 AND headway ceiling
    2. Distribute extras to worst-scoring routes
    3. Convergence loop: re-score and re-distribute until stable (max 2 passes)
    4. Return CitySchedule

  Service Maximization (mode='service_max'):
    1. Use fleet_size from config (no override, no rebalancing)
    2. Ignore configured headway profile
    3. Compute natural headway = ceil(max_cycle_time / fleet_size) per route
    4. Create flat single-band headway profile with constant natural headway
    5. Run scheduler with flat profile → even spacing at minimum achievable frequency
    6. Return CitySchedule

Usage:
    from src.city_scheduler import schedule_city

    result = schedule_city(city_config, mode='planning')   # default
    result = schedule_city(city_config, mode='efficiency')
    result = schedule_city(city_config, mode='service_max')
"""

from __future__ import annotations
__version__ = "2026-04-09-p4"

import math
from datetime import time as _time

import pandas as pd

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

# ── Phase 2+3 soft imports (missing files don't break existing functionality) ──
try:
    from src.recommender import generate_recommendations
    _HAS_RECOMMENDER = True
except ImportError:
    _HAS_RECOMMENDER = False

try:
    from src.depot_model import simulate_depot
    _HAS_DEPOT = True
except ImportError:
    _HAS_DEPOT = False

try:
    from src.network_analyzer import analyze_network
    _HAS_NETWORK = True
except ImportError:
    _HAS_NETWORK = False

try:
    from src.scenario_store import save_scenario
    _HAS_SCENARIO = True
except ImportError:
    _HAS_SCENARIO = False

# ---------------------------------------------------------------------------
# Tunable constants — change here to affect all modes globally
# ---------------------------------------------------------------------------

# Safety buffer added on top of the coverage formula result:
#   H_min = ceil((cycle + RT) / fleet) + SPIKE_SAFETY_BUFFER
# Increase if you still see occasional spikes at the theoretical minimum.
SPIKE_SAFETY_BUFFER: int = 3

# Maximum number of spikes (gaps > 2×H) allowed TOTAL (UP + DN combined) per day
# in non-peak hours before the binary search increments H.
# 1 = at most one charging gap across the entire day, either direction
#     (a single charging event creates one gap in each direction, so combined = 2 →
#      exceeds limit → H is forced higher until the gap falls below the threshold).
# 0 = zero tolerance — H climbs until no spike at all (strict, may push H very high).
MAX_SPIKES_ALLOWED: int = 1


# ---------------------------------------------------------------------------
# Service Maximization helpers
# ---------------------------------------------------------------------------

def _charging_rt(ri: RouteInput) -> float:
    """
    Estimate charging round-trip time (minutes) for this route.
    RT = travel_to_depot + charge_time + travel_back_to_nearest_node.
    Uses config values; falls back to conservative 60 min if any field is missing.
    """
    cfg = ri.config
    try:
        # Nearest node from depot = shortest travel time
        nodes = [cfg.start_point, cfg.end_point]
        nodes += [n.strip() for n in getattr(cfg, "intermediates", []) if n and n.strip()]
        min_tt = float("inf")
        for node in nodes:
            try:
                tt = cfg.get_travel_time(cfg.depot, node)
                min_tt = min(min_tt, tt)
            except (KeyError, Exception):
                pass
        nearest_tt = min_tt if min_tt < float("inf") else 30.0
    except Exception:
        nearest_tt = 30.0

    try:
        trig    = getattr(cfg, "trigger_soc_percent", 40)
        tgt     = getattr(cfg, "target_soc_percent",  90)
        batt    = getattr(cfg, "battery_kwh",         210)
        chkw    = getattr(cfg, "depot_charger_kw",    60)
        cheff   = getattr(cfg, "depot_charger_efficiency", 0.85)
        min_chg = getattr(cfg, "min_charge_duration_min", 15)
        kwh     = max(0, tgt - trig) / 100 * batt
        chg_min = max(min_chg, kwh / max(0.1, chkw * cheff) * 60)
    except Exception:
        chg_min = 20.0

    return nearest_tt * 2 + chg_min   # to depot + charge + back


def _detect_peak_windows(headway_df) -> list:
    """
    Detect peak time windows from the headway profile.
    Peak bands = rows where headway_min equals the minimum across all bands.
    Returns list of (start_hour_float, end_hour_float) tuples.

    Example: headway_df with 15 min at 08-11 and 16-20, 20 min elsewhere
    → returns [(8.0, 11.0), (16.0, 20.0)]

    Falls back to empty list (no peak windows excluded) if headway_df is empty.
    """
    try:
        if headway_df is None or len(headway_df) == 0:
            return []
        min_hw = float(headway_df["headway_min"].min())
        windows = []
        for _, row in headway_df.iterrows():
            if float(row["headway_min"]) == min_hw:
                try:
                    from datetime import datetime as _dt_p
                    tf = _dt_p.strptime(str(row["time_from"]).strip(), "%H:%M")
                    tt = _dt_p.strptime(str(row["time_to"]).strip(),   "%H:%M")
                    windows.append((tf.hour + tf.minute / 60,
                                    tt.hour + tt.minute / 60))
                except Exception:
                    pass
        return windows
    except Exception:
        return []


def _count_spikes(buses, headway_min: float, headway_df=None,
                  max_allowed: int = MAX_SPIKES_ALLOWED) -> dict:
    """
    Count spikes > 2×headway_min per direction, restricted to non-peak hours.

    Peak windows are derived from headway_df (bands with the minimum headway).
    Falls back to hardcoded 08:00–11:00 and 16:00–20:00 if headway_df is None.

    Returns {"UP": n, "DN": n, "exceeds_limit": bool}.

    exceeds_limit is True when (UP + DN) > max_allowed.
    A single charging event creates one gap in each direction (UP + DN = 2),
    so with max_allowed=1 the binary search increments H until the gap falls
    below the 2×H threshold, yielding zero combined spikes at convergence.
    """
    from datetime import datetime as _dt2
    REF = _dt2(2025, 1, 1)

    # Detect peak windows from headway profile
    if headway_df is not None:
        _peak_windows = _detect_peak_windows(headway_df)
    else:
        _peak_windows = [(8.0, 11.0), (16.0, 20.0)]   # fallback

    def _is_peak(t):
        if t is None:
            return False
        h = t.hour + t.minute / 60
        return any(s <= h < e for s, e in _peak_windows)

    threshold = headway_min * 2
    result    = {"UP": 0, "DN": 0}
    for direction in ("UP", "DN"):
        deps = sorted([
            t.actual_departure for b in buses for t in b.trips
            if t.trip_type == "Revenue" and t.direction == direction
            and t.actual_departure is not None
        ])
        for i in range(1, len(deps)):
            gap = (deps[i] - deps[i - 1]).total_seconds() / 60
            if gap > threshold and not _is_peak(deps[i - 1]):
                result[direction] += 1

    # A single charging event creates one gap in UP and one in DN simultaneously.
    # Counting per-direction would allow 1+1=2 spikes to pass undetected.
    # We therefore check the combined total so one physical event = one spike.
    result["exceeds_limit"] = (result["UP"] + result["DN"]) > max_allowed
    return result


def _natural_headway(ri: RouteInput) -> float:
    """
    Compute minimum constant headway that satisfies the spike-tolerance rule:
      (UP + DN) combined spikes ≤ MAX_SPIKES_ALLOWED per day, non-peak hours only.

    A single charging event creates exactly one gap in each direction, so
    combined = 2 for one event.  With MAX_SPIKES_ALLOWED=1 the search keeps
    climbing until 2×H exceeds the charging round-trip time → zero spikes.

    Algorithm:
      1. Start at physics minimum: H = ceil((max_cycle + charging_RT) / fleet) + buffer
      2. Run scheduler with flat H headway
      3. Count combined spikes (>2H, non-peak, UP+DN total)
      4. If combined > MAX_SPIKES_ALLOWED: H += 1 and retry (max 25 iterations)
      5. Return the first H where the combined spike count is within tolerance

    Falls back to ceil(max_cycle/fleet) if scheduler fails.
    """
    config = ri.config
    fleet  = max(1, config.fleet_size)

    # Compute max cycle time from travel time profile
    max_cycle = 0.0
    for _, row in ri.travel_time_df.iterrows():
        try:
            up  = float(row["up_min"])
            dn  = float(row["dn_min"])
            brk = config.preferred_layover_min
            cycle = up + dn + brk * 2
            max_cycle = max(max_cycle, cycle)
        except Exception:
            continue
    if max_cycle == 0:
        max_cycle = 2 * 50 + 2 * config.preferred_layover_min

    # Charging RT
    rt = _charging_rt(ri)

    # Physics minimum (coverage formula) + configurable safety buffer
    h_physics = math.ceil((max_cycle + rt) / fleet) + SPIKE_SAFETY_BUFFER
    h_start   = max(5, h_physics)

    # Binary search / increment: try H from h_start, increment until spike rule satisfied
    for h_try in range(h_start, h_start + 25):
        try:
            flat_df = _flat_headway_df(ri, float(h_try))
            trial_trips = generate_trips(config, flat_df, ri.travel_time_df,
                                         scheduling_mode="efficiency")
            trial_buses = schedule_buses(config, trial_trips,
                                         headway_df=flat_df,
                                         travel_time_df=ri.travel_time_df,
                                         scheduling_mode="efficiency")
            spikes = _count_spikes(trial_buses, float(h_try),
                                   headway_df=ri.headway_df,
                                   max_allowed=MAX_SPIKES_ALLOWED)
            if not spikes["exceeds_limit"]:
                return float(h_try)
        except Exception:
            continue

    # Fallback if all trials failed
    return max(5.0, math.ceil(max_cycle / fleet))


def _flat_headway_df(ri: RouteInput, headway_min: float) -> pd.DataFrame:
    """
    Create a single-band headway_df spanning the full operating window.
    Replaces the configured profile entirely.
    """
    return pd.DataFrame([{
        "time_from":   ri.config.operating_start.strftime("%H:%M"),
        "time_to":     ri.config.operating_end.strftime("%H:%M"),
        "headway_min": headway_min,
    }])


# ---------------------------------------------------------------------------
# Single-route runner
# ---------------------------------------------------------------------------

def _run_single_route(
    ri: RouteInput,
    fleet_override: int | None = None,
    headway_df_override: "pd.DataFrame | None" = None,
    scheduling_mode: str = "planning",
    rec_k: float = 1.0,
    rec_alpha: float = 0.15,
) -> RouteResult:
    """
    Schedule one route.
    fleet_override:      temporarily patches config.fleet_size before running.
    headway_df_override: replaces ri.headway_df for this run only.
    scheduling_mode:     forwarded to generate_trips() and schedule_buses().
    rec_k:               scaling factor for recommended headway (H_base = k × H_phys).
                         k=1.0 = minimum stable; k=1.1 = +10% margin.
    rec_alpha:           multiplicative off-peak spread.
                         H_offpeak = H_peak × (1 + alpha).
                         alpha=0.15 ≈ small difference; 0.30 = strong difference.
    Both rec_k and rec_alpha are stored on RouteResult for audit/UI display.
    """
    config         = ri.config
    original_fleet = config.fleet_size

    if fleet_override is not None and fleet_override != config.fleet_size:
        config.fleet_size = fleet_override

    headway_df = headway_df_override if headway_df_override is not None else ri.headway_df

    try:
        trips         = generate_trips(config, headway_df, ri.travel_time_df,
                                       scheduling_mode=scheduling_mode)
        revenue_count = len([t for t in trips if t.trip_type == "Revenue"])
        buses         = schedule_buses(
            config, trips,
            headway_df=headway_df,
            travel_time_df=ri.travel_time_df,
            scheduling_mode=scheduling_mode,
        )
        metrics = compute_metrics(
            config, buses,
            total_revenue_trips=revenue_count,
            headway_df=headway_df,
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

        # ── Per-band physics minimum + recommended headway profile ────────────
        # Uses time-of-day cycle variation: each band looks up its own travel
        # time from travel_time_df rather than using the global worst-case.
        _rt         = _charging_rt(ri)
        _fleet      = max(1, config.fleet_size)
        _min_break  = config.preferred_layover_min

        # Global worst-case for the scalar physics_min_headway (peak bands)
        _max_cycle = 0.0
        for _, _row in ri.travel_time_df.iterrows():
            try:
                _up = float(_row["up_min"]); _dn = float(_row["dn_min"])
                _max_cycle = max(_max_cycle, _up + _dn + _min_break * 2)
            except Exception:
                pass
        if _max_cycle == 0:
            _max_cycle = 2 * 50 + 2 * _min_break
        _h_phys   = math.ceil((_max_cycle + _rt) / _fleet) + SPIKE_SAFETY_BUFFER
        # Multiplicative formula: H_peak = k × H_phys, H_offpeak = H_peak × (1 + alpha)
        _rec_peak = math.ceil(rec_k * _h_phys)
        _rec_offp = math.ceil(_rec_peak * (1.0 + rec_alpha))

        # Identify peak bands (those with the minimum configured headway)
        _peak_windows = _detect_peak_windows(headway_df)

        # Per-band recommendations and feasibility
        _rec_profile  = []
        _feas_details = []
        _any_infeas   = False

        from datetime import datetime as _dt_band

        def _band_tt(time_from_str) -> float:
            """Fetch travel time for this band from travel_time_df."""
            try:
                t = _dt_band.strptime(str(time_from_str).strip(), "%H:%M")
            except Exception:
                return 50.0
            for _, _trow in ri.travel_time_df.iterrows():
                try:
                    tf = _dt_band.strptime(str(_trow["time_from"]).strip(), "%H:%M")
                    tt = _dt_band.strptime(str(_trow["time_to"]).strip(),   "%H:%M")
                    if tf <= t < tt:
                        return float(_trow.get("up_min", _trow.get("dn_min", 50)))
                except Exception:
                    continue
            return 50.0

        for _, _hw_row in headway_df.iterrows():
            try:
                _tf_str  = str(_hw_row["time_from"]).strip()
                _tt_str  = str(_hw_row["time_to"]).strip()
                _cfg_hw  = int(_hw_row["headway_min"])
            except Exception:
                continue
            # Time-of-day cycle for this band
            _band_travel  = _band_tt(_tf_str)
            _band_cycle   = _band_travel * 2 + _min_break * 2
            _band_h_phys  = math.ceil((_band_cycle + _rt) / _fleet) + SPIKE_SAFETY_BUFFER

            # Is this band a peak band?
            try:
                _bh = _dt_band.strptime(_tf_str, "%H:%M")
                _band_h_float = _bh.hour + _bh.minute / 60
                _is_peak = any(s <= _band_h_float < e for s, e in _peak_windows)
            except Exception:
                _is_peak = False

            _band_rec    = _rec_peak if _is_peak else _rec_offp
            _band_rec    = max(_band_rec, _band_h_phys)  # never below band's own minimum
            _infeasible  = _cfg_hw < _band_h_phys

            if _infeasible:
                _any_infeas = True

            _rec_profile.append({
                "time_from":   _tf_str,
                "time_to":     _tt_str,
                "headway_min": _band_rec,
                "is_peak":     _is_peak,
                "physics_min": _band_h_phys,
                "cfg_hw":      _cfg_hw,
            })
            _feas_details.append({
                "band":        f"{_tf_str}–{_tt_str}",
                "cfg_hw":      _cfg_hw,
                "physics_min": _band_h_phys,
                "rec":         _band_rec,
                "status":      "❌ INFEASIBLE" if _infeasible else "✅ OK",
            })

        _feas_status = "INFEASIBLE" if _any_infeas else "OK"

        # ── Spike counts (all modes) ──────────────────────────────────────────
        # Use the widest headway band (off-peak H) as the comparison threshold,
        # since spikes are only measured during non-peak windows.
        try:
            _h_spike = float(headway_df["headway_min"].max())
            _sc = _count_spikes(buses, _h_spike, headway_df=headway_df)
            _spike_up = _sc["UP"]
            _spike_dn = _sc["DN"]
        except Exception:
            _spike_up = _spike_dn = 0

        return RouteResult(
            route_code=config.route_code,
            config=config,
            headway_df=headway_df,
            travel_time_df=ri.travel_time_df,
            buses=buses,
            metrics=metrics,
            pvr=pvr_slices.pvr_peak,
            fleet_allocated=config.fleet_size,
            fleet_original=original_fleet,
            surplus=max(0, config.fleet_size - pvr_slices.pvr_peak),
            deficit=max(0, pvr_slices.pvr_peak - config.fleet_size),
            physics_min_headway=_h_phys,
            rec_peak_headway=_rec_peak,
            rec_offpeak_headway=_rec_offp,
            recommended_headway_profile=_rec_profile,
            headway_feasibility_status=_feas_status,
            headway_feasibility_details=_feas_details,
            headway_source="user",          # set to "recommended" or "scaled:kX.X" by UI
            headway_k=rec_k,
            headway_alpha=rec_alpha,
            spike_count_up=_spike_up,
            spike_count_dn=_spike_dn,
        )
    finally:
        config.fleet_size = original_fleet


# ---------------------------------------------------------------------------
# Mode: Planning-Compliant
# ---------------------------------------------------------------------------

def _schedule_headway_driven(city: CityConfig) -> CitySchedule:
    """Optimizer OFF — Planning-Compliant."""

    results: dict[str, RouteResult] = {}
    for code, ri in city.routes.items():
        results[code] = _run_single_route(ri, scheduling_mode="planning")

    pre_balance = compute_fleet_balance(city)
    transfers   = compute_rebalancing_plan(city)

    if not transfers:
        post_pvr   = {code: r.pvr for code, r in results.items()}
        post_fleet = {code: r.fleet_allocated for code, r in results.items()}
        stability  = check_rebalance_stability(city, pre_balance, post_fleet, post_pvr)
        return CitySchedule(city_config=city, results=results,
                            transfers=[], stability_flags=stability)

    adjusted_fleet = apply_transfers(city, transfers)
    for code, new_fleet in adjusted_fleet.items():
        if new_fleet != city.routes[code].config.fleet_size:
            ri = city.routes[code]
            results[code] = _run_single_route(ri, fleet_override=new_fleet,
                                              scheduling_mode="planning")
            results[code].fleet_allocated = new_fleet

    post_pvr   = {code: r.pvr for code, r in results.items()}
    stability  = check_rebalance_stability(city, pre_balance, adjusted_fleet, post_pvr)

    return CitySchedule(city_config=city, results=results,
                        transfers=transfers, stability_flags=stability)


# ---------------------------------------------------------------------------
# Mode: Efficiency-Maximising
# ---------------------------------------------------------------------------

def _find_min_fleet(ri: RouteInput, max_fleet: int = 20) -> int:
    """
    Binary search for minimum fleet satisfying ALL P1-P6 rules
    AND a headway ceiling of 3× the configured peak headway.

    The headway ceiling prevents the optimizer from declaring a schedule
    feasible that has unacceptably large service gaps even if all hard
    rules are technically met.
    """
    lo, hi = 1, max_fleet
    best   = max_fleet

    # Headway ceiling: max acceptable gap = 3 × peak configured headway
    try:
        peak_hw = float(ri.headway_df["headway_min"].min())
        max_acceptable_gap = peak_hw * 3
    except Exception:
        max_acceptable_gap = 180.0  # 3 hours fallback

    while lo <= hi:
        mid = (lo + hi) // 2
        try:
            result    = _run_single_route(ri, fleet_override=mid,
                                          scheduling_mode="efficiency")
            soc_ok    = result.metrics.min_soc_seen >= ri.config.min_soc_percent
            breaks_ok = result.metrics.negative_breaks == 0
            coverage  = (result.metrics.revenue_trips_assigned /
                         max(1, result.metrics.revenue_trips_total))
            trips_ok  = coverage >= 0.80
            # NEW: headway ceiling — reject schedules with unacceptably large gaps
            headway_ok = result.metrics.max_headway_gap_min <= max_acceptable_gap

            if soc_ok and breaks_ok and trips_ok and headway_ok:
                best = mid
                hi   = mid - 1
            else:
                lo   = mid + 1
        except Exception:
            lo = mid + 1

    return best


def _schedule_kpi_driven(city: CityConfig) -> CitySchedule:
    """
    Optimizer ON — Efficiency-Maximising with convergence loop.

    Pass 1: binary-search minimum fleet per route.
    Pass 2 (convergence): re-score with adjusted fleet, redistribute extras
            to routes whose scores worsened (max 1 extra pass).
    """

    # ── Pass 1: minimum fleet ────────────────────────────────────────────────
    min_fleets: dict[str, int] = {}
    for code, ri in city.routes.items():
        min_fleets[code] = _find_min_fleet(ri)

    total_min  = sum(min_fleets.values())
    user_total = city.total_fleet
    adjusted   = dict(min_fleets)
    transfers: list[Transfer] = []

    def _distribute_extras(extras: int, base_alloc: dict[str, int],
                            scores: dict[str, float]) -> tuple[dict, list[Transfer]]:
        """Distribute surplus buses to worst-scoring routes, return new alloc + transfers."""
        alloc = dict(base_alloc)
        new_tx: list[Transfer] = []
        ranked = sorted(scores, key=lambda c: -scores[c])
        ctr = len(transfers)
        while extras > 0 and ranked:
            for code in ranked:
                if extras <= 0:
                    break
                alloc[code] += 1
                extras -= 1
                ctr += 1
                new_tx.append(Transfer(
                    bus_id=f"EXTRA-{ctr:03d}",
                    from_route="POOL",
                    to_route=code,
                    reason="kpi_improvement",
                ))
        return alloc, new_tx

    if user_total > total_min:
        extras = user_total - total_min

        # Score routes at minimum fleet
        scores: dict[str, float] = {}
        for code, ri in city.routes.items():
            try:
                r = _run_single_route(ri, fleet_override=min_fleets[code],
                                      scheduling_mode="efficiency")
                scores[code] = r.metrics.weighted_score()
            except Exception:
                scores[code] = float("inf")

        adjusted, transfers = _distribute_extras(extras, adjusted, scores)

        # ── Pass 2: convergence ──────────────────────────────────────────────
        # Re-score with adjusted fleet; if any route's score worsened vs Pass 1,
        # redistribute up to 1 extra bus to it.
        rescore: dict[str, float] = {}
        for code, ri in city.routes.items():
            try:
                r = _run_single_route(ri, fleet_override=adjusted[code],
                                      scheduling_mode="efficiency")
                rescore[code] = r.metrics.weighted_score()
            except Exception:
                rescore[code] = scores.get(code, float("inf"))

        # Routes that got worse → redistribute 1 bus each if still extras
        extra_pool = sum(1 for code in city.routes
                         if rescore.get(code, 0) > scores.get(code, 0) * 1.05)
        if extra_pool > 0:
            adjusted, extra_tx = _distribute_extras(
                extra_pool,
                adjusted,
                {c: rescore[c] for c in rescore if rescore[c] > scores.get(c, 0) * 1.05},
            )
            transfers.extend(extra_tx)

    elif user_total > 0 and user_total < total_min:
        remaining       = max(0, user_total - len(city.routes))
        total_pvr_weight = sum(min_fleets.values())
        codes            = sorted(min_fleets, key=lambda c: -min_fleets[c])
        alloc_so_far     = 0
        adjusted         = {}
        for code in codes:
            share          = 1 + round(remaining * min_fleets[code] / max(1, total_pvr_weight))
            adjusted[code] = max(1, share)
            alloc_so_far  += adjusted[code]
        while alloc_so_far > user_total and alloc_so_far > len(city.routes):
            for code in reversed(codes):
                if adjusted[code] > 1 and alloc_so_far > user_total:
                    adjusted[code] -= 1
                    alloc_so_far   -= 1

    # ── Final run ────────────────────────────────────────────────────────────
    results: dict[str, RouteResult] = {}
    for code, ri in city.routes.items():
        fleet = adjusted.get(code, min_fleets.get(code, ri.config.fleet_size))
        results[code] = _run_single_route(ri, fleet_override=fleet,
                                          scheduling_mode="efficiency")
        results[code].fleet_allocated = fleet
        results[code].fleet_original  = ri.config.fleet_size

    return CitySchedule(city_config=city, results=results,
                        transfers=transfers, stability_flags=[])


# ---------------------------------------------------------------------------
# Mode: Service Maximization
# ---------------------------------------------------------------------------

def _schedule_service_maximization(city: CityConfig) -> CitySchedule:
    """
    Service Maximization — uses config fleet, ignores headway profile,
    targets constant minimum-achievable headway for even spacing.

    For each route:
      1. natural_hw = ceil(max_cycle_time / fleet_size)
      2. Create flat headway_df = {operating_start → operating_end: natural_hw}
      3. Run scheduler with flat headway_df
    """
    results: dict[str, RouteResult] = {}
    for code, ri in city.routes.items():
        nat_hw   = _natural_headway(ri)
        flat_df  = _flat_headway_df(ri, nat_hw)
        result   = _run_single_route(ri, headway_df_override=flat_df,
                                     scheduling_mode="efficiency")
        # Store the computed natural headway in the result for UI display
        result.headway_df = flat_df
        results[code] = result

    return CitySchedule(city_config=city, results=results,
                        transfers=[], stability_flags=[])


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def schedule_city(
    city: CityConfig,
    optimize: bool = False,   # kept for backward compat
    mode: str = "planning",   # "planning" | "efficiency" | "resource_optimization" | "service_max"
) -> CitySchedule:
    """
    Main entry point for citywide scheduling.

    Args:
        city:     CityConfig with all routes loaded
        optimize: legacy flag — True maps to mode='resource_optimization'
        mode:     'planning'              → Planning-Compliant (headway profile respected)
                  'resource_optimization' → Resource Optimization (minimum fleet, KPI-driven)
                  'efficiency'            → legacy alias for 'resource_optimization'
                  'service_max'           → Service Maximization (fixed fleet, constant headway)

    Returns:
        CitySchedule with per-route results + transfer records + stability flags
    """
    # Backward compat: old callers pass optimize=True
    if optimize and mode == "planning":
        mode = "resource_optimization"

    # Mode alias: 'resource_optimization' is the new canonical name; 'efficiency' still works
    if mode == "resource_optimization":
        mode = "efficiency"

    if mode == "efficiency":
        result = _schedule_kpi_driven(city)
    elif mode == "service_max":
        result = _schedule_service_maximization(city)
    else:
        result = _schedule_headway_driven(city)

    # Post-processing: depot model, recommendations, network analysis, scenario save
    _post_process(result, mode)
    return result


def _post_process(cs: CitySchedule, mode: str = "planning") -> None:
    """
    Run analysis modules on completed CitySchedule. Modifies cs in-place.
    All modules are optional — missing imports skip silently.
    """
    # Depot model
    if _HAS_DEPOT:
        try:
            for code, r in cs.results.items():
                slots = getattr(cs.city_config, "depot_charger_slots", 0) or 0
                r.depot_log = simulate_depot(r.buses, r.config, slots_slow=slots)
        except Exception:
            pass

    # Network/corridor analysis
    if _HAS_NETWORK:
        try:
            cs.corridors = analyze_network(cs)
        except Exception:
            cs.corridors = []

    # Recommender
    if _HAS_RECOMMENDER:
        try:
            cs.recommendations = generate_recommendations(cs)
        except Exception:
            pass

    # Auto-save scenario
    if _HAS_SCENARIO:
        try:
            save_scenario(cs, mode=mode)
        except Exception:
            pass


# ── Mode display names (used by dashboard for user-facing labels) ────────────

MODE_DISPLAY_NAMES: dict[str, str] = {
    "planning":                "Planning-Compliant",
    "efficiency":              "Resource Optimization",   # renamed for v9
    "resource_optimization":   "Resource Optimization",
    "service_max":             "Service Maximization",
}


def mode_display_name(mode: str) -> str:
    """Return user-facing display name for a scheduling mode."""
    return MODE_DISPLAY_NAMES.get(mode, mode)
