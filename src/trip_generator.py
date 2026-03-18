"""
trip_generator.py - Generates the trip pool for a route.

TWO-THREAD GENERATION:
  Buses start at nearest_node = end_point (ADHEWADA) after the morning dead run.
  So we generate two independent trip threads:

  DN thread: end_point → start_point, first at op_start
             (buses are at end_point, they serve these immediately)

  UP thread: start_point → end_point, first at op_start + dn_travel + min_break
             (buses arrive at start_point after serving first DN, then serve UP)

  This eliminates repositioning dead runs — buses naturally alternate DN→UP→DN→UP.

Headway controls service density. Actual departure times set by scheduler (P4-first).
latest_departure = op_end so buses are never rejected on window grounds.
"""
from __future__ import annotations
from datetime import datetime, timedelta, time
import pandas as pd
from src.models import Trip, RouteConfig

_REF_DATE = datetime(2025, 1, 1)
_OFF_PEAK_START = _REF_DATE.replace(hour=11, minute=0)
_OFF_PEAK_END   = _REF_DATE.replace(hour=16, minute=0)


def _nearest_node_for_buses(config):
    """
    The node where buses land after the morning dead run (nearest to depot).
    Used as the starting point for the DN revenue thread.
    Falls back to config.end_point if no distance data is available.
    """
    nodes = [config.start_point, config.end_point] + \
            [n.strip() for n in getattr(config, 'intermediates', []) if n and n.strip()]
    best_node, best_tt = config.end_point, float('inf')
    for node in nodes:
        try:
            tt = config.get_travel_time(config.depot, node)
            if tt < best_tt:
                best_tt, best_node = tt, node
        except (KeyError, AttributeError):
            continue
    return best_node


def _time_to_dt(t):
    return _REF_DATE.replace(hour=t.hour, minute=t.minute, second=0, microsecond=0)


def _hhmm_to_dt(hhmm):
    p = hhmm.strip().split(":")
    return _REF_DATE.replace(hour=int(p[0]), minute=int(p[1]), second=0, microsecond=0)


def _get_headway_at(departure, headway_df):
    for _, row in headway_df.iterrows():
        if _hhmm_to_dt(row["time_from"]) <= departure < _hhmm_to_dt(row["time_to"]):
            return int(row["headway_min"])
    return int(headway_df.iloc[-1]["headway_min"])


def _get_travel_time(departure, direction, travel_time_df):
    col = "up_min" if direction == "UP" else "dn_min"
    for _, row in travel_time_df.iterrows():
        if _hhmm_to_dt(row["time_from"]) <= departure < _hhmm_to_dt(row["time_to"]):
            return int(row[col])
    return int(travel_time_df.iloc[-1][col])


def _generate_revenue_trips(config, headway_df, travel_time_df):
    """
    Generate DN and UP trips as two independent threads.

    DN thread starts at op_start (buses are already at end_point).
    UP thread starts at op_start + first_dn_travel + min_break
      (time when a bus completing the first DN is ready at start_point).

    Both threads advance by the same headway profile independently.
    This ensures buses serve DN Revenue → UP Revenue → DN Revenue → ...
    without any repositioning dead runs between them.
    """
    op_start    = _time_to_dt(config.operating_start)
    op_end      = _time_to_dt(config.operating_end)
    shift_split = _time_to_dt(config.shift_split)
    start_loc   = config.start_point
    end_loc     = config.end_point
    min_break   = config.preferred_layover_min
    off_peak_extra = getattr(config, 'off_peak_layover_extra_min', 0)

    # ── Determine near/far terminals ──────────────────────────────────────────
    # Buses land at `dn_start_loc` (nearest terminal to depot) after morning dead run.
    # `far_loc` is whichever terminal is NOT nearest — the other end of the route.
    #
    # Example A — R4-A (ADHEWADA nearest):
    #   dn_start_loc = ADHEWADA GAM  (end_point)
    #   far_loc      = GANGAJALIA     (start_point)
    #   DN thread: ADHEWADA → GANGAJALIA   UP thread: GANGAJALIA → ADHEWADA
    #
    # Example B — R11 (GANGAJALIA nearest):
    #   dn_start_loc = GANGAJALIA     (start_point)
    #   far_loc      = SHIHOR          (end_point)
    #   DN thread: GANGAJALIA → SHIHOR     UP thread: SHIHOR → GANGAJALIA
    #
    # Without this distinction both threads would use the same location on both
    # ends whenever nearest_node == start_point, producing 0-km same-stop trips.
    dn_start_loc = _nearest_node_for_buses(config)
    far_loc      = end_loc if dn_start_loc == start_loc else start_loc

    # Distances for each direction
    try:
        near_to_far_dist = config.get_distance(dn_start_loc, far_loc)
    except KeyError:
        near_to_far_dist = config.get_distance(start_loc, end_loc)   # fallback

    try:
        far_to_near_dist = config.get_distance(far_loc, dn_start_loc)
    except KeyError:
        far_to_near_dist = near_to_far_dist   # symmetric fallback

    trips = []

    # ── Natural slot interval ─────────────────────────────────────────────────
    first_dn_travel = _get_travel_time(op_start, "DN", travel_time_df)
    first_up_travel = _get_travel_time(
        op_start + timedelta(minutes=first_dn_travel + min_break), "UP", travel_time_df)
    cycle_time  = first_dn_travel + min_break + first_up_travel + min_break
    natural_gap = cycle_time / max(1, config.fleet_size)

    def _slot_interval(dep):
        """
        Effective slot interval = max(user headway, natural fleet gap).
        During off-peak (11:00-16:00) expand by off_peak_extra so trip density
        drops and headways widen, matching the extended driver break.
        """
        base = max(_get_headway_at(dep, headway_df), natural_gap)
        if _OFF_PEAK_START <= dep < _OFF_PEAK_END:
            base += off_peak_extra
        return base

    # ── DN thread: dn_start_loc → far_loc ────────────────────────────────────
    # Buses arrive at dn_start_loc after the morning dead run and serve these
    # trips immediately. Direction label is always "DN" regardless of which
    # physical terminal is nearest — the scheduler uses locations, not labels.
    dn_time = op_start
    while dn_time < op_end:
        dn_travel  = _get_travel_time(dn_time, "DN", travel_time_df)
        dn_arrival = dn_time + timedelta(minutes=dn_travel)
        dn_shift   = 1 if dn_time < shift_split else 2
        trips.append(Trip(
            direction="DN", trip_type="Revenue",
            start_location=dn_start_loc, end_location=far_loc,
            earliest_departure=dn_time,
            latest_departure=op_end,
            travel_time_min=dn_travel, distance_km=near_to_far_dist,
            handover=(dn_time < shift_split <= dn_arrival),
            shift=dn_shift,
        ))
        dn_time += timedelta(minutes=_slot_interval(dn_time))

    # ── UP thread: far_loc → dn_start_loc ────────────────────────────────────
    # After completing the first DN trip buses are at far_loc.
    # UP trips return them to dn_start_loc, closing the cycle correctly
    # regardless of which terminal is nearer to the depot.
    up_time = op_start + timedelta(minutes=first_dn_travel + min_break)

    while up_time < op_end:
        up_travel  = _get_travel_time(up_time, "UP", travel_time_df)
        up_arrival = up_time + timedelta(minutes=up_travel)
        up_shift   = 1 if up_time < shift_split else 2
        trips.append(Trip(
            direction="UP", trip_type="Revenue",
            start_location=far_loc, end_location=dn_start_loc,
            earliest_departure=up_time,
            latest_departure=op_end,
            travel_time_min=up_travel, distance_km=far_to_near_dist,
            handover=(up_time < shift_split <= up_arrival),
            shift=up_shift,
        ))
        up_time += timedelta(minutes=_slot_interval(up_time))
    trips.sort(key=lambda t: t.earliest_departure)
    return trips


def _generate_dead_runs(config, fleet_size):
    """Placeholder pool entries — actual morning dead runs are built in scheduler Phase 1."""
    op_start       = _time_to_dt(config.operating_start)
    departure_time = op_start - timedelta(minutes=config.dead_run_buffer_min)
    try:
        dist = config.get_distance(config.depot, config.start_point)
    except KeyError:
        dist = 0
    trips = []
    for i in range(fleet_size):
        bd = departure_time + timedelta(minutes=i)
        trips.append(Trip(
            direction="DEPOT", trip_type="Dead",
            start_location=config.depot, end_location=config.start_point,
            earliest_departure=bd, latest_departure=bd,
            travel_time_min=config.dead_run_buffer_min, distance_km=dist, shift=1,
        ))
    return trips


def _generate_return_dead_runs(config):
    """Evening return pool: both endpoints → DEPOT (scheduler picks by bus location)."""
    op_end = _time_to_dt(config.operating_end)
    trips  = []
    for from_loc in [config.start_point, config.end_point]:
        try:
            dist = config.get_distance(from_loc, config.depot)
            tt   = config.get_travel_time(from_loc, config.depot)
        except KeyError:
            continue
        trips.append(Trip(
            direction="DEPOT", trip_type="Dead",
            start_location=from_loc, end_location=config.depot,
            earliest_departure=op_end, latest_departure=op_end,
            travel_time_min=tt, distance_km=dist, shift=2,
        ))
    return trips


def generate_trips(config, headway_df, travel_time_df):
    """
    Main entry point. Returns all trips sorted by earliest_departure.
    DN revenue trips start at op_start (buses are at end_point).
    UP revenue trips start after first DN completes + break.
    Actual departure times set by scheduler (bus-driven, P4-first).
    """
    all_trips = (
        _generate_dead_runs(config, config.fleet_size) +
        _generate_revenue_trips(config, headway_df, travel_time_df) +
        _generate_return_dead_runs(config)
    )
    all_trips.sort(key=lambda t: t.earliest_departure)
    return all_trips
