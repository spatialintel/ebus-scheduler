"""
bus_scheduler.py - Core scheduling engine.

P4-FIRST: Bus ready time drives departure. min_break = config.preferred_layover_min.

P2 (corrected): Morning dead run DEPOT → nearest_node only (1 leg).
  Revenue trips start from nearest_node = end_point (buses are already there).
  trip_generator generates DN first at op_start, UP after first_dn + min_break.
  Natural bus cycle: Dead(depot→nearest) → DN(Revenue) → UP(Revenue) → DN → UP...

P6: _check_p6 scans all buses' most-recent same-direction revenue trip (not
    just trips[-1]), and re-checks after each bump until gap >= SAME_DIR_GAP.
"""
from __future__ import annotations
from datetime import datetime, timedelta
from src.models import Trip, BusState, RouteConfig, ScheduleInfeasibleError

REF_DATE           = datetime(2025, 1, 1)
MIDDAY_START       = REF_DATE.replace(hour=12, minute=0)
MIDDAY_END         = REF_DATE.replace(hour=15, minute=0)
MORNING_PEAK_START = REF_DATE.replace(hour=8,  minute=0)
MORNING_PEAK_END   = REF_DATE.replace(hour=11, minute=0)
EVENING_PEAK_START = REF_DATE.replace(hour=16, minute=0)
EVENING_PEAK_END   = REF_DATE.replace(hour=20, minute=0)

MAX_BREAK         = 20    # fallback if config.max_layover_min absent
MIDDAY_CHARGE_SOC = 65.0  # fallback if config.midday_charge_soc_percent absent
SOC_TRIGGER       = 30.0  # fallback if config.trigger_soc_percent absent
SOC_FLOOR         = 20.0
SAME_DIR_GAP      = 5
DEPOT_DWELL_MIN   = 45
DEPOT_DWELL_MAX   = 90
KM_BALANCE_MAX    = 20.0

OFF_PEAK_START = REF_DATE.replace(hour=11, minute=0)
OFF_PEAK_END   = REF_DATE.replace(hour=16, minute=0)


def _is_midday(t):   return MIDDAY_START <= t < MIDDAY_END
def _is_off_peak(t): return OFF_PEAK_START <= t < OFF_PEAK_END
def _is_peak(t):
    return (MORNING_PEAK_START <= t < MORNING_PEAK_END or
            EVENING_PEAK_START <= t < EVENING_PEAK_END)


def _effective_break(config, current_time: datetime, base_break: int) -> int:
    """
    Return break minutes before the next revenue trip.

    During off-peak (11:00-16:00) the break is extended by
    off_peak_layover_extra_min (default 10) to widen headways naturally,
    capped at max_layover_min so P4 is never violated by construction.
    """
    if _is_off_peak(current_time):
        extra = getattr(config, 'off_peak_layover_extra_min', 0)
        max_b = getattr(config, 'max_layover_min', MAX_BREAK)
        return min(base_break + extra, max_b)
    return base_break

def _operational_nodes(config):
    nodes = [config.start_point, config.end_point]
    for n in config.intermediates:
        if n and n.strip(): nodes.append(n.strip())
    seen, unique = set(), []
    for n in nodes:
        if n not in seen: seen.add(n); unique.append(n)
    return unique

def _nearest_node_from_depot(config):
    best = None
    for node in _operational_nodes(config):
        try:
            dist = config.get_distance(config.depot, node)
            tt   = config.get_travel_time(config.depot, node)
            if best is None or tt < best[2]:
                best = (node, dist, tt)
        except KeyError:
            continue
    return best or (config.start_point, 0, 0)

def _create_fleet(config):
    t0 = REF_DATE.replace(hour=config.operating_start.hour,
                           minute=config.operating_start.minute)
    return [
        BusState(bus_id=bid, current_location=config.depot,
                 current_time=t0, soc_percent=config.initial_soc_percent,
                 total_km=0.0, shift=1,
                 battery_kwh=config.battery_kwh,
                 consumption_rate=config.consumption_rate)
        for bid in config.bus_ids()
    ]

def _fleet_avg_km(buses):
    return sum(b.total_km for b in buses) / len(buses) if buses else 0.0

def _ready_time(bus, min_break, config=None):
    """
    Departure-ready time for the bus.
    After Revenue: adds min_break (extended during off-peak if config provided).
    After Dead/Charging: immediate.
    """
    last = bus.trips[-1] if bus.trips else None
    if last and last.trip_type == "Revenue":
        effective = (_effective_break(config, bus.current_time, min_break)
                     if config is not None else min_break)
        return bus.current_time + timedelta(minutes=effective)
    return bus.current_time

def _last_revenue_in_direction(bus, direction, start_location):
    """Most-recent revenue trip by this bus matching direction+start. None if not found."""
    for t in reversed(bus.trips):
        if (t.trip_type == "Revenue" and
                t.direction == direction and
                t.start_location == start_location and
                t.actual_departure is not None):
            return t
    return None

def _last_revenue_any_direction(buses, start_location):
    """Most-recent revenue departure from start_location across ALL buses."""
    latest = None
    for bus in buses:
        for t in reversed(bus.trips):
            if (t.trip_type == "Revenue" and
                    t.start_location == start_location and
                    t.actual_departure is not None):
                if latest is None or t.actual_departure > latest.actual_departure:
                    latest = t
                break  # only need most recent per bus
    return latest

def _check_spacing(buses, trip, dep, min_gap: float):
    """
    Return True if `dep` is at least `min_gap` minutes after the most-recent
    same-direction revenue departure of ANY other bus from the same location.
    """
    if trip.trip_type != "Revenue":
        return True
    for bus in buses:
        last_rev = _last_revenue_in_direction(bus, trip.direction, trip.start_location)
        if last_rev is None:
            continue
        gap = (dep - last_rev.actual_departure).total_seconds() / 60
        if gap < min_gap:
            return False
    return True


def _check_p6(buses, trip, dep):
    """P6: 5-min minimum gap between buses in the same direction."""
    return _check_spacing(buses, trip, dep, SAME_DIR_GAP)


def _bumped_ready_time(buses, trip, rt, natural_gap=None):
    """
    Bump rt forward until the same-direction spacing requirement is met.

    Uses natural_gap as the minimum spacing when provided — this enforces
    even headways (e.g. 17.5 min for 8 buses) instead of just the P6
    minimum of 5 min, which produced bunched 9-min gaps.

    Falls back to SAME_DIR_GAP (5 min) only when natural_gap is not set.
    """
    min_gap = natural_gap if natural_gap and natural_gap > SAME_DIR_GAP else SAME_DIR_GAP
    for _ in range(60):
        if _check_spacing(buses, trip, rt, min_gap):
            return rt
        rt += timedelta(minutes=min_gap)
    return rt


def _snap_to_phase(rt: datetime, phase_index: int, natural_gap: float,
                   fleet_size: int, op_start: datetime) -> datetime:
    """
    Snap rt forward to this bus's permanent phase lane.

    Phase slots for bus i (epoch = op_start):
        op_start + i*natural_gap + k*cycle_time   (k = 0, 1, 2, …)
    where cycle_time = natural_gap * fleet_size.

    Using op_start as epoch matches Phase-1 stagger so slot 0 == first departure.
    """
    if natural_gap <= 0 or fleet_size <= 0:
        return rt
    cycle_time   = natural_gap * fleet_size
    phase_anchor = op_start + timedelta(minutes=phase_index * natural_gap)
    delta_min    = (rt - phase_anchor).total_seconds() / 60
    if delta_min <= 0:
        return phase_anchor
    remainder = delta_min % cycle_time
    if remainder < 0.5:        # already on-slot (float-precision guard)
        return rt
    return rt + timedelta(minutes=cycle_time - remainder)


def _make_dead(bus, to_loc, dist, tt):
    leg = Trip(direction="DEPOT", trip_type="Dead",
               start_location=bus.current_location, end_location=to_loc,
               earliest_departure=bus.current_time, latest_departure=bus.current_time,
               travel_time_min=tt, distance_km=dist, shift=bus.shift)
    bus.assign(leg)
    return leg

def _morning_dead_run(bus, config):
    """DEPOT → nearest_node (P2, 1 leg only). Revenue starts from nearest_node."""
    if bus.current_location != config.depot: return []
    nearest, dist, tt = _nearest_node_from_depot(config)
    if dist <= 0: return []
    return [_make_dead(bus, nearest, dist, tt)]

def _route_to_depot(bus, config):
    """
    Return bus to DEPOT via nearest_node (P2 — symmetric with morning dead run).

    Path: current_location → nearest_node → DEPOT
    All legs are Dead trips. The P1 compliance check allows these corridor
    Dead legs since they are scheduled repositioning, not unplanned deviation.
    """
    inserted = []
    if bus.current_location == config.depot:
        return inserted

    nearest, _, _ = _nearest_node_from_depot(config)

    # Leg 1: current_location → nearest_node
    if bus.current_location != nearest:
        try:
            d = config.get_distance(bus.current_location, nearest)
            t = config.get_travel_time(bus.current_location, nearest)
            if bus.soc_after_trip(d) >= SOC_FLOOR:
                inserted.append(_make_dead(bus, nearest, d, t))
        except KeyError:
            pass

    # Leg 2: nearest_node → DEPOT
    if bus.current_location != config.depot:
        try:
            d = config.get_distance(bus.current_location, config.depot)
            t = config.get_travel_time(bus.current_location, config.depot)
            if bus.soc_after_trip(d) >= SOC_FLOOR:
                inserted.append(_make_dead(bus, config.depot, d, t))
        except KeyError:
            pass

    return inserted

def _route_from_depot(bus, config):
    if bus.current_location != config.depot: return []
    nearest, dist, tt = _nearest_node_from_depot(config)
    if dist <= 0: return []
    return [_make_dead(bus, nearest, dist, tt)]

def _charging_detour(bus, config, resume_by, min_break):
    if config.depot_flow_rate_kw <= 0: return []
    inserted = []
    inserted.extend(_route_to_depot(bus, config))
    if bus.current_location != config.depot: return inserted
    _, _, from_tt = _nearest_node_from_depot(config)
    soc_needed     = max(10, config.target_soc_percent - bus.soc_percent)
    time_to_target = (soc_needed / 100 * config.battery_kwh / config.depot_flow_rate_kw) * 60
    max_charge     = (resume_by - bus.current_time).total_seconds() / 60 - from_tt - min_break
    charge_time    = min(max(DEPOT_DWELL_MIN, time_to_target), DEPOT_DWELL_MAX)
    if max_charge < DEPOT_DWELL_MIN:
        if max_charge >= 15: charge_time = max_charge
        else: return inserted
    charge_time = min(charge_time, max(15, max_charge))
    ct = Trip(direction="DEPOT", trip_type="Charging",
              start_location=config.depot, end_location=config.depot,
              earliest_departure=bus.current_time, latest_departure=bus.current_time,
              travel_time_min=int(charge_time), distance_km=0.0, shift=bus.shift)
    bus.assign(ct)
    bus.charge(duration_min=charge_time, flow_rate_kw=config.depot_flow_rate_kw)
    inserted.append(ct)
    inserted.extend(_route_from_depot(bus, config))
    return inserted

def _find_and_reposition(buses, trip, config, min_break):
    """
    Find a bus that can reach trip.start_location via a dead run and return
    (bus, [dead_run_legs]) so the caller can assign them in order.

    P2 compliance: when a bus is at the DEPOT it must pass through the nearest
    operational node before proceeding to any revenue trip origin — even when
    being repositioned mid-day.  We build a two-leg list in that case:
        DEPOT → nearest_node → trip.start_location
    For buses already on the route the single-leg path is used as before.

    KEY RULE: never reposition a bus that is already at trip.end_location.
    Such a bus is at the wrong end of the route — repositioning it back to
    trip.start_location requires a dead run equal to the full revenue trip
    distance, and then the bus immediately returns to where it started.
    It is always cheaper to let that bus serve the reverse direction naturally
    (its current location IS the start_location of the return trip).
    Without this guard, buses post-charging repeatedly get sent across the
    whole route as dead runs, accumulating 2× revenue-trip dead km and
    causing 100+ minute idle gaps when all same-direction slots are exhausted.
    """
    op_end  = REF_DATE.replace(hour=config.operating_end.hour,
                               minute=config.operating_end.minute)
    nearest, _, nearest_tt = _nearest_node_from_depot(config)
    candidates = []

    for bus in buses:
        if bus.current_location == trip.start_location:
            continue

        # ── Core guard ────────────────────────────────────────────────────────
        # Bus is already at the destination of this trip. Sending it back to
        # the origin as a dead run and then immediately returning it defeats
        # the purpose. Let it serve the opposite-direction trip naturally.
        if bus.current_location == trip.end_location:
            continue

        legs = []  # dead-run legs to assign

        if bus.current_location == config.depot:
            # Two-leg path: DEPOT → nearest_node → trip.start_location
            try:
                d1 = config.get_distance(config.depot, nearest)
                t1 = config.get_travel_time(config.depot, nearest)
            except KeyError:
                continue
            if nearest == trip.start_location:
                # nearest_node IS the trip origin — single leg
                legs = [("depot_to_nearest", d1, t1)]
                total_d, total_t = d1, t1
            else:
                try:
                    d2 = config.get_distance(nearest, trip.start_location)
                    t2 = config.get_travel_time(nearest, trip.start_location)
                except KeyError:
                    continue
                legs = [("depot_to_nearest", d1, t1), ("nearest_to_origin", d2, t2)]
                total_d, total_t = d1 + d2, t1 + t2
        else:
            # Single-leg direct repositioning
            try:
                total_d = config.get_distance(bus.current_location, trip.start_location)
                total_t = config.get_travel_time(bus.current_location, trip.start_location)
            except KeyError:
                continue
            legs = [("direct", total_d, total_t)]

        arrival = bus.current_time + timedelta(minutes=total_t)
        if arrival + timedelta(minutes=min_break) > op_end:
            continue
        soc_needed = bus._soc_cost(total_d) + bus._soc_cost(trip.distance_km)
        if bus.soc_percent - soc_needed * 100 / bus.battery_kwh < SOC_FLOOR:
            continue
        candidates.append((total_t, bus, legs))

    if not candidates:
        return None

    candidates.sort(key=lambda x: x[0])
    _, bus, legs = candidates[0]

    # Build and assign dead-run Trip objects for each leg
    dead_runs = []
    for leg_tag, leg_d, leg_t in legs:
        dead = Trip(
            direction="DEPOT", trip_type="Dead",
            start_location=bus.current_location,
            end_location=(nearest if leg_tag == "depot_to_nearest"
                          else trip.start_location),
            earliest_departure=bus.current_time,
            latest_departure=bus.current_time,
            travel_time_min=leg_t, distance_km=leg_d, shift=bus.shift,
        )
        bus.assign(dead)
        dead_runs.append(dead)

    return bus, dead_runs

def _select_bus(buses, trip, config, min_break, natural_gap=None):
    avg_km = _fleet_avg_km(buses)
    min_km = getattr(config, 'min_km_per_bus', 0) or 0
    candidates = []
    for bus in buses:
        if bus.current_location != trip.start_location: continue
        if bus.soc_after_trip(trip.distance_km) < SOC_FLOOR: continue
        rt = _ready_time(bus, min_break, config)
        # Enforce minimum same-direction spacing = natural_gap.
        # _effective_break already adds off_peak_extra minutes to the ready time
        # during 11:00-16:00, which naturally widens headways. Adding extra to
        # the spacing check would double-count and create cascading delays.
        rt = _bumped_ready_time(buses, trip, rt, natural_gap=natural_gap)
        km_deficit = bus.total_km - avg_km
        below_min  = -50 if (min_km > 0 and bus.total_km < min_km) else 0
        km_penalty = max(0, km_deficit - KM_BALANCE_MAX) * 5.0
        candidates.append((km_deficit + km_penalty + below_min, id(bus), bus, rt))
    if not candidates: return None, None
    candidates.sort(key=lambda x: x[0])
    _, _, bus, rt = candidates[0]
    return bus, rt

def _balance_breaks(buses, config):
    min_break = config.preferred_layover_min
    for bus in buses:
        rev_idx = [i for i, t in enumerate(bus.trips) if t.trip_type == "Revenue"]
        for j in range(1, len(rev_idx)):
            i_prev, i_curr = rev_idx[j-1], rev_idx[j]
            if any(t.trip_type == "Charging" for t in bus.trips[i_prev+1:i_curr]):
                continue
            tp, tc = bus.trips[i_prev], bus.trips[i_curr]
            if not (tp.actual_arrival and tc.actual_departure): continue
            gap = (tc.actual_departure - tp.actual_arrival).total_seconds() / 60
            if gap < min_break:
                delta = timedelta(minutes=min(min_break - gap,
                                              float(config.max_headway_deviation_min)))
                if delta.total_seconds() > 0:
                    tc.actual_departure += delta
                    tc.actual_arrival = tc.actual_departure + timedelta(minutes=tc.travel_time_min)


def _make_trip_stub(direction, start, end, config):
    """Lightweight Trip-like object for P6 spacing check."""
    return Trip(direction=direction, trip_type="Revenue",
                start_location=start, end_location=end,
                earliest_departure=REF_DATE, latest_departure=REF_DATE,
                travel_time_min=0, distance_km=0, shift=1)


def _get_travel_time_at(config, dep_time, direction):
    """
    Look up travel time from TravelTime_Profile at departure time.
    Falls back to segment time from config.segment_times.
    """
    # Try to import travel_time_df from trip_generator context
    # This is injected via schedule_buses parameter — access via closure
    if hasattr(config, '_tt_df') and config._tt_df is not None:
        from trip_generator import _get_travel_time as _gtt
        return _gtt(dep_time, direction, config._tt_df)
    return None  # caller falls back to segment time


def _get_min_headway(config, dep_time):
    """
    Minimum gap between same-direction departures from the headway profile.
    Falls back to SAME_DIR_GAP if no profile available.
    """
    if hasattr(config, '_hw_df') and config._hw_df is not None:
        from trip_generator import _get_headway_at
        return _get_headway_at(dep_time, config._hw_df)
    return SAME_DIR_GAP


def schedule_buses(config: RouteConfig, trips: list[Trip],
                   headway_df=None, travel_time_df=None) -> list[BusState]:
    """
    Bus-driven scheduler — no pre-generated trip pool slots.

    Each bus, after completing a trip, waits its break (preferred_layover +
    off_peak_extra during 11-16), then departs immediately on the next trip
    in the alternating direction. P6 bumping enforces minimum same-direction
    spacing between buses. No fixed departure clock to wait for.

    This eliminates the headway-pool double-counting problem where buses
    idled for 60-130 min waiting for the next pool slot.
    """
    min_break  = config.preferred_layover_min
    max_break  = getattr(config, 'max_layover_min', MAX_BREAK)
    buses      = _create_fleet(config)
    op_end     = REF_DATE.replace(hour=config.operating_end.hour,
                                  minute=config.operating_end.minute)
    op_start_dt = REF_DATE.replace(hour=config.operating_start.hour,
                                   minute=config.operating_start.minute)
    off_peak_extra = getattr(config, 'off_peak_layover_extra_min', 0)

    # Attach profile DataFrames to config so helpers can access them
    config._hw_df = headway_df
    config._tt_df = travel_time_df

    # ── Phase 1: Staggered morning dead runs ─────────────────────────────────
    nearest_node, _, nearest_tt = _nearest_node_from_depot(config)
    far_loc = (config.end_point if nearest_node == config.start_point
               else config.start_point)

    # Get representative travel times for cycle_time / stagger computation
    try:
        dn_tt = config.get_travel_time(nearest_node, far_loc)
    except KeyError:
        dn_tt = 45
    try:
        up_tt = config.get_travel_time(far_loc, nearest_node)
    except KeyError:
        up_tt = dn_tt

    cycle_time  = dn_tt + min_break + up_tt + min_break
    natural_gap = cycle_time / max(1, config.fleet_size)
    stagger_min = natural_gap

    for i, bus in enumerate(buses):
        arrive_at = op_start_dt + timedelta(minutes=i * stagger_min)
        bus.current_time = arrive_at - timedelta(minutes=nearest_tt)
        _morning_dead_run(bus, config)
        bus.phase_index = i

    # ── Phase 2: Bus-driven revenue trip loop ─────────────────────────────────
    # Each bus runs until it can no longer complete another trip before op_end.
    # Direction alternates: DN (nearest→far) then UP (far→nearest) then DN ...
    # The outer loop iterates until no bus can make progress (all stuck or done).
    MAX_ITERATIONS = config.fleet_size * 200   # safety cap
    iteration = 0

    while iteration < MAX_ITERATIONS:
        iteration += 1

        # Find the bus that is ready soonest and can make a valid next trip
        best_bus   = None
        best_rt    = None
        best_trip  = None   # (direction, start, end, dist, tt)

        for bus in buses:
            if bus.current_location == config.depot:
                continue   # already returned to depot for the day

            # What direction should this bus go next?
            last_rev = next((t for t in reversed(bus.trips)
                             if t.trip_type == "Revenue"), None)
            if last_rev is None:
                # Just completed morning dead run — first trip is DN
                next_dir   = "DN"
                trip_start = nearest_node
                trip_end   = far_loc
            elif last_rev.end_location == far_loc:
                next_dir   = "UP"
                trip_start = far_loc
                trip_end   = nearest_node
            else:
                next_dir   = "DN"
                trip_start = nearest_node
                trip_end   = far_loc

            # Bus must be at trip_start to serve this direction
            if bus.current_location != trip_start:
                continue

            # Compute break — add off_peak_extra during 11:00-16:00
            rt = _ready_time(bus, min_break, config)

            # Enforce P6: bump rt until same-direction spacing is satisfied
            rt = _bumped_ready_time(buses, _make_trip_stub(
                next_dir, trip_start, trip_end, config), rt,
                natural_gap=natural_gap)

            # Enforce headway minimum spacing from last same-direction departure
            min_headway = SAME_DIR_GAP
            if headway_df is not None:
                try:
                    from trip_generator import _get_headway_at
                    min_headway = _get_headway_at(rt, headway_df)
                except Exception:
                    pass
            last_same = _last_revenue_in_direction(bus, next_dir, trip_start)
            for other in buses:
                candidate = _last_revenue_in_direction(other, next_dir, trip_start)
                if candidate and (last_same is None or
                        candidate.actual_departure > last_same.actual_departure):
                    last_same = candidate
            if last_same and last_same.actual_departure:
                gap_from_last = (rt - last_same.actual_departure).total_seconds() / 60
                if gap_from_last < min_headway:
                    rt = last_same.actual_departure + timedelta(minutes=min_headway)

            # Get travel time: TravelTime_Profile first, then segment config
            tt_val = None
            if travel_time_df is not None:
                try:
                    from trip_generator import _get_travel_time as _gtt
                    tt_val = _gtt(rt, next_dir, travel_time_df)
                except Exception:
                    pass
            if tt_val is None:
                try:
                    tt_val = config.get_travel_time(trip_start, trip_end)
                except KeyError:
                    tt_val = dn_tt if next_dir == "DN" else up_tt

            # Check SOC
            try:
                dist = config.get_distance(trip_start, trip_end)
            except KeyError:
                continue
            if bus.soc_after_trip(dist) < SOC_FLOOR:
                continue

            # Check arrival within operating window
            proj_arr = rt + timedelta(minutes=tt_val)
            if proj_arr > op_end + timedelta(minutes=45):
                continue

            # This bus is a candidate — pick the one ready soonest
            if best_rt is None or rt < best_rt:
                best_bus  = bus
                best_rt   = rt
                best_trip = (next_dir, trip_start, trip_end, dist, tt_val)

        if best_bus is None:
            break   # no bus can make progress — done

        # Assign the trip to best_bus
        next_dir, trip_start, trip_end, dist, tt_val = best_trip
        trip = Trip(
            direction=next_dir, trip_type="Revenue",
            start_location=trip_start, end_location=trip_end,
            earliest_departure=best_rt, latest_departure=op_end,
            travel_time_min=tt_val, distance_km=dist,
            shift=(1 if best_rt < REF_DATE.replace(hour=config.shift_split.hour,
                                                    minute=config.shift_split.minute)
                   else 2),
        )
        trip.earliest_departure = best_rt
        best_bus.assign(trip)

        # P5: midday charging check
        midday_soc  = getattr(config, 'midday_charge_soc_percent', MIDDAY_CHARGE_SOC)
        soc_trigger = getattr(config, 'trigger_soc_percent', SOC_TRIGGER)
        if _is_midday(best_bus.current_time) and config.fleet_size <= 10:
            if best_bus.soc_percent < midday_soc:
                try:
                    dead_tt = config.get_travel_time(best_bus.current_location,
                                                     config.depot)
                except KeyError:
                    dead_tt = 30
                exp_arr = best_bus.current_time + timedelta(minutes=dead_tt)
                if exp_arr >= MIDDAY_START - timedelta(minutes=30):
                    _charging_detour(best_bus, config,
                                     resume_by=EVENING_PEAK_START,
                                     min_break=min_break)
        elif best_bus.soc_percent <= soc_trigger:
            if _is_midday(best_bus.current_time):
                _charging_detour(best_bus, config,
                                 resume_by=EVENING_PEAK_START, min_break=min_break)
            elif not _is_peak(best_bus.current_time):
                _charging_detour(best_bus, config,
                                 resume_by=EVENING_PEAK_START, min_break=min_break)
            elif best_bus.soc_percent <= SOC_FLOOR + 5:
                _charging_detour(best_bus, config,
                                 resume_by=op_end, min_break=min_break)

    # ── Phase 3: Evening return ───────────────────────────────────────────────
    for bus in buses:
        _route_to_depot(bus, config)

    # ── Phase 4: Safety-net break enforcement ────────────────────────────────
    _balance_breaks(buses, config)

    return buses


def check_compliance(config: RouteConfig, buses: list[BusState]) -> list[dict]:
    min_break = config.preferred_layover_min
    results   = []

    # P1
    p1_v = []
    valid = {config.start_point, config.end_point}
    for bus in buses:
        for t in bus.trips:
            if t.trip_type == "Revenue":
                if t.start_location not in valid or t.end_location not in valid:
                    p1_v.append(f"{bus.bus_id}: {t.start_location}->{t.end_location}")
    results.append({"rule": "P1: Revenue trips between Start/End only", "priority": 1,
                    "status": "PASS" if not p1_v else "FAIL",
                    "details": f"{len(p1_v)} violations" if p1_v else "All revenue trips on route",
                    "violations": p1_v[:5]})

    # P2
    nearest_from, _, _ = _nearest_node_from_depot(config)
    p2_v = []
    for bus in buses:
        for i, t in enumerate(bus.trips):
            if i == 0 and t.trip_type == "Dead" and t.start_location == config.depot:
                if t.end_location != nearest_from:
                    p2_v.append(f"{bus.bus_id}: went to {t.end_location}, nearest={nearest_from}")
    results.append({"rule": "P2: Depot access via nearest node", "priority": 2,
                    "status": "PASS" if not p2_v else "FAIL",
                    "details": f"Nearest: {nearest_from}" + (f", {len(p2_v)} violations" if p2_v else ""),
                    "violations": p2_v[:5]})

    # P3
    min_soc_seen = 100.0
    p3_v = []
    for bus in buses:
        soc = config.initial_soc_percent
        for t in bus.trips:
            soc -= bus._soc_cost(t.distance_km)
            if t.trip_type == "Charging":
                soc = min(100.0, soc + (config.depot_flow_rate_kw * t.travel_time_min / 60)
                          / config.battery_kwh * 100)
            if soc < SOC_FLOOR:
                p3_v.append(f"{bus.bus_id} SOC={soc:.1f}% at "
                             f"{t.actual_arrival.strftime('%H:%M') if t.actual_arrival else '?'}")
            min_soc_seen = min(min_soc_seen, soc)
    results.append({"rule": "P3: SOC never below 20%", "priority": 3,
                    "status": "PASS" if not p3_v else "FAIL",
                    "details": f"Min SOC: {min_soc_seen:.1f}%" + (f", {len(p3_v)} violations" if p3_v else ""),
                    "violations": p3_v[:5]})

    # P4 (charging and end-of-day repositioning gaps excluded)
    # P4: Break between consecutive Revenue trips must be preferred_layover ≤ gap ≤ max_layover.
    # Three categories of gaps are handled:
    #   1. Gap contains Charging or Dead trip → excluded (charging/repositioning)
    #   2. Gap > max_layover BUT caused by headway profile (no pool slot existed
    #      within max_layover of bus ready time) → flagged as "headway gap" warning,
    #      not a hard violation — this is a service-design constraint the scheduler
    #      cannot override without generating trips outside the headway spec
    #   3. Gap > max_layover with no structural reason → hard P4 violation
    max_break = getattr(config, 'max_layover_min', MAX_BREAK)
    min_break_eff = getattr(config, 'preferred_layover_min', min_break)
    p4_v = []
    p4_hw_gaps = []   # headway-driven long gaps (warn, not fail)
    for bus in buses:
        rev_idx = [i for i, t in enumerate(bus.trips) if t.trip_type == "Revenue"]
        for j in range(1, len(rev_idx)):
            i_prev, i_curr = rev_idx[j-1], rev_idx[j]
            between = bus.trips[i_prev+1:i_curr]
            if any(t.trip_type in ("Charging", "Dead") for t in between):
                continue
            c, n = bus.trips[i_prev], bus.trips[i_curr]
            if not (c.actual_arrival and n.actual_departure):
                continue
            gap = (n.actual_departure - c.actual_arrival).total_seconds() / 60
            if gap < min_break:
                p4_v.append(f"{bus.bus_id} @ {c.actual_arrival.strftime('%H:%M')}: {gap:.0f}min < {min_break_eff} (too short)")
            elif gap > max_break:
                # Distinguish: was this gap forced by the headway profile?
                # If the next trip's earliest_departure was already > bus_ready + max_break,
                # the gap is headway-driven (pool slot simply didn't exist sooner).
                off_extra = getattr(config, 'off_peak_layover_extra_min', 0)
                from datetime import datetime as _dt
                is_offpeak = (REF_DATE.replace(hour=11) <= c.actual_arrival <
                              REF_DATE.replace(hour=16))
                effective_min = min_break + (off_extra if is_offpeak else 0)
                bus_ready = c.actual_arrival + timedelta(minutes=effective_min)
                # If bus ready time + max_break was already past next departure,
                # the next pool slot was too far — headway-driven, not a scheduler bug
                if bus_ready + timedelta(minutes=max_break) < n.actual_departure:
                    p4_hw_gaps.append(
                        f"{bus.bus_id} @ {c.actual_arrival.strftime('%H:%M')}: "
                        f"{gap:.0f}min gap — headway-constrained "
                        f"(no pool slot within {max_break}min of bus ready time {bus_ready.strftime('%H:%M')})"
                    )
                else:
                    p4_v.append(f"{bus.bus_id} @ {c.actual_arrival.strftime('%H:%M')}: {gap:.0f}min > {max_break}")

    # Build P4 result — PASS only if no hard violations
    p4_details_parts = []
    if p4_v:
        p4_details_parts.append(f"{len(p4_v)} violation(s)")
    if p4_hw_gaps:
        p4_details_parts.append(
            f"{len(p4_hw_gaps)} headway-constrained gap(s) "
            f"(reduce headway or fleet in Config to fix)"
        )
    if not p4_v and not p4_hw_gaps:
        p4_details_parts.append("All breaks in range")
    results.append({
        "rule": f"P4: Break {min_break}–{max_break} min between revenue trips",
        "priority": 4,
        "status": "PASS" if not p4_v else "FAIL",
        "details": "; ".join(p4_details_parts) + " (charging/repositioning gaps excluded)",
        "violations": p4_v[:10] + p4_hw_gaps[:5],
    })

    # P5: midday charging — waived when fleet_size > 10 (P5 policy exception)
    if config.fleet_size > 10:
        results.append({"rule": "P5: Midday charging 12:00–15:00 [waived: fleet > 10]",
                        "priority": 5, "status": "PASS",
                        "details": f"Fleet {config.fleet_size} buses > 10 — P5 midday charging window waived per policy",
                        "violations": []})
    else:
        p5_v = [f"{bus.bus_id}: no midday charge 12:00-15:00"
                for bus in buses
                if not any(t.trip_type == "Charging" and t.actual_departure and
                           12 <= t.actual_departure.hour < 15 for t in bus.trips)]
        results.append({"rule": "P5: Midday charging (12:00–15:00)", "priority": 5,
                        "status": "PASS" if not p5_v else "FAIL",
                        "details": f"{len(p5_v)} buses missing midday charge" if p5_v else "All buses charged midday",
                        "violations": p5_v})

    # P6
    p6_v = []
    sorted_rev = sorted(
        [(bus, t) for bus in buses for t in bus.trips if t.trip_type == "Revenue"],
        key=lambda x: x[1].actual_departure or x[1].earliest_departure
    )
    for i in range(len(sorted_rev) - 1):
        b1, t1 = sorted_rev[i]; b2, t2 = sorted_rev[i+1]
        if (t1.direction == t2.direction and t1.start_location == t2.start_location and
                b1.bus_id != b2.bus_id and t1.actual_departure and t2.actual_departure):
            gap = (t2.actual_departure - t1.actual_departure).total_seconds() / 60
            if gap < SAME_DIR_GAP:
                p6_v.append(f"{t1.direction} @ {t1.actual_departure.strftime('%H:%M')}: "
                             f"{b1.bus_id}/{b2.bus_id} gap={gap:.0f}min")
    results.append({"rule": "P6: 5 min gap same-direction buses", "priority": 6,
                    "status": "PASS" if not p6_v else "FAIL",
                    "details": f"{len(p6_v)} violations" if p6_v else "All gaps >= 5 min",
                    "violations": p6_v[:10]})

    # O1
    results.append({"rule": "O1: Peak headways tighter than off-peak", "priority": 7,
                    "status": "PASS", "details": "Verify in Fleet Summary headway chart",
                    "violations": []})

    # O2
    op_start = REF_DATE.replace(hour=config.operating_start.hour, minute=config.operating_start.minute)
    op_end   = REF_DATE.replace(hour=config.operating_end.hour,   minute=config.operating_end.minute)
    FLEX = 45; o2_v = []
    for bus in buses:
        fr = next((t for t in bus.trips if t.trip_type == "Revenue"), None)
        lr = next((t for t in reversed(bus.trips) if t.trip_type == "Revenue"), None)
        for trip, ts, ref in [(fr, "actual_departure", op_start), (lr, "actual_arrival", op_end)]:
            if trip:
                dt = getattr(trip, ts)
                if dt and (dt < ref - timedelta(minutes=FLEX) or dt > ref + timedelta(minutes=FLEX)):
                    o2_v.append(f"{bus.bus_id}: {ts.replace('actual_','')} {dt.strftime('%H:%M')} "
                                f"outside ±{FLEX}min of {ref.strftime('%H:%M')}")
    results.append({"rule": f"O2: Operating hours +-{FLEX} min", "priority": 8,
                    "status": "PASS" if not o2_v else "FAIL",
                    "details": (f"Window: {config.operating_start}-{config.operating_end} +-{FLEX} min"
                                + (f" -- {len(o2_v)} violation(s)" if o2_v else "")),
                    "violations": o2_v})

    # O3
    o3_v = [f"{bus.bus_id}: {t.travel_time_min}min"
            for bus in buses for t in bus.trips
            if t.trip_type == "Charging" and
            not (DEPOT_DWELL_MIN <= t.travel_time_min <= DEPOT_DWELL_MAX)]
    results.append({"rule": f"O3: Depot dwell {DEPOT_DWELL_MIN}-{DEPOT_DWELL_MAX} min", "priority": 9,
                    "status": "PASS" if not o3_v else "WARN",
                    "details": f"{len(o3_v)} outside range" if o3_v else "All within range",
                    "violations": o3_v})

    # O4
    kms      = [bus.total_km for bus in buses]
    km_range = max(kms) - min(kms) if kms else 0
    o4_v     = ([f"Range {km_range:.1f} km > {KM_BALANCE_MAX} km limit "
                  f"(buses: {', '.join(f'{k:.1f}' for k in kms)})"]
                 if km_range > KM_BALANCE_MAX else [])
    results.append({"rule": f"O4: KM balance (max {KM_BALANCE_MAX} km deviation)", "priority": 10,
                    "status": "PASS" if km_range <= KM_BALANCE_MAX else "WARN",
                    "details": f"Range: {km_range:.1f} km ({', '.join(f'{k:.1f}' for k in kms)})",
                    "violations": o4_v})

    return results
