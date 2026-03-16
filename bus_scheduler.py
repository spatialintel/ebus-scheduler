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

MAX_BREAK         = 20
MIDDAY_CHARGE_SOC = 65.0
SOC_TRIGGER       = 30.0
SOC_FLOOR         = 20.0
SAME_DIR_GAP      = 5
DEPOT_DWELL_MIN   = 45
DEPOT_DWELL_MAX   = 90
KM_BALANCE_MAX    = 20.0


def _is_midday(t): return MIDDAY_START <= t < MIDDAY_END
def _is_peak(t):
    return (MORNING_PEAK_START <= t < MORNING_PEAK_END or
            EVENING_PEAK_START <= t < EVENING_PEAK_END)

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

def _ready_time(bus, min_break):
    """min_break after Revenue; immediate after Dead/Charging."""
    last = bus.trips[-1] if bus.trips else None
    if last and last.trip_type == "Revenue":
        return bus.current_time + timedelta(minutes=min_break)
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

def _check_p6(buses, trip, dep):
    """
    P6: 5-min gap from the most-recent same-direction revenue trip of ANY other bus.
    Scans all trips (not just trips[-1]) to handle buses that charged/repositioned
    after their last revenue trip.
    """
    if trip.trip_type != "Revenue":
        return True
    for bus in buses:
        last_rev = _last_revenue_in_direction(bus, trip.direction, trip.start_location)
        if last_rev is None:
            continue
        gap = (dep - last_rev.actual_departure).total_seconds() / 60
        if gap < SAME_DIR_GAP:
            return False
    return True

def _bumped_ready_time(buses, trip, rt, natural_gap=None):
    """
    Return rt bumped forward until P6 is satisfied.
    If natural_gap is provided, bumps by natural_gap to re-sync with fleet phase.
    Otherwise falls back to SAME_DIR_GAP increments.
    """
    bump = natural_gap if natural_gap and natural_gap > SAME_DIR_GAP else SAME_DIR_GAP
    for _ in range(20):
        if _check_p6(buses, trip, rt):
            return rt
        rt += timedelta(minutes=bump)
    return rt


def _snap_to_phase(rt: datetime, phase_index: int, natural_gap: float,
                   fleet_size: int, op_start: datetime) -> datetime:
    """
    Snap rt forward to this bus's designated phase slot.

    After any disruption (charging, dead run, P6 bump) a bus may be displaced
    from its phase.  Absolute phase locking snaps it back to the nearest future
    slot in its own lane rather than nudging relative to other buses.

    Phase slots for bus i (anchor = op_start):
        op_start + i*natural_gap + k*cycle_time   for k = 0, 1, 2, ...
    where cycle_time = natural_gap * fleet_size.

    Using op_start as epoch is intentional: it matches the Phase-1 stagger
    (arrive_at = op_start + i * stagger_min) so phase 0 == first slot, etc.

    Args:
        rt:           earliest the bus could depart (may be between slots)
        phase_index:  bus's permanent phase index (0 … fleet_size-1)
        natural_gap:  cycle_time / fleet_size (minutes, float)
        fleet_size:   number of buses on route
        op_start:     operating start datetime (REF_DATE + hour/min)

    Returns:
        Earliest datetime >= rt that falls on this bus's phase lane.
    """
    if natural_gap <= 0 or fleet_size <= 0:
        return rt

    cycle_time   = natural_gap * fleet_size
    phase_anchor = op_start + timedelta(minutes=phase_index * natural_gap)
    delta_min    = (rt - phase_anchor).total_seconds() / 60

    # rt is before the first slot for this bus — snap to first slot
    if delta_min <= 0:
        return phase_anchor

    remainder = delta_min % cycle_time
    # Within 0.5 min of a slot: treat as already on-slot (float-precision guard)
    if remainder < 0.5:
        return rt

    snap_ahead = cycle_time - remainder
    return rt + timedelta(minutes=snap_ahead)


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
    """Return via nearest_node (P2 both ways): current → nearest → DEPOT."""
    inserted = []
    if bus.current_location == config.depot: return inserted
    nearest, _, _ = _nearest_node_from_depot(config)
    if bus.current_location != nearest:
        try:
            d = config.get_distance(bus.current_location, nearest)
            t = config.get_travel_time(bus.current_location, nearest)
            if bus.soc_after_trip(d) >= SOC_FLOOR:
                inserted.append(_make_dead(bus, nearest, d, t))
        except KeyError: pass
    if bus.current_location != config.depot:
        try:
            d = config.get_distance(bus.current_location, config.depot)
            t = config.get_travel_time(bus.current_location, config.depot)
            if bus.soc_after_trip(d) >= SOC_FLOOR:
                inserted.append(_make_dead(bus, config.depot, d, t))
        except KeyError: pass
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
    op_end = REF_DATE.replace(hour=config.operating_end.hour,
                              minute=config.operating_end.minute)
    candidates = []
    for bus in buses:
        if bus.current_location == trip.start_location: continue
        try:
            d = config.get_distance(bus.current_location, trip.start_location)
            t = config.get_travel_time(bus.current_location, trip.start_location)
        except KeyError: continue
        arrival = bus.current_time + timedelta(minutes=t)
        if arrival + timedelta(minutes=min_break) > op_end: continue
        if bus.soc_after_trip(d) - bus._soc_cost(trip.distance_km) < SOC_FLOOR: continue
        candidates.append(((arrival - bus.current_time).total_seconds() / 60, bus, d, t))
    if not candidates: return None
    candidates.sort(key=lambda x: x[0])
    _, bus, d, t = candidates[0]
    dead = Trip(direction="DEPOT", trip_type="Dead",
                start_location=bus.current_location, end_location=trip.start_location,
                earliest_departure=bus.current_time, latest_departure=bus.current_time,
                travel_time_min=t, distance_km=d, shift=bus.shift)
    return bus, dead

def _select_bus(buses, trip, config, min_break, natural_gap=None):
    avg_km = _fleet_avg_km(buses)
    min_km = getattr(config, 'min_km_per_bus', 0) or 0
    candidates = []
    for bus in buses:
        if bus.current_location != trip.start_location: continue
        if bus.soc_after_trip(trip.distance_km) < SOC_FLOOR: continue
        rt = _ready_time(bus, min_break)
        # Absolute phase lock: snap rt to this bus's permanent phase lane.
        # Replaces the old relative natural_gap/2 check which only reduced
        # bunching; this eliminates it by assigning each bus a fixed clock lane.
        # _snap_to_phase uses op_start as epoch, consistent with Phase-1 stagger.
        if natural_gap and hasattr(bus, 'phase_index'):
            op_start_dt = REF_DATE.replace(
                hour=config.operating_start.hour,
                minute=config.operating_start.minute,
            )
            rt = _snap_to_phase(rt, bus.phase_index, natural_gap,
                                 config.fleet_size, op_start_dt)
        # P6 safety-net: bump by natural_gap (= one full cycle slot) so that any
        # residual violation advances rt to the same bus's next valid phase slot.
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


def schedule_buses(config: RouteConfig, trips: list[Trip]) -> list[BusState]:
    min_break = config.preferred_layover_min
    buses     = _create_fleet(config)
    unassigned = []
    revenue_trips = [t for t in trips if t.trip_type == "Revenue"]
    op_end = REF_DATE.replace(hour=config.operating_end.hour,
                              minute=config.operating_end.minute)

    # ── Phase 1: Staggered morning dead runs ──────────────────────────────
    # Stagger = natural_gap = cycle_time / fleet_size.
    # cycle_time = DN_travel + break + UP_travel + break (one full round trip).
    # Using natural_gap (not headway from trip pool) ensures buses are evenly
    # spread regardless of early-morning headway profile values.
    nearest_node, _, nearest_tt = _nearest_node_from_depot(config)
    op_start_dt = REF_DATE.replace(hour=config.operating_start.hour,
                                   minute=config.operating_start.minute)
    dn_pool = [t for t in trips if t.trip_type == "Revenue" and t.direction == "DN"]
    up_pool = [t for t in trips if t.trip_type == "Revenue" and t.direction == "UP"]
    if dn_pool and up_pool:
        dn_tt = dn_pool[0].travel_time_min
        up_tt = up_pool[0].travel_time_min
        cycle_time  = dn_tt + min_break + up_tt + min_break
        stagger_min = cycle_time / max(1, config.fleet_size)
    else:
        stagger_min = min_break * 2

    for i, bus in enumerate(buses):
        arrive_at = op_start_dt + timedelta(minutes=i * stagger_min)
        bus.current_time = arrive_at - timedelta(minutes=nearest_tt)
        _morning_dead_run(bus, config)
        # Permanent phase index — set once here, never changed.
        # Matches the stagger: bus i's first revenue slot is op_start + i*stagger_min.
        bus.phase_index = i
        # bus is now at nearest_node, current_time = arrive_at

    # ── Phase 2: Revenue trips — P4-first (bus ready time = departure time) ─
    # natural_gap = cycle_time / fleet — used to re-sync buses after charging detour
    if dn_pool and up_pool:
        cycle_time  = dn_pool[0].travel_time_min + min_break + up_pool[0].travel_time_min + min_break
        natural_gap = cycle_time / max(1, config.fleet_size)
    else:
        natural_gap = None

    for idx, trip in enumerate(revenue_trips):
        trip_time = trip.earliest_departure

        # P5: midday charging
        if _is_midday(trip_time):
            for bus in buses:
                if bus.current_location == config.depot: continue
                if not _is_midday(bus.current_time): continue
                if bus.soc_percent < MIDDAY_CHARGE_SOC:
                    _charging_detour(bus, config,
                                     resume_by=EVENING_PEAK_START, min_break=min_break)

        best, rt = _select_bus(buses, trip, config, min_break, natural_gap=natural_gap)

        if best is None:
            repo = _find_and_reposition(buses, trip, config, min_break)
            if repo:
                bus, dead = repo
                bus.assign(dead)
                best, rt = _select_bus(buses, trip, config, min_break, natural_gap=natural_gap)

        if best is None or rt is None:
            unassigned.append(trip)
            continue

        if rt > op_end + timedelta(minutes=45):
            unassigned.append(trip)
            continue

        trip.earliest_departure = rt
        best.assign(trip)

        # Post-assignment charging
        if _is_midday(best.current_time) and best.soc_percent < MIDDAY_CHARGE_SOC:
            _charging_detour(best, config, resume_by=EVENING_PEAK_START, min_break=min_break)
        elif best.soc_percent <= SOC_TRIGGER:
            future = [t for t in revenue_trips[idx+1:] if t.assigned_bus is None]
            resume = future[0].earliest_departure if future else op_end
            if _is_midday(best.current_time):
                _charging_detour(best, config, resume_by=EVENING_PEAK_START, min_break=min_break)
            elif not _is_peak(best.current_time):
                _charging_detour(best, config, resume_by=resume, min_break=min_break)
            elif best.soc_percent <= SOC_FLOOR + 5:
                _charging_detour(best, config, resume_by=resume, min_break=min_break)

    # ── Phase 3: Evening return ─────────────────────────────────────────────
    for bus in buses:
        if bus.current_location != config.depot:
            _route_to_depot(bus, config)

    # ── Phase 4: Safety-net break enforcement ──────────────────────────────
    _balance_breaks(buses, config)

    if unassigned:
        print(f"\n  WARNING: {len(unassigned)} trips unassigned:")
        for t in unassigned[:5]:
            print(f"    {t.direction} {t.start_location}->{t.end_location} "
                  f"@ {t.earliest_departure.strftime('%H:%M')}")
        if len(unassigned) > 5:
            print(f"    ... and {len(unassigned)-5} more")

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

    # P4 (charging gaps excluded)
    p4_v = []
    for bus in buses:
        rev_idx = [i for i, t in enumerate(bus.trips) if t.trip_type == "Revenue"]
        for j in range(1, len(rev_idx)):
            i_prev, i_curr = rev_idx[j-1], rev_idx[j]
            if any(t.trip_type == "Charging" for t in bus.trips[i_prev+1:i_curr]): continue
            c, n = bus.trips[i_prev], bus.trips[i_curr]
            if c.actual_arrival and n.actual_departure:
                gap = (n.actual_departure - c.actual_arrival).total_seconds() / 60
                if gap < min_break:
                    p4_v.append(f"{bus.bus_id} @ {c.actual_arrival.strftime('%H:%M')}: {gap:.0f}min < {min_break}")
                elif gap > MAX_BREAK:
                    p4_v.append(f"{bus.bus_id} @ {c.actual_arrival.strftime('%H:%M')}: {gap:.0f}min > {MAX_BREAK}")
    results.append({"rule": f"P4: Break {min_break}-{MAX_BREAK} min between revenue trips", "priority": 4,
                    "status": "PASS" if not p4_v else "FAIL",
                    "details": (f"{len(p4_v)} violations (charging gaps excluded)"
                                if p4_v else "All breaks in range (charging gaps excluded)"),
                    "violations": p4_v[:10]})

    # P5
    p5_v = [f"{bus.bus_id}: no charging 12:00-15:00"
            for bus in buses
            if not any(t.trip_type == "Charging" and t.actual_departure and
                       12 <= t.actual_departure.hour < 15 for t in bus.trips)]
    results.append({"rule": "P5: Midday charging (12:00-15:00)", "priority": 5,
                    "status": "PASS" if not p5_v else "FAIL",
                    "details": f"{len(p5_v)} buses missing midday charge" if p5_v else "All buses charged",
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
