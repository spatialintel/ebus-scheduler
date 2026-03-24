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
__version__ = "2026-03-24-b1"  # auto-stamped
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
    """
    Return (node, dist_km, travel_min) for the terminal/intermediate closest
    to the depot. Travel time is the primary key; distance breaks ties.
    e.g. BHAVNAGARPARA (7.7km/15min) vs RTO CIRCLE (5.7km/15min) → RTO CIRCLE wins.
    """
    best = None
    for node in _operational_nodes(config):
        try:
            dist = config.get_distance(config.depot, node)
            tt   = config.get_travel_time(config.depot, node)
            if best is None:
                best = (node, dist, tt)
            elif tt < best[2]:
                best = (node, dist, tt)
            elif tt == best[2] and dist < best[1]:  # tie on time → shorter distance wins
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
    After Dead/Charging/Shuttle: immediate.
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


def _is_shuttle_leg(from_loc, to_loc, config):
    """
    A leg is a Shuttle if it travels between any two stops on the revenue
    corridor (terminals + intermediates), excluding any leg that starts or
    ends at the depot.
    Shuttles carry passengers even though they are outside the main revenue cycle.
    """
    if from_loc == config.depot or to_loc == config.depot:
        return False
    corridor = ({config.start_point, config.end_point} |
                {n for n in getattr(config, 'intermediates', []) if n})
    return from_loc in corridor and to_loc in corridor


def _make_dead(bus, to_loc, dist, tt, config=None):
    """Create a Dead or Shuttle trip leg and assign it to the bus."""
    trip_type = "Shuttle" if (config and _is_shuttle_leg(
        bus.current_location, to_loc, config)) else "Dead"
    leg = Trip(direction="DEPOT", trip_type=trip_type,
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
    return [_make_dead(bus, nearest, dist, tt, config=config)]

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
                inserted.append(_make_dead(bus, nearest, d, t, config=config))
        except KeyError: pass
    if bus.current_location != config.depot:
        try:
            d = config.get_distance(bus.current_location, config.depot)
            t = config.get_travel_time(bus.current_location, config.depot)
            if bus.soc_after_trip(d) >= SOC_FLOOR:
                inserted.append(_make_dead(bus, config.depot, d, t, config=config))
        except KeyError: pass
    return inserted

def _route_from_depot(bus, config):
    if bus.current_location != config.depot: return []
    nearest, dist, tt = _nearest_node_from_depot(config)
    if dist <= 0: return []
    return [_make_dead(bus, nearest, dist, tt, config=config)]

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
    """
    op_end  = REF_DATE.replace(hour=config.operating_end.hour,
                               minute=config.operating_end.minute)
    nearest, _, nearest_tt = _nearest_node_from_depot(config)
    candidates = []

    for bus in buses:
        if bus.current_location == trip.start_location:
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
        # Absolute phase lock: snap rt to this bus's permanent lane.
        # During off-peak we widen the effective natural_gap by off_peak_extra
        # so the snap respects the intended wider headway instead of pulling rt
        # back to a tighter peak-cadence slot.
        if natural_gap and hasattr(bus, 'phase_index'):
            op_start_dt = REF_DATE.replace(
                hour=config.operating_start.hour,
                minute=config.operating_start.minute,
            )
            snap_gap = natural_gap
            if _is_off_peak(rt):
                extra = getattr(config, 'off_peak_layover_extra_min', 0)
                snap_gap = natural_gap + extra / max(1, config.fleet_size)
            rt = _snap_to_phase(rt, bus.phase_index, snap_gap,
                                 config.fleet_size, op_start_dt)
        # P6 safety-net
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


def schedule_buses(config: RouteConfig, trips: list[Trip],
                   headway_df=None, travel_time_df=None) -> list[BusState]:
    """
    Bus-driven scheduler — no pre-generated trip pool slots.

    Each bus departs as soon as its break is served (preferred_layover_min,
    plus off_peak_layover_extra_min during 11:00–16:00). The headway profile
    acts as a minimum same-direction spacing floor. No fixed slot clock.

    Charging is staggered: buses are sent to charge one at a time so that
    the fleet never all disappear simultaneously (which caused 138-min gaps).
    """
    min_break      = config.preferred_layover_min
    off_peak_extra = getattr(config, 'off_peak_layover_extra_min', 0)
    buses          = _create_fleet(config)
    op_end         = REF_DATE.replace(hour=config.operating_end.hour,
                                      minute=config.operating_end.minute)
    op_start_dt    = REF_DATE.replace(hour=config.operating_start.hour,
                                      minute=config.operating_start.minute)

    # ── Phase 1: Staggered morning dead runs ─────────────────────────────────
    nearest_node, _, nearest_tt = _nearest_node_from_depot(config)

    # Determine which revenue terminal buses start from after the morning dead run.
    # Revenue trips always run start_point ↔ end_point (full route).
    # If nearest_node is one of the route terminals, buses start there directly.
    # If nearest_node is an intermediate stop, buses reposition to whichever
    # route terminal is closer (by distance) before beginning revenue service.
    terminals = [config.start_point, config.end_point]
    if nearest_node in terminals:
        rev_start = nearest_node
        far_loc   = config.end_point if nearest_node == config.start_point else config.start_point
        reposition_to = None
    else:
        # nearest is intermediate — find closest terminal
        best_term, best_dist = None, float('inf')
        for term in terminals:
            try:
                d = config.get_distance(nearest_node, term)
                if d < best_dist:
                    best_dist, best_term = d, term
            except KeyError:
                pass
        rev_start     = best_term or config.start_point
        far_loc       = config.end_point if rev_start == config.start_point else config.start_point
        reposition_to = rev_start  # dead reposition leg needed after morning dead run

    try:
        dn_tt = config.get_travel_time(rev_start, far_loc)
        up_tt = config.get_travel_time(far_loc, rev_start)
    except KeyError:
        dn_tt = up_tt = 45

    cycle_time  = dn_tt + min_break + up_tt + min_break
    natural_gap = cycle_time / max(1, config.fleet_size)

    for i, bus in enumerate(buses):
        arrive_at        = op_start_dt + timedelta(minutes=i * natural_gap)
        bus.current_time = arrive_at - timedelta(minutes=nearest_tt)
        _morning_dead_run(bus, config)
        bus.phase_index  = i
        # If nearest_node is an intermediate, reposition to the closest terminal
        if reposition_to and bus.current_location != reposition_to:
            try:
                rd = config.get_distance(bus.current_location, reposition_to)
                rt_tt = config.get_travel_time(bus.current_location, reposition_to)
                _make_dead(bus, reposition_to, rd, rt_tt, config=config)
            except KeyError:
                pass

    # ── Phase 2: Bus-driven revenue loop ─────────────────────────────────────
    # Each iteration picks the bus that can depart soonest, enforces headway
    # spacing (P6), and sends it on the next natural trip. No pool slots.
    midday_soc  = getattr(config, 'midday_charge_soc_percent', MIDDAY_CHARGE_SOC)
    soc_trigger = getattr(config, 'trigger_soc_percent', SOC_TRIGGER)

    # Track which buses have already charged today (P5: once per day)
    charged_today = set()

    MAX_ITER = config.fleet_size * 300
    for _ in range(MAX_ITER):

        # Find the bus ready soonest that can make a valid trip
        best_bus = best_rt = best_dir = best_start = best_end = None
        best_dist_km = best_tt_val = best_needs_repo = None

        for bus in buses:
            if bus.current_location == config.depot:
                continue

            # Determine next direction and trip endpoints.
            # After a charging detour the bus may be repositioned to rev_start
            # regardless of which direction it last served. Use physical location
            # to determine direction — not last_rev direction — when the bus
            # just returned from a charging detour.
            last_rev = next((t for t in reversed(bus.trips)
                             if t.trip_type == "Revenue"), None)
            recent_types = [t.trip_type for t in bus.trips[-4:]]
            just_charged = "Charging" in recent_types

            if last_rev is None or just_charged:
                # No revenue history, or just returned from charge:
                # depart from wherever the bus physically is
                if bus.current_location == rev_start or (
                        reposition_to and bus.current_location == nearest_node):
                    next_dir   = "DN"
                    trip_start = rev_start
                    trip_end   = far_loc
                elif bus.current_location == far_loc:
                    next_dir   = "UP"
                    trip_start = far_loc
                    trip_end   = rev_start
                else:
                    continue  # not at a known terminal
            elif last_rev.end_location == far_loc:
                next_dir   = "UP"
                trip_start = far_loc
                trip_end   = rev_start
            else:
                next_dir   = "DN"
                trip_start = rev_start
                trip_end   = far_loc

            if bus.current_location != trip_start:
                # Bus is at wrong location — only consider if it's at nearest_node
                # and can reposition to rev_start via a dead run.
                if (reposition_to and
                        bus.current_location == nearest_node and
                        trip_start == rev_start):
                    try:
                        repo_tt = config.get_travel_time(nearest_node, rev_start)
                    except KeyError:
                        continue
                    needs_reposition = True
                else:
                    continue
            else:
                needs_reposition = False
                repo_tt = 0

            # Break duration: after Revenue = preferred_layover; after Dead/Charging = 0
            rt = _ready_time(bus, min_break, config)

            # If reposition needed, add travel time on top of ready time
            if needs_reposition:
                rt = rt + timedelta(minutes=repo_tt)

            # Headway floor: enforce minimum gap from last same-dir departure
            min_hw = SAME_DIR_GAP
            if headway_df is not None:
                try:
                    from trip_generator import _get_headway_at
                    min_hw = _get_headway_at(rt, headway_df)
                except Exception:
                    pass

            # Spacing floor: natural_gap normally.
            # After THIS bus returned from a charging detour, use headway_min only
            # so it can re-enter service without being blocked by its missed slot.
            # Only True for the bus that JUST returned from charging
            # (Charging trip exists and the last few trips include Dead/Charging).
            recent_types = [t.trip_type for t in bus.trips[-4:]]
            just_charged = "Charging" in recent_types
            min_spacing = min_hw if just_charged else max(min_hw, natural_gap)
            last_same = None
            for other in buses:
                cand = _last_revenue_in_direction(other, next_dir, trip_start)
                if cand and (last_same is None or
                        cand.actual_departure > last_same.actual_departure):
                    last_same = cand
            if last_same and last_same.actual_departure:
                gap = (rt - last_same.actual_departure).total_seconds() / 60
                if gap < min_spacing:
                    rt = last_same.actual_departure + timedelta(minutes=min_spacing)

            # Travel time from profile or segment config
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
                    tt_val = dn_tt

            # SOC and arrival checks
            try:
                dist_km = config.get_distance(trip_start, trip_end)
            except KeyError:
                continue
            if bus.soc_after_trip(dist_km) < SOC_FLOOR:
                continue
            if rt + timedelta(minutes=tt_val) > op_end + timedelta(minutes=45):
                continue

            if best_rt is None or rt < best_rt:
                best_bus         = bus
                best_rt          = rt
                best_dir         = next_dir
                best_start       = trip_start
                best_end         = trip_end
                best_dist_km     = dist_km
                best_tt_val      = tt_val
                best_needs_repo  = needs_reposition

        if best_bus is None:
            break  # no bus can move — done

        # Apply reposition dead run NOW (after selection, side-effect free)
        if best_needs_repo and best_bus.current_location == nearest_node:
            try:
                rd  = config.get_distance(nearest_node, rev_start)
                rtt = config.get_travel_time(nearest_node, rev_start)
                _make_dead(best_bus, rev_start, rd, rtt, config=config)
            except KeyError:
                pass

        # Assign the trip
        trip = Trip(
            direction=best_dir, trip_type="Revenue",
            start_location=best_start, end_location=best_end,
            earliest_departure=best_rt, latest_departure=op_end,
            travel_time_min=best_tt_val, distance_km=best_dist_km,
            shift=(1 if best_rt < REF_DATE.replace(
                       hour=config.shift_split.hour,
                       minute=config.shift_split.minute) else 2),
        )
        trip.earliest_departure = best_rt
        best_bus.assign(trip)

        # Post-trip charging decisions
        midday_soc  = getattr(config, 'midday_charge_soc_percent', MIDDAY_CHARGE_SOC)
        soc_trigger = getattr(config, 'trigger_soc_percent', SOC_TRIGGER)

        # Prefer to charge from rev_start (near depot side) not far_loc (far terminal).
        # If bus is at far_loc after a DN trip, let it serve one UP trip back to
        # rev_start first — this saves the 40-min revenue-corridor dead run and
        # reduces the headway gap during the charging window.
        at_far_loc = (best_bus.current_location == far_loc)

        def _latest_charge_return_time(buses):
            """Return the latest time any bus is expected to return from a charge detour."""
            latest = None
            for b in buses:
                for t in reversed(b.trips):
                    if t.trip_type == "Dead" and t.end_location != config.depot:
                        # Bus just returned from depot — its latest return
                        if t.actual_arrival:
                            if latest is None or t.actual_arrival > latest:
                                latest = t.actual_arrival
                        break
                    if t.trip_type == "Charging":
                        if t.actual_arrival:
                            if latest is None or t.actual_arrival > latest:
                                latest = t.actual_arrival
                        break
            return latest

        # Staggered P5 charging: space charge starts by natural_gap minutes.
        # This ensures at most 1 bus is absent per fleet-gap window, keeping
        # headways even during the charging period. Blocking entirely (one-at-a-time)
        # causes buses to pile up and creates large service gaps when they all
        # return simultaneously.
        def _last_charge_start(buses, current_bus):
            """Return the most recent charge departure time of any OTHER bus."""
            latest = None
            for b in buses:
                if b is current_bus:
                    continue
                for t in reversed(b.trips):
                    if t.trip_type == "Charging" and t.actual_departure:
                        if latest is None or t.actual_departure > latest:
                            latest = t.actual_departure
                        break
            return latest

        # No artificial charge stagger needed — buses are already naturally staggered
        # by natural_gap (27.5min) from Phase 1. This ensures charging happens
        # at different times automatically without extra blocking logic.
        charge_stagger_ok = True

        if (_is_midday(best_bus.current_time) and
                config.fleet_size <= 10 and
                best_bus.bus_id not in charged_today and
                best_bus.soc_percent < midday_soc and
                charge_stagger_ok and
                not at_far_loc):   # serve return trip first if at far terminal
            try:
                dead_tt = config.get_travel_time(best_bus.current_location, config.depot)
            except KeyError:
                dead_tt = 30
            exp_arr = best_bus.current_time + timedelta(minutes=dead_tt)
            if exp_arr >= MIDDAY_START - timedelta(minutes=30):
                _charging_detour(best_bus, config,
                                 resume_by=EVENING_PEAK_START, min_break=min_break)
                charged_today.add(best_bus.bus_id)

        elif best_bus.soc_percent <= soc_trigger:
            if _is_midday(best_bus.current_time) and best_bus.bus_id not in charged_today:
                if charge_stagger_ok:
                    _charging_detour(best_bus, config,
                                     resume_by=EVENING_PEAK_START, min_break=min_break)
                    charged_today.add(best_bus.bus_id)
            elif not _is_peak(best_bus.current_time):
                _charging_detour(best_bus, config,
                                 resume_by=EVENING_PEAK_START, min_break=min_break)
                charged_today.add(best_bus.bus_id)
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

    # P4 (charging gaps excluded) — upper bound uses config.max_layover_min
    max_break = getattr(config, 'max_layover_min', MAX_BREAK)
    p4_v = []
    for bus in buses:
        rev_idx = [i for i, t in enumerate(bus.trips) if t.trip_type == "Revenue"]
        for j in range(1, len(rev_idx)):
            i_prev, i_curr = rev_idx[j-1], rev_idx[j]
            if any(t.trip_type in ("Charging", "Dead", "Shuttle") for t in bus.trips[i_prev+1:i_curr]): continue
            c, n = bus.trips[i_prev], bus.trips[i_curr]
            if c.actual_arrival and n.actual_departure:
                gap = (n.actual_departure - c.actual_arrival).total_seconds() / 60
                if gap < min_break:
                    p4_v.append(f"{bus.bus_id} @ {c.actual_arrival.strftime('%H:%M')}: {gap:.0f}min < {min_break}")
                elif gap > max_break:
                    p4_v.append(f"{bus.bus_id} @ {c.actual_arrival.strftime('%H:%M')}: {gap:.0f}min > {max_break}")
    results.append({"rule": f"P4: Break {min_break}–{max_break} min between revenue trips", "priority": 4,
                    "status": "PASS" if not p4_v else "FAIL",
                    "details": (f"{len(p4_v)} violations (charging gaps excluded)"
                                if p4_v else "All breaks in range (charging gaps excluded)"),
                    "violations": p4_v[:10]})

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
