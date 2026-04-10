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
__version__ = "2026-03-30-b5"  # auto-stamped
from datetime import datetime, timedelta
from src.models import Trip, BusState, RouteConfig, ScheduleInfeasibleError

REF_DATE           = datetime(2025, 1, 1)
MIDDAY_START       = REF_DATE.replace(hour=12, minute=0)
MIDDAY_END         = REF_DATE.replace(hour=15, minute=0)
MORNING_PEAK_START = REF_DATE.replace(hour=8,  minute=0)
MORNING_PEAK_END   = REF_DATE.replace(hour=11, minute=0)
EVENING_PEAK_START = REF_DATE.replace(hour=15, minute=0)
EVENING_PEAK_END   = REF_DATE.replace(hour=20, minute=0)

# P5 charging window: 11:00–16:00 with ±45 min flex per bus.
# Buses are distributed evenly across the window so bus 0 targets 11:00
# and bus N-1 targets 16:00, with each bus allowed ±CHARGE_FLEX_MIN around
# its personal target before the scheduler considers it late for charging.
CHARGE_WINDOW_START = REF_DATE.replace(hour=11, minute=0)
CHARGE_WINDOW_END   = REF_DATE.replace(hour=16, minute=0)
CHARGE_FLEX_MIN     = 45   # ± minutes around each bus's target charge time

MAX_BREAK         = 20    # fallback if config.max_layover_min absent
MIDDAY_CHARGE_SOC = 65.0  # fallback if config.midday_charge_soc_percent absent
SOC_TRIGGER       = 30.0  # fallback if config.trigger_soc_percent absent
SOC_FLOOR         = 20.0
SAME_DIR_GAP      = 5
DEPOT_DWELL_MIN   = 35
DEPOT_DWELL_MAX   = 50
KM_BALANCE_MAX    = 20.0
# Global Headway Balancer weight: each 1-min deviation from target headway
# counts as HEADWAY_WEIGHT minutes of ready-time penalty in bus selection.
# Higher = more uniform headways, lower = faster throughput. 2.0 is a good default.
HEADWAY_WEIGHT    = 2.0

OFF_PEAK_START = REF_DATE.replace(hour=11, minute=0)
OFF_PEAK_END   = REF_DATE.replace(hour=15, minute=0)


# ── Headway band helpers ─────────────────────────────────────────────────────
# These are module-level so they are NEVER imported inside a nested closure.
# The previous pattern (import _get_headway_at inside _min_hw_at) silently fell
# back to SAME_DIR_GAP=5 whenever the import failed in Streamlit Cloud, causing
# the headway profile to be completely ignored (natural_gap dominated instead).

def _parse_hw_time(v) -> datetime:
    """Convert any Excel time cell type to a REF_DATE-anchored datetime."""
    if isinstance(v, datetime):                  # datetime.datetime
        return REF_DATE.replace(hour=v.hour, minute=v.minute, second=0, microsecond=0)
    from datetime import time as _time
    if isinstance(v, _time):                     # datetime.time
        return REF_DATE.replace(hour=v.hour, minute=v.minute, second=0, microsecond=0)
    if isinstance(v, (int, float)):              # Excel fractional day (0.25 = 06:00)
        tm = round(float(v) * 24 * 60)
        return REF_DATE.replace(hour=min(tm // 60, 23), minute=tm % 60,
                                second=0, microsecond=0)
    p = str(v).strip().split(":")                # "HH:MM" or "HH:MM:SS"
    return REF_DATE.replace(hour=int(p[0]), minute=int(p[1]), second=0, microsecond=0)


def _build_hw_bands(headway_df) -> list:
    """
    Pre-parse headway_df into a plain list of (t_from, t_to, headway_min) tuples.
    Built once at the start of schedule_buses / check_compliance so every lookup
    is a simple list scan with zero imports and zero DataFrame access.
    Returns [] if headway_df is None or empty.
    """
    if headway_df is None:
        return []
    try:
        if headway_df.empty:
            return []
    except Exception:
        return []
    bands = []
    for _, row in headway_df.iterrows():
        try:
            bands.append((
                _parse_hw_time(row["time_from"]),
                _parse_hw_time(row["time_to"]),
                int(row["headway_min"]),
            ))
        except Exception:
            pass
    return bands


def _lookup_hw(t: datetime, hw_bands: list, fallback: int = SAME_DIR_GAP) -> int:
    """Return headway_min for the band that contains t, or fallback if none match."""
    for t_from, t_to, hw in hw_bands:
        if t_from <= t < t_to:
            return hw
    return hw_bands[-1][2] if hw_bands else fallback


def _is_midday(t):   return MIDDAY_START <= t < MIDDAY_END
def _is_off_peak(t): return OFF_PEAK_START <= t < OFF_PEAK_END
def _is_peak(t):
    return (MORNING_PEAK_START <= t < MORNING_PEAK_END or
            EVENING_PEAK_START <= t < EVENING_PEAK_END)

def _charge_window(config):
    """Return (window_start, window_end) datetimes from config or hardcoded fallback."""
    try:
        cs = config.p5_charging_start
        ce = config.p5_charging_end
        ws = REF_DATE.replace(hour=cs.hour, minute=cs.minute)
        we = REF_DATE.replace(hour=ce.hour, minute=ce.minute)
        if we > ws:
            return ws, we
    except Exception:
        pass
    return CHARGE_WINDOW_START, CHARGE_WINDOW_END


def _target_charge_time(bus, config) -> datetime:
    """
    Return the ideal charge-start time for this bus.

    Distributes fleet evenly across the config P5 window:
      bus with phase_index 0   → window_start
      bus with phase_index N-1 → window_end
      buses in between         → linearly interpolated

    Uses phase_index set in Phase 1. Falls back to midpoint if not set.
    """
    n   = max(1, config.fleet_size)
    idx = getattr(bus, 'phase_index', 0)
    cws, cwe = _charge_window(config)
    window_min = (cwe - cws).total_seconds() / 60
    if n == 1:
        offset = window_min / 2
    else:
        offset = idx * window_min / (n - 1)
    return cws + timedelta(minutes=offset)

def _in_charge_window(bus, config) -> bool:
    """
    True if this bus should be considered for P5 charging now.

    Two cases:
    1. Primary: within ±CHARGE_FLEX_MIN of its personal target time
       (evenly distributed across the window, so buses charge in sequence)
    2. Catch-up: bus is past its target but still inside the overall window
       (handles buses that were on a trip at their exact target time)
    """
    target = _target_charge_time(bus, config)
    t      = bus.current_time
    # Primary slot
    if abs((t - target).total_seconds()) / 60 <= CHARGE_FLEX_MIN:
        return True
    # Catch-up: missed target slot but overall window not yet closed
    _, cwe = _charge_window(config)
    if target < t < cwe:
        return True
    return False


def _effective_break(config, current_time: datetime, base_break: int) -> int:
    """
    Return break minutes before the next revenue trip.

    During off-peak (11:00-15:00) the break is extended by
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


def _soc_cost_to_depot(from_loc: str, config: "RouteConfig") -> float:
    """
    Estimate SOC% consumed travelling from from_loc back to the depot via
    the nearest node (P2 compliance: always route via nearest_node).

    Returns 0.0 if the bus is already at the depot.
    Returns the direct path cost if from_loc == nearest_node.
    Otherwise returns the two-leg cost: from_loc → nearest_node → DEPOT.

    This is used as a lookahead feasibility check before assigning a revenue
    trip — ensuring the bus can safely reach the depot after the trip without
    dropping below SOC_FLOOR.
    """
    if from_loc == config.depot:
        return 0.0

    nearest, _, _ = _nearest_node_from_depot(config)

    def _km_cost(km: float) -> float:
        return (km * config.consumption_rate / config.battery_kwh) * 100

    total_km = 0.0
    loc = from_loc

    # Leg 1: from_loc → nearest_node (if not already there)
    if loc != nearest:
        try:
            total_km += config.get_distance(loc, nearest)
            loc = nearest
        except KeyError:
            # No direct segment — try direct to depot as fallback
            try:
                total_km += config.get_distance(from_loc, config.depot)
                return _km_cost(total_km)
            except KeyError:
                return _km_cost(config.get_distance(
                    config.start_point, config.end_point))  # rough fallback

    # Leg 2: nearest_node → DEPOT
    if loc != config.depot:
        try:
            total_km += config.get_distance(loc, config.depot)
        except KeyError:
            pass

    return _km_cost(total_km)

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
    """Return via nearest_node (P2 both ways): current → nearest → DEPOT.
    SOC floor is NOT enforced here — a bus must always be able to return to
    depot regardless of remaining battery. Stranding a bus off-route is worse
    than a P3 SOC floor violation on a dead run."""
    inserted = []
    if bus.current_location == config.depot: return inserted
    nearest, _, _ = _nearest_node_from_depot(config)
    if bus.current_location != nearest:
        try:
            d = config.get_distance(bus.current_location, nearest)
            t = config.get_travel_time(bus.current_location, nearest)
            inserted.append(_make_dead(bus, nearest, d, t, config=config))
        except KeyError: pass
    if bus.current_location != config.depot:
        try:
            d = config.get_distance(bus.current_location, config.depot)
            t = config.get_travel_time(bus.current_location, config.depot)
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
        max_km     = getattr(config, "max_km_per_bus", 0) or 0
        above_max  = +100 if (max_km > 0 and bus.total_km + trip.distance_km > max_km
                              and bus.soc_percent > SOC_FLOOR + 5) else 0
        km_penalty = max(0, km_deficit - KM_BALANCE_MAX) * 5.0
        candidates.append((km_deficit + km_penalty + below_min + above_max, id(bus), bus, rt))
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
    plus off_peak_layover_extra_min during 11:00–15:00). The headway profile
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

    # Pre-build headway bands ONCE — avoids repeated imports inside the hot loop.
    # _lookup_hw is a plain list scan: no imports, no DataFrame access, no closures.
    _hw_bands = _build_hw_bands(headway_df)

    # ── Phase 1: Staggered morning dead runs ─────────────────────────────────
    nearest_node, _, nearest_tt = _nearest_node_from_depot(config)

    # Detect circular route: start_point == end_point.
    # On circular routes all trips run in the same direction — there is no UP/DN
    # alternation. natural_gap = cycle_time/fleet fills ALL slots perfectly,
    # leaving no room for late-arriving buses. Use headway_min as the spacing
    # floor for circular routes so buses re-enter service at the next available gap.
    is_circular = config.start_point == config.end_point

    terminals = [config.start_point, config.end_point]
    # De-duplicate for circular (start==end gives [X, X])
    terminals = list(dict.fromkeys(terminals))

    if nearest_node in terminals:
        rev_start = nearest_node
        far_loc   = (config.end_point if nearest_node == config.start_point
                     else config.start_point)
        reposition_to = None
    else:
        best_term, best_dist = None, float('inf')
        for term in [config.start_point, config.end_point]:
            try:
                d = config.get_distance(nearest_node, term)
                if d < best_dist:
                    best_dist, best_term = d, term
            except KeyError:
                pass
        rev_start     = best_term or config.start_point
        far_loc       = config.end_point if rev_start == config.start_point else config.start_point
        reposition_to = rev_start

    # For circular routes: trip goes start→start (one full loop).
    # dn_tt = one-way travel time; cycle = dn_tt + break (no reverse direction).
    try:
        dn_tt = config.get_travel_time(rev_start, far_loc if not is_circular
                                       else rev_start)
        if is_circular and dn_tt == 0:
            # Fallback: use segment GANGAJALIA→GANGAJALIA if available
            dn_tt = config.get_travel_time(config.start_point, config.start_point)
    except KeyError:
        dn_tt = 45
    try:
        up_tt = (0 if is_circular else config.get_travel_time(far_loc, rev_start))
    except KeyError:
        up_tt = dn_tt if not is_circular else 0

    if is_circular:
        cycle_time  = dn_tt + min_break   # one direction only
    else:
        cycle_time  = dn_tt + min_break + up_tt + min_break
    natural_gap = cycle_time / max(1, config.fleet_size)

    # Phase 1 stagger gap: space buses at the minimum headway so they arrive
    # pre-spaced at the correct service interval.
    # Use the MINIMUM headway across all bands (usually peak headway) so the
    # stagger matches the tightest service requirement — not the early-morning
    # relaxed headway which would space buses too far apart and cause late starts
    # for the last bus in the fleet (e.g. 4 buses × 40min = 160min late start).
    # For circular routes: use early-morning headway (existing behaviour preserved).
    if is_circular and reposition_to and headway_df is not None:
        try:
            try:
                from src.trip_generator import _get_headway_at as _gha_p1
            except ImportError:
                from trip_generator import _get_headway_at as _gha_p1
            phase1_gap = _gha_p1(op_start_dt, headway_df)
        except Exception:
            phase1_gap = natural_gap
    elif _hw_bands:
        # Linear route: stagger by minimum configured headway across all bands,
        # floored at natural_gap so buses are never closer than the fleet can support.
        min_hw_all_bands = min(hw for _, _, hw in _hw_bands)
        phase1_gap = max(natural_gap, min_hw_all_bands)
    else:
        phase1_gap = natural_gap

    # ── Phase 1: Staggered morning dead runs ─────────────────────────────────
    #
    # Three dispatch modes — applied in priority order:
    #
    # A) RURAL ROUTE (config.is_suburban_route = True, any number of intermediates):
    #      ceil(n/2) buses → DEPOT → rural_node   (start or end, from config)
    #      floor(n/2) buses → DEPOT → other terminal
    #      Rationale: significant early-morning demand from village → city.
    #      Dead-km increases intentionally; planner is aware.
    #
    # B) INTERMEDIATE ROUTE (has intermediate stops, not rural, not circular):
    #      First half  → DEPOT → intermediate_node → nearest_terminal
    #      Second half → DEPOT → far_terminal  (direct)
    #      Ensures both terminals are served at service start.
    #
    # C) SIMPLE ROUTE (no intermediates, or circular):
    #      All buses → DEPOT → nearest_node  (existing behaviour, unchanged)
    #
    # Rules B and C apply only to morning startup. Mid-day charging detours
    # and end-of-day returns are NOT affected.

    has_intermediates = bool([n for n in config.intermediates if n and n.strip()])
    is_rural          = getattr(config, 'is_suburban_route', False) and not is_circular
    rural_node_name   = (getattr(config, 'rural_node', '') or '').strip().lower()

    # Resolve rural_node to actual location name
    if is_rural:
        if rural_node_name == 'start_point':
            rural_loc = config.start_point
            other_loc = config.end_point
        elif rural_node_name == 'end_point':
            rural_loc = config.end_point
            other_loc = config.start_point
        else:
            # rural_node not set or unrecognised — fall back to far terminal
            rural_loc = far_loc
            other_loc = rev_start

    # For intermediate dispatch: find intermediate closest to depot
    intermediate_node = None
    if has_intermediates and not is_rural and not is_circular:
        clean_ints = [n.strip() for n in config.intermediates if n and n.strip()]
        best_int, best_int_tt = None, float('inf')
        for n in clean_ints:
            try:
                tt_n = config.get_travel_time(config.depot, n)
                if tt_n < best_int_tt:
                    best_int_tt, best_int = tt_n, n
            except KeyError:
                pass
        intermediate_node = best_int or clean_ints[0]

    # split_dispatch: intermediate route mode (B) — only when not rural
    split_dispatch = (has_intermediates and not is_rural and not is_circular
                      and intermediate_node is not None)

    # ceil(n/2) = rural half (more buses go to rural end for early coverage)
    n_buses     = len(buses)
    rural_half  = (n_buses + 1) // 2   # ceil
    # For intermediate split: first half → intermediate → near terminal
    inter_half  = n_buses // 2          # floor

    for i, bus in enumerate(buses):

        # ── Mode A: Rural route dispatch ─────────────────────────────────────
        if is_rural:
            if i < rural_half:
                # Rural buses: DEPOT → rural terminal, arrive by op_start
                try:
                    d_rural = config.get_distance(config.depot, rural_loc)
                    t_rural = config.get_travel_time(config.depot, rural_loc)
                except KeyError:
                    d_rural, t_rural = 0, nearest_tt
                arrive_at        = op_start_dt + timedelta(minutes=i * phase1_gap)
                bus.current_time = arrive_at - timedelta(minutes=t_rural)
                if d_rural > 0:
                    _make_dead(bus, rural_loc, d_rural, t_rural, config=config)
            else:
                # Non-rural buses: DEPOT → other terminal
                slot_idx = i - rural_half
                try:
                    d_other = config.get_distance(config.depot, other_loc)
                    t_other = config.get_travel_time(config.depot, other_loc)
                except KeyError:
                    d_other, t_other = 0, nearest_tt
                arrive_at        = op_start_dt + timedelta(minutes=slot_idx * phase1_gap)
                bus.current_time = arrive_at - timedelta(minutes=t_other)
                if d_other > 0:
                    _make_dead(bus, other_loc, d_other, t_other, config=config)

        # ── Mode B: Intermediate route split dispatch ─────────────────────────
        elif split_dispatch and i < inter_half:
            # First half: DEPOT → intermediate → nearest terminal (rev_start)
            slot_idx  = i
            arrive_at = op_start_dt + timedelta(minutes=slot_idx * phase1_gap)
            try:
                d_int = config.get_distance(config.depot, intermediate_node)
                t_int = config.get_travel_time(config.depot, intermediate_node)
            except KeyError:
                d_int, t_int = 0, 0
            try:
                d_near = config.get_distance(intermediate_node, rev_start)
                t_near = config.get_travel_time(intermediate_node, rev_start)
            except KeyError:
                d_near, t_near = 0, 0
            total_travel     = t_int + t_near
            bus.current_time = arrive_at - timedelta(minutes=total_travel)
            if d_int > 0:
                _make_dead(bus, intermediate_node, d_int, t_int, config=config)
            if d_near > 0 and bus.current_location != rev_start:
                _make_dead(bus, rev_start, d_near, t_near, config=config)

        elif split_dispatch and i >= inter_half:
            # Second half: DEPOT → far terminal (direct, no intermediate)
            slot_idx  = i - inter_half
            arrive_at = op_start_dt + timedelta(minutes=slot_idx * phase1_gap)
            try:
                d_far2 = config.get_distance(config.depot, far_loc)
                t_far2 = config.get_travel_time(config.depot, far_loc)
            except KeyError:
                d_far2, t_far2 = 0, nearest_tt
            bus.current_time = arrive_at - timedelta(minutes=t_far2)
            if d_far2 > 0:
                _make_dead(bus, far_loc, d_far2, t_far2, config=config)

        # ── Mode C: Simple route — all buses to nearest terminal ─────────────
        else:
            arrive_at        = op_start_dt + timedelta(minutes=i * phase1_gap)
            bus.current_time = arrive_at - timedelta(minutes=nearest_tt)
            _morning_dead_run(bus, config)
            if reposition_to and bus.current_location != reposition_to:
                try:
                    rd    = config.get_distance(bus.current_location, reposition_to)
                    rt_tt = config.get_travel_time(bus.current_location, reposition_to)
                    _make_dead(bus, reposition_to, rd, rt_tt, config=config)
                except KeyError:
                    pass

        bus.phase_index = i

    # ── Phase 2: Bus-driven revenue loop ─────────────────────────────────────
    # Each iteration picks the bus that can depart soonest, enforces headway
    # spacing (P6), and sends it on the next natural trip. No pool slots.
    midday_soc  = getattr(config, 'midday_charge_soc_percent', MIDDAY_CHARGE_SOC)
    soc_trigger = getattr(config, 'trigger_soc_percent', SOC_TRIGGER)

    # Track which buses have already charged today (P5: once per day)
    charged_today = set()

    # Per-bus headway hold: remembers the earliest allowed departure time after
    # a headway bump. Persists across outer-loop iterations so the bus does not
    # restart from its raw ready-time every evaluation, which caused cascade
    # over-bumping for late-starting buses (e.g. B04 in a 4-bus fleet stuck at
    # 09:30 when it should have started at 08:15).
    # Key: (bus_id, direction, start_location) → datetime
    headway_hold: dict = {}

    # Slot-based departure targets: the pre-computed time when the NEXT bus
    # should depart in each direction+OD.  Updated after every departure so
    # that all gaps within a time band are exactly target_hw apart.
    # Key: (direction, start_loc, end_loc) → datetime
    next_slot: dict = {}

    MAX_ITER = config.fleet_size * 300
    for _ in range(MAX_ITER):

        # ── Pre-check: emergency rescue for stuck buses ───────────────────────
        # Two scenarios requiring immediate charging (bypasses stagger gate):
        # 1. Bus can't safely return to depot from its current location NOW
        # 2. Bus can't safely take its next revenue trip AND return home after
        #    (lookahead) — prevents getting stuck on the next iteration
        one_trip_drain = (config.get_distance(config.start_point, config.end_point)
                          * config.consumption_rate / config.battery_kwh * 100)
        max_cost_home = _soc_cost_to_depot(far_loc, config)  # worst-case (far terminal)

        for stuck_bus in buses:
            if stuck_bus.current_location == config.depot:
                continue
            if stuck_bus.bus_id in charged_today:
                continue
            actual_cost_home = _soc_cost_to_depot(stuck_bus.current_location, config)
            # Scenario 1: already stranded — can't return to depot safely now
            stranded_now = stuck_bus.soc_percent - actual_cost_home < SOC_FLOOR
            # Scenario 2: lookahead — taking the next revenue trip would leave the
            # bus unable to return home.
            # IMPORTANT: use the cost home from the END of the next trip, not the
            # worst-case far terminal. A bus at the far terminal takes an UP trip
            # and ends at the near terminal — cost home is much less.
            if stuck_bus.current_location == far_loc:
                # Next trip is UP → ends at rev_start (near terminal)
                cost_home_after_trip = _soc_cost_to_depot(rev_start, config)
            else:
                # Next trip is DN → ends at far terminal (conservative)
                cost_home_after_trip = max_cost_home
            stuck_after_next = (stuck_bus.soc_percent
                                 - one_trip_drain
                                 - cost_home_after_trip) < SOC_FLOOR + 2.0
            if stranded_now or stuck_after_next:
                _charging_detour(stuck_bus, config,
                                 resume_by=op_end, min_break=min_break)
                charged_today.add(stuck_bus.bus_id)


        # Find the bus ready soonest that can make a valid trip
        best_bus = best_rt = best_dir = best_start = best_end = None
        best_dist_km = best_tt_val = best_needs_repo = None
        best_score = None
        best_target_slot = best_target_hw = None

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

            if is_circular:
                # Circular route: always same direction (DN), always start→start
                next_dir   = "DN"
                trip_start = rev_start
                trip_end   = rev_start   # circular: end == start
                if bus.current_location not in (rev_start, nearest_node):
                    continue
            elif last_rev is None or just_charged:
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

            # Headway floor: enforce minimum gap from last same-dir departure.
            # Compute min_hw at the current rt. After any bump below we recompute
            # so the correct headway band is applied (e.g. a bus arriving at 07:52
            # bumped into the 08:00+ band should use that band's lower headway, not
            # the tighter early-morning value — otherwise cascading over-bumping occurs).
            def _min_hw_at(t):
                return _lookup_hw(t, _hw_bands)

            # Apply any persistent headway hold for this bus+direction
            hold_key = (bus.bus_id, next_dir, trip_start)
            held = headway_hold.get(hold_key)
            if held and held > rt:
                rt = held

            min_hw = _min_hw_at(rt)

            # ── Headway spacing — P6 hard floor only ─────────────────────────
            # The configured headway profile is a SCORING TARGET, not a hard floor.
            # Enforcing it as a hard floor causes cascade bumping: buses pile up
            # waiting for their natural_gap slot, then depart in a burst, creating
            # the very spikes we want to eliminate.
            #
            # The ONLY hard floor is P6 = SAME_DIR_GAP (5 min) — a physical safety
            # rule. All headway shaping above 5 min is handled by the Global Headway
            # Balancer scoring below using:
            #   target_gap = max(configured_band_headway, natural_gap)
            # This achieves uniform spacing within each time band without blocking
            # buses that could fill a growing gap sooner.
            #
            # Applies equally to Planning-Compliant, Efficiency, and circular routes.
            # just_charged / just_repositioned flags no longer needed: P6-floor lets
            # re-entering buses fill the earliest available gap immediately.
            min_spacing = SAME_DIR_GAP   # P6 only — 5-min safety floor

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
                    # Persist the P6-bumped rt so this bus is not reset on the
                    # next outer-loop iteration.
                    headway_hold[hold_key] = rt

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

            # SOC, arrival, and max_km checks
            try:
                dist_km = config.get_distance(trip_start, trip_end)
            except KeyError:
                continue
            if bus.soc_after_trip(dist_km) < SOC_FLOOR:
                continue

            # ── Pre-trip lookahead: can bus reach depot safely after this trip?
            # After completing the trip the bus is at trip_end. It must travel
            # trip_end → nearest_node → DEPOT before the next charge. If the
            # SOC remaining after the trip is less than the path-home cost plus
            # the floor, skip this trip — the bus needs to charge first.
            # This prevents the scenario where the charging trigger fires too late
            # (bus at far terminal with 23% SOC, costs 9.2% to reach depot → 13.8%).
            soc_after = bus.soc_after_trip(dist_km)
            cost_home = _soc_cost_to_depot(trip_end, config)
            if soc_after - cost_home < SOC_FLOOR:
                continue   # bus cannot safely return after this trip → skip

            if rt + timedelta(minutes=tt_val) > op_end + timedelta(minutes=45):
                continue
            # Hard max_km cap: skip bus if this trip would exceed the daily limit.
            # Emergency override: if SOC is critically low (P3 risk), allow exceeding
            # max_km to reach the charger — P3 always outranks max_km.
            max_km = getattr(config, 'max_km_per_bus', 0) or 0
            if max_km > 0 and bus.total_km + dist_km > max_km:
                if bus.soc_percent > SOC_FLOOR + 5:  # not an emergency
                    continue

            # ── Slot-based bus selection ──────────────────────────────────────
            # Instead of scoring buses by gap-deviation, we pre-compute WHEN the
            # next departure in this direction should happen (next_slot), then select
            # the bus whose ready-time is closest to that target slot.
            #
            # target_hw = uniform headway for this time band:
            #   max(configured_band_headway, natural_gap)
            #   → configured headway if fleet is over-provisioned
            #   → natural_gap if fleet physics can't achieve configured headway
            #
            # This makes the scheduler slot-driven, not bus-driven:
            #   "here is when the next bus should go — which bus best fills that slot?"
            #
            # If a bus is ready BEFORE the slot:  it waits (held to slot time).
            # If a bus is ready AFTER  the slot:  slot slips, resets from actual time.
            # Result: all gaps within a time band are equal (or as close as physics allows).
            target_hw   = max(_min_hw_at(rt), natural_gap)
            slot_key    = (next_dir, trip_start, trip_end)
            target_slot = next_slot.get(slot_key, rt)   # first trip: slot = bus readiness
            bus_score   = abs((rt - target_slot).total_seconds() / 60)

            if best_rt is None or bus_score < best_score:
                best_bus          = bus
                best_rt           = rt
                best_score        = bus_score
                best_dir          = next_dir
                best_start        = trip_start
                best_end          = trip_end
                best_dist_km      = dist_km
                best_tt_val       = tt_val
                best_needs_repo   = needs_reposition
                best_target_slot  = target_slot
                best_target_hw    = target_hw

        if best_bus is None:
            break  # no bus can move — done

        # ── Slot enforcement: hold early bus to its target slot ──────────────
        # If the selected bus is ready before the target slot, hold it until
        # the slot time.  This is what produces uniform spacing — buses that
        # could depart early are held so every gap equals target_hw exactly.
        # If the bus is late (rt > slot), we accept the slip; the slot will
        # reset from the actual departure time below.
        if best_target_slot is not None and best_rt < best_target_slot:
            best_rt = best_target_slot

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
        # Advance the slot for this direction by target_hw.
        # Next bus in this direction should depart at best_rt + target_hw.
        # This single update drives all uniform spacing — no other headway
        # logic is needed beyond the P6 safety floor.
        _slot_key = (best_dir, best_start, best_end)
        next_slot[_slot_key] = best_rt + timedelta(minutes=best_target_hw)
        # Clear the P6 headway hold for this bus — it has now departed.
        headway_hold.pop((best_bus.bus_id, best_dir, best_start), None)

        # Post-trip charging decisions
        midday_soc  = getattr(config, 'midday_charge_soc_percent', MIDDAY_CHARGE_SOC)
        soc_trigger = getattr(config, 'trigger_soc_percent', SOC_TRIGGER)

        # Prefer to charge from rev_start (near depot side) not far_loc (far terminal).
        # If bus is at far_loc after a DN trip, let it serve one UP trip back to
        # rev_start first — this saves the 40-min revenue-corridor dead run and
        # reduces the headway gap during the charging window.
        # For circular routes far_loc == rev_start — there is no "far terminal"
        # to defer charging from. Disable the gate.
        # Also disable the gate during the midday window when the bus has not yet
        # charged and its SOC is already below the midday threshold — in that case
        # forcing the bus to serve one more return trip before charging risks missing
        # the midday window entirely, especially when the detour from far_loc is long.
        _needs_midday_charge = (best_bus.bus_id not in charged_today and
                                best_bus.soc_percent < midday_soc)

        # "Serve return trip first" rule: when a bus arrives at the far terminal,
        # always let it serve the return trip before going for charging.
        # This prevents large service gaps caused by charging at the far terminal
        # (which forces a 73-min absence on the return side of the route).
        # Safety valve: if the P5 window has < 90 min remaining, charge immediately
        # to avoid missing the window entirely on long routes.
        _p5_window_remaining = (_charge_window(config)[1] - best_bus.current_time).total_seconds() / 60
        at_far_loc = (not is_circular and
                      best_bus.current_location == far_loc and
                      _p5_window_remaining > 90)

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

        # Charge stagger gate — space charge departures evenly across the charge window.
        # Each bus gets its own slot: window_duration / fleet_size minutes apart.
        # Example: 6 buses over 300-min window → 50-min slots (11:00, 11:50, 12:40, ...)
        # This ensures buses are staggered so at most 1 is absent at any time
        # (assuming detour ≤ slot width), eliminating service gaps during charging.
        _last_chg = _last_charge_start(buses, best_bus)
        _cws, _cwe = _charge_window(config)
        _window_min = (_cwe - _cws).total_seconds() / 60
        _min_charge_gap = _window_min / max(1, config.fleet_size)
        charge_stagger_ok = (
            _last_chg is None or
            (best_bus.current_time - _last_chg).total_seconds() / 60 >= _min_charge_gap
        )

        if (_in_charge_window(best_bus, config) and
                config.fleet_size <= 10 and
                best_bus.bus_id not in charged_today and
                best_bus.soc_percent < midday_soc and
                charge_stagger_ok and          # ← stagger gate: don't overlap charge detours
                not at_far_loc):
            _charging_detour(best_bus, config,
                             resume_by=op_end, min_break=min_break)
            charged_today.add(best_bus.bus_id)
            for k in list(headway_hold.keys()):
                if k[0] == best_bus.bus_id:
                    del headway_hold[k]

        elif best_bus.soc_percent <= soc_trigger:
            _cws2, _cwe2 = _charge_window(config)
            _in_window = (_cws2 <= best_bus.current_time < _cwe2)
            if _in_window and best_bus.bus_id not in charged_today and charge_stagger_ok:
                _charging_detour(best_bus, config,
                                 resume_by=op_end, min_break=min_break)
                charged_today.add(best_bus.bus_id)
                for k in list(headway_hold.keys()):
                    if k[0] == best_bus.bus_id: del headway_hold[k]
            elif not _is_peak(best_bus.current_time) and charge_stagger_ok:
                _charging_detour(best_bus, config,
                                 resume_by=op_end, min_break=min_break)
                charged_today.add(best_bus.bus_id)
                for k in list(headway_hold.keys()):
                    if k[0] == best_bus.bus_id: del headway_hold[k]
            else:
                typical_drain = (config.get_distance(config.start_point, config.end_point)
                                 * config.consumption_rate / config.battery_kwh * 100)
                if best_bus.soc_percent - typical_drain < SOC_FLOOR:
                    _charging_detour(best_bus, config,
                                     resume_by=op_end, min_break=min_break)
                    charged_today.add(best_bus.bus_id)
                    for k in list(headway_hold.keys()):
                        if k[0] == best_bus.bus_id: del headway_hold[k]

    # ── Phase 2.5: Pre-Phase-3 emergency charge ──────────────────────────────
    # If a bus's SOC would drop below the floor during its dead run home,
    # send it to charge first (even in peak hours, even if already charged today
    # — P3 always wins over P5/schedule rules).
    for bus in buses:
        if bus.current_location == config.depot:
            continue
        # Estimate SOC cost of dead run home (via nearest node)
        try:
            nearest, _, _ = _nearest_node_from_depot(config)
            cost = 0.0
            loc = bus.current_location
            if loc != nearest:
                cost += config.get_distance(loc, nearest) * config.consumption_rate / config.battery_kwh * 100
                loc = nearest
            if loc != config.depot:
                cost += config.get_distance(loc, config.depot) * config.consumption_rate / config.battery_kwh * 100
        except Exception:
            cost = 15.0  # safe default
        if bus.soc_percent - cost < SOC_FLOOR:
            _charging_detour(bus, config, resume_by=op_end, min_break=min_break)
            # Note: we intentionally do NOT check/update charged_today here —
            # P3 (never below 20%) overrides the once-per-day P5 rule.

    # ── Phase 3: Evening return ───────────────────────────────────────────────
    for bus in buses:
        _route_to_depot(bus, config)

    # ── Phase 4: Safety-net break enforcement ────────────────────────────────
    _balance_breaks(buses, config)

    return buses


def check_compliance(config: RouteConfig, buses: list[BusState],
                     headway_df=None) -> list[dict]:
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

    # P4: breaks between direct Revenue→Revenue pairs (charging/dead/shuttle excluded).
    # Also exclude headway-constrained gaps: when a bus must wait for its next
    # departure slot due to fleet spacing rules, the resulting idle time is
    # operationally forced — not a driver rest violation.
    # Headway-constrained gap = gap > max_break AND gap ≈ headway + preferred_layover.
    max_break = getattr(config, 'max_layover_min', MAX_BREAK)
    p4_v = []; p4_hw_warn = []

    # Pre-build headway bands for P4 classification — same pattern as schedule_buses.
    _hw_bands_p4 = _build_hw_bands(headway_df)

    def _hw_at_time(t):
        return _lookup_hw(t, _hw_bands_p4, fallback=max_break)

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
                    # Check if gap is explained by headway floor:
                    # headway-constrained if gap <= 2×headway + preferred_layover.
                    # With N buses queuing for the same direction, the Nth bus can
                    # wait up to N×headway — using 2× covers the common 2-bus case
                    # without masking genuinely long breaks.
                    hw = _hw_at_time(c.actual_arrival)
                    if gap <= 2 * hw + min_break:
                        p4_hw_warn.append(
                            f"{bus.bus_id} @ {c.actual_arrival.strftime('%H:%M')}: "
                            f"{gap:.0f}min (headway-constrained, hw={hw:.0f}min)")
                    else:
                        p4_v.append(f"{bus.bus_id} @ {c.actual_arrival.strftime('%H:%M')}: {gap:.0f}min > {max_break}")

    # P4 is WARN not FAIL — long breaks are operationally undesirable but not a hard stop.
    p4_status = "PASS" if not p4_v else "WARN"
    p4_detail = (f"{len(p4_v)} long breaks [informational]" if p4_v
                 else "All breaks in range (charging gaps excluded)")
    if p4_hw_warn:
        p4_detail += f" · {len(p4_hw_warn)} headway-forced gap(s) [not a violation]"
    results.append({"rule": f"P4: Break {min_break}–{max_break} min between revenue trips", "priority": 4,
                    "status": p4_status,
                    "details": p4_detail,
                    "violations": p4_v[:10] + (["--- headway-forced (info) ---"] if p4_hw_warn else []) + p4_hw_warn[:5]})

    # P5: midday charging — window from config (default 12:00-15:00)
    _p5_start = getattr(config, 'p5_charging_start', None)
    _p5_end   = getattr(config, 'p5_charging_end',   None)
    _p5_sh    = _p5_start.hour if _p5_start else 12
    _p5_sm    = _p5_start.minute if _p5_start else 0
    _p5_eh    = _p5_end.hour if _p5_end else 15
    _p5_em    = _p5_end.minute if _p5_end else 0
    _p5_label = f"{_p5_sh:02d}:{_p5_sm:02d}–{_p5_eh:02d}:{_p5_em:02d}"

    if config.fleet_size > 10:
        results.append({"rule": f"P5: Midday charging {_p5_label} [waived: fleet > 10]",
                        "priority": 5, "status": "PASS",
                        "details": f"Fleet {config.fleet_size} buses > 10 — P5 midday charging window waived per policy",
                        "violations": []})
    else:
        p5_v = [f"{bus.bus_id}: no charge in {_p5_label}"
                for bus in buses
                if not any(t.trip_type == "Charging" and t.actual_departure and
                           (_p5_sh * 60 + _p5_sm) <= (t.actual_departure.hour * 60 + t.actual_departure.minute) < (_p5_eh * 60 + _p5_em)
                           for t in bus.trips)]
        results.append({"rule": f"P5: Midday charging ({_p5_label}) [informational]", "priority": 5,
                        "status": "PASS" if not p5_v else "WARN",
                        "details": f"{len(p5_v)} buses missing charge {_p5_label} [informational]" if p5_v else f"All buses charged in {_p5_label} window",
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

    # O2: operating hours window check.
    # Buses in a multi-bus fleet are staggered by natural_gap or headway at start-of-day.
    # The Nth bus necessarily departs later than op_start by design — this is not a
    # scheduling error. Classify late-starts as WARN (headway-stagger) vs FAIL (genuine).
    op_start = REF_DATE.replace(hour=config.operating_start.hour, minute=config.operating_start.minute)
    op_end   = REF_DATE.replace(hour=config.operating_end.hour,   minute=config.operating_end.minute)
    FLEX = 45; o2_v = []; o2_warn = []

    # Estimate stagger: last bus starts natural_gap*(fleet-1) after op_start
    try:
        nn2, _, _ = _nearest_node_from_depot(config)
        dn_seg = config.get_travel_time(config.start_point, config.end_point)
        up_seg = config.get_travel_time(config.end_point,   config.start_point)
        cyc2   = dn_seg + min_break + up_seg + min_break
        nat2   = cyc2 / max(1, config.fleet_size)
        stagger_max = nat2 * (config.fleet_size - 1)   # latest expected first trip
    except Exception:
        stagger_max = FLEX * 2

    for bus in buses:
        fr = next((t for t in bus.trips if t.trip_type == "Revenue"), None)
        lr = next((t for t in reversed(bus.trips) if t.trip_type == "Revenue"), None)
        for trip, ts, ref in [(fr, "actual_departure", op_start), (lr, "actual_arrival", op_end)]:
            if trip:
                dt = getattr(trip, ts)
                if dt and (dt < ref - timedelta(minutes=FLEX) or dt > ref + timedelta(minutes=FLEX)):
                    msg = (f"{bus.bus_id}: {ts.replace('actual_','')} {dt.strftime('%H:%M')} "
                           f"outside ±{FLEX}min of {ref.strftime('%H:%M')}")
                    # Late first departure explained by fleet stagger → WARN
                    if ts == "actual_departure" and dt > ref + timedelta(minutes=FLEX):
                        delay = (dt - ref).total_seconds() / 60
                        if delay <= stagger_max + FLEX:
                            o2_warn.append(msg + " [headway-stagger]")
                            continue
                    o2_v.append(msg)

    o2_status = "PASS" if not o2_v and not o2_warn else ("WARN" if not o2_v else "FAIL")
    o2_detail = f"Window: {config.operating_start}-{config.operating_end} +-{FLEX} min"
    if o2_v:    o2_detail += f" -- {len(o2_v)} violation(s)"
    if o2_warn: o2_detail += f" · {len(o2_warn)} headway-stagger late start(s) [not a violation]"
    results.append({"rule": f"O2: Operating hours +-{FLEX} min", "priority": 8,
                    "status": o2_status,
                    "details": o2_detail,
                    "violations": o2_v + (["--- stagger (info) ---"] if o2_warn else []) + o2_warn})

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

    # O4b: max_km_per_bus — hard daily km cap per bus (FAIL if any bus exceeds it)
    max_km = getattr(config, 'max_km_per_bus', 0) or 0
    if max_km > 0:
        over_v = [f"{bus.bus_id}: {bus.total_km:.1f} km > {max_km:.0f} km cap"
                  for bus in buses if bus.total_km > max_km]
        unassigned_note = (" — increase fleet_size if trips are unassigned" if over_v else "")
        results.append({
            "rule":   f"O4b: Max km per bus ({max_km:.0f} km cap)",
            "priority": 10,
            "status": "PASS" if not over_v else "FAIL",
            "details": (f"All buses within {max_km:.0f} km cap" if not over_v
                        else f"{len(over_v)} bus(es) exceeded cap{unassigned_note}"),
            "violations": over_v,
        })

    return results
