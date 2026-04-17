"""
depot_model.py — Full depot capacity model for eBus Scheduler.

Simulates charger queue (slow + fast), parking capacity, and peak electrical
load from a completed schedule. Returns DepotLog dataclass for dashboard
display and recommender input.

Usage:
    from src.depot_model import simulate_depot, DepotLog
    depot_log = simulate_depot(buses, config)
"""

from __future__ import annotations
__version__ = "2026-04-17-p2"

from dataclasses import dataclass, field
from datetime import datetime, timedelta
import heapq


@dataclass
class ChargingEvent:
    """Single charging event at the depot."""
    bus_id: str
    arrival_time: datetime
    requested_duration_min: float
    charger_type: str = "slow"          # "slow" | "fast"
    actual_start: datetime | None = None
    actual_end: datetime | None = None
    wait_min: float = 0.0


@dataclass
class DepotLog:
    """Complete depot simulation output."""
    events: list[ChargingEvent] = field(default_factory=list)

    # Queue metrics
    peak_queue_depth_slow: int = 0
    peak_queue_depth_fast: int = 0
    total_wait_min_slow: float = 0.0
    total_wait_min_fast: float = 0.0
    buses_that_waited: int = 0

    # Utilisation (fraction of slot-minutes occupied)
    utilisation_pct_slow: float = 0.0
    utilisation_pct_fast: float = 0.0

    # Parking
    peak_parking_count: int = 0         # max buses at depot simultaneously

    # Electrical load
    peak_electrical_load_kw: float = 0.0
    load_profile_kw: list[dict] = field(default_factory=list)  # [{time_bin, load_kw}]

    # Throughput
    throughput_per_hour: float = 0.0    # buses processed / hour

    @property
    def has_congestion(self) -> bool:
        return self.buses_that_waited > 0

    @property
    def utilisation_pct(self) -> float:
        """Combined utilisation across all charger types."""
        return max(self.utilisation_pct_slow, self.utilisation_pct_fast)

    def summary(self) -> str:
        lines = [
            f"Charging events: {len(self.events)}",
            f"Peak queue: slow={self.peak_queue_depth_slow}, fast={self.peak_queue_depth_fast}",
            f"Total wait: slow={self.total_wait_min_slow:.0f} min, fast={self.total_wait_min_fast:.0f} min",
            f"Buses that waited: {self.buses_that_waited}",
            f"Utilisation: slow={self.utilisation_pct_slow:.1f}%, fast={self.utilisation_pct_fast:.1f}%",
            f"Peak parking: {self.peak_parking_count}",
            f"Peak load: {self.peak_electrical_load_kw:.0f} kW",
            f"Throughput: {self.throughput_per_hour:.1f} buses/hr",
        ]
        return "\n".join(lines)


def simulate_depot(
    buses: list,
    config,
    slots_slow: int = 0,            # 0 = unlimited
    slots_fast: int = 0,            # 0 = no fast chargers
    fast_charger_kw: float = 0.0,   # fast charger power
    parking_capacity: int = 0,      # 0 = unlimited
    bin_minutes: int = 15,          # load profile granularity
) -> DepotLog:
    """
    Simulate depot operations from a completed schedule.

    Extracts all Charging trips from buses, runs them through a FIFO
    queue model with configurable slot counts, and computes utilisation,
    parking, and electrical load metrics.

    Args:
        buses:       list[BusState] with completed trip histories
        config:      RouteConfig for charger specs
        slots_slow:  max slow charger slots (0 = unlimited)
        slots_fast:  max fast charger slots (0 = none)
        fast_charger_kw: fast charger power in kW
        parking_capacity: max buses at depot (0 = unlimited)
        bin_minutes: load profile bin size in minutes
    """
    log = DepotLog()

    # ── Extract charging events from schedule ────────────────────────────
    raw_events: list[ChargingEvent] = []
    for bus in buses:
        for trip in bus.trips:
            if trip.trip_type == "Charging" and trip.actual_departure is not None:
                duration = trip.travel_time_min
                raw_events.append(ChargingEvent(
                    bus_id=bus.bus_id,
                    arrival_time=trip.actual_departure,
                    requested_duration_min=duration,
                ))

    if not raw_events:
        return log

    raw_events.sort(key=lambda e: e.arrival_time)

    # ── Determine charger type per event ─────────────────────────────────
    slow_kw = getattr(config, "depot_charger_kw", 60) * getattr(config, "depot_charger_efficiency", 0.85)
    fast_kw_eff = fast_charger_kw * getattr(config, "depot_charger_efficiency", 0.85) if fast_charger_kw > 0 else 0

    for ev in raw_events:
        # Assign to fast charger if available and charge time > 20 min
        if slots_fast > 0 and ev.requested_duration_min > 20:
            ev.charger_type = "fast"
        else:
            ev.charger_type = "slow"

    # ── FIFO queue simulation ────────────────────────────────────────────
    # Priority queue of (release_time, slot_index) per charger type
    slow_slots: list[datetime] = []  # heap of release times
    fast_slots: list[datetime] = []

    max_slow = slots_slow if slots_slow > 0 else 999
    max_fast = slots_fast if slots_fast > 0 else 0

    current_queue_slow = 0
    current_queue_fast = 0

    for ev in raw_events:
        if ev.charger_type == "fast" and max_fast > 0:
            slots_heap = fast_slots
            max_s = max_fast
        else:
            slots_heap = slow_slots
            max_s = max_slow
            ev.charger_type = "slow"  # fallback if no fast

        if len(slots_heap) < max_s:
            # Slot available immediately
            ev.actual_start = ev.arrival_time
            ev.wait_min = 0.0
        else:
            # Wait for earliest slot to free
            earliest_free = heapq.heappop(slots_heap)
            if earliest_free > ev.arrival_time:
                ev.actual_start = earliest_free
                ev.wait_min = (earliest_free - ev.arrival_time).total_seconds() / 60.0
            else:
                ev.actual_start = ev.arrival_time
                ev.wait_min = 0.0

        ev.actual_end = ev.actual_start + timedelta(minutes=ev.requested_duration_min)
        heapq.heappush(slots_heap, ev.actual_end)

        # Track queue depth
        if ev.charger_type == "slow":
            q_depth = max(0, len([e for e in raw_events
                                  if e.charger_type == "slow"
                                  and e.arrival_time <= ev.arrival_time
                                  and (e.actual_end is None or e.actual_end > ev.arrival_time)])
                         - max_slow)
            log.peak_queue_depth_slow = max(log.peak_queue_depth_slow, q_depth)
            if ev.wait_min > 0:
                log.total_wait_min_slow += ev.wait_min
        else:
            q_depth = max(0, len([e for e in raw_events
                                  if e.charger_type == "fast"
                                  and e.arrival_time <= ev.arrival_time
                                  and (e.actual_end is None or e.actual_end > ev.arrival_time)])
                         - max_fast)
            log.peak_queue_depth_fast = max(log.peak_queue_depth_fast, q_depth)
            if ev.wait_min > 0:
                log.total_wait_min_fast += ev.wait_min

        if ev.wait_min > 0:
            log.buses_that_waited += 1

    log.events = raw_events

    # ── Utilisation ──────────────────────────────────────────────────────
    if raw_events:
        first_arrival = min(e.arrival_time for e in raw_events)
        last_departure = max(e.actual_end for e in raw_events if e.actual_end)
        window_min = max(1, (last_departure - first_arrival).total_seconds() / 60.0)

        slow_occupied = sum(e.requested_duration_min for e in raw_events if e.charger_type == "slow")
        fast_occupied = sum(e.requested_duration_min for e in raw_events if e.charger_type == "fast")

        if max_slow > 0:
            log.utilisation_pct_slow = round(slow_occupied / (max_slow * window_min) * 100, 1)
        if max_fast > 0:
            log.utilisation_pct_fast = round(fast_occupied / (max_fast * window_min) * 100, 1)

        # Throughput
        hours = window_min / 60.0
        log.throughput_per_hour = round(len(raw_events) / max(0.1, hours), 1)

    # ── Parking occupancy ────────────────────────────────────────────────
    # Count all buses at depot at each event boundary
    all_depot_times: list[tuple[datetime, int]] = []  # (time, +1 arrive / -1 depart)
    for bus in buses:
        for trip in bus.trips:
            if trip.actual_departure is None or trip.actual_arrival is None:
                continue
            if trip.end_location == getattr(config, "depot", "DEPOT"):
                all_depot_times.append((trip.actual_arrival, +1))
            if trip.start_location == getattr(config, "depot", "DEPOT"):
                all_depot_times.append((trip.actual_departure, -1))

    all_depot_times.sort(key=lambda x: x[0])
    current_at_depot = 0
    for _, delta in all_depot_times:
        current_at_depot += delta
        log.peak_parking_count = max(log.peak_parking_count, current_at_depot)

    # ── Electrical load profile ──────────────────────────────────────────
    if raw_events:
        ref = min(e.actual_start for e in raw_events if e.actual_start)
        end = max(e.actual_end for e in raw_events if e.actual_end)
        t = ref
        while t < end:
            bin_end = t + timedelta(minutes=bin_minutes)
            # Count active chargers in this bin
            active_slow = sum(1 for e in raw_events
                            if e.charger_type == "slow"
                            and e.actual_start and e.actual_end
                            and e.actual_start < bin_end and e.actual_end > t)
            active_fast = sum(1 for e in raw_events
                            if e.charger_type == "fast"
                            and e.actual_start and e.actual_end
                            and e.actual_start < bin_end and e.actual_end > t)
            load = active_slow * slow_kw + active_fast * fast_kw_eff
            log.load_profile_kw.append({
                "time_bin": t.strftime("%H:%M"),
                "load_kw": round(load, 1),
                "active_slow": active_slow,
                "active_fast": active_fast,
            })
            log.peak_electrical_load_kw = max(log.peak_electrical_load_kw, load)
            t = bin_end

    return log
