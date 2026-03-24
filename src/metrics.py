"""
metrics.py — Computes schedule quality KPIs.

Usage:
    from src.metrics import compute_metrics
    m = compute_metrics(config, buses)
    print(m)
    print(f"Score: {m.weighted_score():.3f}")
"""

from __future__ import annotations
__version__ = "2026-03-24-b1"  # auto-stamped

from dataclasses import dataclass, field
from src.models import BusState, RouteConfig


@dataclass
class ScheduleMetrics:
    """KPI summary for a complete schedule. Lower score = better."""

    # Core metrics
    revenue_trips_assigned: int = 0
    revenue_trips_total: int = 0
    total_km: float = 0.0
    revenue_km: float = 0.0
    dead_km: float = 0.0
    dead_km_ratio: float = 0.0          # dead_km / total_km

    # Fleet balance
    km_per_bus: list[float] = field(default_factory=list)
    km_range: float = 0.0               # max - min bus km
    km_std: float = 0.0                 # standard deviation of bus km
    trips_per_bus: list[int] = field(default_factory=list)

    # Break quality
    breaks_at_minimum: int = 0          # count of breaks exactly at min_layover
    avg_break_min: float = 0.0
    negative_breaks: int = 0            # should always be 0

    # SOC
    min_soc_seen: float = 100.0         # lowest SOC any bus reached
    buses_below_trigger: int = 0        # buses that dropped below trigger SOC
    charging_stops: int = 0

    # Optimizer weights (configurable)
    WEIGHTS: dict = field(default_factory=lambda: {
        "dead_km_ratio": 0.25,
        "km_range_norm": 0.35,          # normalised by avg km
        "breaks_at_minimum_pct": 0.15,
        "soc_penalty": 0.25,
    }, repr=False)

    def weighted_score(self) -> float:
        """Single number for optimizer. Lower = better."""
        w = self.WEIGHTS
        avg_km = sum(self.km_per_bus) / len(self.km_per_bus) if self.km_per_bus else 1.0
        km_range_norm = self.km_range / avg_km if avg_km > 0 else 0.0

        total_breaks = self.breaks_at_minimum + max(1, len(self.km_per_bus))
        breaks_pct = self.breaks_at_minimum / total_breaks

        # SOC penalty: higher if buses go below min_soc or many below trigger
        soc_penalty = (100 - self.min_soc_seen) / 100 + self.buses_below_trigger * 0.1

        return (
            w["dead_km_ratio"] * self.dead_km_ratio
            + w["km_range_norm"] * km_range_norm
            + w["breaks_at_minimum_pct"] * breaks_pct
            + w["soc_penalty"] * soc_penalty
        )

    def summary(self) -> str:
        lines = [
            f"Revenue trips: {self.revenue_trips_assigned}/{self.revenue_trips_total}",
            f"Total km: {self.total_km:.1f} (revenue: {self.revenue_km:.1f}, dead: {self.dead_km:.1f})",
            f"Dead km ratio: {self.dead_km_ratio:.1%}",
            f"KM per bus: {', '.join(f'{k:.1f}' for k in self.km_per_bus)}",
            f"KM range: {self.km_range:.1f} (std: {self.km_std:.1f})",
            f"Breaks at minimum: {self.breaks_at_minimum}",
            f"Min SOC seen: {self.min_soc_seen:.1f}%",
            f"Charging stops: {self.charging_stops}",
            f"Weighted score: {self.weighted_score():.4f}",
        ]
        return "\n".join(lines)


def compute_metrics(
    config: RouteConfig,
    buses: list[BusState],
    total_revenue_trips: int | None = None,
) -> ScheduleMetrics:
    """
    Compute all KPIs from a completed schedule.
    """
    m = ScheduleMetrics()

    all_trips = []
    for bus in buses:
        all_trips.extend(bus.trips)

    rev = [t for t in all_trips if t.trip_type == "Revenue"]
    dead = [t for t in all_trips if t.trip_type == "Dead"]
    chg = [t for t in all_trips if t.trip_type == "Charging"]

    m.revenue_trips_assigned = len(rev)
    m.revenue_trips_total = total_revenue_trips or len(rev)
    m.revenue_km = sum(t.distance_km for t in rev)
    m.dead_km = sum(t.distance_km for t in dead)
    m.total_km = m.revenue_km + m.dead_km
    m.dead_km_ratio = m.dead_km / m.total_km if m.total_km > 0 else 0.0
    m.charging_stops = len(chg)

    # Per-bus metrics
    m.km_per_bus = [bus.total_km for bus in buses]
    m.trips_per_bus = [len(bus.trips) for bus in buses]

    if m.km_per_bus:
        m.km_range = max(m.km_per_bus) - min(m.km_per_bus)
        avg = sum(m.km_per_bus) / len(m.km_per_bus)
        m.km_std = (sum((k - avg) ** 2 for k in m.km_per_bus) / len(m.km_per_bus)) ** 0.5

    # Break analysis
    for bus in buses:
        for i in range(len(bus.trips) - 1):
            curr = bus.trips[i]
            nxt = bus.trips[i + 1]
            if curr.actual_arrival and nxt.actual_departure:
                gap = (nxt.actual_departure - curr.actual_arrival).total_seconds() / 60
                if gap < 0:
                    m.negative_breaks += 1
                elif abs(gap - config.min_layover_min) < 1:
                    m.breaks_at_minimum += 1

    # SOC tracking — reconstruct from trips
    for bus in buses:
        soc = config.initial_soc_percent
        for trip in bus.trips:
            soc -= (trip.distance_km * config.consumption_rate / config.battery_kwh) * 100
            if trip.trip_type == "Charging":
                charge_kwh = config.depot_flow_rate_kw * (trip.travel_time_min / 60)
                soc = min(100.0, soc + (charge_kwh / config.battery_kwh) * 100)
            m.min_soc_seen = min(m.min_soc_seen, soc)
        if soc < config.trigger_soc_percent:
            m.buses_below_trigger += 1

    return m
