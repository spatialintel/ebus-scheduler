"""
city_models.py — Data models for citywide multi-route scheduling.

Phase 1: Whole-day fleet rebalancing across routes sharing a single depot.
Phase 2 hook: Transfer.reason supports "interlining" for trip-level cross-route.
"""

from __future__ import annotations
__version__ = "2026-04-08-p1"

from dataclasses import dataclass, field
from src.models import RouteConfig, BusState
from src.metrics import ScheduleMetrics
import pandas as pd


@dataclass
class RouteInput:
    """Everything needed to schedule one route — config + profiles."""
    config: RouteConfig
    headway_df: pd.DataFrame
    travel_time_df: pd.DataFrame


@dataclass
class RouteResult:
    """Output of scheduling a single route."""
    route_code: str
    config: RouteConfig
    headway_df: pd.DataFrame
    travel_time_df: pd.DataFrame
    buses: list[BusState]
    metrics: ScheduleMetrics
    pvr: int                    # Peak Vehicle Requirement (theoretical minimum)
    fleet_allocated: int        # actual buses assigned (after rebalancing)
    fleet_original: int         # fleet_size from config Excel
    surplus: int = 0            # positive = excess buses available to donate
    deficit: int = 0            # positive = route needs more buses


@dataclass
class Transfer:
    """Record of a bus moved between routes during rebalancing."""
    bus_id: str
    from_route: str             # route_code donor (or "POOL" if unassigned)
    to_route: str               # route_code recipient
    reason: str = "surplus_rebalance"  # Phase 2: "interlining" | "soc_relief"


@dataclass
class CityConfig:
    """Aggregated city-level configuration."""
    routes: dict[str, RouteInput]       # route_code → RouteInput
    total_fleet: int = 0                # sum of all fleet_size (or user override)
    depot_name: str = "DEPOT"           # shared depot (Phase 1 assumption)
    depot_charger_slots: int = 0        # 0 = unlimited (Phase 2: shared charger scheduling)

    @property
    def route_codes(self) -> list[str]:
        return sorted(self.routes.keys())

    @property
    def total_configured_fleet(self) -> int:
        """Sum of fleet_size across all route configs."""
        return sum(ri.config.fleet_size for ri in self.routes.values())


@dataclass
class CitySchedule:
    """Complete citywide schedule output."""
    city_config: CityConfig
    results: dict[str, RouteResult]     # route_code → RouteResult
    transfers: list[Transfer] = field(default_factory=list)

    # ── Aggregate KPIs ────────────────────────────────────────────────────────

    @property
    def total_buses_used(self) -> int:
        return sum(r.fleet_allocated for r in self.results.values())

    @property
    def total_revenue_trips(self) -> int:
        return sum(r.metrics.revenue_trips_assigned for r in self.results.values())

    @property
    def total_revenue_km(self) -> float:
        return sum(r.metrics.revenue_km for r in self.results.values())

    @property
    def total_dead_km(self) -> float:
        return sum(r.metrics.dead_km for r in self.results.values())

    @property
    def citywide_dead_km_ratio(self) -> float:
        total = self.total_revenue_km + self.total_dead_km
        return self.total_dead_km / total if total > 0 else 0.0

    @property
    def citywide_utilization_pct(self) -> float:
        """Fleet utilization = buses with >0 revenue trips / total buses."""
        active = sum(1 for r in self.results.values()
                     for b in r.buses if any(t.trip_type == "Revenue" for t in b.trips))
        return (active / self.total_buses_used * 100) if self.total_buses_used > 0 else 0.0

    @property
    def min_soc_citywide(self) -> float:
        return min((r.metrics.min_soc_seen for r in self.results.values()), default=100.0)

    def route_summary_rows(self) -> list[dict]:
        """One row per route for the dashboard summary table."""
        rows = []
        for code in sorted(self.results):
            r = self.results[code]
            m = r.metrics
            donated = sum(1 for t in self.transfers if t.from_route == code)
            received = sum(1 for t in self.transfers if t.to_route == code)
            rows.append({
                "Route": code,
                "Name": r.config.route_name,
                "PVR": r.pvr,
                "Config Fleet": r.fleet_original,
                "Allocated": r.fleet_allocated,
                "Donated": donated,
                "Received": received,
                "Rev Trips": m.revenue_trips_assigned,
                "Rev KM": round(m.revenue_km, 1),
                "Dead KM": round(m.dead_km, 1),
                "Dead %": f"{m.dead_km_ratio:.1%}",
                "Min SOC": f"{m.min_soc_seen:.1f}%",
                "Score": f"{m.weighted_score():.4f}",
            })
        return rows
