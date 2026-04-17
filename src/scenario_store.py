"""
scenario_store.py — Scenario persistence for eBus Scheduler.

Auto-saves every scheduling run. Supports comparison across runs.
Uses Streamlit session_state (works on Streamlit Cloud).

Usage:
    from src.scenario_store import save_scenario, list_scenarios, load_scenario, compare_scenarios

    save_scenario(city_schedule, mode="planning", label="Baseline")
    scenarios = list_scenarios()
    comparison = compare_scenarios(["run_001", "run_002"])
"""

from __future__ import annotations
__version__ = "2026-04-17-p3"

from dataclasses import dataclass, field
from datetime import datetime
import hashlib
import json

try:
    import streamlit as st
    _HAS_ST = True
except ImportError:
    _HAS_ST = False


@dataclass
class ScenarioSnapshot:
    """Lightweight snapshot of a scheduling run for comparison."""
    run_id: str
    timestamp: str
    mode: str
    label: str
    n_routes: int
    total_fleet: int
    total_pvr: int
    total_revenue_trips: int
    total_revenue_km: float
    total_dead_km: float
    dead_km_ratio: float
    min_soc: float
    citywide_los: str
    per_route: list[dict]       # [{route, fleet, pvr, rev_trips, los, score}, ...]
    config_hash: str = ""       # hash of all route configs for identity


def _session_key():
    return "ebus_scenario_history"


def _get_store() -> list[ScenarioSnapshot]:
    """Get scenario store from session_state."""
    if not _HAS_ST:
        return []
    if _session_key() not in st.session_state:
        st.session_state[_session_key()] = []
    return st.session_state[_session_key()]


def _config_hash(city_schedule) -> str:
    """Hash all route configs for identity check."""
    parts = []
    for code in sorted(city_schedule.results):
        r = city_schedule.results[code]
        parts.append(f"{code}:{r.config.fleet_size}:{r.config.route_name}")
    return hashlib.md5("|".join(parts).encode()).hexdigest()[:8]


def save_scenario(
    city_schedule,
    mode: str = "planning",
    label: str = "",
) -> ScenarioSnapshot:
    """
    Save a scenario snapshot. Auto-generates run_id and timestamp.
    Returns the saved snapshot.
    """
    store = _get_store()
    run_id = f"run_{len(store)+1:03d}"
    ts = datetime.now().strftime("%Y-%m-%d %H:%M")

    if not label:
        from src.city_scheduler import mode_display_name
        label = f"{mode_display_name(mode)} — {ts}"

    per_route = []
    for code in sorted(city_schedule.results):
        r = city_schedule.results[code]
        m = r.metrics
        per_route.append({
            "route": code,
            "fleet": r.fleet_allocated,
            "pvr": r.pvr,
            "rev_trips": m.revenue_trips_assigned,
            "dead_pct": round(m.dead_km_ratio * 100, 1),
            "min_soc": round(m.min_soc_seen, 1),
            "los": getattr(m, "los_grade", ""),
            "score": round(m.weighted_score(), 4),
            "avg_wait": round(getattr(m, "avg_wait_min", 0), 1),
        })

    snap = ScenarioSnapshot(
        run_id=run_id,
        timestamp=ts,
        mode=mode,
        label=label,
        n_routes=len(city_schedule.results),
        total_fleet=city_schedule.total_buses_used,
        total_pvr=sum(r.pvr for r in city_schedule.results.values()),
        total_revenue_trips=city_schedule.total_revenue_trips,
        total_revenue_km=round(city_schedule.total_revenue_km, 1),
        total_dead_km=round(city_schedule.total_dead_km, 1),
        dead_km_ratio=round(city_schedule.citywide_dead_km_ratio, 4),
        min_soc=round(city_schedule.min_soc_citywide, 1),
        citywide_los=city_schedule.planning_summary["service_quality"]["citywide_los"],
        per_route=per_route,
        config_hash=_config_hash(city_schedule),
    )

    store.append(snap)
    if _HAS_ST:
        st.session_state[_session_key()] = store

    return snap


def list_scenarios() -> list[ScenarioSnapshot]:
    """List all saved scenarios, newest first."""
    return list(reversed(_get_store()))


def load_scenario(run_id: str) -> ScenarioSnapshot | None:
    """Load a specific scenario by run_id."""
    for s in _get_store():
        if s.run_id == run_id:
            return s
    return None


def clear_scenarios() -> None:
    """Clear all saved scenarios."""
    if _HAS_ST:
        st.session_state[_session_key()] = []


def compare_scenarios(run_ids: list[str] = None) -> list[dict]:
    """
    Compare scenarios side by side.
    If run_ids is None, compares all saved scenarios.
    Returns list of dicts for dashboard table.
    """
    store = _get_store()
    if run_ids:
        selected = [s for s in store if s.run_id in run_ids]
    else:
        selected = store

    if len(selected) < 2:
        return []

    rows = []
    metrics = [
        ("Mode", "mode"),
        ("Fleet", "total_fleet"),
        ("PVR", "total_pvr"),
        ("Revenue trips", "total_revenue_trips"),
        ("Revenue km", "total_revenue_km"),
        ("Dead km", "total_dead_km"),
        ("Dead %", "dead_km_ratio"),
        ("Min SOC", "min_soc"),
        ("LOS", "citywide_los"),
    ]

    for label, attr in metrics:
        row = {"Metric": label}
        for s in selected:
            val = getattr(s, attr)
            if isinstance(val, float) and attr == "dead_km_ratio":
                row[f"{s.run_id} ({s.mode})"] = f"{val:.1%}"
            elif isinstance(val, float):
                row[f"{s.run_id} ({s.mode})"] = f"{val:.1f}"
            else:
                row[f"{s.run_id} ({s.mode})"] = str(val)
        rows.append(row)

    return rows


def scenario_summary_rows() -> list[dict]:
    """Format all scenarios for a compact dashboard table."""
    return [
        {
            "Run": s.run_id,
            "Time": s.timestamp,
            "Mode": s.mode,
            "Label": s.label,
            "Fleet": s.total_fleet,
            "PVR": s.total_pvr,
            "Rev trips": s.total_revenue_trips,
            "Dead %": f"{s.dead_km_ratio:.1%}",
            "Min SOC": f"{s.min_soc:.0f}%",
            "LOS": s.citywide_los,
        }
        for s in list_scenarios()
    ]
