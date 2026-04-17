"""
recommender.py — Actionable recommendation engine for eBus Scheduler.

Analyses a completed CitySchedule and produces prioritised, actionable
recommendations. Each recommendation has a category, action, reason,
expected impact, and confidence level.

This is the primary product differentiator — metrics without actions are
not useful to planners.

Usage:
    from src.recommender import generate_recommendations, Recommendation
    recs = generate_recommendations(city_schedule)
    for r in recs[:5]:
        print(f"[P{r.priority}] {r.action} — {r.reason}")
"""

from __future__ import annotations
__version__ = "2026-04-17-p2"

from dataclasses import dataclass, field


# ── Data model ────────────────────────────────────────────────────────────────

@dataclass
class Recommendation:
    """Single actionable recommendation."""
    category: str           # fleet_adjustment | headway_change | charging_window |
                            # depot_infrastructure | corridor_coordination | soc_risk
    priority: int           # 1 = safety/compliance, 5 = minor optimisation
    route_codes: list[str]  # affected routes (empty = citywide)
    action: str             # what to do (imperative, specific)
    reason: str             # why (data-driven explanation)
    expected_impact: str    # what changes if action is taken
    confidence: str         # "high" | "medium" | "low"

    def __repr__(self):
        routes = ", ".join(self.route_codes) if self.route_codes else "citywide"
        return f"[P{self.priority}|{self.confidence}] {self.category}: {self.action} ({routes})"


# ── LOS thresholds per route category ─────────────────────────────────────────

_LOS_TARGETS = {
    "trunk":    "B",    # trunk routes should achieve LOS A or B
    "standard": "C",    # standard routes should achieve LOS B or C
    "feeder":   "D",    # feeder routes can tolerate LOS C or D
}

_GRADE_NUM = {"A": 1, "B": 2, "C": 3, "D": 4, "E": 5, "F": 6}


def _grade_below(actual: str, target: str) -> bool:
    """True if actual LOS grade is worse than target."""
    return _GRADE_NUM.get(actual, 6) > _GRADE_NUM.get(target, 3)


# ── Main entry point ─────────────────────────────────────────────────────────

def generate_recommendations(city_schedule) -> list[Recommendation]:
    """
    Analyse CitySchedule and return prioritised recommendations.

    Checks (in priority order):
      P1 — SOC risk (bus near floor during revenue service)
      P2 — Headway infeasibility (physics > configured)
      P2 — Fleet deficit (allocated < PVR)
      P3 — LOS below target for route category
      P3 — Fleet surplus/deficit transfer opportunities
      P4 — Depot charger utilisation
      P5 — Minor optimisations (km balance, dead-km)
    """
    recs: list[Recommendation] = []

    _check_soc_risk(city_schedule, recs)
    _check_headway_infeasibility(city_schedule, recs)
    _check_fleet_deficit(city_schedule, recs)
    _check_los_grade(city_schedule, recs)
    _check_fleet_transfers(city_schedule, recs)
    _check_depot_utilisation(city_schedule, recs)
    _check_charging_window(city_schedule, recs)
    _check_km_balance(city_schedule, recs)
    _check_dead_km(city_schedule, recs)

    # Sort by priority (lowest = most critical)
    recs.sort(key=lambda r: (r.priority, r.category))
    return recs


# ── Individual checks ─────────────────────────────────────────────────────────

def _check_soc_risk(cs, recs: list):
    """P1: Any bus within 5% of SOC floor = safety risk."""
    for code, r in cs.results.items():
        margin = r.metrics.min_soc_seen - r.config.min_soc_percent
        if margin < 5:
            trigger = r.config.trigger_soc_percent
            recs.append(Recommendation(
                category="soc_risk",
                priority=1,
                route_codes=[code],
                action=f"Increase trigger_soc from {trigger:.0f}% to {trigger + 5:.0f}% "
                       f"or add terminal charger on {code}",
                reason=f"Bus reached {r.metrics.min_soc_seen:.1f}% SOC "
                       f"(floor={r.config.min_soc_percent}%, margin only {margin:.1f}%)",
                expected_impact=f"Prevents SOC floor violation. "
                               f"Earlier charging trigger adds ~1 extra charge stop per day.",
                confidence="high",
            ))


def _check_headway_infeasibility(cs, recs: list):
    """P2: Headway configured below physics minimum."""
    for code, r in cs.results.items():
        status = getattr(r, "headway_feasibility_status", "UNKNOWN")
        if status != "INFEASIBLE":
            continue
        details = getattr(r, "headway_feasibility_details", []) or []
        for d in details:
            if not isinstance(d, dict) or d.get("status") != "INFEASIBLE":
                continue
            cfg_hw = d.get("cfg_hw", "?")
            phys_min = d.get("physics_min", "?")
            band = d.get("band", "?")
            rec_hw = d.get("rec", phys_min)
            recs.append(Recommendation(
                category="headway_change",
                priority=2,
                route_codes=[code],
                action=f"Increase {code} headway in band {band} from {cfg_hw} to {rec_hw} min",
                reason=f"Configured headway ({cfg_hw} min) is below physics minimum "
                       f"({phys_min} min). Scheduler silently uses {phys_min} min.",
                expected_impact="Eliminates silent headway override. "
                               "Dashboard shows actual scheduled headway.",
                confidence="high",
            ))


def _check_fleet_deficit(cs, recs: list):
    """P2: Route allocated fewer buses than PVR."""
    for code, r in cs.results.items():
        if r.fleet_allocated < r.pvr:
            deficit = r.pvr - r.fleet_allocated
            recs.append(Recommendation(
                category="fleet_adjustment",
                priority=2,
                route_codes=[code],
                action=f"Add {deficit} bus(es) to {code} "
                       f"(current: {r.fleet_allocated}, PVR: {r.pvr})",
                reason=f"Fleet ({r.fleet_allocated}) is below Peak Vehicle Requirement ({r.pvr}). "
                       f"Service coverage will be degraded.",
                expected_impact=f"Meets PVR. Headway gaps should reduce by ~{deficit * 15:.0f}%.",
                confidence="high",
            ))


def _check_los_grade(cs, recs: list):
    """P3: LOS grade below target for route category."""
    for code, r in cs.results.items():
        grade = getattr(r.metrics, "los_grade", "")
        if not grade:
            continue
        category = getattr(r.config, "route_category", "standard")
        target = _LOS_TARGETS.get(category, "C")
        if _grade_below(grade, target):
            cv = r.metrics.headway_cv
            max_gap = r.metrics.max_headway_gap_min
            # Suggest specific action based on gap analysis
            if r.fleet_allocated <= r.pvr:
                action = (f"Add 1–2 buses to {code} to improve LOS from {grade} to {target} "
                         f"(current fleet: {r.fleet_allocated})")
                impact = "Additional bus reduces headway CV and max gap."
            else:
                action = (f"Review {code} headway profile — CV={cv:.2f}, max gap={max_gap:.0f} min. "
                         f"Consider tightening peak headway or adjusting charging window.")
                impact = "Better headway regularity improves passenger wait times."
            recs.append(Recommendation(
                category="fleet_adjustment",
                priority=3,
                route_codes=[code],
                action=action,
                reason=f"LOS {grade} is below target {target} for {category} route. "
                       f"Headway CV={cv:.3f}, max gap={max_gap:.0f} min.",
                expected_impact=impact,
                confidence="medium",
            ))


def _check_fleet_transfers(cs, recs: list):
    """P3: Surplus on one route + deficit on another = transfer opportunity."""
    surplus_routes = []
    deficit_routes = []
    for code, r in cs.results.items():
        if r.fleet_allocated > r.pvr + 1:  # more than 1 extra
            surplus_routes.append((code, r.fleet_allocated - r.pvr))
        elif r.fleet_allocated < r.pvr:
            deficit_routes.append((code, r.pvr - r.fleet_allocated))

    if surplus_routes and deficit_routes:
        for s_code, s_count in surplus_routes:
            for d_code, d_count in deficit_routes:
                transfer = min(s_count, d_count)
                recs.append(Recommendation(
                    category="fleet_adjustment",
                    priority=3,
                    route_codes=[s_code, d_code],
                    action=f"Transfer {transfer} bus(es) from {s_code} to {d_code}",
                    reason=f"{s_code} has {s_count} surplus (fleet={cs.results[s_code].fleet_allocated}, "
                           f"PVR={cs.results[s_code].pvr}). "
                           f"{d_code} has {d_count} deficit.",
                    expected_impact=f"{d_code} meets PVR. {s_code} retains "
                                   f"{cs.results[s_code].fleet_allocated - transfer} buses (still ≥ PVR).",
                    confidence="high",
                ))


def _check_depot_utilisation(cs, recs: list):
    """P4: Depot charger utilisation warnings."""
    # Check from depot_log if available (Phase 2 depot_model integration)
    for code, r in cs.results.items():
        depot_log = getattr(r, "depot_log", None)
        if depot_log is None:
            continue
        util = getattr(depot_log, "utilisation_pct_slow", 0) or 0
        peak_q = getattr(depot_log, "peak_queue_depth_slow", 0) or 0
        if util > 85:
            recs.append(Recommendation(
                category="depot_infrastructure",
                priority=4,
                route_codes=[code],
                action=f"Add 1 charger slot at depot (utilisation={util:.0f}%, "
                       f"peak queue={peak_q} buses)",
                reason=f"Depot charger utilisation exceeds 85%. "
                       f"{peak_q} buses queued simultaneously at peak.",
                expected_impact="Reduces charging wait time. "
                               "Buses return to revenue service faster.",
                confidence="medium",
            ))


def _check_charging_window(cs, recs: list):
    """P4: P5 window may need adjustment."""
    for code, r in cs.results.items():
        # Check if last bus charges too close to window end
        p5_end = r.config.p5_charging_end
        if p5_end is None:
            continue
        last_charge_time = None
        for bus in r.buses:
            for trip in bus.trips:
                if trip.trip_type == "Charging" and trip.actual_arrival:
                    if last_charge_time is None or trip.actual_arrival > last_charge_time:
                        last_charge_time = trip.actual_arrival
        if last_charge_time:
            try:
                p5_end_min = p5_end.hour * 60 + p5_end.minute
                last_min = last_charge_time.hour * 60 + last_charge_time.minute
                if last_min > p5_end_min - 15:  # within 15 min of window end
                    recs.append(Recommendation(
                        category="charging_window",
                        priority=4,
                        route_codes=[code],
                        action=f"Extend {code} p5_charging_end from "
                               f"{p5_end.strftime('%H:%M')} to "
                               f"{(last_charge_time.hour * 60 + last_charge_time.minute + 30) // 60:02d}:"
                               f"{(last_charge_time.minute + 30) % 60:02d}",
                        reason=f"Last bus finished charging at "
                               f"{last_charge_time.strftime('%H:%M')}, "
                               f"only {p5_end_min - last_min:.0f} min before window closes.",
                        expected_impact="Prevents rushed/missed charging for late buses.",
                        confidence="medium",
                    ))
            except Exception:
                pass


def _check_km_balance(cs, recs: list):
    """P5: KM imbalance across buses on a route."""
    for code, r in cs.results.items():
        if r.metrics.km_range > 30:  # more than 30 km difference
            avg_km = (sum(r.metrics.km_per_bus) / len(r.metrics.km_per_bus)
                     if r.metrics.km_per_bus else 0)
            recs.append(Recommendation(
                category="fleet_adjustment",
                priority=5,
                route_codes=[code],
                action=f"Review {code} km balance — range is {r.metrics.km_range:.0f} km "
                       f"(avg {avg_km:.0f} km/bus)",
                reason=f"Some buses work significantly more than others. "
                       f"Max-min difference: {r.metrics.km_range:.0f} km.",
                expected_impact="Better fleet wear distribution. "
                               "May extend battery life.",
                confidence="low",
            ))


def _check_dead_km(cs, recs: list):
    """P5: High dead-km ratio."""
    for code, r in cs.results.items():
        if r.metrics.dead_km_ratio > 0.15:  # more than 15% dead km
            recs.append(Recommendation(
                category="fleet_adjustment",
                priority=5,
                route_codes=[code],
                action=f"Investigate {code} dead-km ratio ({r.metrics.dead_km_ratio:.1%}) — "
                       f"consider terminal charger to reduce depot round-trips",
                reason=f"Dead km ({r.metrics.dead_km:.0f} km) exceeds 15% of total "
                       f"({r.metrics.total_km:.0f} km). Most dead-km is depot charging trips.",
                expected_impact="Terminal charger eliminates depot round-trip dead-km. "
                               "Typical saving: 40–60% dead-km reduction.",
                confidence="medium",
            ))


# ── Summary helpers ───────────────────────────────────────────────────────────

def top_recommendations(recs: list[Recommendation], n: int = 5) -> list[Recommendation]:
    """Return the top-N recommendations by priority."""
    return recs[:n]


def recommendations_by_route(recs: list[Recommendation], route_code: str) -> list[Recommendation]:
    """Filter recommendations for a specific route."""
    return [r for r in recs if route_code in r.route_codes or not r.route_codes]


def recommendation_summary_rows(recs: list[Recommendation]) -> list[dict]:
    """Format recommendations as rows for a dashboard table."""
    return [
        {
            "Priority": f"P{r.priority}",
            "Category": r.category.replace("_", " ").title(),
            "Route(s)": ", ".join(r.route_codes) if r.route_codes else "Citywide",
            "Action": r.action,
            "Reason": r.reason,
            "Impact": r.expected_impact,
            "Confidence": r.confidence.title(),
        }
        for r in recs
    ]
