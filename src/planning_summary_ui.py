"""
planning_summary_ui.py — Streamlit helper for rendering the Planning Summary.

Imported by app.py. Renders the Planning Summary as the first element on the
citywide dashboard, BEFORE any tabs. This is what the planner sees first.

Usage in app.py:
    from src.planning_summary_ui import render_planning_summary
    render_planning_summary(city_schedule)
"""

from __future__ import annotations
__version__ = "2026-04-15-p1"

import streamlit as st


# ── LOS grade colour mapping ──────────────────────────────────────────────────

_LOS_COLORS = {
    "A": "#1E8449",  # green — excellent
    "B": "#52A043",  # light green — good
    "C": "#B7950B",  # amber — acceptable
    "D": "#D35400",  # orange — marginal
    "E": "#C0392B",  # red — poor
    "F": "#922B21",  # dark red — failing
}


def _los_badge(grade: str) -> str:
    """Return HTML badge for an LOS grade."""
    color = _LOS_COLORS.get(grade, "#666666")
    return (
        f'<span style="display:inline-block; padding:2px 10px; '
        f'border-radius:4px; background:{color}; color:#fff; '
        f'font-weight:600; font-size:0.9rem;">LOS {grade or "—"}</span>'
    )


def render_planning_summary(city_schedule) -> None:
    """
    Render the Planning Summary as the first element on the citywide dashboard.

    Sections:
      1. Fleet Overview (KPI row)
      2. Service Quality (citywide LOS + routes below LOS C)
      3. Warnings (SOC risks, headway infeasibilities, fleet deficits)
    """
    if city_schedule is None:
        return

    summary = city_schedule.planning_summary

    # ── Header ───────────────────────────────────────────────────────────────
    st.markdown(
        '<div style="background: linear-gradient(90deg, #1B4F72 0%, #2E86C1 100%); '
        'padding: 12px 20px; border-radius: 8px; margin-bottom: 16px; color: white;">'
        '<div style="font-size: 1.1rem; font-weight: 700;">📋 Planning Summary</div>'
        '<div style="font-size: 0.85rem; opacity: 0.9;">'
        'Auto-generated from the current schedule. Review before diving into tabs.'
        '</div></div>',
        unsafe_allow_html=True,
    )

    # ── Section 1: Fleet Overview ────────────────────────────────────────────
    fleet = summary["fleet"]
    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.metric("Total Fleet", fleet["total_allocated"],
                  help="Total buses allocated across all routes")
    with c2:
        st.metric("Total PVR", fleet["total_pvr"],
                  help="Peak Vehicle Requirement — theoretical minimum")
    with c3:
        delta = fleet["surplus"] if fleet["surplus"] > 0 else -fleet["deficit"]
        st.metric("Surplus / Deficit", f"{delta:+d}",
                  delta_color=("normal" if delta >= 0 else "inverse"))
    with c4:
        sq = summary["service_quality"]
        st.metric("Citywide LOS", sq["citywide_los"],
                  help="Weighted average Level of Service across routes")

    # ── Section 2: Service Quality ───────────────────────────────────────────
    sq = summary["service_quality"]
    routes_below = sq["routes_below_los_c"]

    if routes_below:
        st.markdown(
            f'<div style="background: #FEF9E7; border-left: 4px solid #D35400; '
            f'padding: 10px 14px; border-radius: 4px; margin: 8px 0;">'
            f'<strong>⚠ {len(routes_below)} route(s) below LOS C:</strong></div>',
            unsafe_allow_html=True,
        )
        cols_per_row = 3
        for i in range(0, len(routes_below), cols_per_row):
            cols = st.columns(cols_per_row)
            for j, rb in enumerate(routes_below[i:i + cols_per_row]):
                with cols[j]:
                    st.markdown(
                        f"**{rb['route']}** &nbsp; {_los_badge(rb['los'])}  \n"
                        f"Max gap: {rb['max_gap_min']:.0f} min &nbsp;·&nbsp; "
                        f"Avg wait: {rb['avg_wait_min']:.0f} min &nbsp;·&nbsp; "
                        f"CV: {rb['headway_cv']:.2f}",
                        unsafe_allow_html=True,
                    )

    # ── Section 3: Recommendations [Phase 2] ────────────────────────────────
    recs = getattr(city_schedule, "recommendations", [])
    if recs:
        st.markdown(
            '<div style="background: var(--color-background-secondary); '
            'border-left: 4px solid #2E86C1; padding: 10px 14px; '
            'border-radius: 4px; margin: 8px 0;">'
            '<strong>📋 Top recommendations</strong></div>',
            unsafe_allow_html=True,
        )
        _PRIO_EMOJI = {1: "🔴", 2: "🟠", 3: "🟡", 4: "🔵", 5: "⚪"}
        for rec in recs[:5]:
            emoji = _PRIO_EMOJI.get(rec.priority, "⚪")
            routes = ", ".join(rec.route_codes) if rec.route_codes else "Citywide"
            with st.container():
                st.markdown(
                    f"{emoji} **[{routes}]** {rec.action}  \n"
                    f"<span style='font-size:0.85rem; color:var(--color-text-secondary);'>"
                    f"↳ {rec.reason}</span>",
                    unsafe_allow_html=True,
                )

    # ── Section 4: Warnings ──────────────────────────────────────────────────
    warnings = summary["warnings"]
    if warnings:
        with st.expander(f"⚠ {len(warnings)} warning(s)", expanded=True):
            for w in warnings:
                st.markdown(f"- {w}")
    else:
        st.success("✓ No warnings. All routes meet compliance and SOC thresholds.")

    st.markdown("---")


def render_route_category_legend() -> None:
    """Small inline legend explaining route categories."""
    st.caption(
        "**Route categories:** "
        "**Trunk** = high-frequency corridor (target LOS A–B) · "
        "**Standard** = regular urban route (target LOS B–C) · "
        "**Feeder** = low-frequency connector (target LOS C–D acceptable)"
    )
