"""
city_app.py — Citywide eBus Scheduler Dashboard

Multi-route scheduling with fleet rebalancing.
Run: streamlit run src/city_app.py
"""
from __future__ import annotations
__version__ = "2026-04-08-p1"

import sys
from pathlib import Path
from datetime import datetime, timedelta

import streamlit as st
import pandas as pd

try:
    import plotly.graph_objects as go
    import plotly.express as px
    _PLOTLY_OK = True
except ModuleNotFoundError:
    _PLOTLY_OK = False

sys.path.insert(0, str(Path(__file__).parent))

from src.city_config_loader import load_city_config_from_files
from src.city_scheduler import schedule_city
from src.city_models import CityConfig, CitySchedule, RouteResult
from src.fleet_analyzer import compute_pvr_all, compute_fleet_balance
from src.config_loader import ConfigError

st.set_page_config(page_title="eBus Citywide Scheduler", page_icon="🏙️", layout="wide",
                   initial_sidebar_state="expanded")

# ── Styles ────────────────────────────────────────────────────────────────────
st.markdown("""
<style>
  @import url('https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500;600;700&family=DM+Mono:wght@400;500&display=swap');
  html, body, [class*="css"] { font-family: 'DM Sans', sans-serif; }
  .block-container { padding-top: 1.5rem; padding-bottom: 2rem; }
  .kpi-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(140px, 1fr)); gap: 12px; margin-bottom: 1.2rem; }
  .kpi { background: #fff; border: 1px solid #e8eaed; border-radius: 10px; padding: 14px 16px; }
  .kpi-val { font-size: 1.65rem; font-weight: 700; color: #1a1a2e; line-height: 1.1; font-family: 'DM Mono', monospace; }
  .kpi-label { font-size: 0.72rem; font-weight: 500; color: #888; text-transform: uppercase; letter-spacing: .05em; margin-top: 4px; }
  .kpi-sub { font-size: 0.68rem; color: #aaa; margin-top: 2px; }
  .kpi-ok .kpi-val { color: #16a34a; }
  .kpi-warn .kpi-val { color: #d97706; }
  .kpi-bad .kpi-val { color: #dc2626; }
  .section-title { font-size: 1.1rem; font-weight: 700; color: #1a1a2e; margin: 1.2rem 0 0.4rem 0; padding-bottom: 6px; border-bottom: 2px solid #e8eaed; }
  section[data-testid="stSidebar"] .stButton button { background: #4f46e5; color: white !important; border: none; border-radius: 8px; font-weight: 600; width: 100%; }
  section[data-testid="stSidebar"] .stButton button:hover { background: #4338ca; }
  .transfer-card { background: #f0f9ff; border: 1px solid #bae6fd; border-radius: 8px; padding: 10px 14px; margin-bottom: 6px; }
  .transfer-arrow { color: #0284c7; font-weight: 700; }
</style>
""", unsafe_allow_html=True)


# ── Helpers ───────────────────────────────────────────────────────────────────

def kpi(label, value, status="", sub=""):
    cls = f"kpi kpi-{status}" if status else "kpi"
    sub_html = f'<div class="kpi-sub">{sub}</div>' if sub else ""
    return f'<div class="{cls}"><div class="kpi-val">{value}</div><div class="kpi-label">{label}</div>{sub_html}</div>'


def _bus_gantt(result: RouteResult) -> go.Figure | None:
    """Simplified Gantt for one route in citywide context."""
    if not _PLOTLY_OK:
        return None
    REF = datetime(2025, 1, 1)
    bars = []
    for bus in result.buses:
        for trip in bus.trips:
            if trip.actual_departure is None or trip.actual_arrival is None:
                continue
            color = {"Revenue": "#4f46e5", "Dead": "#9ca3af", "Charging": "#f59e0b",
                     "Shuttle": "#06b6d4"}.get(trip.trip_type, "#d1d5db")
            bars.append(dict(
                bus=bus.bus_id, start=trip.actual_departure, end=trip.actual_arrival,
                trip_type=trip.trip_type, direction=trip.direction, color=color,
            ))
    if not bars:
        return None
    df = pd.DataFrame(bars)
    fig = go.Figure()
    bus_ids = sorted(df["bus"].unique())
    for _, row in df.iterrows():
        y = bus_ids.index(row["bus"])
        fig.add_trace(go.Bar(
            x=[row["end"] - row["start"]], y=[y], base=[row["start"]],
            orientation="h", marker_color=row["color"],
            hovertext=f"{row['bus']} {row['trip_type']} {row['direction']}",
            hoverinfo="text", showlegend=False,
        ))
    fig.update_layout(
        barmode="overlay", height=max(200, len(bus_ids) * 35 + 60),
        yaxis=dict(tickvals=list(range(len(bus_ids))), ticktext=bus_ids,
                   autorange="reversed"),
        xaxis=dict(title="Time", type="date",
                   range=[REF.replace(hour=5, minute=30), REF.replace(hour=22)]),
        margin=dict(l=80, r=20, t=20, b=40),
        plot_bgcolor="white",
    )
    return fig


# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("## 🏙️ Citywide eBus Scheduler")
    st.caption("Upload config Excel files for all routes.")
    st.divider()

    uploaded_files = st.file_uploader(
        "Route Config Files",
        type=["xlsx", "xlsm"],
        accept_multiple_files=True,
        label_visibility="collapsed",
        help="Upload one Excel per route (same format as single-route scheduler)",
    )

    optimize = st.toggle(
        "Optimizer ON",
        value=False,
        help="ON = find minimum fleet per route (ignores headway). OFF = follow headway, rebalance surplus.",
    )

    total_fleet_override = st.number_input(
        "Total Fleet Override",
        min_value=0, value=0, step=1,
        help="0 = use sum of fleet_size from all configs. Set >0 to cap total citywide fleet.",
    )

    run_btn = st.button(
        "▶ Generate Citywide Schedule",
        type="primary",
        disabled=not uploaded_files,
    )

    st.divider()
    st.caption(
        "**Optimizer OFF**: Respects headway profile. Surplus buses transferred to deficit routes.\n\n"
        "**Optimizer ON**: Finds minimum fleet per route. Excess distributed to worst-scoring routes."
    )


# ── Main ──────────────────────────────────────────────────────────────────────

if not uploaded_files:
    st.markdown("## Welcome to Citywide eBus Scheduler")
    st.markdown("Upload config Excel files for each route in the sidebar. "
                "The system will schedule all routes and rebalance fleet across them.")

    c1, c2, c3, c4 = st.columns(4)
    icons = ["📁", "⚡", "🔄", "📊"]
    steps = [
        ("Upload", "Drop config Excel files — one per route. Same format as single-route scheduler."),
        ("Schedule", "Each route scheduled independently using its headway & travel time profiles."),
        ("Rebalance", "Surplus buses from over-allocated routes transferred to under-served routes."),
        ("Review", "Citywide KPIs, per-route drill-down, fleet transfer summary."),
    ]
    for col, (icon, (title, desc)) in zip([c1, c2, c3, c4], zip(icons, steps)):
        with col:
            st.markdown(f"### {icon} {title}")
            st.caption(desc)
    st.stop()


if run_btn:
    with st.spinner("Loading configs and generating citywide schedule..."):
        try:
            city_config, load_warnings = load_city_config_from_files(uploaded_files)

            # Apply fleet override if set
            if total_fleet_override > 0:
                city_config.total_fleet = total_fleet_override

            result = schedule_city(city_config, optimize=optimize)

        except ConfigError as e:
            st.error(f"Config error: {e}")
            st.stop()
        except Exception as e:
            st.error(f"Error: {e}")
            import traceback
            st.code(traceback.format_exc())
            st.stop()

    st.session_state["city_result"] = result
    st.session_state["city_config"] = city_config
    st.session_state["load_warnings"] = load_warnings
    st.session_state["has_city_results"] = True


if not st.session_state.get("has_city_results"):
    if uploaded_files:
        st.info("Click **▶ Generate Citywide Schedule** in the sidebar to run.")
    st.stop()

# ── Results ───────────────────────────────────────────────────────────────────

cs: CitySchedule = st.session_state["city_result"]
city_config: CityConfig = st.session_state["city_config"]
load_warnings = st.session_state.get("load_warnings", [])

# Show load warnings
if load_warnings:
    with st.expander(f"⚠ {len(load_warnings)} loading warning(s)", expanded=False):
        for w in load_warnings:
            st.warning(w)

# ── Title + Mode Badge ────────────────────────────────────────────────────────
mode_label = "Optimizer ON (KPI-Driven)" if optimize else "Optimizer OFF (Headway-Driven)"
st.markdown(f"## 🏙️ Citywide Schedule — {len(cs.results)} Routes")
st.caption(f"Mode: **{mode_label}** · Depot: **{city_config.depot_name}**")

# ── Citywide KPI Bar ─────────────────────────────────────────────────────────
total_buses = cs.total_buses_used
total_rev = cs.total_revenue_trips
total_rev_km = cs.total_revenue_km
total_dead_km = cs.total_dead_km
dead_ratio = cs.citywide_dead_km_ratio
util_pct = cs.citywide_utilization_pct
min_soc = cs.min_soc_citywide
n_transfers = len(cs.transfers)

st.markdown(
    '<div class="kpi-grid">' +
    kpi("Routes", str(len(cs.results))) +
    kpi("Total Fleet", str(total_buses),
        sub=f"configured: {city_config.total_configured_fleet}") +
    kpi("Revenue Trips", str(total_rev)) +
    kpi("Revenue KM", f"{total_rev_km:,.0f}") +
    kpi("Dead KM %", f"{dead_ratio:.1%}",
        "ok" if dead_ratio < 0.15 else "warn") +
    kpi("Min SOC", f"{min_soc:.1f}%",
        "ok" if min_soc >= 25 else "warn" if min_soc >= 20 else "bad") +
    kpi("Utilization", f"{util_pct:.0f}%",
        "ok" if util_pct >= 85 else "warn") +
    kpi("Transfers", str(n_transfers),
        "ok" if n_transfers == 0 else "warn") +
    '</div>', unsafe_allow_html=True,
)

# ── Tabs ──────────────────────────────────────────────────────────────────────
tab_overview, tab_rebalance, tab_route_detail, tab_fleet_config = st.tabs([
    "📊 Overview", "🔄 Fleet Rebalancing", "🗺 Route Detail", "⚙️ Fleet Config",
])

# ════════════════════════════════════════════════════════════════════
# TAB 1: Overview
# ════════════════════════════════════════════════════════════════════
with tab_overview:
    st.markdown('<div class="section-title">Route Summary</div>', unsafe_allow_html=True)

    summary_df = pd.DataFrame(cs.route_summary_rows())
    st.dataframe(summary_df, use_container_width=True, hide_index=True)

    # Fleet allocation chart
    if _PLOTLY_OK and len(cs.results) > 1:
        st.markdown('<div class="section-title">Fleet Allocation: PVR vs Allocated</div>',
                    unsafe_allow_html=True)

        codes = sorted(cs.results.keys())
        pvrs = [cs.results[c].pvr for c in codes]
        allocs = [cs.results[c].fleet_allocated for c in codes]
        originals = [cs.results[c].fleet_original for c in codes]

        fig = go.Figure()
        fig.add_trace(go.Bar(name="PVR (minimum)", x=codes, y=pvrs,
                             marker_color="#94a3b8"))
        fig.add_trace(go.Bar(name="Config Fleet", x=codes, y=originals,
                             marker_color="#c7d2fe"))
        fig.add_trace(go.Bar(name="Final Allocated", x=codes, y=allocs,
                             marker_color="#4f46e5"))
        fig.update_layout(
            barmode="group", height=350,
            legend=dict(orientation="h", yanchor="bottom", y=1.02, x=0.5, xanchor="center"),
            margin=dict(l=40, r=20, t=20, b=40),
            plot_bgcolor="white",
        )
        st.plotly_chart(fig, use_container_width=True)

    # KPI comparison across routes
    if _PLOTLY_OK and len(cs.results) > 1:
        st.markdown('<div class="section-title">Dead KM % by Route</div>',
                    unsafe_allow_html=True)
        codes = sorted(cs.results.keys())
        dead_pcts = [cs.results[c].metrics.dead_km_ratio * 100 for c in codes]
        fig2 = go.Figure(go.Bar(
            x=codes, y=dead_pcts, marker_color=[
                "#16a34a" if d < 15 else "#d97706" if d < 25 else "#dc2626"
                for d in dead_pcts
            ]
        ))
        fig2.update_layout(
            height=280, yaxis_title="Dead KM %",
            margin=dict(l=40, r=20, t=20, b=40),
            plot_bgcolor="white",
        )
        st.plotly_chart(fig2, use_container_width=True)


# ════════════════════════════════════════════════════════════════════
# TAB 2: Fleet Rebalancing
# ════════════════════════════════════════════════════════════════════
with tab_rebalance:
    if not cs.transfers:
        st.success("✅ No fleet rebalancing needed — all routes adequately staffed.")
    else:
        st.markdown(f'<div class="section-title">Fleet Transfers ({len(cs.transfers)})</div>',
                    unsafe_allow_html=True)

        for t in cs.transfers:
            st.markdown(
                f'<div class="transfer-card">'
                f'<strong>{t.from_route}</strong> '
                f'<span class="transfer-arrow">→</span> '
                f'<strong>{t.to_route}</strong> '
                f'<span style="color:#666; font-size:0.85rem; margin-left:8px;">({t.reason})</span>'
                f'</div>',
                unsafe_allow_html=True,
            )

        # Before/after table
        st.markdown('<div class="section-title">Before & After</div>', unsafe_allow_html=True)
        ba_rows = []
        for code in sorted(cs.results):
            r = cs.results[code]
            donated = sum(1 for t in cs.transfers if t.from_route == code)
            received = sum(1 for t in cs.transfers if t.to_route == code)
            change = received - donated
            ba_rows.append({
                "Route": code,
                "PVR": r.pvr,
                "Before": r.fleet_original,
                "After": r.fleet_allocated,
                "Change": f"+{change}" if change > 0 else str(change) if change < 0 else "—",
                "Donated": donated,
                "Received": received,
            })
        st.dataframe(pd.DataFrame(ba_rows), use_container_width=True, hide_index=True)

    # Surplus/deficit analysis
    st.markdown('<div class="section-title">Fleet Balance Analysis (PVR-based)</div>',
                unsafe_allow_html=True)
    balance = compute_fleet_balance(city_config)
    bal_df = pd.DataFrame([
        {
            "Route": code,
            "PVR": b["pvr"],
            "Allocated": b["allocated"],
            "Surplus": b["surplus"] if b["surplus"] > 0 else "",
            "Deficit": b["deficit"] if b["deficit"] > 0 else "",
            "Headroom %": f"{b['headroom_pct']:+.0f}%",
        }
        for code, b in sorted(balance.items())
    ])
    st.dataframe(bal_df, use_container_width=True, hide_index=True)


# ════════════════════════════════════════════════════════════════════
# TAB 3: Route Detail (drill-down)
# ════════════════════════════════════════════════════════════════════
with tab_route_detail:
    route_codes = sorted(cs.results.keys())
    selected_route = st.selectbox("Select Route", route_codes,
                                  format_func=lambda c: f"{c} — {cs.results[c].config.route_name}")

    if selected_route:
        r = cs.results[selected_route]
        m = r.metrics

        st.markdown(f"### {r.route_code} — {r.config.route_name}")

        # Per-route KPIs
        shuttle_count = sum(1 for b in r.buses for t in b.trips if t.trip_type == "Shuttle")
        dead_count = sum(1 for b in r.buses for t in b.trips if t.trip_type == "Dead")

        st.markdown(
            '<div class="kpi-grid">' +
            kpi("Fleet", f"{r.fleet_allocated}",
                sub=f"config: {r.fleet_original} · PVR: {r.pvr}") +
            kpi("Rev Trips", str(m.revenue_trips_assigned),
                sub=f"of {m.revenue_trips_total} planned") +
            kpi("Total KM", f"{m.total_km:.0f}") +
            kpi("Dead %", f"{m.dead_km_ratio:.1%}",
                "ok" if m.dead_km_ratio < 0.15 else "warn") +
            kpi("Min SOC", f"{m.min_soc_seen:.1f}%",
                "ok" if m.min_soc_seen >= 25 else "warn") +
            kpi("KM Range", f"{m.km_range:.1f}") +
            kpi("Score", f"{m.weighted_score():.4f}") +
            '</div>', unsafe_allow_html=True,
        )

        # Gantt chart
        if _PLOTLY_OK:
            st.markdown('<div class="section-title">Schedule Timeline</div>',
                        unsafe_allow_html=True)
            fig = _bus_gantt(r)
            if fig:
                st.plotly_chart(fig, use_container_width=True)

        # Bus-level detail
        st.markdown('<div class="section-title">Bus Summary</div>', unsafe_allow_html=True)
        bus_rows = []
        for bus in r.buses:
            rev_trips = [t for t in bus.trips if t.trip_type == "Revenue"]
            dead_trips = [t for t in bus.trips if t.trip_type == "Dead"]
            chg_trips = [t for t in bus.trips if t.trip_type == "Charging"]
            bus_rows.append({
                "Bus": bus.bus_id,
                "Total Trips": len(bus.trips),
                "Revenue": len(rev_trips),
                "Dead": len(dead_trips),
                "Charging": len(chg_trips),
                "Total KM": round(bus.total_km, 1),
                "Final SOC": f"{bus.soc_percent:.1f}%",
                "Last Location": bus.current_location,
            })
        st.dataframe(pd.DataFrame(bus_rows), use_container_width=True, hide_index=True)

        # Trip-level schedule
        with st.expander("📋 Full Trip Schedule"):
            trip_rows = []
            for bus in r.buses:
                for trip in bus.trips:
                    trip_rows.append({
                        "Bus": trip.assigned_bus,
                        "Type": trip.trip_type,
                        "Dir": trip.direction,
                        "From": trip.start_location,
                        "To": trip.end_location,
                        "Depart": trip.actual_departure.strftime("%H:%M") if trip.actual_departure else "",
                        "Arrive": trip.actual_arrival.strftime("%H:%M") if trip.actual_arrival else "",
                        "KM": round(trip.distance_km, 1),
                    })
            if trip_rows:
                trip_df = pd.DataFrame(trip_rows)
                trip_df = trip_df.sort_values("Depart")
                st.dataframe(trip_df, use_container_width=True, hide_index=True)


# ════════════════════════════════════════════════════════════════════
# TAB 4: Fleet Config Editor
# ════════════════════════════════════════════════════════════════════
with tab_fleet_config:
    st.markdown('<div class="section-title">Adjust Fleet Size per Route</div>',
                unsafe_allow_html=True)
    st.caption("Edit fleet sizes below and click Re-run to regenerate with updated allocation.")

    # Build editable config table
    edit_rows = []
    for code in sorted(city_config.routes.keys()):
        ri = city_config.routes[code]
        edit_rows.append({
            "Route": code,
            "Name": ri.config.route_name,
            "Fleet Size": ri.config.fleet_size,
            "PVR": compute_pvr_all(city_config).get(code, 0),
            "Operating": f"{ri.config.operating_start.strftime('%H:%M')}–{ri.config.operating_end.strftime('%H:%M')}",
            "Peak Headway": int(ri.headway_df["headway_min"].min()) if len(ri.headway_df) > 0 else 30,
        })

    edit_df = pd.DataFrame(edit_rows)
    edited = st.data_editor(
        edit_df,
        column_config={
            "Route": st.column_config.TextColumn(disabled=True),
            "Name": st.column_config.TextColumn(disabled=True),
            "Fleet Size": st.column_config.NumberColumn(min_value=1, max_value=30, step=1),
            "PVR": st.column_config.NumberColumn(disabled=True),
            "Operating": st.column_config.TextColumn(disabled=True),
            "Peak Headway": st.column_config.NumberColumn(disabled=True),
        },
        use_container_width=True,
        hide_index=True,
    )

    total_edited = edited["Fleet Size"].sum()
    st.caption(f"**Total fleet: {total_edited}** (configured: {city_config.total_configured_fleet})")

    if st.button("🔄 Re-run with Updated Fleet", type="primary"):
        # Apply edited fleet sizes to city_config
        for _, row in edited.iterrows():
            code = row["Route"]
            new_fleet = int(row["Fleet Size"])
            if code in city_config.routes:
                city_config.routes[code].config.fleet_size = new_fleet

        city_config.total_fleet = int(total_edited)

        with st.spinner("Re-running citywide schedule..."):
            try:
                result = schedule_city(city_config, optimize=optimize)
                st.session_state["city_result"] = result
                st.session_state["city_config"] = city_config
                st.rerun()
            except Exception as e:
                st.error(f"Error: {e}")
