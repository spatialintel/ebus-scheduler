"""
app.py — eBus Scheduler Dashboard
"""
from __future__ import annotations
__version__ = "2026-04-09-p2"
import sys, tempfile
from pathlib import Path
from datetime import datetime, timedelta, time as dtime

import streamlit as st
import pandas as pd

try:
    import plotly.graph_objects as go
    _PLOTLY_OK = True
except ModuleNotFoundError:
    _PLOTLY_OK = False

sys.path.insert(0, str(Path(__file__).parent))

from src.config_loader import load_config, ConfigError
from src.distance_engine import enrich_distances
from src.trip_generator import generate_trips
from src.bus_scheduler import schedule_buses, check_compliance
from src.output_formatter import write_schedule
from src.metrics import compute_metrics

# Citywide imports — safe fallback if files not yet deployed
try:
    from src.city_config_loader import load_city_config_from_files
    from src.city_scheduler import schedule_city, _natural_headway, _flat_headway_df
    from src.city_models import CityConfig, CitySchedule, RouteResult
    from src.fleet_analyzer import (
        compute_pvr_all, compute_pvr_slices_all,
        compute_fleet_balance,
    )
    _CITY_OK = True
except Exception:
    _CITY_OK = False

st.set_page_config(page_title="eBus Scheduler", page_icon="🚌", layout="wide",
                   initial_sidebar_state="expanded")

st.markdown("""
<style>
  @import url('https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500;600;700&family=DM+Mono:wght@400;500&display=swap');
  html, body, [class*="css"] { font-family: 'DM Sans', sans-serif; }
  .block-container { padding-top: 1.5rem; padding-bottom: 2rem; }

  /* KPI cards */
  .kpi-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(130px, 1fr)); gap: 12px; margin-bottom: 1.2rem; }
  .kpi { background: #fff; border: 1px solid #e8eaed; border-radius: 10px; padding: 14px 16px; }
  .kpi-val { font-size: 1.65rem; font-weight: 700; color: #1a1a2e; line-height: 1.1; font-family: 'DM Mono', monospace; }
  .kpi-label { font-size: 0.72rem; font-weight: 500; color: #888; text-transform: uppercase; letter-spacing: .05em; margin-top: 4px; }
  .kpi-sub { font-size: 0.68rem; color: #aaa; margin-top: 2px; }
  .kpi-ok .kpi-val { color: #16a34a; }
  .kpi-warn .kpi-val { color: #d97706; }
  .kpi-bad .kpi-val { color: #dc2626; }

  /* Compliance scorecard */
  .rule-row { display: flex; align-items: center; gap: 12px; padding: 10px 14px; border-radius: 8px; margin-bottom: 6px; background: #fafafa; border: 1px solid #f0f0f0; }
  .rule-row.fail { background: #fff5f5; border-color: #fecaca; }
  .rule-row.warn { background: #fffbeb; border-color: #fde68a; }
  .rule-row.pass { background: #f0fdf4; border-color: #bbf7d0; }
  .rule-badge { font-size: 1.1rem; flex-shrink: 0; }
  .rule-name { font-weight: 600; font-size: 0.88rem; color: #222; flex: 1; }
  .rule-detail { font-size: 0.78rem; color: #666; }
  .rule-tag { font-size: 0.7rem; font-weight: 600; padding: 2px 8px; border-radius: 20px; }
  .tag-p { background: #fee2e2; color: #dc2626; }
  .tag-o { background: #dbeafe; color: #2563eb; }

  /* Bus timeline */
  .bus-header { font-size: 1rem; font-weight: 700; color: #1a1a2e; margin: 1rem 0 0.3rem 0; display: flex; align-items: center; gap: 10px; }
  .bus-pill { font-size: 0.72rem; font-weight: 600; padding: 2px 10px; border-radius: 20px; background: #e0e7ff; color: #4338ca; }

  /* Section headers */
  .section-title { font-size: 1.1rem; font-weight: 700; color: #1a1a2e; margin: 1.2rem 0 0.4rem 0; padding-bottom: 6px; border-bottom: 2px solid #e8eaed; }

  div[data-testid="stDownloadButton"] button { background: #4f46e5; color: white; border: none; border-radius: 8px; font-weight: 600; padding: 0.5rem 1.5rem; }
  div[data-testid="stDownloadButton"] button:hover { background: #4338ca; }
  section[data-testid="stSidebar"] .stButton button { background: #4f46e5; color: white !important; border: none; border-radius: 8px; font-weight: 600; width: 100%; }
  section[data-testid="stSidebar"] .stButton button:hover { background: #4338ca; }
</style>
""", unsafe_allow_html=True)


# ── Helpers ──────────────────────────────────────────────────────────────────

def kpi(label, value, status="", sub=""):
    cls = f"kpi {('kpi-ok' if status=='ok' else 'kpi-warn' if status=='warn' else 'kpi-bad' if status=='bad' else '')}"
    sub_html = f'<div class="kpi-sub">{sub}</div>' if sub else ""
    return f'<div class="{cls}"><div class="kpi-val">{value}</div><div class="kpi-label">{label}</div>{sub_html}</div>'

def _true_driver_break(bus, i):
    """
    Return the idle break minutes after trip i, or None if not a true driver break.

    A true driver break is the gap between two consecutive Revenue trips with
    NO Charging, Dead, or Shuttle trips between them. Gaps that include a
    charging detour (Revenue → Shuttle → Dead → Charging → Dead → Shuttle →
    Revenue) are excluded — those minutes are spent at the depot, not idle.
    """
    if bus.trips[i].trip_type != "Revenue":
        return None
    # Find the next revenue trip
    nxt_rev = next((t for t in bus.trips[i+1:] if t.trip_type == "Revenue"), None)
    if nxt_rev is None:
        return None
    idx_curr = i
    idx_next = bus.trips.index(nxt_rev)
    # Check for any detour trips between the two revenue trips
    between = bus.trips[idx_curr+1:idx_next]
    if any(t.trip_type in ("Charging", "Dead", "Shuttle") for t in between):
        return None
    if bus.trips[i].actual_arrival and nxt_rev.actual_departure:
        return max(0, int((nxt_rev.actual_departure - bus.trips[i].actual_arrival).total_seconds() / 60))
    return None


def build_schedule_df(config, buses):
    rows = []
    for bus in buses:
        soc = config.initial_soc_percent
        for i, trip in enumerate(bus.trips):
            soc -= (trip.distance_km * config.consumption_rate / config.battery_kwh) * 100
            if trip.trip_type == "Charging":
                soc = min(100.0, soc + config.depot_flow_rate_kw * (trip.travel_time_min/60) / config.battery_kwh * 100)
            brk = _true_driver_break(bus, i)
            if trip.trip_type in ("Dead", "Charging", "Shuttle"): direction = "DEPOT"
            elif trip.direction == "UP": direction = f"{config.route_code}UP"
            else: direction = f"{config.route_code}DN"
            rows.append({
                "Bus": trip.assigned_bus or bus.bus_id, "Direction": direction,
                "Type": trip.trip_type, "From": trip.start_location, "To": trip.end_location,
                "Departure": trip.actual_departure.strftime("%H:%M") if trip.actual_departure else "",
                "Arrival": trip.actual_arrival.strftime("%H:%M") if trip.actual_arrival else "",
                "Break (min)": brk, "Distance (km)": round(trip.distance_km, 1),
                "SOC (%)": round(soc, 1), "Shift": trip.shift,
            })
    return pd.DataFrame(rows)

def build_fleet_df(config, buses):
    rows = []
    for bus in buses:
        rev  = [t for t in bus.trips if t.trip_type == "Revenue"]
        shut = [t for t in bus.trips if t.trip_type == "Shuttle"]
        dead = [t for t in bus.trips if t.trip_type == "Dead"]
        chg  = [t for t in bus.trips if t.trip_type == "Charging"]
        first = next((t.actual_departure for t in bus.trips if t.actual_departure), None)
        last  = next((t.actual_arrival for t in reversed(bus.trips) if t.actual_arrival), None)
        rows.append({
            "Bus": bus.bus_id,
            "Revenue Trips": len(rev),
            "Shuttle Trips": len(shut),
            "Dead Runs": len(dead),
            "Charging": len(chg),
            "Total KM": round(bus.total_km, 1),
            "Revenue KM": round(sum(t.distance_km for t in rev), 1),
            "Dead KM": round(sum(t.distance_km for t in dead), 1),
            "Final SOC (%)": round(bus.soc_percent, 1),
            "Start": first.strftime("%H:%M") if first else "—",
            "End": last.strftime("%H:%M") if last else "—",
        })
    return pd.DataFrame(rows)

def build_route_depiction(config, buses):
    rows = []
    for bus in buses:
        soc = config.initial_soc_percent
        for i, trip in enumerate(bus.trips):
            soc -= (trip.distance_km * config.consumption_rate / config.battery_kwh) * 100
            if trip.trip_type == "Charging":
                soc = min(100, soc + (config.depot_flow_rate_kw * trip.travel_time_min / 60) / config.battery_kwh * 100)
            brk = _true_driver_break(bus, i) or 0
            rows.append({
                "Bus": bus.bus_id,
                "Dep": trip.actual_departure.strftime("%H:%M") if trip.actual_departure else "",
                "Arr": trip.actual_arrival.strftime("%H:%M") if trip.actual_arrival else "",
                "From": trip.start_location, "To": trip.end_location,
                "Type": trip.trip_type,
                "Dist": round(trip.distance_km, 1),
                "SOC": round(soc, 1),
                "Break": brk,
                "_dep_dt": trip.actual_departure,
                "_type": trip.trip_type,
                "_dir": trip.direction,
            })
    df = pd.DataFrame(rows)
    if not df.empty:
        df = df.sort_values(["Bus", "_dep_dt"]).reset_index(drop=True)
    return df

def build_route_diagram(config, buses, selected_bus=None):
    """
    Two-panel diagram:
    Panel A: Route topology (scaled by distance).
    Panel B: Gantt-style schedule — one wide lane per bus, no overlapping.
    """
    from src.bus_scheduler import _nearest_node_from_depot
    from plotly.subplots import make_subplots

    BUS_COLORS = ["#3B5BDB","#C92A2A","#2B8A3E","#E67700",
                  "#7950F2","#0C8599","#C2255C","#5C940D","#A61E4D"]
    DEAD_CLR = "#F08C00"
    CHG_CLR  = "#F59F00"
    ROW_H    = 0.32   # half-height of revenue bar (in y-units)
    ROW_SEP  = 2.6    # vertical space per bus lane

    nearest_name, _, _ = _nearest_node_from_depot(config)

    def _dist(a, b):
        try: return float(config.get_distance(a, b))
        except: return 0.0
    def _ttime(a, b):
        try: return float(config.get_travel_time(a, b))
        except: return 0.0

    route_nodes = [config.start_point]
    for n in getattr(config, "intermediates", []):
        if n and n.strip(): route_nodes.append(n.strip())
    if config.end_point not in route_nodes:
        route_nodes.append(config.end_point)

    cum = {route_nodes[0]: 0.0}
    for i in range(1, len(route_nodes)):
        f, t = route_nodes[i-1], route_nodes[i]
        cum[t] = cum[f] + _dist(f, t)
    total_km = max(cum.values()) or 1.0
    depot_x  = cum.get(nearest_name, 0.0)
    d2n = _dist(config.depot, nearest_name)
    t2n = _ttime(config.depot, nearest_name)

    op_s = config.operating_start.hour + config.operating_start.minute/60
    op_e = config.operating_end.hour   + config.operating_end.minute/60

    def _socs(bus):
        soc, out = config.initial_soc_percent, []
        for t in bus.trips:
            soc -= t.distance_km * config.consumption_rate / config.battery_kwh * 100
            if t.trip_type == "Charging":
                soc = min(100.0, soc + config.depot_flow_rate_kw
                          * t.travel_time_min / 60 / config.battery_kwh * 100)
            out.append(round(max(0, soc), 1))
        return out

    def _soc_clr(s):
        return "#2B8A3E" if s >= 50 else "#E67700" if s >= 30 else "#C92A2A"

    total_dead = sum(t.distance_km for b in buses for t in b.trips if t.trip_type=="Dead")
    total_rev  = sum(1 for b in buses for t in b.trips if t.trip_type=="Revenue")
    total_shut = sum(1 for b in buses for t in b.trips if t.trip_type=="Shuttle")
    total_chg  = sum(1 for b in buses for t in b.trips if t.trip_type=="Charging")
    avg_soc    = sum(b.soc_percent for b in buses) / max(len(buses), 1)
    n_buses    = len(buses)

    def bus_y(idx):
        return (n_buses - idx) * ROW_SEP

    # ── Figure ────────────────────────────────────────────────────────────────
    fig = make_subplots(
        rows=2, cols=1,
        row_heights=[0.18, 0.82],
        vertical_spacing=0.04,
        subplot_titles=[
            f"<b>Route {config.route_code}</b>  "
            f"<span style=\'color:#6B7280;font-weight:normal\'>{config.route_name} — topology</span>",
            f"<b>Bus Schedule</b>  "
            "<span style=\'font-size:10px;color:#9CA3AF\'>hover bars for trip details</span>",
        ],
    )

    # ════ Panel A — Topology ═════════════════════════════════════════════════
    xpad = total_km * 0.06
    fig.add_trace(go.Scatter(
        x=[cum[n] for n in route_nodes], y=[1]*len(route_nodes),
        mode="lines", line=dict(color="#4C6EF5", width=8),
        hoverinfo="skip", showlegend=False,
    ), row=1, col=1)
    fig.add_trace(go.Scatter(
        x=[depot_x, depot_x], y=[-0.35, 0.78],
        mode="lines", line=dict(color=DEAD_CLR, width=2, dash="dot"),
        hoverinfo="skip", showlegend=False,
    ), row=1, col=1)
    for i in range(len(route_nodes)-1):
        f, t = route_nodes[i], route_nodes[i+1]
        d, tt = _dist(f,t), _ttime(f,t)
        if not d: continue
        mid = (cum[f]+cum[t])/2
        fig.add_annotation(x=mid, y=1.38,
            text=f"<b>{d:.1f} km</b>  ·  {tt:.0f} min",
            showarrow=False, row=1, col=1,
            font=dict(size=11, color="#374151"),
            bgcolor="rgba(255,255,255,0.92)", borderpad=3)
    if d2n:
        fig.add_annotation(x=depot_x + total_km*0.03, y=0.22,
            text=f"<b>{d2n:.1f} km</b>  ·  {t2n:.0f} min",
            showarrow=False, row=1, col=1,
            font=dict(size=10, color=DEAD_CLR),
            bgcolor="rgba(255,255,255,0.9)", borderpad=3)
    for node in route_nodes:
        is_term = node in (config.start_point, config.end_point)
        is_near = node == nearest_name
        clr = "#4C6EF5" if is_term else "#2B8A3E" if is_near else "#0C8599"
        sym = "circle" if is_term else "diamond" if is_near else "square"
        lbl = node.replace("BUS STAND","BS").replace("  "," ").strip()
        fig.add_trace(go.Scatter(
            x=[cum[node]], y=[1], mode="markers+text",
            marker=dict(symbol=sym, size=24 if is_term else 18, color=clr,
                        line=dict(color="white", width=2)),
            text=[f"<b>{lbl}</b>"], textposition="bottom center",
            textfont=dict(size=11, color=clr),
            hovertemplate=f"<b>{node}</b>" +
                ("<br><i>Nearest depot node (P2)</i>" if is_near else "") + "<extra></extra>",
            showlegend=False,
        ), row=1, col=1)
    fig.add_trace(go.Scatter(
        x=[depot_x], y=[-0.35], mode="markers+text",
        marker=dict(symbol="square", size=24, color=DEAD_CLR,
                    line=dict(color="white", width=2)),
        text=["<b>DEPOT</b>"], textposition="bottom center",
        textfont=dict(size=11, color=DEAD_CLR),
        hovertemplate=f"<b>{config.depot}</b><br>Dead km total: {total_dead:.1f}<extra></extra>",
        showlegend=False,
    ), row=1, col=1)
    fig.add_annotation(
        x=total_km/2, y=1.85,
        text=(f"<b>{config.fleet_size} buses</b>  ·  "
              f"<b>{total_rev}</b> revenue  +  <b>{total_shut}</b> shuttle  ·  "
              f"Dead {total_dead:.1f} km  ·  {total_chg} charge stops  ·  "
              f"Avg final SOC {avg_soc:.0f}%"),
        showarrow=False, row=1, col=1,
        font=dict(size=12, color="#1F2937"),
        bgcolor="rgba(238,242,255,0.97)",
        borderpad=8, bordercolor="#4C6EF5", borderwidth=1,
    )
    fig.update_xaxes(range=[-xpad, total_km+xpad], showgrid=False, zeroline=False,
                     tickformat=".1f", ticksuffix=" km",
                     title_text="Cumulative distance (km)",
                     color="#9CA3AF", row=1, col=1)
    fig.update_yaxes(range=[-1.0, 2.15], showgrid=False, zeroline=False,
                     showticklabels=False, showline=False, row=1, col=1)

    # ════ Panel B — Gantt ═════════════════════════════════════════════════════

    # Hour gridlines
    for h in range(int(op_s), int(op_e)+1):
        fig.add_shape(type="line", x0=h, x1=h,
                      y0=ROW_SEP*0.3, y1=(n_buses+0.4)*ROW_SEP,
                      line=dict(color="rgba(0,0,0,0.05)", width=1),
                      xref="x2", yref="y2")

    # Peak shading
    for x0, x1, lbl, clr in [
        (8, 11,  "Peak AM",  "rgba(99,102,241,0.06)"),
        (11, 15, "Off-peak", "rgba(16,185,129,0.04)"),
        (16, 20, "Peak PM",  "rgba(99,102,241,0.06)"),
    ]:
        fig.add_vrect(x0=x0, x1=x1, fillcolor=clr, line_width=0, row=2, col=1)
        fig.add_annotation(
            x=(x0+x1)/2, y=(n_buses+0.45)*ROW_SEP,
            xref="x2", yref="y2",
            text=f"<span style=\'font-size:10px;color:#9CA3AF\'><b>{lbl}</b></span>",
            showarrow=False, xanchor="center",
        )

    legend_shown = set()

    for bidx, bus in enumerate(buses):
        bclr   = BUS_COLORS[bidx % len(BUS_COLORS)]
        yc     = bus_y(bidx)
        is_sel = selected_bus is None or bus.bus_id == selected_bus
        alpha  = 1.0 if is_sel else 0.15
        socs   = _socs(bus)

        # Lane background stripe — alternating subtle color
        stripe_clr = "rgba(248,249,250,0.9)" if bidx % 2 == 0 else "rgba(255,255,255,0)"
        fig.add_shape(type="rect",
                      x0=op_s-0.3, x1=op_e+0.3,
                      y0=yc - ROW_SEP*0.45, y1=yc + ROW_SEP*0.45,
                      fillcolor=stripe_clr, line_width=0,
                      xref="x2", yref="y2")

        # Bus label left
        fig.add_annotation(
            x=op_s - 0.22, y=yc, xref="x2", yref="y2",
            text=f"<b style=\'color:{bclr}\' >{bus.bus_id}</b>",
            showarrow=False, xanchor="right",
            font=dict(size=14, color=bclr),
        )

        # Collect revenue trip pairs to annotate breaks BELOW lane (no overlap)
        rev_trips = []

        for tidx, (trip, soc_after) in enumerate(zip(bus.trips, socs)):
            if trip.actual_departure is None: continue
            dep_h = trip.actual_departure.hour + trip.actual_departure.minute/60
            arr_h = (trip.actual_arrival.hour  + trip.actual_arrival.minute/60
                     if trip.actual_arrival else dep_h + trip.travel_time_min/60)
            sc    = _soc_clr(soc_after)
            deps  = trip.actual_departure.strftime("%H:%M")
            arrs  = trip.actual_arrival.strftime("%H:%M") if trip.actual_arrival else "?"
            hover = (f"<b>{bus.bus_id}</b> — {trip.trip_type}<br>"
                     f"{deps} → {arrs}<br>"
                     f"{trip.start_location} → {trip.end_location}<br>"
                     f"SOC after: <b style=\'color:{sc}\'>{soc_after}%</b>"
                     f"  ·  {trip.distance_km:.1f} km<extra></extra>")

            show_leg = bus.bus_id not in legend_shown and is_sel

            if trip.trip_type == "Revenue":
                rev_trips.append((dep_h, arr_h, trip))
                # Main bar
                fig.add_shape(type="rect",
                    x0=dep_h, x1=arr_h,
                    y0=yc - ROW_H, y1=yc + ROW_H,
                    fillcolor=bclr, opacity=0.88*alpha, line_width=0,
                    xref="x2", yref="y2")
                # Direction arrow label centred in bar
                arrow = "▲  UP" if trip.direction == "UP" else "▼  DN"
                if arr_h - dep_h > 0.55:
                    fig.add_annotation(
                        x=(dep_h+arr_h)/2, y=yc, xref="x2", yref="y2",
                        text=f"<span style=\'color:white;font-size:10px\'><b>{arrow}</b></span>",
                        showarrow=False, xanchor="center", yanchor="middle",
                    )
                # Departure circle
                fig.add_shape(type="circle",
                    x0=dep_h-0.04, x1=dep_h+0.04,
                    y0=yc-0.12, y1=yc+0.12,
                    fillcolor="white", line=dict(color=bclr, width=2),
                    xref="x2", yref="y2")
                # Invisible hover scatter
                fig.add_trace(go.Scatter(
                    x=[(dep_h+arr_h)/2], y=[yc],
                    mode="markers",
                    marker=dict(size=1, color=bclr, opacity=0.01),
                    name=bus.bus_id, legendgroup=bus.bus_id,
                    showlegend=show_leg,
                    hovertemplate=hover,
                ), row=2, col=1)
                if show_leg: legend_shown.add(bus.bus_id)

            elif trip.trip_type == "Charging":
                # Amber bar, slightly narrower
                fig.add_shape(type="rect",
                    x0=dep_h, x1=arr_h,
                    y0=yc - ROW_H*0.65, y1=yc + ROW_H*0.65,
                    fillcolor=CHG_CLR, opacity=0.95*alpha,
                    line=dict(color="#92400E", width=1.5),
                    xref="x2", yref="y2")
                dur = int(trip.travel_time_min)
                if arr_h - dep_h > 0.3:
                    fig.add_annotation(
                        x=(dep_h+arr_h)/2, y=yc, xref="x2", yref="y2",
                        text=f"<b style=\'color:#1F2937\'>⚡ {dur}m</b>",
                        showarrow=False, xanchor="center", yanchor="middle",
                        font=dict(size=11),
                    )
                fig.add_trace(go.Scatter(
                    x=[(dep_h+arr_h)/2], y=[yc],
                    mode="markers",
                    marker=dict(size=1, color=CHG_CLR, opacity=0.01),
                    showlegend=False,
                    hovertemplate=hover,
                ), row=2, col=1)

            else:  # Dead or Shuttle — thin dashed connector
                w = 2.5 if trip.trip_type == "Shuttle" else 1.5
                dash = "dash" if trip.trip_type == "Shuttle" else "dot"
                fig.add_trace(go.Scatter(
                    x=[dep_h, arr_h], y=[yc, yc],
                    mode="lines",
                    line=dict(color=DEAD_CLR, width=w, dash=dash),
                    opacity=0.6*alpha,
                    showlegend=False,
                    hovertemplate=hover,
                ), row=2, col=1)

        # Break annotations BELOW the lane — no overlap with bars
        for i in range(1, len(rev_trips)):
            _, arr_h_prev, tp = rev_trips[i-1]
            dep_h_next, _, tc = rev_trips[i]
            # only direct Revenue→Revenue breaks (no Dead/Charge between)
            idx_p = bus.trips.index(tp)
            idx_c = bus.trips.index(tc)
            between = bus.trips[idx_p+1:idx_c]
            if any(t.trip_type in ("Charging","Dead","Shuttle") for t in between):
                continue
            gap = (tc.actual_departure - tp.actual_arrival).total_seconds()/60
            if gap < 2: continue
            mid_h  = (arr_h_prev + dep_h_next) / 2
            clr_b  = "#C92A2A" if gap > 20 else "#495057"
            weight = "bold" if gap > 20 else "normal"
            fig.add_annotation(
                x=mid_h, y=yc - ROW_H - 0.25, xref="x2", yref="y2",
                text=f"<span style=\'color:{clr_b};font-weight:{weight};font-size:10px\'>{gap:.0f}m</span>",
                showarrow=False, xanchor="center", yanchor="top",
            )

        # Final SOC badge — right of lane
        final_soc = socs[-1] if socs else 0
        sclr = _soc_clr(final_soc)
        fig.add_annotation(
            x=op_e + 0.12, y=yc, xref="x2", yref="y2",
            text=f"<b style=\'color:{sclr}\'>{final_soc:.0f}%</b>",
            showarrow=False, xanchor="left", yanchor="middle",
            font=dict(size=12, color=sclr),
        )

    # Legend entries for trip types
    for lbl, clr, sym in [
        ("Revenue trip",   "#4C6EF5", "square"),
        ("Charging ⚡",    CHG_CLR,   "star"),
        ("Dead run",       DEAD_CLR,  "line-ew"),
        ("Shuttle",        "#85C1E9", "line-ew-open"),
    ]:
        fig.add_trace(go.Scatter(
            x=[None], y=[None], mode="markers",
            marker=dict(symbol=sym, size=11, color=clr),
            name=lbl, showlegend=True, legendgroup="types",
        ), row=2, col=1)

    # Panel B axes
    tick_vals = list(range(int(op_s), int(op_e)+1))
    tick_text = [f"{h:02d}:00" for h in tick_vals]
    fig.update_xaxes(
        range=[op_s-0.35, op_e+0.55],
        tickvals=tick_vals, ticktext=tick_text,
        showgrid=False, zeroline=False,
        title_text="Time of day",
        color="#6B7280", row=2, col=1,
    )
    fig.update_yaxes(
        range=[ROW_SEP*0.1, (n_buses+0.7)*ROW_SEP],
        showgrid=False, zeroline=False,
        showticklabels=False,
        row=2, col=1,
    )

    panel_b_px = max(120 * n_buses, 520)
    fig.update_layout(
        height=220 + panel_b_px,
        margin=dict(l=70, r=100, t=55, b=30),
        plot_bgcolor="white",
        paper_bgcolor="rgba(0,0,0,0)",
        hovermode="closest",
        legend=dict(
            orientation="v", x=1.01, y=0.5,
            xanchor="left", yanchor="middle",
            font=dict(size=11),
            bgcolor="rgba(255,255,255,0.96)",
            bordercolor="#E5E7EB", borderwidth=1,
            tracegroupgap=3,
        ),
    )
    return fig


def build_headway_chart_data(config, buses):
    rows = []
    for bus in buses:
        for trip in bus.trips:
            if trip.trip_type == "Revenue" and trip.actual_departure:
                rows.append({"Bus": bus.bus_id, "Direction": trip.direction,
                             "Departure": trip.actual_departure})
    return pd.DataFrame(rows)

def _apply_config_overrides(config, overrides):
    from src.models import RouteConfig
    return RouteConfig(
        route_code=config.route_code, route_name=config.route_name,
        depot=config.depot, start_point=config.start_point,
        end_point=config.end_point, intermediates=config.intermediates,
        fleet_size=overrides.get("fleet_size", config.fleet_size),
        battery_kwh=overrides.get("battery_kwh", config.battery_kwh),
        consumption_rate=overrides.get("consumption_rate", config.consumption_rate),
        initial_soc_percent=overrides.get("initial_soc_percent", config.initial_soc_percent),
        depot_charger_kw=overrides.get("depot_charger_kw", config.depot_charger_kw),
        depot_charger_efficiency=overrides.get("depot_charger_efficiency", config.depot_charger_efficiency),
        terminal_charger_kw=config.terminal_charger_kw,
        terminal_charger_efficiency=config.terminal_charger_efficiency,
        trigger_soc_percent=overrides.get("trigger_soc_percent", config.trigger_soc_percent),
        target_soc_percent=overrides.get("target_soc_percent", config.target_soc_percent),
        min_soc_percent=overrides.get("min_soc_percent", config.min_soc_percent),
        min_charge_duration_min=overrides.get("min_charge_duration_min", config.min_charge_duration_min),
        operating_start=overrides.get("operating_start", config.operating_start),
        operating_end=overrides.get("operating_end", config.operating_end),
        shift_split=overrides.get("shift_split", config.shift_split),
        min_layover_min=overrides.get("min_layover_min", config.min_layover_min),
        preferred_layover_min=overrides.get("preferred_layover_min", config.preferred_layover_min),
        dead_run_buffer_min=overrides.get("dead_run_buffer_min", config.dead_run_buffer_min),
        max_headway_deviation_min=overrides.get("max_headway_deviation_min", config.max_headway_deviation_min),
        km_balance_tolerance_pct=overrides.get("km_balance_tolerance_pct", config.km_balance_tolerance_pct),
        segment_distances=config.segment_distances, segment_times=config.segment_times,
        location_coords=config.location_coords,
        min_km_per_bus=overrides.get("min_km_per_bus", config.min_km_per_bus),
        max_km_per_bus=overrides.get("max_km_per_bus", getattr(config, 'max_km_per_bus', 0)),
        max_layover_min=overrides.get("max_layover_min",
                                      getattr(config, "max_layover_min", 20)),
        midday_charge_soc_percent=overrides.get("midday_charge_soc_percent",
                                                 getattr(config, "midday_charge_soc_percent", 65.0)),
        off_peak_layover_extra_min=overrides.get("off_peak_layover_extra_min",
                                                  getattr(config, "off_peak_layover_extra_min", 0)),
        avg_speed_kmph=overrides.get("avg_speed_kmph",
                                     getattr(config, "avg_speed_kmph", 30.0)),
    )

def _run_core(config, headway_df, travel_time_df, optimize):
    enrich_distances(config)
    trips = generate_trips(config, headway_df, travel_time_df)
    revenue_trips = [t for t in trips if t.trip_type == "Revenue"]
    if optimize:
        from src.optimizer import optimize_schedule
        buses, metrics, _ = optimize_schedule(config, headway_df, travel_time_df, verbose=False)
        assigned_rev = sum(1 for b in buses for t in b.trips if t.trip_type == 'Revenue')
        metrics = compute_metrics(config, buses,
                                  total_revenue_trips=len(revenue_trips),
                                  assigned_revenue_trips=assigned_rev)
    else:
        buses = schedule_buses(config, trips,
                               headway_df=headway_df, travel_time_df=travel_time_df)
        # Bus-driven scheduler creates new Trip objects (not pool references).
        # Count Revenue trips directly from bus schedules.
        assigned_rev = sum(1 for b in buses for t in b.trips if t.trip_type == 'Revenue')
        metrics = compute_metrics(config, buses,
                                  total_revenue_trips=len(revenue_trips),
                                  assigned_revenue_trips=assigned_rev)
    compliance = check_compliance(config, buses, headway_df=headway_df)
    with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as f:
        write_schedule(config, buses, f.name)
        out = Path(f.name).read_bytes()
    return config, buses, metrics, trips, out, compliance

def auto_detect_fleet(raw_config, headway_df, travel_time_df, max_fleet=20):
    """
    Find the minimum fleet_size (1..max_fleet) where all P1-P6 rules PASS
    and every revenue trip is assigned.  Distances are enriched once and
    shared across all trials via the same dict reference.
    Returns (detected_n, config, buses, metrics, trips, output_bytes, compliance).
    """
    # Enrich distances once (mutations are in-place on the shared dict)
    seed_cfg = _apply_config_overrides(raw_config, {"fleet_size": 1})
    enrich_distances(seed_cfg)

    last_result = None
    for n in range(1, max_fleet + 1):
        cfg = _apply_config_overrides(raw_config, {"fleet_size": n})
        trips = generate_trips(cfg, headway_df, travel_time_df)
        buses = schedule_buses(cfg, trips)
        rev_total    = sum(1 for t in trips   if t.trip_type == "Revenue")
        rev_assigned = sum(1 for b in buses for t in b.trips if t.trip_type == "Revenue")
        compliance   = check_compliance(cfg, buses, headway_df=headway_df)
        hard_pass    = all(r["status"] == "PASS"
                           for r in compliance if r.get("priority", 99) <= 6)
        if hard_pass and rev_assigned == rev_total:
            metrics = compute_metrics(cfg, buses, total_revenue_trips=rev_total)
            with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as f:
                write_schedule(cfg, buses, f.name)
                out = Path(f.name).read_bytes()
            return n, cfg, buses, metrics, trips, out, compliance
        # keep last attempt as fallback
        last_result = (n, cfg, buses, rev_total, compliance)

    # Fallback: return best attempt with max fleet
    n, cfg, buses, rev_total, compliance = last_result
    metrics = compute_metrics(cfg, buses, total_revenue_trips=rev_total)
    with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as f:
        write_schedule(cfg, buses, f.name)
        out = Path(f.name).read_bytes()
    return n, cfg, buses, metrics, [], out, compliance


def run_pipeline(uploaded_file, optimize, config_overrides=None, headway_overrides=None, service_max=False):
    with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as tmp:
        tmp.write(uploaded_file.getvalue()); tmp_path = tmp.name
    config, headway_df, travel_time_df = load_config(tmp_path)
    st.session_state["raw_config"] = config
    st.session_state["raw_headway_df"] = headway_df.copy()
    st.session_state["raw_travel_time_df"] = travel_time_df.copy()
    if config_overrides: config = _apply_config_overrides(config, config_overrides)
    if headway_overrides is not None: headway_df = headway_overrides

    # Auto fleet-size detection when fleet_size == 0 in the Excel
    if config.fleet_size == 0:
        st.session_state["auto_fleet_mode"] = True
        detected_n, cfg, buses, metrics, trips, out, compliance = auto_detect_fleet(
            config, headway_df, travel_time_df)
        st.session_state["detected_fleet_size"] = detected_n
        return cfg, buses, metrics, trips, out, compliance

    st.session_state["auto_fleet_mode"] = False
    st.session_state.pop("detected_fleet_size", None)
    if service_max:
        from src.city_models import RouteInput
        ri = RouteInput(config=config, headway_df=headway_df, travel_time_df=travel_time_df)
        nat_hw = _natural_headway(ri) if _CITY_OK else None
        if nat_hw:
            headway_df = _flat_headway_df(ri, nat_hw)
            st.session_state["service_max_headway"] = nat_hw
    return _run_core(config, headway_df, travel_time_df, optimize)

def rerun_from_overrides(config_overrides, headway_overrides=None, optimize=False, service_max=False):
    raw_config = st.session_state.get("raw_config")
    raw_hw = st.session_state.get("raw_headway_df")
    raw_tt = st.session_state.get("raw_travel_time_df")
    if raw_config is None: return None
    config = _apply_config_overrides(raw_config, config_overrides)
    headway_df = headway_overrides if headway_overrides is not None else raw_hw
    # Honour auto-detect if user set fleet_size back to 0 in the Config tab
    if config.fleet_size == 0:
        st.session_state["auto_fleet_mode"] = True
        detected_n, cfg, buses, metrics, trips, out, compliance = auto_detect_fleet(
            config, headway_df, raw_tt)
        st.session_state["detected_fleet_size"] = detected_n
        return cfg, buses, metrics, trips, out, compliance
    st.session_state["auto_fleet_mode"] = False
    st.session_state.pop("detected_fleet_size", None)
    if service_max and _CITY_OK:
        from src.city_models import RouteInput
        ri2 = RouteInput(config=config, headway_df=headway_df, travel_time_df=raw_tt)
        nat_hw2 = _natural_headway(ri2)
        headway_df = _flat_headway_df(ri2, nat_hw2)
        st.session_state["service_max_headway"] = nat_hw2
    return _run_core(config, headway_df, raw_tt, optimize)


# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("## 🚌 eBus Scheduler")

    if _CITY_OK:
        app_mode = st.radio(
            "Mode", ["🚌 Single Route", "🏙️ Citywide"], index=0,
            help="Single Route: one config. Citywide: multiple configs with fleet rebalancing.",
        )
    else:
        app_mode = "🚌 Single Route"

    st.divider()

    if app_mode == "🚌 Single Route":
        st.caption("Upload route config Excel to generate a schedule.")
        uploaded = st.file_uploader("Config Excel", type=["xlsx"], label_visibility="collapsed")
        single_mode = st.radio(
            "Scheduling mode",
            ["📋 Planning-Compliant", "⚡ Efficiency-Maximising", "🎯 Service Maximization"],
            index=0, label_visibility="collapsed",
        )
        if single_mode == "📋 Planning-Compliant":
            optimize = False; service_max_single = False
            st.info("📋 **Planning-Compliant** — follows headway profile strictly. "
                    "Fleet size from config.", icon="📋")
        elif single_mode == "⚡ Efficiency-Maximising":
            optimize = True; service_max_single = False
            st.info("⚡ **Efficiency-Maximising** — finds minimum fleet satisfying all rules. "
                    "Config fleet size overridden; headway floor still enforced.", icon="⚡")
        else:
            optimize = False; service_max_single = True
            st.info("🎯 **Service Maximization** — uses config fleet, ignores headway profile. "
                    "Computes minimum achievable constant headway for even spacing.", icon="🎯")
        run_btn = st.button("▶ Generate Schedule", type="primary", disabled=uploaded is None)
        st.divider()
        st.caption("Rules enforced: P4 break from config, P2 via nearest node, P5 midday charge, P3 SOC ≥ 20%.")
    else:
        st.caption("Upload config Excel files for all routes.")
        uploaded_files = st.file_uploader(
            "Route Config Files", type=["xlsx", "xlsm"],
            accept_multiple_files=True, label_visibility="collapsed",
        )
        city_sched_mode = st.radio(
            "Scheduling mode",
            ["📋 Planning-Compliant", "⚡ Efficiency-Maximising", "🎯 Service Maximization"],
            index=0, label_visibility="collapsed",
        )
        if city_sched_mode == "📋 Planning-Compliant":
            city_mode = "planning"
            st.info("📋 **Planning-Compliant** — headway profile respected, surplus rebalanced.", icon="📋")
        elif city_sched_mode == "⚡ Efficiency-Maximising":
            city_mode = "efficiency"
            st.info("⚡ **Efficiency-Maximising** — minimum fleet per route, KPI-driven. "
                    "Config fleet size overridden; headway floor still enforced.", icon="⚡")
        else:
            city_mode = "service_max"
            st.info("🎯 **Service Maximization** — config fleet used as-is. "
                    "Headway profile ignored; constant minimum-achievable headway computed per route.", icon="🎯")
        total_fleet_override = st.number_input(
            "Total Fleet Override", min_value=0, value=0, step=1,
            help="0 = use sum from configs. >0 = cap total citywide fleet.",
        )
        city_run_btn = st.button(
            "▶ Generate Citywide Schedule", type="primary",
            disabled=not uploaded_files,
        )
        st.divider()
        st.caption("📋 Planning-Compliant · ⚡ Efficiency-Maximising · 🎯 Service Maximization")


# ── Main ──────────────────────────────────────────────────────────────────────
# Ensure variables exist regardless of which sidebar mode was active
if 'uploaded' not in dir(): uploaded = None
if 'optimize' not in dir(): optimize = False
if 'run_btn' not in dir(): run_btn = False
if 'uploaded_files' not in dir(): uploaded_files = []
if 'city_run_btn' not in dir(): city_run_btn = False
if 'total_fleet_override' not in dir(): total_fleet_override = 0
if 'service_max_single' not in dir(): service_max_single = False
if 'city_mode' not in dir(): city_mode = "planning"
# Legacy compat: city_optimize used in Fleet Config re-run button
city_optimize = (city_mode == "efficiency")

if app_mode == "🚌 Single Route":
    # ═══════════════════════════════ SINGLE ROUTE ════════════════════════════════

    if uploaded is None:
        st.markdown("## Welcome to eBus Scheduler")
        c1, c2, c3, c4 = st.columns(4)
        icons = ["📁", "⚙️", "📊", "⬇️"]
        steps = [("Upload", "Drop your config Excel with route, fleet, headway, and travel time data."),
                 ("Generate", "Engine creates trips, assigns buses with min-break enforcement and midday charging."),
                 ("Review", "Check compliance, route depiction, headways, SOC curves, and KM balance."),
                 ("Download", "Get formatted schedule Excel with full trip-by-trip detail.")]
        for col, (icon, (title, desc)) in zip([c1, c2, c3, c4], zip(icons, steps)):
            with col:
                st.markdown(f"### {icon} {title}")
                st.caption(desc)

    elif run_btn:
        with st.spinner("Running scheduler..."):
            try:
                result = run_pipeline(uploaded, optimize, service_max=service_max_single)
            except ConfigError as e:
                st.error(f"Config error: {e}"); st.stop()
            except Exception as e:
                st.error(f"Error: {e}"); st.stop()
        config, buses, metrics, trips, output_bytes, compliance = result
        st.session_state.update({
            "config": config, "buses": buses, "metrics": metrics, "trips": trips,
            "output_bytes": output_bytes, "compliance": compliance,
            "prev_compliance": None, "has_results": True,
        })

    if st.session_state.get("has_results"):
        config = st.session_state["config"]
        buses  = st.session_state["buses"]
        metrics = st.session_state["metrics"]
        output_bytes = st.session_state["output_bytes"]
        compliance = st.session_state.get("compliance", [])
        prev_compliance = st.session_state.get("prev_compliance")

        # ── Route title + download ────────────────────────────────────────────
        col_title, col_dl = st.columns([3, 1])
        with col_title:
            st.markdown(f"## Route {config.route_code} — {config.route_name}")
        with col_dl:
            st.download_button(
                f"⬇ Download Schedule",
                data=output_bytes,
                file_name=f"{config.route_code}_schedule.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )

        # ── KPI bar ──────────────────────────────────────────────────────────
        all_pass = all(r["status"] in ("PASS",) for r in compliance if r.get("priority", 99) <= 6)
        fail_count = sum(1 for r in compliance if r["status"] == "FAIL")
        soc_ok = metrics.min_soc_seen >= 25
        km_ok = metrics.km_range <= 20
        shuttle_count = sum(1 for b in buses for t in b.trips if t.trip_type == "Shuttle")
        # trip_ok: all pool trips are covered (revenue + shuttle together)
        # Shuttle trips are passenger-carrying corridor legs added by the scheduler.
        # They are not in the pool, so we only check revenue against pool target.
        trip_ok = metrics.revenue_trips_assigned >= metrics.revenue_trips_total
        unserved = max(0, metrics.revenue_trips_total - metrics.revenue_trips_assigned)

        # Revenue Trips KPI: show R · S · D counts on one line
        # R = full Revenue trips (pool), S = Shuttle trips (corridor legs), D = Dead runs
        dead_count = sum(1 for b in buses for t in b.trips if t.trip_type == "Dead")
        rev_parts  = [f"{metrics.revenue_trips_assigned}R"]
        if shuttle_count > 0:
            rev_parts.append(f"{shuttle_count}S")
        rev_parts.append(f"{dead_count}D")
        rev_label  = " · ".join(rev_parts)
        total_trips = metrics.revenue_trips_assigned + shuttle_count + dead_count
        rev_sub     = f"{total_trips} total trips · of {metrics.revenue_trips_total} planned revenue"
        rev_status = "ok" if unserved == 0 else "warn" if unserved <= 5 else "bad"

        st.markdown(
            '<div class="kpi-grid">' +
            kpi("Revenue Trips", rev_label, rev_status, sub=rev_sub) +
            kpi("Total KM", f"{metrics.total_km:.0f}") +
            kpi("Dead KM %", f"{metrics.dead_km_ratio:.1%}",
                "ok" if metrics.dead_km_ratio < 0.15 else "warn") +
            kpi("KM Deviation", f"{metrics.km_range:.1f} km",
                "ok" if km_ok else "warn") +
            kpi("Min SOC", f"{metrics.min_soc_seen:.1f}%",
                "ok" if soc_ok else "warn") +
            kpi("Charging Stops", str(metrics.charging_stops)) +
            kpi("Compliance", f"{10-fail_count}/10",
                "ok" if fail_count == 0 else "warn" if fail_count <= 2 else "bad") +
            kpi("Fleet", f"{config.fleet_size} buses") +
            '</div>', unsafe_allow_html=True,
        )

        # ── Tabs ─────────────────────────────────────────────────────────────
        tab_compliance, tab_route, tab_fleet, tab_schedule, tab_bus, tab_config = st.tabs([
            "✅ Compliance", "🗺 Route Depiction", "📊 Fleet Summary",
            "📋 Full Schedule", "🚌 Bus Detail", "⚙️ Config",
        ])

        # ════════════════════════════════════════════════════════════════════
        # TAB 0: Compliance
        # ════════════════════════════════════════════════════════════════════
        with tab_compliance:
            if prev_compliance:
                st.info("Showing before/after comparison from config edit.")

            STATUS = {"PASS": ("✅", "pass"), "FAIL": ("❌", "fail"),
                      "WARN": ("⚠️", "warn"), "INFO": ("✅", "pass")}

            p_rules = [r for r in compliance if r.get("priority", 99) <= 6]
            o_rules = [r for r in compliance if r.get("priority", 99) > 6]

            for section_label, rules, tag_cls, tag_text in [
                ("Priority Rules (P1–P6)", p_rules, "tag-p", "P"),
                ("Operational Rules (O1–O4)", o_rules, "tag-o", "O"),
            ]:
                st.markdown(f'<div class="section-title">{section_label}</div>', unsafe_allow_html=True)
                for i, rule in enumerate(rules):
                    effective = rule["status"]
                    if effective == "INFO" and not rule.get("violations"): effective = "PASS"
                    icon, cls = STATUS.get(effective, ("❓", ""))
                    viols = rule.get("violations", [])
                    if not viols and effective == "WARN": viols = [rule.get("details", "")]

                    # before/after badge
                    badge = ""
                    if prev_compliance:
                        prev_idx = next((j for j, r in enumerate(prev_compliance)
                                         if r["rule"] == rule["rule"]), None)
                        if prev_idx is not None and prev_compliance[prev_idx]["status"] != rule["status"]:
                            pi, _ = STATUS.get(prev_compliance[prev_idx]["status"], ("❓",""))
                            badge = f' <span style="font-size:.75rem;color:#888">{pi}→{icon}</span>'

                    label = f"{icon} {rule['rule']}"
                    with st.expander(label, expanded=(effective == "FAIL")):
                        st.caption(rule.get("details", ""))
                        if viols:
                            for v in viols: st.markdown(f"- {v}")
                        else:
                            st.markdown("_No violations._")

            if prev_compliance:
                changed = [r["rule"] for r in compliance
                           if any(p["rule"] == r["rule"] and p["status"] != r["status"]
                                  for p in prev_compliance)]
                if changed: st.warning(f"Rules changed: {', '.join(changed)}")
                else: st.success("No rule statuses changed.")

        # ════════════════════════════════════════════════════════════════════
        # TAB 1: Route Depiction
        # ════════════════════════════════════════════════════════════════════
        with tab_route:
            # ── Bus filter + diagram ─────────────────────────────────────────
            if not _PLOTLY_OK:
                st.warning("Install **plotly** to enable the route diagram (`pip install plotly` or add to requirements.txt).")
            else:
                filter_col, spacer = st.columns([2, 5])
                with filter_col:
                    bus_opts = ["All buses"] + config.bus_ids()
                    sel = st.selectbox("Filter diagram by bus", bus_opts,
                                       label_visibility="collapsed")
                selected_bus_filter = None if sel == "All buses" else sel
                try:
                    route_fig = build_route_diagram(config, buses,
                                                    selected_bus=selected_bus_filter)
                    st.plotly_chart(route_fig, use_container_width=True,
                                    config={"displayModeBar": True,
                                            "modeBarButtonsToRemove": ["lasso2d","select2d"],
                                            "toImageButtonOptions": {"filename": f"route_{config.route_code}"}})
                except Exception as e:
                    st.warning(f"Could not render route diagram: {e}")
                st.caption(
                    "Straight-line representation: distances in Panel A are cumulative along the route; "
                    "depot is shown off-line for clarity. "
                    "Panel B is a time–space diagram: parallel diagonal lines = even headways; "
                    "converging lines = bunching; gaps = charging or off-peak window."
                )
            st.divider()

            # ── Per-bus trip tables ──────────────────────────────────────────
            st.markdown('<div class="section-title">Per-Bus Trip Detail</div>', unsafe_allow_html=True)
            route_df = build_route_depiction(config, buses)
            for bus in buses:
                bus_data = route_df[route_df["Bus"] == bus.bus_id].reset_index(drop=True)
                if bus_data.empty: continue

                rev = [t for t in bus.trips if t.trip_type == "Revenue"]
                dead_km = sum(t.distance_km for t in bus.trips if t.trip_type == "Dead")
                chg = [t for t in bus.trips if t.trip_type == "Charging"]
                first = next((t.actual_departure for t in bus.trips if t.actual_departure), None)
                last  = next((t.actual_arrival for t in reversed(bus.trips) if t.actual_arrival), None)

                st.markdown(
                    f'<div class="bus-header">'
                    f'<span class="bus-pill">{bus.bus_id}</span>'
                    f'{len(rev)} revenue trips &nbsp;·&nbsp; {bus.total_km:.1f} km &nbsp;·&nbsp;'
                    f' SOC {bus.soc_percent:.1f}% final &nbsp;·&nbsp;'
                    f' {first.strftime("%H:%M") if first else "?"} – {last.strftime("%H:%M") if last else "?"}'
                    f'</div>', unsafe_allow_html=True,
                )

                display_rows = []
                for _, row in bus_data.iterrows():
                    t = row["_type"]; d = row["_dir"]
                    marker = "🔋" if t=="Charging" else "⚫" if t=="Dead" else "🔵" if d=="UP" else "🟢"
                    brk = row["Break"]
                    display_rows.append({
                        "": marker, "Dep": row["Dep"], "Arr": row["Arr"],
                        "From": row["From"], "To": row["To"], "Type": row["Type"],
                        "Dist": row["Dist"],
                        "SOC": f'{row["SOC"]}{"⚠️" if row["SOC"] < 30 else ""}',
                        "Break": f"{brk} min" if brk > 0 else "",
                        "Dead KM": row["Dist"] if t == "Dead" else "",
                    })

                df_show = pd.DataFrame(display_rows)
                st.dataframe(df_show, hide_index=True,
                             height=min(420, 36 * len(df_show) + 38),
                             column_config={"": st.column_config.TextColumn(width="small")})
                st.caption(
                    f"Dead KM: {dead_km:.1f} &nbsp;|&nbsp; "
                    f"Charging: {len(chg)} stop{'s' if len(chg)!=1 else ''} &nbsp;|&nbsp; "
                    f"Shift split: {config.shift_split}"
                )
                st.divider()

        # ════════════════════════════════════════════════════════════════════
        # TAB 2: Fleet Summary
        # ════════════════════════════════════════════════════════════════════
        with tab_fleet:
            fleet_df = build_fleet_df(config, buses)
            st.dataframe(fleet_df, hide_index=True, use_container_width=True)

            col1, col2 = st.columns(2)
            with col1:
                st.markdown('<div class="section-title">KM per Bus</div>', unsafe_allow_html=True)
                km_chart = fleet_df[["Bus","Revenue KM","Dead KM"]].set_index("Bus")
                st.bar_chart(km_chart, color=["#4f46e5","#94a3b8"])
            with col2:
                st.markdown('<div class="section-title">Final SOC per Bus (%)</div>', unsafe_allow_html=True)
                soc_chart = fleet_df[["Bus","Final SOC (%)"]].set_index("Bus")
                st.bar_chart(soc_chart, color="#16a34a")

            st.markdown('<div class="section-title">Departure Headways</div>', unsafe_allow_html=True)
            st.caption("Time gaps between consecutive departures in the same direction. "
                       "Irregular gaps are expected when bus cycle time ≠ fleet × headway — "
                       "see Config tab to tune.")
            hw_data = build_headway_chart_data(config, buses)
            if not hw_data.empty:
                col_up, col_dn = st.columns(2)
                for col, direction, color in [(col_up, "UP", "#4f46e5"), (col_dn, "DN", "#16a34a")]:
                    with col:
                        dir_data = hw_data[hw_data["Direction"]==direction].sort_values("Departure")
                        if len(dir_data) < 2: continue
                        deps = dir_data["Departure"].tolist()
                        gaps = [{"Dep": deps[i].strftime("%H:%M"),
                                 "Headway (min)": round((deps[i]-deps[i-1]).total_seconds()/60)}
                                for i in range(1, len(deps))]
                        gdf = pd.DataFrame(gaps).set_index("Dep")
                        st.markdown(f"**{direction}** headways")
                        st.bar_chart(gdf, height=220, color=color)

        # ════════════════════════════════════════════════════════════════════
        # TAB 3: Full Schedule
        # ════════════════════════════════════════════════════════════════════
        with tab_schedule:
            schedule_df = build_schedule_df(config, buses)
            fc1, fc2, fc3 = st.columns(3)
            with fc1:
                type_filter = st.multiselect("Trip Type", ["Revenue","Dead","Charging"],
                                             default=["Revenue","Dead","Charging"])
            with fc2:
                bus_filter = st.multiselect("Bus", config.bus_ids(), default=config.bus_ids())
            with fc3:
                dir_filter = st.multiselect("Direction", schedule_df["Direction"].unique().tolist(),
                                            default=schedule_df["Direction"].unique().tolist())
            filtered = schedule_df[
                schedule_df["Type"].isin(type_filter) &
                schedule_df["Bus"].isin(bus_filter) &
                schedule_df["Direction"].isin(dir_filter)
            ]
            st.dataframe(filtered, hide_index=True, height=520, use_container_width=True)
            st.caption(f"Showing {len(filtered)} of {len(schedule_df)} trips")

        # ════════════════════════════════════════════════════════════════════
        # TAB 4: Bus Detail
        # ════════════════════════════════════════════════════════════════════
        with tab_bus:
            selected_bus = st.selectbox("Select Bus", config.bus_ids(), label_visibility="collapsed")
            bus_obj = next(b for b in buses if b.bus_id == selected_bus)
            bus_df = build_schedule_df(config, [bus_obj])
            rev_count = len([t for t in bus_obj.trips if t.trip_type=="Revenue"])
            dead_km = sum(t.distance_km for t in bus_obj.trips if t.trip_type=="Dead")
            util = rev_count / max(1, len(bus_obj.trips)) * 100

            st.markdown(
                '<div class="kpi-grid">' +
                kpi("Revenue Trips", str(rev_count)) +
                kpi("Total KM", f"{bus_obj.total_km:.1f}") +
                kpi("Dead KM", f"{dead_km:.1f}", "ok" if dead_km < 30 else "warn") +
                kpi("Final SOC", f"{bus_obj.soc_percent:.1f}%",
                    "ok" if bus_obj.soc_percent > 30 else "warn") +
                kpi("Utilisation", f"{util:.0f}%",
                    "ok" if util > 70 else "warn") +
                '</div>', unsafe_allow_html=True,
            )

            st.dataframe(bus_df, hide_index=True, height=380, use_container_width=True)

            soc_data = bus_df[bus_df["Departure"] != ""][["Departure","SOC (%)"]].set_index("Departure")
            if not soc_data.empty:
                st.markdown('<div class="section-title">SOC Progression</div>', unsafe_allow_html=True)
                st.line_chart(soc_data, color="#4f46e5")

        # ════════════════════════════════════════════════════════════════════
        # TAB 5: Config
        # ════════════════════════════════════════════════════════════════════
        with tab_config:
            # Auto-fleet detection banner
            if st.session_state.get("auto_fleet_mode"):
                detected_n = st.session_state.get("detected_fleet_size", "?")
                st.success(
                    f"🚌 **Auto Fleet Detection:** Your Excel had fleet_size = 0. "
                    f"The scheduler found that **{detected_n} buses** are the minimum required "
                    f"to serve all trips while satisfying P1–P6. "
                    f"You can lock this value below and regenerate."
                )
            if st.session_state.get("service_max_headway"):
                nat_hw = st.session_state["service_max_headway"]
                st.info(
                    f"🎯 **Service Maximization active:** Configured headway profile was replaced "
                    f"with a constant **{nat_hw:.0f}-min** headway — the minimum achievable with "
                    f"this fleet and route length. To change, adjust fleet size in the config."
                )

            st.caption("Edit any value and click **Apply & Regenerate** — no re-upload needed.")
            with st.form("config_edit_form"):
                col_a, col_b = st.columns(2)

                with col_a:
                    st.markdown("#### 🚍 Fleet & Battery")
                    new_fleet = st.number_input(
                        "Fleet Size (0 = auto-detect minimum)",
                        value=config.fleet_size, min_value=0, max_value=50, step=1,
                        help="Set to 0 to let the scheduler find the minimum fleet that satisfies all P1-P6 rules.")
                    new_battery = st.number_input("Battery (kWh)", value=float(config.battery_kwh), min_value=10.0, step=10.0)
                    new_cons    = st.number_input("Consumption (kWh/km)", value=float(config.consumption_rate), min_value=0.1, step=0.1, format="%.2f")
                    new_init_soc= st.number_input("Initial SOC (%)", value=float(config.initial_soc_percent), min_value=20.0, max_value=100.0, step=5.0)
                    new_min_km  = st.number_input("Min KM per Bus (0=none)", value=float(config.min_km_per_bus), min_value=0.0, step=10.0)
                    new_avg_spd = st.number_input(
                        "Average Speed (km/hr) — fallback when segment time is missing",
                        value=float(getattr(config, "avg_speed_kmph", 30.0)),
                        min_value=5.0, max_value=120.0, step=5.0,
                        help="Used by the distance engine to estimate travel time when a segment's time is not in the Excel.")

                    st.markdown("#### ⏰ Operating Hours")
                    new_op_start = st.text_input("Start (HH:MM)", value=config.operating_start.strftime("%H:%M"))
                    new_op_end   = st.text_input("End (HH:MM)",   value=config.operating_end.strftime("%H:%M"))
                    new_shift    = st.text_input("Shift Split (HH:MM)", value=config.shift_split.strftime("%H:%M"))

                with col_b:
                    st.markdown("#### 🔋 Charging")
                    new_depot_kw    = st.number_input("Depot Charger (kW)", value=float(config.depot_charger_kw), min_value=0.0, step=10.0)
                    new_depot_eff   = st.number_input("Depot Efficiency (0–1)", value=float(config.depot_charger_efficiency), min_value=0.0, max_value=1.0, step=0.05, format="%.2f")
                    new_trigger_soc = st.number_input(
                        "Charge Trigger SOC (%)",
                        value=float(config.trigger_soc_percent), min_value=20.0, max_value=100.0, step=5.0,
                        help="SOC level below which the scheduler sends a bus for a reactive charge stop.")
                    new_target_soc  = st.number_input("Charge Target SOC (%)", value=float(config.target_soc_percent), min_value=20.0, max_value=100.0, step=5.0)
                    new_midday_soc  = st.number_input(
                        "Midday Charge Trigger SOC (%) — P5",
                        value=float(getattr(config, "midday_charge_soc_percent", 65.0)),
                        min_value=20.0, max_value=100.0, step=5.0,
                        help="Bus must be below this SOC to be sent for midday charging (P5 window 12:00-15:00). Fleet > 10 waives this rule.")

                    st.markdown("#### 🔁 Break & Layover")
                    new_pref_layover = st.number_input(
                        "Min Driver Break (min) — P4",
                        value=int(config.preferred_layover_min), min_value=1, step=1,
                        help="Minimum break enforced between every pair of revenue trips (P4).")
                    new_max_layover  = st.number_input(
                        "Max Layover (min) — P4 upper bound",
                        value=int(getattr(config, "max_layover_min", 20)),
                        min_value=1, max_value=120, step=1,
                        help="Maximum allowed gap between revenue trips. Replaces the hardcoded 20-minute ceiling.")
                    new_off_peak_extra = st.number_input(
                        "Off-Peak Extra Break (min) — 11:00–15:00",
                        value=int(getattr(config, "off_peak_layover_extra_min", 0)),
                        min_value=0, max_value=60, step=1,
                        help="Added on top of Min Driver Break during off-peak hours to widen headways. Capped at Max Layover.")
                    new_min_layover  = st.number_input("Min Terminus Layover (min)", value=int(config.min_layover_min), min_value=0, step=1)
                    new_dead_buf     = st.number_input("Dead Run Buffer (min)", value=int(config.dead_run_buffer_min), min_value=0, step=1)
                    new_adj_buf      = st.number_input(
                        "Break Adjustment Buffer (min)",
                        value=int(config.max_headway_deviation_min), min_value=0, step=1,
                        help="Max minutes the safety-net pass can shift a trip to enforce the driver break.")

                # Segment distances (read-only)
                with st.expander("📍 Segment Distances & Times (read-only)"):
                    seg_rows = [{"Segment": k, "Distance (km)": d,
                                 "Time (min)": config.segment_times.get(k, 0)}
                                for k, d in sorted(config.segment_distances.items())]
                    st.dataframe(pd.DataFrame(seg_rows), hide_index=True)

                # Headway profile (editable) ─────────────────────────────────────
                st.markdown("#### 🕐 Headway Profile")
                st.caption(
                    "Edit **Headway (min)** per time band. "
                    "Time columns are read-only. "
                    "Changes take effect when you click Apply & Regenerate."
                )
                _hw_src = st.session_state.get("raw_headway_df", pd.DataFrame())
                if not _hw_src.empty:
                    edited_hw_df = st.data_editor(
                        _hw_src[["time_from", "time_to", "headway_min"]].copy(),
                        column_config={
                            "time_from": st.column_config.TextColumn("From", disabled=True),
                            "time_to":   st.column_config.TextColumn("To",   disabled=True),
                            "headway_min": st.column_config.NumberColumn(
                                "Headway (min)", min_value=5, max_value=120, step=1,
                                help=(
                                    "Minimum gap (minutes) between consecutive same-direction "
                                    "departures from the same terminal in this time band. "
                                    "Applies fleet-wide."
                                ),
                            ),
                        },
                        hide_index=True,
                        use_container_width=True,
                        num_rows="fixed",
                        key="headway_editor",
                    )
                else:
                    edited_hw_df = None
                    st.caption("Run a schedule first to enable headway editing.")

                apply_btn = st.form_submit_button("🔄 Apply & Regenerate", type="primary")

            if apply_btn:
                def _pt(s):
                    try: h, m = s.strip().split(":"); return dtime(int(h), int(m))
                    except: return None

                overrides = {
                    "fleet_size": new_fleet, "battery_kwh": new_battery,
                    "consumption_rate": new_cons, "initial_soc_percent": new_init_soc,
                    "min_km_per_bus": new_min_km, "avg_speed_kmph": new_avg_spd,
                    "depot_charger_kw": new_depot_kw, "depot_charger_efficiency": new_depot_eff,
                    "trigger_soc_percent": new_trigger_soc, "target_soc_percent": new_target_soc,
                    "midday_charge_soc_percent": new_midday_soc,
                    "preferred_layover_min": new_pref_layover, "max_layover_min": new_max_layover,
                    "off_peak_layover_extra_min": new_off_peak_extra,
                    "min_layover_min": new_min_layover, "dead_run_buffer_min": new_dead_buf,
                    "max_headway_deviation_min": new_adj_buf,
                }
                for key, val in [("operating_start", new_op_start),
                                  ("operating_end", new_op_end), ("shift_split", new_shift)]:
                    t = _pt(val)
                    if t: overrides[key] = t

                hw_overrides = edited_hw_df.copy() if edited_hw_df is not None else None

                with st.spinner("Regenerating..."):
                    try:
                        result = rerun_from_overrides(overrides, headway_overrides=hw_overrides,
                                                         service_max=st.session_state.get("service_max_headway") is not None)
                    except Exception as e:
                        st.error(f"Error: {e}"); result = None

                if result:
                    new_cfg, new_buses, new_metrics, new_trips, new_out, new_comp = result
                    st.session_state["prev_compliance"] = st.session_state.get("compliance")
                    st.session_state.update({
                        "config": new_cfg, "buses": new_buses, "metrics": new_metrics,
                        "trips": new_trips, "output_bytes": new_out, "compliance": new_comp,
                    })
                    st.success("✅ Regenerated! Switch tabs to see updated results.")
                    st.rerun()



elif app_mode == "🏙️ Citywide":
    # ═══════════════════════════════ CITYWIDE ════════════════════════════════════

    if not st.session_state.get("has_city_results") and not uploaded_files:
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

    if city_run_btn and uploaded_files:
        with st.spinner("Loading configs and generating citywide schedule..."):
            try:
                city_config, load_warnings = load_city_config_from_files(uploaded_files)
                if total_fleet_override > 0:
                    city_config.total_fleet = total_fleet_override
                city_result = schedule_city(city_config, mode=city_mode)
            except ConfigError as e:
                st.error(f"Config error: {e}"); st.stop()
            except Exception as e:
                st.error(f"Error: {e}"); import traceback; st.code(traceback.format_exc()); st.stop()
        st.session_state["city_result"] = city_result
        st.session_state["city_config"] = city_config
        st.session_state["load_warnings"] = load_warnings
        st.session_state["has_city_results"] = True
        st.session_state["city_mode_used"] = city_mode

    if not st.session_state.get("has_city_results"):
        if uploaded_files:
            st.info("Click **▶ Generate Citywide Schedule** in the sidebar to run.")
        st.stop()

    cs: CitySchedule = st.session_state["city_result"]
    city_cfg: CityConfig = st.session_state["city_config"]
    load_warnings = st.session_state.get("load_warnings", [])

    if load_warnings:
        with st.expander(f"⚠ {len(load_warnings)} loading warning(s)", expanded=False):
            for w in load_warnings:
                st.warning(w)

    _mode_map = {
        "planning":    "📋 Planning-Compliant",
        "efficiency":  "⚡ Efficiency-Maximising",
        "service_max": "🎯 Service Maximization",
    }
    _stored_city_mode = st.session_state.get("city_mode_used", "planning")
    mode_label = _mode_map.get(_stored_city_mode, "📋 Planning-Compliant")
    st.markdown(f"## 🏙️ Citywide Schedule — {len(cs.results)} Routes")
    st.caption(f"Mode: **{mode_label}** · Depot: **{city_cfg.depot_name}**")
    _stored_city_mode = st.session_state.get("city_mode_used", "planning")
    if _stored_city_mode == "efficiency":
        st.info("⚡ **Efficiency-Maximising**: binary-searches minimum fleet per route satisfying "
                "all P1–P6 rules. Config fleet size overridden; headway floor still enforced.", icon="⚡")
    elif _stored_city_mode == "service_max":
        st.info("🎯 **Service Maximization**: config fleet used as-is. Headway profile replaced "
                "with constant minimum-achievable headway per route for even spacing.", icon="🎯")
    else:
        st.info("📋 **Planning-Compliant**: headway profile respected. "
                "Surplus buses redistributed to deficit routes based on time-sliced PVR.", icon="📋")

    # Citywide KPIs
    st.markdown(
        '<div class="kpi-grid">' +
        kpi("Routes", str(len(cs.results))) +
        kpi("Total Fleet", str(cs.total_buses_used),
            sub=f"configured: {city_cfg.total_configured_fleet}") +
        kpi("Revenue Trips", str(cs.total_revenue_trips)) +
        kpi("Revenue KM", f"{cs.total_revenue_km:,.0f}") +
        kpi("Dead KM %", f"{cs.citywide_dead_km_ratio:.1%}",
            "ok" if cs.citywide_dead_km_ratio < 0.15 else "warn") +
        kpi("Min SOC", f"{cs.min_soc_citywide:.1f}%",
            "ok" if cs.min_soc_citywide >= 25 else "warn" if cs.min_soc_citywide >= 20 else "bad") +
        kpi("Utilization", f"{cs.citywide_utilization_pct:.0f}%",
            "ok" if cs.citywide_utilization_pct >= 85 else "warn") +
        kpi("Transfers", str(len(cs.transfers)),
            "ok" if len(cs.transfers) == 0 else "warn") +
        kpi("Avg HW Dev", f"{cs.avg_headway_deviation_min:.1f} min",
            "ok" if cs.avg_headway_deviation_min < 5 else "warn"
            if cs.avg_headway_deviation_min < 10 else "bad",
            sub="vs configured") +
        kpi("Max Gap", f"{cs.max_headway_gap_min:.0f} min",
            "ok" if cs.max_headway_gap_min < 45 else "warn"
            if cs.max_headway_gap_min < 60 else "bad",
            sub="largest departure gap") +
        '</div>', unsafe_allow_html=True,
    )

    tab_overview, tab_rebalance, tab_pvr, tab_headways, tab_route_detail, tab_fleet_config, tab_stability = st.tabs([
        "📊 Overview", "🔄 Fleet Rebalancing", "📐 PVR Analysis",
        "📈 Headways", "🗺 Route Detail", "⚙️ Fleet Config", "🔍 Stability",
    ])

    # ── CITY TAB 1: Overview ──────────────────────────────────────────────────
    with tab_overview:
        st.markdown('<div class="section-title">Route Summary</div>', unsafe_allow_html=True)
        summary_df = pd.DataFrame(cs.route_summary_rows())
        st.dataframe(summary_df, use_container_width=True, hide_index=True)

        if _PLOTLY_OK and len(cs.results) > 1:
            st.markdown('<div class="section-title">Fleet Allocation: PVR vs Allocated</div>',
                        unsafe_allow_html=True)
            codes = sorted(cs.results.keys())
            fig = go.Figure()
            fig.add_trace(go.Bar(name="PVR (minimum)", x=codes,
                                 y=[cs.results[c].pvr for c in codes], marker_color="#94a3b8"))
            fig.add_trace(go.Bar(name="Config Fleet", x=codes,
                                 y=[cs.results[c].fleet_original for c in codes], marker_color="#c7d2fe"))
            fig.add_trace(go.Bar(name="Final Allocated", x=codes,
                                 y=[cs.results[c].fleet_allocated for c in codes], marker_color="#4f46e5"))
            fig.update_layout(barmode="group", height=350,
                              legend=dict(orientation="h", yanchor="bottom", y=1.02, x=0.5, xanchor="center"),
                              margin=dict(l=40, r=20, t=20, b=40), plot_bgcolor="white")
            st.plotly_chart(fig, use_container_width=True)

        if _PLOTLY_OK and len(cs.results) > 1:
            st.markdown('<div class="section-title">Dead KM % by Route</div>', unsafe_allow_html=True)
            codes = sorted(cs.results.keys())
            dead_pcts = [cs.results[c].metrics.dead_km_ratio * 100 for c in codes]
            fig2 = go.Figure(go.Bar(x=codes, y=dead_pcts, marker_color=[
                "#16a34a" if d < 15 else "#d97706" if d < 25 else "#dc2626" for d in dead_pcts]))
            fig2.update_layout(height=280, yaxis_title="Dead KM %",
                               margin=dict(l=40, r=20, t=20, b=40), plot_bgcolor="white")
            st.plotly_chart(fig2, use_container_width=True)

    # ── CITY TAB 2: Fleet Rebalancing ─────────────────────────────────────────
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
                    f'</div>', unsafe_allow_html=True)

            st.markdown('<div class="section-title">Before & After</div>', unsafe_allow_html=True)
            ba_rows = []
            for code in sorted(cs.results):
                r = cs.results[code]
                donated = sum(1 for t in cs.transfers if t.from_route == code)
                received = sum(1 for t in cs.transfers if t.to_route == code)
                change = received - donated
                ba_rows.append({"Route": code, "PVR": r.pvr, "Before": r.fleet_original,
                                "After": r.fleet_allocated,
                                "Change": f"+{change}" if change > 0 else str(change) if change < 0 else "—",
                                "Donated": donated, "Received": received})
            st.dataframe(pd.DataFrame(ba_rows), use_container_width=True, hide_index=True)

        st.markdown('<div class="section-title">Fleet Balance Analysis (PVR-based)</div>',
                    unsafe_allow_html=True)
        balance = compute_fleet_balance(city_cfg)
        bal_df = pd.DataFrame([
            {"Route": code, "PVR": b["pvr"], "Allocated": b["allocated"],
             "Surplus": b["surplus"] if b["surplus"] > 0 else "",
             "Deficit": b["deficit"] if b["deficit"] > 0 else "",
             "Headroom %": f"{b['headroom_pct']:+.0f}%"}
            for code, b in sorted(balance.items())
        ])
        st.dataframe(bal_df, use_container_width=True, hide_index=True)

    # ── CITY TAB 3: PVR Analysis ──────────────────────────────────────────────
    with tab_pvr:
        st.caption(
            "**PVR (Peak)** — minimum buses at tightest headway band, drives rebalancing.  "
            "**PVR (Off-Peak)** — minimum buses during 11:00–15:00.  "
            "**PVR (Charging)** — conservative upper bound during P5 midday charging (+25%)."
        )
        try:
            slices_all = compute_pvr_slices_all(city_cfg)
            pvr_rows   = [s.as_dict() for s in slices_all.values()]
            pvr_df     = pd.DataFrame(pvr_rows)
            alloc_map  = {code: r.fleet_allocated for code, r in cs.results.items()}
            pvr_df["Allocated"] = pvr_df["Route"].map(alloc_map)
            pvr_df["vs PVR Peak"] = pvr_df.apply(
                lambda row: (
                    f"+{row['Allocated'] - row['PVR (Peak)']} surplus"
                    if row["Allocated"] > row["PVR (Peak)"]
                    else (f"{row['Allocated'] - row['PVR (Peak)']} deficit"
                          if row["Allocated"] < row["PVR (Peak)"] else "✅ exact")
                ), axis=1
            )
            st.dataframe(pvr_df, hide_index=True, use_container_width=True)

            if _PLOTLY_OK and len(cs.results) > 1:
                st.markdown('<div class="section-title">Fleet vs PVR Slices</div>',
                            unsafe_allow_html=True)
                codes = sorted(cs.results.keys())
                fig_pvr = go.Figure()
                fig_pvr.add_trace(go.Bar(name="PVR (Peak)",     x=codes,
                    y=[slices_all[c].pvr_peak     for c in codes], marker_color="#dc2626"))
                fig_pvr.add_trace(go.Bar(name="PVR (Off-Peak)", x=codes,
                    y=[slices_all[c].pvr_offpeak  for c in codes], marker_color="#f97316"))
                fig_pvr.add_trace(go.Bar(name="PVR (Charging)", x=codes,
                    y=[slices_all[c].pvr_charging for c in codes], marker_color="#fbbf24"))
                fig_pvr.add_trace(go.Bar(name="Allocated",      x=codes,
                    y=[alloc_map.get(c, 0)        for c in codes], marker_color="#4f46e5"))
                fig_pvr.update_layout(
                    barmode="group", height=350,
                    legend=dict(orientation="h", yanchor="bottom", y=1.02, x=0.5, xanchor="center"),
                    margin=dict(l=40, r=20, t=20, b=40), plot_bgcolor="white",
                )
                st.plotly_chart(fig_pvr, use_container_width=True)
        except Exception as e:
            st.warning(f"PVR analysis unavailable: {e}")

    # ── CITY TAB 4: Headways (per-route selector) ─────────────────────────────
    with tab_headways:
        st.caption(
            "Departure gaps between consecutive same-direction buses. "
            "Select a route to inspect actual vs configured headways."
        )
        hw_route_codes = sorted(cs.results.keys())
        selected_hw_route = st.selectbox(
            "Route", hw_route_codes,
            format_func=lambda c: f"{c} — {cs.results[c].config.route_name}",
            key="city_hw_route",
        )
        if selected_hw_route:
            rr_hw  = cs.results[selected_hw_route]
            hw_data = build_headway_chart_data(rr_hw.config, rr_hw.buses)
            if hw_data.empty:
                st.info("No revenue trips found for this route.")
            else:
                col_up, col_dn = st.columns(2)
                for col, direction, color in [
                    (col_up, "UP", "#4f46e5"),
                    (col_dn, "DN", "#16a34a"),
                ]:
                    with col:
                        dir_data = hw_data[hw_data["Direction"] == direction].sort_values("Departure")
                        if len(dir_data) < 2:
                            st.caption(f"**{direction}** — fewer than 2 departures.")
                            continue
                        deps = dir_data["Departure"].tolist()
                        gaps = [
                            {"Dep": deps[i].strftime("%H:%M"),
                             "Headway (min)": round((deps[i] - deps[i-1]).total_seconds() / 60)}
                            for i in range(1, len(deps))
                        ]
                        gdf = pd.DataFrame(gaps).set_index("Dep")
                        st.markdown(f"**{direction}** headways — {selected_hw_route}")
                        st.bar_chart(gdf, height=260, color=color)

                with st.expander("📋 Configured headway profile"):
                    st.dataframe(rr_hw.headway_df, hide_index=True, use_container_width=True)

    # ── CITY TAB 6: Route Detail (drill-down with full Gantt) ─────────────────
    with tab_route_detail:
        route_codes = sorted(cs.results.keys())
        selected_route = st.selectbox("Select Route", route_codes,
                                      format_func=lambda c: f"{c} — {cs.results[c].config.route_name}")
        if selected_route:
            r = cs.results[selected_route]
            m = r.metrics
            st.markdown(f"### {r.route_code} — {r.config.route_name}")

            shuttle_ct = sum(1 for b in r.buses for t in b.trips if t.trip_type == "Shuttle")
            dead_ct = sum(1 for b in r.buses for t in b.trips if t.trip_type == "Dead")
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
                '</div>', unsafe_allow_html=True)

            # Reuse the full Gantt diagram from single-route mode
            if _PLOTLY_OK:
                st.markdown('<div class="section-title">Schedule Timeline</div>',
                            unsafe_allow_html=True)
                try:
                    gantt_fig = build_route_diagram(r.config, r.buses)
                    st.plotly_chart(gantt_fig, use_container_width=True)
                except Exception as e:
                    st.warning(f"Could not render route diagram: {e}")

            st.markdown('<div class="section-title">Bus Summary</div>', unsafe_allow_html=True)
            bus_rows = []
            for bus in r.buses:
                rev_trips = [t for t in bus.trips if t.trip_type == "Revenue"]
                bus_rows.append({
                    "Bus": bus.bus_id, "Total Trips": len(bus.trips),
                    "Revenue": len(rev_trips),
                    "Dead": sum(1 for t in bus.trips if t.trip_type == "Dead"),
                    "Charging": sum(1 for t in bus.trips if t.trip_type == "Charging"),
                    "Total KM": round(bus.total_km, 1),
                    "Final SOC": f"{bus.soc_percent:.1f}%",
                    "Last Location": bus.current_location,
                })
            st.dataframe(pd.DataFrame(bus_rows), use_container_width=True, hide_index=True)

            # ── Full trip schedule (default open) ───────────────────────────
            st.markdown('<div class="section-title">Full Trip Schedule</div>',
                        unsafe_allow_html=True)
            trip_rows = []
            for bus in r.buses:
                for trip in bus.trips:
                    trip_rows.append({
                        "Bus": trip.assigned_bus, "Type": trip.trip_type,
                        "Dir": trip.direction, "From": trip.start_location,
                        "To": trip.end_location,
                        "Depart": trip.actual_departure.strftime("%H:%M") if trip.actual_departure else "",
                        "Arrive": trip.actual_arrival.strftime("%H:%M") if trip.actual_arrival else "",
                        "KM": round(trip.distance_km, 1),
                    })
            if trip_rows:
                st.dataframe(pd.DataFrame(trip_rows).sort_values("Depart"),
                             use_container_width=True, hide_index=True)

            # ── Headway editor for this route ────────────────────────────────
            st.markdown('<div class="section-title">✏️ Edit Headway Profile</div>',
                        unsafe_allow_html=True)
            st.caption("Edit headways then click **Re-run this route** to regenerate with updated profile. "
                       "Only this route is re-scheduled — all others remain unchanged.")
            _hw_edit_key = f"city_hw_edit_{selected_route}"
            _hw_df_edit = r.headway_df.copy()
            _edited_hw = st.data_editor(
                _hw_df_edit,
                column_config={
                    "time_from":   st.column_config.TextColumn("From", width="small"),
                    "time_to":     st.column_config.TextColumn("To",   width="small"),
                    "headway_min": st.column_config.NumberColumn("Headway (min)",
                                      min_value=1, max_value=180, step=1),
                },
                use_container_width=True, hide_index=True,
                key=_hw_edit_key,
            )
            if st.button(f"🔄 Re-run {selected_route} with edited headway", key=f"city_hw_rerun_{selected_route}"):
                with st.spinner(f"Re-scheduling {selected_route}…"):
                    try:
                        from src.trip_generator import generate_trips as _gen_trips
                        from src.bus_scheduler import schedule_buses as _sched_buses
                        from src.metrics import compute_metrics as _comp_metrics
                        _ri = city_cfg.routes[selected_route]
                        _rc = _ri.config
                        _new_trips  = _gen_trips(_rc, _edited_hw, _ri.travel_time_df)
                        _new_buses  = _sched_buses(_rc, _new_trips,
                                                   headway_df=_edited_hw,
                                                   travel_time_df=_ri.travel_time_df)
                        _new_rev    = len([t for t in _new_trips if t.trip_type=="Revenue"])
                        _new_metrics = _comp_metrics(_rc, _new_buses, total_revenue_trips=_new_rev)
                        # Relabel buses
                        for _i, _b in enumerate(_new_buses, 1):
                            _old_id = _b.bus_id
                            _b.bus_id = f"{_rc.route_code}-B{_i:02d}"
                            for _t in _b.trips:
                                if _t.assigned_bus == _old_id:
                                    _t.assigned_bus = _b.bus_id
                        # Update RouteResult in session state
                        from src.fleet_analyzer import compute_pvr_slices as _cpvr
                        from src.city_models import RouteResult as _RR
                        _new_ri = type(_ri)(config=_ri.config, headway_df=_edited_hw,
                                            travel_time_df=_ri.travel_time_df)
                        _pvr_s = _cpvr(_new_ri)
                        _new_rr = _RR(
                            route_code=_rc.route_code, config=_rc,
                            headway_df=_edited_hw, travel_time_df=_ri.travel_time_df,
                            buses=_new_buses, metrics=_new_metrics,
                            pvr=_pvr_s.pvr_peak,
                            fleet_allocated=cs.results[selected_route].fleet_allocated,
                            fleet_original=cs.results[selected_route].fleet_original,
                        )
                        cs.results[selected_route] = _new_rr
                        city_cfg.routes[selected_route] = _new_ri
                        st.session_state["city_result"] = cs
                        st.session_state["city_config"] = city_cfg
                        st.success(f"✅ {selected_route} re-scheduled with updated headway.")
                        st.rerun()
                    except Exception as _e:
                        st.error(f"Re-schedule error: {_e}")

            # Per-bus trip detail tables (same as single-route tab)
            st.divider()
            st.markdown('<div class="section-title">Per-Bus Trip Detail</div>', unsafe_allow_html=True)
            route_dep_df = build_route_depiction(r.config, r.buses)
            for bus in r.buses:
                bus_data = route_dep_df[route_dep_df["Bus"] == bus.bus_id].reset_index(drop=True)
                if bus_data.empty: continue
                rev = [t for t in bus.trips if t.trip_type == "Revenue"]
                dead_km_bus = sum(t.distance_km for t in bus.trips if t.trip_type == "Dead")
                chg = [t for t in bus.trips if t.trip_type == "Charging"]
                first = next((t.actual_departure for t in bus.trips if t.actual_departure), None)
                last  = next((t.actual_arrival for t in reversed(bus.trips) if t.actual_arrival), None)
                st.markdown(
                    f'<div class="bus-header">'
                    f'<span class="bus-pill">{bus.bus_id}</span>'
                    f'{len(rev)} revenue trips &nbsp;·&nbsp; {bus.total_km:.1f} km &nbsp;·&nbsp;'
                    f' SOC {bus.soc_percent:.1f}% final &nbsp;·&nbsp;'
                    f' {first.strftime("%H:%M") if first else "?"} – {last.strftime("%H:%M") if last else "?"}'
                    f'</div>', unsafe_allow_html=True)
                display_rows = []
                for _, row in bus_data.iterrows():
                    tp = row["_type"]; dr = row["_dir"]
                    marker = "🔋" if tp=="Charging" else "⚫" if tp=="Dead" else "🔵" if dr=="UP" else "🟢"
                    brk = row["Break"]
                    display_rows.append({
                        "": marker, "Dep": row["Dep"], "Arr": row["Arr"],
                        "From": row["From"], "To": row["To"], "Type": row["Type"],
                        "Dist": row["Dist"],
                        "SOC": f'{row["SOC"]}{"⚠️" if row["SOC"] < 30 else ""}',
                        "Break": f"{brk} min" if brk > 0 else "",
                    })
                st.dataframe(pd.DataFrame(display_rows), hide_index=True,
                             height=min(420, 36 * len(display_rows) + 38),
                             column_config={"": st.column_config.TextColumn(width="small")})
                st.divider()

    # ── CITY TAB 7: Fleet Config Editor ───────────────────────────────────────
    with tab_fleet_config:
        st.markdown('<div class="section-title">Adjust Fleet Size per Route</div>',
                    unsafe_allow_html=True)
        st.caption("Edit fleet sizes below and click Re-run to regenerate with updated allocation.")
        pvrs = compute_pvr_all(city_cfg)
        edit_rows = []
        for code in sorted(city_cfg.routes.keys()):
            ri = city_cfg.routes[code]
            edit_rows.append({
                "Route": code, "Name": ri.config.route_name,
                "Fleet Size": ri.config.fleet_size,
                "PVR": pvrs.get(code, 0),
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
            use_container_width=True, hide_index=True,
        )
        total_edited = edited["Fleet Size"].sum()
        st.caption(f"**Total fleet: {total_edited}** (configured: {city_cfg.total_configured_fleet})")

        if st.button("🔄 Re-run with Updated Fleet", type="primary"):
            for _, row in edited.iterrows():
                code = row["Route"]
                new_fleet = int(row["Fleet Size"])
                if code in city_cfg.routes:
                    city_cfg.routes[code].config.fleet_size = new_fleet
            city_cfg.total_fleet = int(total_edited)
            with st.spinner("Re-running citywide schedule..."):
                try:
                    city_result = schedule_city(city_cfg, mode=st.session_state.get("city_mode_used","planning"))
                    st.session_state["city_result"] = city_result
                    st.session_state["city_config"] = city_cfg
                    st.rerun()
                except Exception as e:
                    st.error(f"Error: {e}")

    # ── CITY TAB 8: Stability ─────────────────────────────────────────────────
    with tab_stability:
        st.caption(
            "Post-rebalance PVR stability check. "
            "A **drift > 0.5** means the re-run shifted cycle time enough to change the PVR, "
            "making the transfer plan potentially stale. Re-running the citywide scheduler resolves this."
        )
        stability_flags = getattr(cs, "stability_flags", [])
        if not stability_flags:
            st.info("Stability check not applicable in Efficiency-Maximising mode, "
                    "or no transfers were needed.")
        else:
            flag_rows = [f.as_dict() for f in stability_flags]
            flag_df   = pd.DataFrame(flag_rows)
            unstable  = flag_df[flag_df["Status"] == "⚠️ Drifted"]
            if unstable.empty:
                st.success("✅ All routes stable — no PVR drift detected after rebalancing.")
            else:
                st.warning(
                    f"⚠️ {len(unstable)} route(s) show PVR drift > 0.5. "
                    "Consider re-running the citywide schedule to stabilise."
                )
            st.dataframe(flag_df, hide_index=True, use_container_width=True)
