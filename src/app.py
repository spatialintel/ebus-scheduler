"""
app.py — eBus Scheduler Dashboard
"""
from __future__ import annotations
__version__ = "2026-03-24-b4"  # auto-stamped
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

def kpi(label, value, status=""):
    cls = f"kpi {('kpi-ok' if status=='ok' else 'kpi-warn' if status=='warn' else 'kpi-bad' if status=='bad' else '')}"
    return f'<div class="{cls}"><div class="kpi-val">{value}</div><div class="kpi-label">{label}</div></div>'

def build_schedule_df(config, buses):
    rows = []
    for bus in buses:
        soc = config.initial_soc_percent
        for i, trip in enumerate(bus.trips):
            soc -= (trip.distance_km * config.consumption_rate / config.battery_kwh) * 100
            if trip.trip_type == "Charging":
                soc = min(100.0, soc + config.depot_flow_rate_kw * (trip.travel_time_min/60) / config.battery_kwh * 100)
            brk = None
            if i + 1 < len(bus.trips):
                nxt = bus.trips[i+1]
                if trip.actual_arrival and nxt.actual_departure:
                    brk = max(0, int((nxt.actual_departure - trip.actual_arrival).total_seconds() / 60))
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
        rev = [t for t in bus.trips if t.trip_type == "Revenue"]
        dead = [t for t in bus.trips if t.trip_type == "Dead"]
        chg = [t for t in bus.trips if t.trip_type == "Charging"]
        first = next((t.actual_departure for t in bus.trips if t.actual_departure), None)
        last  = next((t.actual_arrival for t in reversed(bus.trips) if t.actual_arrival), None)
        rows.append({
            "Bus": bus.bus_id, "Revenue Trips": len(rev), "Dead Runs": len(dead),
            "Charging": len(chg), "Total KM": round(bus.total_km, 1),
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
            brk = 0
            if i + 1 < len(bus.trips):
                nxt = bus.trips[i+1]
                if trip.actual_arrival and nxt.actual_departure:
                    brk = max(0, int((nxt.actual_departure - trip.actual_arrival).total_seconds() / 60))
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
    Two-panel interactive route diagram.

    Panel A — Route Topology: clean schematic, stops scaled by distance.
    Panel B — Time-Space (Marey): Revenue lines prominent, dead/shuttle subtle,
               charging shown as horizontal bars, bus labels on last trip end,
               headway gap callouts for large gaps.
    """
    from src.bus_scheduler import _nearest_node_from_depot
    from plotly.subplots import make_subplots

    # ── Palette ───────────────────────────────────────────────────────────────
    BUS_COLORS = ["#4338CA","#059669","#D97706","#DC2626",
                  "#7C3AED","#0284C7","#BE185D","#0D9488","#92400E"]
    DIM       = 0.10   # opacity for non-selected bus
    GRID_CLR  = "rgba(0,0,0,0.05)"
    PEAK_CLR  = "rgba(99,102,241,0.07)"
    OFFPK_CLR = "rgba(16,185,129,0.05)"

    nearest_name, _, _ = _nearest_node_from_depot(config)

    def _dist(a, b):
        try: return float(config.get_distance(a, b))
        except Exception: return 0.0
    def _time(a, b):
        try: return float(config.get_travel_time(a, b))
        except Exception: return 0.0

    # ── Route nodes & cumulative positions ───────────────────────────────────
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

    depot_y   = cum.get(nearest_name, 0.0)
    node_y    = dict(cum)
    node_y[config.depot] = depot_y

    op_s = config.operating_start.hour + config.operating_start.minute / 60
    op_e = config.operating_end.hour   + config.operating_end.minute   / 60

    # ── SOC per trip ─────────────────────────────────────────────────────────
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
        return "#16a34a" if s >= 50 else "#F59E0B" if s >= 30 else "#DC2626"

    # ── Stats for topology summary ────────────────────────────────────────────
    total_dead_km = sum(t.distance_km for b in buses
                        for t in b.trips if t.trip_type == "Dead")
    total_rev  = sum(1 for b in buses for t in b.trips if t.trip_type == "Revenue")
    total_shut = sum(1 for b in buses for t in b.trips if t.trip_type == "Shuttle")
    total_chg  = sum(1 for b in buses for t in b.trips if t.trip_type == "Charging")
    avg_soc    = sum(b.soc_percent for b in buses) / max(len(buses), 1)

    # Peak headway
    peak_gaps = []
    for bus in buses:
        rev = sorted([t for t in bus.trips if t.trip_type=="Revenue"
                      and t.actual_departure], key=lambda t: t.actual_departure)
        for i in range(1, len(rev)):
            if rev[i-1].direction == rev[i].direction:
                g = (rev[i].actual_departure - rev[i-1].actual_departure).total_seconds()/60
                h = rev[i-1].actual_departure.hour
                if 8 <= h < 11 or 16 <= h < 20:
                    peak_gaps.append(g)
    avg_peak_hw = sum(peak_gaps)/max(len(peak_gaps),1)

    # ── Figure ────────────────────────────────────────────────────────────────
    fig = make_subplots(
        rows=2, cols=1,
        row_heights=[0.22, 0.78],
        vertical_spacing=0.08,
        subplot_titles=[
            f"<b>Route {config.route_code}</b>  "
            f"<span style='color:#6B7280;font-weight:normal'>{config.route_name}</span>",
            "<span style='font-size:13px;font-weight:600'>Time–Space Diagram</span>"
            "<span style='font-size:10px;color:#9CA3AF'>  —  each line = one bus trip</span>",
        ],
    )

    # ════════════════════════════════════════════════════════════════════════
    # PANEL A — Topology
    # ════════════════════════════════════════════════════════════════════════

    # Route backbone
    fig.add_trace(go.Scatter(
        x=[cum[n] for n in route_nodes], y=[1]*len(route_nodes),
        mode="lines", line=dict(color="#6366F1", width=6),
        hoverinfo="skip", showlegend=False,
    ), row=1, col=1)

    # Depot leader
    fig.add_trace(go.Scatter(
        x=[depot_y, depot_y], y=[-0.5, 0.75],
        mode="lines", line=dict(color="#D97706", width=2, dash="dot"),
        hoverinfo="skip", showlegend=False,
    ), row=1, col=1)

    # Segment labels (km + min) above line
    for i in range(len(route_nodes)-1):
        f, t = route_nodes[i], route_nodes[i+1]
        d, tt = _dist(f,t), _time(f,t)
        if not d: continue
        mid = (cum[f]+cum[t])/2
        fig.add_annotation(x=mid, y=1.38,
            text=f"<b>{d:.1f} km</b>  ·  {tt:.0f} min",
            showarrow=False, row=1, col=1,
            font=dict(size=10, color="#374151"),
            bgcolor="rgba(255,255,255,0.9)", borderpad=3)

    # Depot-to-nearest label
    d2n, t2n = _dist(config.depot, nearest_name), _time(config.depot, nearest_name)
    if d2n:
        fig.add_annotation(x=depot_y + total_km*0.03, y=0.15,
            text=f"<b>{d2n:.1f} km</b>  ·  {t2n:.0f} min",
            showarrow=False, row=1, col=1,
            font=dict(size=9, color="#D97706"),
            bgcolor="rgba(255,255,255,0.85)", borderpad=2)

    # Stop markers + labels
    for node in route_nodes:
        is_term   = node in (config.start_point, config.end_point)
        is_near   = node == nearest_name
        clr  = "#4338CA" if is_term else "#059669" if is_near else "#0369A1"
        sym  = "circle" if is_term else "diamond" if is_near else "square"
        sz   = 22 if is_term else 16
        lbl  = node.replace("BUS STAND","BS").replace("  "," ").strip()
        up_n = sum(1 for b in buses for t in b.trips
                   if t.trip_type=="Revenue" and t.direction=="UP" and t.start_location==node)
        dn_n = sum(1 for b in buses for t in b.trips
                   if t.trip_type=="Revenue" and t.direction=="DN" and t.start_location==node)
        fig.add_trace(go.Scatter(
            x=[cum[node]], y=[1], mode="markers+text",
            marker=dict(symbol=sym, size=sz, color=clr,
                        line=dict(color="white", width=2)),
            text=[f"<b>{lbl}</b>"], textposition="bottom center",
            textfont=dict(size=10, color=clr),
            hovertemplate=(f"<b>{node}</b><br>↑ UP: {up_n} dep  ↓ DN: {dn_n} dep"
                           + ("<br><i>Nearest depot node (P2)</i>" if is_near else "")
                           + "<extra></extra>"),
            showlegend=False,
        ), row=1, col=1)

    # Depot marker
    fig.add_trace(go.Scatter(
        x=[depot_y], y=[-0.5], mode="markers+text",
        marker=dict(symbol="square", size=22, color="#D97706",
                    line=dict(color="white", width=2)),
        text=["<b>DEPOT</b>"], textposition="bottom center",
        textfont=dict(size=10, color="#D97706"),
        hovertemplate=(f"<b>{config.depot}</b><br>"
                       f"Dead km (fleet total): {total_dead_km:.1f} km<br>"
                       f"Nearest node: {nearest_name}<extra></extra>"),
        showlegend=False,
    ), row=1, col=1)

    # Summary stats banner
    fig.add_annotation(
        x=total_km/2, y=1.75,
        text=(f"<b>{config.fleet_size} buses</b>  ·  "
              f"<b>{total_rev}</b> revenue  +  <b>{total_shut}</b> shuttle trips  ·  "
              f"Dead {total_dead_km:.1f} km  ·  "
              f"{total_chg} charge stops  ·  "
              f"Peak headway {avg_peak_hw:.0f} min  ·  "
              f"Avg final SOC {avg_soc:.0f}%"),
        showarrow=False, row=1, col=1,
        font=dict(size=11, color="#1F2937"),
        bgcolor="rgba(238,242,255,0.97)",
        borderpad=7, bordercolor="#6366F1", borderwidth=1,
    )

    # ════════════════════════════════════════════════════════════════════════
    # PANEL B — Time-Space (Marey)
    # ════════════════════════════════════════════════════════════════════════

    # Peak / off-peak shading
    for x0, x1, lbl, clr in [
        (8, 11,  "Peak AM",  PEAK_CLR),
        (11, 16, "Off-peak", OFFPK_CLR),
        (16, 20, "Peak PM",  PEAK_CLR),
    ]:
        fig.add_vrect(x0=x0, x1=x1, fillcolor=clr, line_width=0, row=2, col=1)
        fig.add_annotation(
            x=(x0+x1)/2, y=total_km*1.05,
            text=f"<span style='font-size:9px;color:#9CA3AF'>{lbl}</span>",
            showarrow=False, xref="x2", yref="y2", xanchor="center",
        )

    # Horizontal stop reference lines + right-side labels
    for node in route_nodes:
        fig.add_hline(y=node_y[node], line_dash="dot",
                      line_color="rgba(99,102,241,0.15)", line_width=1, row=2, col=1)
        short = node.replace("BUS STAND","BS").replace("  "," ").strip()
        fig.add_annotation(
            x=op_e+0.15, y=node_y[node], xref="x2", yref="y2",
            text=f"<span style='color:#6366F1;font-size:9px'><b>{short}</b></span>",
            showarrow=False, xanchor="left",
        )

    # Depot reference line + label
    fig.add_hline(y=depot_y, line_dash="dot",
                  line_color="rgba(217,119,6,0.2)", line_width=1, row=2, col=1)
    fig.add_annotation(
        x=op_e+0.15, y=depot_y - total_km*0.04, xref="x2", yref="y2",
        text="<span style='color:#D97706;font-size:9px'>DEPOT</span>",
        showarrow=False, xanchor="left",
    )

    # ── Trip lines per bus ────────────────────────────────────────────────────
    legend_shown = set()

    for bus_idx, bus in enumerate(buses):
        bclr = BUS_COLORS[bus_idx % len(BUS_COLORS)]
        is_sel  = selected_bus is None or bus.bus_id == selected_bus
        opacity = 1.0 if is_sel else DIM
        socs    = _socs(bus)

        for trip, soc_after in zip(bus.trips, socs):
            if trip.actual_departure is None: continue

            dep_h = trip.actual_departure.hour + trip.actual_departure.minute/60
            arr_h = (trip.actual_arrival.hour  + trip.actual_arrival.minute/60
                     if trip.actual_arrival else dep_h + trip.travel_time_min/60)
            sy = node_y.get(trip.start_location, depot_y)
            ey = node_y.get(trip.end_location,   depot_y)
            sc = _soc_clr(soc_after)

            dep_s = trip.actual_departure.strftime("%H:%M")
            arr_s = trip.actual_arrival.strftime("%H:%M") if trip.actual_arrival else "?"
            hover = (f"<b>{bus.bus_id}</b> — {trip.trip_type}<br>"
                     f"{dep_s} → {arr_s}<br>"
                     f"{trip.start_location} → {trip.end_location}<br>"
                     f"SOC: <b style='color:{sc}'>{soc_after}%</b>  ·  {trip.distance_km:.1f} km"
                     "<extra></extra>")

            show_leg = bus.bus_id not in legend_shown and is_sel

            if trip.trip_type == "Revenue":
                # Bold colored line for revenue
                fig.add_trace(go.Scatter(
                    x=[dep_h, arr_h], y=[sy, ey],
                    mode="lines",
                    line=dict(color=bclr, width=2.5),
                    opacity=opacity,
                    name=bus.bus_id, legendgroup=bus.bus_id,
                    showlegend=show_leg,
                    hovertemplate=hover,
                ), row=2, col=1)
                # Small SOC dot at departure only
                fig.add_trace(go.Scatter(
                    x=[dep_h], y=[sy], mode="markers",
                    marker=dict(size=6, color=sc,
                                line=dict(color=bclr, width=1.2)),
                    opacity=opacity,
                    name=bus.bus_id, legendgroup=bus.bus_id,
                    showlegend=False,
                    hovertemplate=hover,
                ), row=2, col=1)
                if show_leg: legend_shown.add(bus.bus_id)

            elif trip.trip_type == "Charging":
                # Horizontal bar at depot level — very visible
                fig.add_trace(go.Scatter(
                    x=[dep_h, arr_h], y=[depot_y, depot_y],
                    mode="lines",
                    line=dict(color="#F59E0B", width=6),
                    opacity=opacity,
                    name=bus.bus_id, legendgroup=bus.bus_id,
                    showlegend=False,
                    hovertemplate=hover,
                ), row=2, col=1)
                # Star marker at charge start
                fig.add_trace(go.Scatter(
                    x=[dep_h], y=[depot_y], mode="markers",
                    marker=dict(symbol="star", size=11, color="#F59E0B",
                                line=dict(color="#92400E", width=1)),
                    opacity=opacity,
                    name=bus.bus_id, legendgroup=bus.bus_id,
                    showlegend=False,
                    hovertemplate=hover,
                ), row=2, col=1)

            else:  # Dead or Shuttle — subtle thin dashed
                t_color = "#9CA3AF" if trip.trip_type == "Dead" else "#6EE7B7"
                fig.add_trace(go.Scatter(
                    x=[dep_h, arr_h], y=[sy, ey],
                    mode="lines",
                    line=dict(color=t_color, width=1, dash="dot"),
                    opacity=opacity * 0.6,
                    name=bus.bus_id, legendgroup=bus.bus_id,
                    showlegend=False,
                    hovertemplate=hover,
                ), row=2, col=1)

        # Bus label at the end of the last trip
        if is_sel and bus.trips:
            last = next((t for t in reversed(bus.trips) if t.actual_arrival), None)
            if last:
                lx = last.actual_arrival.hour + last.actual_arrival.minute/60
                ly = node_y.get(last.end_location, depot_y)
                fig.add_annotation(
                    x=lx + 0.05, y=ly, xref="x2", yref="y2",
                    text=f"<b style='color:{bclr}'>{bus.bus_id}</b>",
                    showarrow=False, xanchor="left",
                    font=dict(size=9, color=bclr),
                )

    # Headway gap callouts: annotate gaps > 45min during 12-17h
    all_rev = sorted(
        [(t.actual_departure, t.direction, t.start_location,
          node_y.get(t.start_location, depot_y))
         for b in buses for t in b.trips
         if t.trip_type == "Revenue" and t.actual_departure],
        key=lambda x: x[0]
    )
    for direction in ("UP", "DN"):
        dir_deps = [(h, y) for h, d, _, y in all_rev
                    if d == direction and 12 <= h.hour < 17]
        dir_deps.sort(key=lambda x: x[0])
        for i in range(1, len(dir_deps)):
            gap_min = (dir_deps[i][0] - dir_deps[i-1][0]).total_seconds()/60
            if gap_min > 45:
                mid_h = (dir_deps[i-1][0].hour + dir_deps[i-1][0].minute/60 +
                         dir_deps[i][0].hour   + dir_deps[i][0].minute/60) / 2
                mid_y = (dir_deps[i-1][1] + dir_deps[i][1]) / 2
                fig.add_annotation(
                    x=mid_h, y=mid_y, xref="x2", yref="y2",
                    text=f"<b>{gap_min:.0f} min</b><br><span style='font-size:8px'>charging gap</span>",
                    showarrow=True, arrowhead=0, arrowcolor="#DC2626",
                    ax=0, ay=-28,
                    font=dict(size=9, color="#DC2626"),
                    bgcolor="rgba(254,242,242,0.9)",
                    bordercolor="#DC2626", borderwidth=1, borderpad=3,
                )

    # Legend entries for marker types (shape key)
    for sym, lbl, clr in [
        ("circle",     "SOC ≥ 50%",    "#16a34a"),
        ("circle",     "SOC 30–49%",   "#F59E0B"),
        ("circle",     "SOC < 30%",    "#DC2626"),
        ("star",       "Charging",     "#F59E0B"),
        ("line-ew",    "Dead run",     "#9CA3AF"),
        ("line-ew",    "Shuttle",      "#6EE7B7"),
    ]:
        fig.add_trace(go.Scatter(
            x=[None], y=[None], mode="markers",
            marker=dict(symbol=sym, size=8, color=clr),
            name=lbl, showlegend=True,
            legendgroup="types",
        ), row=2, col=1)

    # ── Layout ────────────────────────────────────────────────────────────────
    xpad = total_km * 0.06
    fig.update_layout(
        height=820,
        margin=dict(l=15, r=140, t=55, b=30),
        plot_bgcolor="white",
        paper_bgcolor="rgba(0,0,0,0)",
        hovermode="closest",
        legend=dict(
            orientation="v", x=1.02, y=0.99,
            xanchor="left", yanchor="top",
            font=dict(size=10),
            bgcolor="rgba(255,255,255,0.95)",
            bordercolor="#E5E7EB", borderwidth=1,
            tracegroupgap=2,
        ),
    )

    # Panel A axes
    fig.update_xaxes(range=[-xpad, total_km+xpad], showgrid=False, zeroline=False,
                     tickformat=".1f", ticksuffix=" km",
                     title_text="Cumulative distance (km)",
                     color="#9CA3AF", row=1, col=1)
    fig.update_yaxes(range=[-1.0, 2.1], showgrid=False, zeroline=False,
                     showticklabels=False, showline=False, row=1, col=1)

    # Panel B axes — hourly gridlines, clean
    tick_vals = list(range(int(op_s), int(op_e)+1))
    tick_text = [f"{h:02d}:00" for h in tick_vals]
    fig.update_xaxes(
        range=[op_s-0.15, op_e+0.45],
        tickvals=tick_vals, ticktext=tick_text,
        showgrid=True, gridcolor=GRID_CLR, gridwidth=1,
        zeroline=False, title_text="Time of day",
        color="#6B7280", row=2, col=1,
    )
    fig.update_yaxes(
        range=[-total_km*0.12, total_km*1.12],
        showgrid=False, zeroline=False,
        showticklabels=False,
        title_text="Route position →",
        row=2, col=1,
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
    compliance = check_compliance(config, buses)
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
        compliance   = check_compliance(cfg, buses)
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


def run_pipeline(uploaded_file, optimize, config_overrides=None, headway_overrides=None):
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
    return _run_core(config, headway_df, travel_time_df, optimize)

def rerun_from_overrides(config_overrides, headway_overrides=None, optimize=False):
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
    return _run_core(config, headway_df, raw_tt, optimize)


# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("## 🚌 eBus Scheduler")
    st.caption("Upload route config Excel to generate a schedule.")
    st.divider()
    uploaded = st.file_uploader("Config Excel", type=["xlsx"], label_visibility="collapsed")
    optimize = st.toggle("Run Optimizer", value=False,
                         help="Tunes headway bands to balance KM across fleet (10–20 sec).")
    run_btn = st.button("▶ Generate Schedule", type="primary", disabled=uploaded is None)
    st.divider()
    st.caption("Rules enforced: P4 break from config, P2 via nearest node, P5 midday charge, P3 SOC ≥ 20%.")


# ── Main ──────────────────────────────────────────────────────────────────────
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
            result = run_pipeline(uploaded, optimize)
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
    trip_ok = metrics.revenue_trips_assigned == metrics.revenue_trips_total
    soc_ok = metrics.min_soc_seen >= 25
    km_ok = metrics.km_range <= 20
    shuttle_count = sum(1 for b in buses for t in b.trips if t.trip_type == "Shuttle")

    st.markdown(
        '<div class="kpi-grid">' +
        kpi("Revenue Trips", f"{metrics.revenue_trips_assigned}/{metrics.revenue_trips_total}",
            "ok" if trip_ok else "bad") +
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
                    "Off-Peak Extra Break (min) — 11:00–16:00",
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

            with st.spinner("Regenerating..."):
                try:
                    result = rerun_from_overrides(overrides)
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
