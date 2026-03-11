"""
config_loader.py — Reads the eBus Config Input Excel and returns:
    1. RouteConfig object (all route parameters + segment distances)
    2. headway_df (pandas DataFrame: time_from, time_to, headway_min)
    3. travel_time_df (pandas DataFrame: time_from, time_to, up_min, dn_min)

Usage:
    from src.config_loader import load_config
    config, headway_df, travel_time_df = load_config("config/eBus_Config_Input.xlsx")
"""

from __future__ import annotations

from datetime import time
from pathlib import Path

import pandas as pd
from openpyxl import load_workbook

from src.models import RouteConfig


class ConfigError(Exception):
    """Raised when the input Excel is missing required data."""
    pass


# ── Helpers ──────────────────────────────────────────────────────────

def _parse_time(val) -> time:
    """Convert 'HH:MM' string or datetime.time to time object."""
    if isinstance(val, time):
        return val
    if isinstance(val, str):
        parts = val.strip().split(":")
        return time(int(parts[0]), int(parts[1]))
    raise ConfigError(f"Cannot parse time from: {val!r}")


def _parse_float(val, field_name: str) -> float:
    if val is None:
        raise ConfigError(f"Missing value for '{field_name}'")
    try:
        return float(val)
    except (ValueError, TypeError):
        raise ConfigError(f"Cannot parse float for '{field_name}': {val!r}")


def _parse_int(val, field_name: str) -> int:
    return int(_parse_float(val, field_name))


def _build_field_map(ws) -> dict[str, tuple]:
    """
    Scan column B of Route_Config sheet. Build a dict of field_name -> (row, value_from_C).
    Skips section headers (value in C is None and label doesn't contain underscore).
    """
    field_map = {}
    for row in range(1, ws.max_row + 1):
        label = ws.cell(row, 2).value
        if label is None:
            continue
        label_clean = str(label).strip()
        field_map[label_clean] = (row, ws.cell(row, 3).value)
    return field_map


def _get(field_map: dict, key: str):
    """Look up a field value. Raises ConfigError if missing."""
    if key not in field_map:
        raise ConfigError(f"Field '{key}' not found in Route_Config sheet")
    return field_map[key][1]


# ── Location parser ──────────────────────────────────────────────────

def _parse_locations(ws, field_map: dict):
    """
    Read the Locations table. Returns:
        locations: dict  {label: name}  e.g. {"Depot": "DEPOT", "Start point": "GANGAJALIA BUS STAND"}
        coords: dict     {name: (lat, lon)}  only for locations that have lat/lon filled
    """
    # Find the "Location Label" header row
    header_row = None
    for row in range(1, ws.max_row + 1):
        if ws.cell(row, 2).value == "Location Label":
            header_row = row
            break
    if header_row is None:
        raise ConfigError("Cannot find 'Location Label' header in Route_Config")

    locations = {}
    coords = {}
    r = header_row + 1
    while r <= ws.max_row:
        label = ws.cell(r, 2).value
        name = ws.cell(r, 3).value
        if label is None or name is None:
            break
        label = str(label).strip()
        name = str(name).strip()
        locations[label] = name

        lat = ws.cell(r, 4).value
        lon = ws.cell(r, 5).value
        if lat is not None and lon is not None:
            try:
                coords[name] = (float(lat), float(lon))
            except (ValueError, TypeError):
                pass
        r += 1

    return locations, coords


# ── Segment parser ───────────────────────────────────────────────────

def _parse_segments(ws):
    """
    Read the segment distance/time table. Uses the 'Resolved From' (col G) and
    'Resolved To' (col H) columns for the actual location names.
    Returns: segment_distances dict, segment_times dict
    """
    # Find the "Sr." header row
    header_row = None
    for row in range(1, ws.max_row + 1):
        if ws.cell(row, 2).value == "Sr.":
            header_row = row
            break
    if header_row is None:
        raise ConfigError("Cannot find 'Sr.' header in segment table")

    distances = {}
    times = {}
    r = header_row + 1
    while r <= ws.max_row:
        sr = ws.cell(r, 2).value
        if sr is None:
            break
        try:
            int(sr)
        except (ValueError, TypeError):
            break

        # Resolved names from formula columns G, H
        res_from = ws.cell(r, 7).value
        res_to = ws.cell(r, 8).value

        # Fall back to generic labels + location lookup if resolved is empty
        if not res_from or not res_to:
            r += 1
            continue

        res_from = str(res_from).strip()
        res_to = str(res_to).strip()

        dist = ws.cell(r, 5).value
        tt = ws.cell(r, 6).value

        if dist is None or tt is None:
            r += 1
            continue

        dist = float(dist)
        tt = int(float(tt))

        # Skip zero-distance pairs (unused intermediate points)
        if dist > 0:
            key = RouteConfig.segment_key(res_from, res_to)
            distances[key] = dist
            times[key] = tt

        r += 1

    return distances, times


# ── Headway parser ───────────────────────────────────────────────────

def _parse_headway(wb) -> pd.DataFrame:
    """Read Headway_Profile sheet into a DataFrame."""
    ws = wb["Headway_Profile"]

    # Find the header row with "Time From"
    header_row = None
    for row in range(1, ws.max_row + 1):
        if ws.cell(row, 2).value == "Time From":
            header_row = row
            break
    if header_row is None:
        raise ConfigError("Cannot find 'Time From' header in Headway_Profile")

    rows = []
    r = header_row + 1
    while r <= ws.max_row:
        tf = ws.cell(r, 2).value
        tt = ws.cell(r, 3).value
        hw = ws.cell(r, 4).value
        if tf is None:
            break
        rows.append({
            "time_from": str(tf).strip(),
            "time_to": str(tt).strip(),
            "headway_min": int(float(hw)),
        })
        r += 1

    if not rows:
        raise ConfigError("Headway_Profile sheet has no data rows")

    return pd.DataFrame(rows)


# ── Travel time parser ───────────────────────────────────────────────

def _parse_travel_time(wb) -> pd.DataFrame:
    """Read TravelTime_Profile sheet into a DataFrame."""
    ws = wb["TravelTime_Profile"]

    header_row = None
    for row in range(1, ws.max_row + 1):
        if ws.cell(row, 2).value == "Time From":
            header_row = row
            break
    if header_row is None:
        raise ConfigError("Cannot find 'Time From' header in TravelTime_Profile")

    rows = []
    r = header_row + 1
    while r <= ws.max_row:
        tf = ws.cell(r, 2).value
        tt = ws.cell(r, 3).value
        up = ws.cell(r, 4).value
        dn = ws.cell(r, 5).value
        if tf is None:
            break
        rows.append({
            "time_from": str(tf).strip(),
            "time_to": str(tt).strip(),
            "up_min": int(float(up)),
            "dn_min": int(float(dn)),
        })
        r += 1

    if not rows:
        raise ConfigError("TravelTime_Profile sheet has no data rows")

    return pd.DataFrame(rows)


# ── Main entry point ─────────────────────────────────────────────────

def load_config(excel_path: str | Path) -> tuple[RouteConfig, pd.DataFrame, pd.DataFrame]:
    """
    Read the eBus Config Input Excel and return:
        config          RouteConfig object
        headway_df      DataFrame with columns: time_from, time_to, headway_min
        travel_time_df  DataFrame with columns: time_from, time_to, up_min, dn_min

    The Excel must have been recalculated (formulas resolved to values).
    Use data_only=True to read calculated values from formula cells.
    """
    path = Path(excel_path)
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")

    wb = load_workbook(path, data_only=True)

    # Verify required sheets
    required_sheets = ["Route_Config", "Headway_Profile", "TravelTime_Profile"]
    for s in required_sheets:
        if s not in wb.sheetnames:
            raise ConfigError(f"Missing required sheet: '{s}'")

    ws = wb["Route_Config"]

    # 1. Parse key-value fields
    fm = _build_field_map(ws)

    # 2. Parse locations and coordinates
    locations, coords = _parse_locations(ws, fm)

    depot = locations.get("Depot")
    start_point = locations.get("Start point")
    end_point = locations.get("End point")

    if not all([depot, start_point, end_point]):
        raise ConfigError(
            f"Missing core locations. Found: Depot={depot}, "
            f"Start={start_point}, End={end_point}"
        )

    intermediates = []
    for key in ["Intermediate point 1", "Intermediate point 2"]:
        name = locations.get(key, "")
        if name:
            intermediates.append(name)

    # 3. Parse segment distances and times
    seg_distances, seg_times = _parse_segments(ws)

    if not seg_distances:
        raise ConfigError("No valid segment distances found in the segment table")

    # 4. Build RouteConfig
    config = RouteConfig(
        route_code=str(_get(fm, "route_code")).strip(),
        route_name=str(_get(fm, "route_name")).strip(),
        depot=depot,
        start_point=start_point,
        end_point=end_point,
        intermediates=intermediates,
        fleet_size=_parse_int(_get(fm, "fleet_size"), "fleet_size"),
        battery_kwh=_parse_float(_get(fm, "battery_kwh"), "battery_kwh"),
        consumption_rate=_parse_float(
            _get(fm, "consumption_rate (kWh/km)"), "consumption_rate"
        ),
        initial_soc_percent=_parse_float(
            _get(fm, "initial_soc_percent"), "initial_soc_percent"
        ),
        depot_charger_kw=_parse_float(_get(fm, "depot_charger_kw"), "depot_charger_kw"),
        depot_charger_efficiency=_parse_float(
            _get(fm, "depot_charger_efficiency"), "depot_charger_efficiency"
        ),
        terminal_charger_kw=_parse_float(
            _get(fm, "terminal_charger_kw"), "terminal_charger_kw"
        ),
        terminal_charger_efficiency=_parse_float(
            _get(fm, "terminal_charger_efficiency"), "terminal_charger_efficiency"
        ),
        trigger_soc_percent=_parse_float(
            _get(fm, "trigger_soc_percent"), "trigger_soc_percent"
        ),
        target_soc_percent=_parse_float(
            _get(fm, "target_soc_percent"), "target_soc_percent"
        ),
        min_soc_percent=_parse_float(
            _get(fm, "min_soc_percent"), "min_soc_percent"
        ),
        min_charge_duration_min=_parse_int(
            _get(fm, "min_charge_duration_min"), "min_charge_duration_min"
        ),
        operating_start=_parse_time(_get(fm, "operating_start")),
        operating_end=_parse_time(_get(fm, "operating_end")),
        shift_split=_parse_time(_get(fm, "shift_split")),
        min_layover_min=_parse_int(_get(fm, "min_layover_min"), "min_layover_min"),
        preferred_layover_min=_parse_int(
            _get(fm, "preferred_layover_min"), "preferred_layover_min"
        ),
        dead_run_buffer_min=_parse_int(
            _get(fm, "dead_run_buffer_min"), "dead_run_buffer_min"
        ),
        max_headway_deviation_min=_parse_int(
            _get(fm, "max_headway_deviation_min"), "max_headway_deviation_min"
        ),
        km_balance_tolerance_pct=_parse_float(
            _get(fm, "km_balance_tolerance_pct"), "km_balance_tolerance_pct"
        ),
        segment_distances=seg_distances,
        segment_times=seg_times,
        location_coords=coords,
        min_km_per_bus=_parse_float(fm.get("min_km_per_bus", (0, 0))[1] or 0, "min_km_per_bus"),
    )

    # 5. Parse headway and travel time profiles
    headway_df = _parse_headway(wb)
    travel_time_df = _parse_travel_time(wb)

    wb.close()
    return config, headway_df, travel_time_df
