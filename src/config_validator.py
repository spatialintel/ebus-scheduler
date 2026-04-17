"""
config_validator.py — Pre-scheduling validation layer.

Catches known silent-failure patterns BEFORE schedule_buses() runs.
Raises ConfigError with actionable messages on failure.
Warnings are returned as a list of strings (non-fatal).

Usage:
    from src.config_validator import validate_route_config
    errors, warnings = validate_route_config(config, headway_df, travel_time_df)
    if errors:
        raise ConfigError("\\n".join(errors))
"""

from __future__ import annotations
__version__ = "2026-04-15-p1"

from datetime import time as _time, datetime as _datetime


class ConfigError(Exception):
    """Raised when config validation fails with non-recoverable errors."""
    pass


def validate_route_config(
    config,
    headway_df=None,
    travel_time_df=None,
) -> tuple[list[str], list[str]]:
    """
    Run all validation checks on a single RouteConfig + profiles.

    Returns:
        (errors: list[str], warnings: list[str])
        errors = fatal — scheduling will fail or produce corrupt results.
        warnings = non-fatal — scheduling will work but results may be suboptimal.
    """
    errors: list[str] = []
    warnings: list[str] = []

    _check_identity(config, errors)
    _check_operating_window(config, errors)
    _check_segments(config, errors, warnings)
    _check_charger_consistency(config, errors, warnings)
    _check_time_fields(config, errors)
    _check_soc_thresholds(config, errors)
    _check_fleet(config, errors, warnings)

    if headway_df is not None:
        _check_headway_profile(config, headway_df, errors, warnings)

    if travel_time_df is not None:
        _check_travel_time_profile(travel_time_df, errors)

    if headway_df is not None and travel_time_df is not None:
        _check_headway_feasibility(config, headway_df, travel_time_df, warnings)

    return errors, warnings


# ── Individual checks ─────────────────────────────────────────────────────────


def _check_identity(config, errors: list):
    """Route must have code, name, depot, start, end."""
    for field in ("route_code", "route_name", "depot", "start_point", "end_point"):
        val = getattr(config, field, None)
        if not val or not str(val).strip():
            errors.append(f"Missing required field: {field}")


def _check_operating_window(config, errors: list):
    """operating_start < shift_split < operating_end."""
    try:
        os = config.operating_start
        ss = config.shift_split
        oe = config.operating_end
        if not (isinstance(os, _time) and isinstance(ss, _time) and isinstance(oe, _time)):
            errors.append(
                f"Operating times must be datetime.time objects, got: "
                f"start={type(os).__name__}, split={type(ss).__name__}, end={type(oe).__name__}. "
                f"This usually means dataclasses.asdict() was used on RouteConfig — use explicit field access instead."
            )
            return
        if os >= ss:
            errors.append(
                f"operating_start ({os}) must be before shift_split ({ss})"
            )
        if ss >= oe:
            errors.append(
                f"shift_split ({ss}) must be before operating_end ({oe})"
            )
        if os >= oe:
            errors.append(
                f"operating_start ({os}) must be before operating_end ({oe})"
            )
    except Exception as e:
        errors.append(f"Operating window check failed: {e}")


def _check_segments(config, errors: list, warnings: list):
    """All critical FROM→TO pairs exist in segment_distances and segment_times."""
    required_pairs = []

    # Depot ↔ start, Depot ↔ end
    for loc in (config.start_point, config.end_point):
        required_pairs.append((config.depot, loc))
        required_pairs.append((loc, config.depot))

    # Start ↔ end (full route)
    required_pairs.append((config.start_point, config.end_point))
    required_pairs.append((config.end_point, config.start_point))

    # Intermediates ↔ depot (if used for split dispatch)
    for inter in getattr(config, "intermediates", []):
        if inter and str(inter).strip():
            required_pairs.append((config.depot, inter.strip()))
            required_pairs.append((inter.strip(), config.depot))

    for from_loc, to_loc in required_pairs:
        key = config.segment_key(from_loc, to_loc)
        if key not in config.segment_distances:
            # Check if star-prefix variant exists
            star_key = config.segment_key(f"★ {from_loc}", to_loc)
            if star_key not in config.segment_distances:
                warnings.append(
                    f"Missing distance for segment '{key}'. "
                    f"Scheduler may fail or use fallback estimate."
                )
        if key not in config.segment_times:
            star_key = config.segment_key(f"★ {from_loc}", to_loc)
            if star_key not in config.segment_times:
                warnings.append(
                    f"Missing travel time for segment '{key}'. "
                    f"Will use avg_speed_kmph fallback ({config.avg_speed_kmph} km/h)."
                )


def _check_charger_consistency(config, errors: list, warnings: list):
    """P5 enabled but no charger = guaranteed failure."""
    fleet = config.fleet_size
    depot_kw = getattr(config, "depot_charger_kw", 0)
    p5_start = getattr(config, "p5_charging_start", None)
    p5_end = getattr(config, "p5_charging_end", None)

    # P5 is active when fleet ≤ 10
    p5_active = fleet <= 10

    if p5_active and (depot_kw is None or depot_kw <= 0):
        errors.append(
            f"P5 charging is active (fleet={fleet} ≤ 10) but depot_charger_kw={depot_kw}. "
            f"Buses cannot charge. Either increase fleet above 10 (waives P5) "
            f"or set depot_charger_kw > 0."
        )

    if p5_active and p5_start and p5_end:
        try:
            if isinstance(p5_start, _time) and isinstance(p5_end, _time):
                if p5_start >= p5_end:
                    errors.append(
                        f"p5_charging_start ({p5_start}) must be before p5_charging_end ({p5_end})"
                    )
        except Exception:
            pass

    # Charger efficiency sanity
    eff = getattr(config, "depot_charger_efficiency", 0.85)
    if eff <= 0 or eff > 1.0:
        errors.append(
            f"depot_charger_efficiency={eff} is invalid (must be 0 < eff ≤ 1.0)"
        )

    # Terminal charger: warn if kw > 0 but efficiency = 0
    tkw = getattr(config, "terminal_charger_kw", 0)
    teff = getattr(config, "terminal_charger_efficiency", 0)
    if tkw and tkw > 0 and (teff is None or teff <= 0):
        warnings.append(
            f"terminal_charger_kw={tkw} but terminal_charger_efficiency={teff}. "
            f"Terminal charging will deliver 0 kWh. Set efficiency > 0."
        )


def _check_time_fields(config, errors: list):
    """All time fields must be datetime.time (catches asdict corruption)."""
    time_fields = [
        "operating_start", "operating_end", "shift_split",
        "p5_charging_start", "p5_charging_end",
    ]
    for fname in time_fields:
        val = getattr(config, fname, None)
        if val is None:
            continue
        if isinstance(val, dict):
            errors.append(
                f"{fname} is a dict {val} instead of datetime.time. "
                f"This is caused by dataclasses.asdict() converting time objects. "
                f"Use explicit field access or copy.copy() instead of asdict()."
            )
        elif not isinstance(val, _time):
            errors.append(
                f"{fname} must be datetime.time, got {type(val).__name__}: {val}"
            )


def _check_soc_thresholds(config, errors: list):
    """SOC thresholds must be logically ordered."""
    min_soc = getattr(config, "min_soc_percent", 20)
    trigger = getattr(config, "trigger_soc_percent", 30)
    target = getattr(config, "target_soc_percent", 60)
    initial = getattr(config, "initial_soc_percent", 95)

    if min_soc >= trigger:
        errors.append(
            f"min_soc_percent ({min_soc}) must be < trigger_soc_percent ({trigger})"
        )
    if trigger >= target:
        errors.append(
            f"trigger_soc_percent ({trigger}) must be < target_soc_percent ({target})"
        )
    if initial < target:
        errors.append(
            f"initial_soc_percent ({initial}) must be ≥ target_soc_percent ({target})"
        )


def _check_fleet(config, errors: list, warnings: list):
    """Fleet size sanity checks."""
    fleet = config.fleet_size
    if fleet < 0:
        errors.append(f"fleet_size={fleet} is negative")
    # fleet_size=0 is valid (auto-detect mode)

    max_km = getattr(config, "max_km_per_bus", 0)
    min_km = getattr(config, "min_km_per_bus", 0)
    if max_km > 0 and min_km > 0 and min_km > max_km:
        errors.append(
            f"min_km_per_bus ({min_km}) > max_km_per_bus ({max_km}). "
            f"No bus can satisfy both constraints."
        )


def _check_headway_profile(config, headway_df, errors: list, warnings: list):
    """Headway profile structure and value checks."""
    required_cols = {"time_from", "time_to", "headway_min"}
    actual_cols = set(headway_df.columns)
    missing = required_cols - actual_cols
    if missing:
        errors.append(
            f"Headway profile missing columns: {missing}. "
            f"Expected: time_from, time_to, headway_min."
        )
        return

    if len(headway_df) == 0:
        errors.append("Headway profile has no rows.")
        return

    for idx, row in headway_df.iterrows():
        hw = row.get("headway_min")
        if hw is None or (isinstance(hw, (int, float)) and hw < 1):
            errors.append(
                f"Headway profile row {idx}: headway_min={hw} is invalid (must be ≥ 1 min)"
            )
        if isinstance(hw, (int, float)) and hw > 120:
            warnings.append(
                f"Headway profile row {idx}: headway_min={hw} is very high (>120 min). "
                f"Service will be extremely sparse in this band."
            )


def _check_travel_time_profile(travel_time_df, errors: list):
    """Travel time profile structure check."""
    required_cols = {"time_from", "time_to", "up_min", "dn_min"}
    actual_cols = set(travel_time_df.columns)
    missing = required_cols - actual_cols
    if missing:
        errors.append(
            f"Travel time profile missing columns: {missing}. "
            f"Expected: time_from, time_to, up_min, dn_min."
        )
        return

    if len(travel_time_df) == 0:
        errors.append("Travel time profile has no rows.")
        return

    for idx, row in travel_time_df.iterrows():
        for col in ("up_min", "dn_min"):
            val = row.get(col)
            if val is not None and isinstance(val, (int, float)) and val <= 0:
                errors.append(
                    f"Travel time row {idx}: {col}={val} must be > 0"
                )


def _check_headway_feasibility(config, headway_df, travel_time_df, warnings: list):
    """
    Warn if configured headway is physically unachievable.
    natural_gap = cycle_time / fleet_size.
    If natural_gap > configured headway, scheduler will silently use natural_gap.
    """
    if config.fleet_size <= 0:
        return  # auto-detect mode, skip feasibility check

    try:
        _REF = _datetime(2025, 1, 1)
        op_start = _REF.replace(hour=config.operating_start.hour,
                                minute=config.operating_start.minute)

        # Get first-band travel times
        first_up = None
        first_dn = None
        for _, row in travel_time_df.iterrows():
            if first_up is None:
                first_up = float(row["up_min"])
                first_dn = float(row["dn_min"])

        if first_up is None or first_dn is None:
            return

        min_break = config.preferred_layover_min
        cycle_time = first_dn + min_break + first_up + min_break
        natural_gap = cycle_time / config.fleet_size

        for _, row in headway_df.iterrows():
            try:
                band_hw = float(row["headway_min"])
                time_from = row["time_from"]
                time_to = row["time_to"]
                if natural_gap > band_hw:
                    rec_fleet = int(cycle_time / band_hw) + 1
                    warnings.append(
                        f"⚠ Headway infeasible in band {time_from}–{time_to}: "
                        f"configured={int(band_hw)} min but natural_gap={natural_gap:.1f} min "
                        f"(cycle={cycle_time:.0f} min ÷ fleet={config.fleet_size}). "
                        f"Scheduler will use {natural_gap:.1f} min in efficiency mode. "
                        f"Fix: set headway ≥ {int(natural_gap) + 1} min "
                        f"or increase fleet to ≥ {rec_fleet} buses."
                    )
            except Exception:
                continue
    except Exception as e:
        warnings.append(f"Feasibility check skipped: {e}")


def validate_city_config(city_config) -> tuple[list[str], list[str]]:
    """
    Validate all routes in a CityConfig.
    Returns aggregated (errors, warnings) across all routes.
    """
    all_errors: list[str] = []
    all_warnings: list[str] = []

    for code, ri in city_config.routes.items():
        errs, warns = validate_route_config(
            ri.config, ri.headway_df, ri.travel_time_df
        )
        for e in errs:
            all_errors.append(f"[{code}] {e}")
        for w in warns:
            all_warnings.append(f"[{code}] {w}")

    return all_errors, all_warnings
