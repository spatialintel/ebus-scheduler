"""
optimizer.py — Improves schedule quality via hill-climbing.

Two optimisation strategies:
1. Headway tuning: adjust headway ±N min per time band, re-run scheduler
2. Balance tuning: adjust bus selection scoring to prioritise underworked buses

Usage:
    from src.optimizer import optimize_schedule
    best_buses, best_metrics, best_headway = optimize_schedule(config, headway_df, travel_time_df)
"""
__version__ = "2026-03-23-b1"  # auto-stamped — confirms Streamlit deployment


from __future__ import annotations

from copy import deepcopy
import pandas as pd

from src.models import RouteConfig, BusState
from src.trip_generator import generate_trips
from src.bus_scheduler import schedule_buses
from src.metrics import compute_metrics, ScheduleMetrics


def _run_schedule(
    config: RouteConfig,
    headway_df: pd.DataFrame,
    travel_time_df: pd.DataFrame,
) -> tuple[list[BusState], ScheduleMetrics]:
    """Run the full pipeline and return buses + metrics."""
    trips = generate_trips(config, headway_df, travel_time_df)
    revenue_count = len([t for t in trips if t.trip_type == "Revenue"])
    buses = schedule_buses(config, trips)
    metrics = compute_metrics(config, buses, total_revenue_trips=revenue_count)
    return buses, metrics


def _try_headway_variation(
    config: RouteConfig,
    base_headway_df: pd.DataFrame,
    travel_time_df: pd.DataFrame,
    band_start_idx: int,
    band_end_idx: int,
    delta: int,
    max_deviation: int,
) -> tuple[pd.DataFrame, list[BusState], ScheduleMetrics] | None:
    """
    Try adjusting headway by `delta` minutes for rows band_start_idx to band_end_idx.
    Returns (modified_headway_df, buses, metrics) or None if invalid.
    """
    modified = base_headway_df.copy()
    for i in range(band_start_idx, min(band_end_idx, len(modified))):
        new_val = modified.iloc[i]["headway_min"] + delta
        if new_val < 5 or new_val > 120:
            return None
        # Check deviation from original
        original = base_headway_df.iloc[i]["headway_min"]
        if abs(new_val - original) > max_deviation:
            return None
        modified.at[modified.index[i], "headway_min"] = new_val

    try:
        buses, metrics = _run_schedule(config, modified, travel_time_df)
        # Reject if we lost revenue trips
        if metrics.revenue_trips_assigned < metrics.revenue_trips_total:
            return None
        return modified, buses, metrics
    except Exception:
        return None


def optimize_schedule(
    config: RouteConfig,
    headway_df: pd.DataFrame,
    travel_time_df: pd.DataFrame,
    max_iterations: int = 50,
    verbose: bool = True,
) -> tuple[list[BusState], ScheduleMetrics, pd.DataFrame]:
    """
    Hill-climbing optimizer.

    1. Run base schedule, compute score
    2. For each time band (morning peak, midday, evening peak, off-peak):
       - Try headway ± 5 min
       - Keep change if weighted score improves
    3. Return best schedule found

    Returns (best_buses, best_metrics, best_headway_df)
    """
    # ── Baseline ──
    base_buses, base_metrics = _run_schedule(config, headway_df, travel_time_df)
    best_score = base_metrics.weighted_score()
    best_buses = base_buses
    best_metrics = base_metrics
    best_headway = headway_df.copy()

    if verbose:
        print(f"\n  Optimizer: baseline score = {best_score:.4f}")
        print(f"  KM range: {base_metrics.km_range:.1f}, Dead ratio: {base_metrics.dead_km_ratio:.1%}")

    max_dev = config.max_headway_deviation_min

    # ── Define time bands ──
    # Split headway slots into bands for targeted adjustment
    n_slots = len(headway_df)
    band_size = max(3, n_slots // 6)  # ~5-6 bands

    bands = []
    for start in range(0, n_slots, band_size):
        end = min(start + band_size, n_slots)
        band_label = f"{headway_df.iloc[start]['time_from']}-{headway_df.iloc[end-1]['time_to']}"
        bands.append((start, end, band_label))

    # ── Hill-climbing ──
    improved = True
    iteration = 0

    while improved and iteration < max_iterations:
        improved = False
        iteration += 1

        for band_start, band_end, band_label in bands:
            for delta in [-5, -3, -2, 2, 3, 5]:
                result = _try_headway_variation(
                    config, best_headway, travel_time_df,
                    band_start, band_end, delta, max_dev,
                )
                if result is None:
                    continue

                new_headway, new_buses, new_metrics = result
                new_score = new_metrics.weighted_score()

                if new_score < best_score:
                    improvement = best_score - new_score
                    best_score = new_score
                    best_buses = new_buses
                    best_metrics = new_metrics
                    best_headway = new_headway
                    improved = True

                    if verbose:
                        print(f"  Iter {iteration}: {band_label} {delta:+d}min → "
                              f"score {new_score:.4f} (improved by {improvement:.4f})")
                    break  # restart bands with new baseline

            if improved:
                break

    if verbose:
        print(f"\n  Optimizer: final score = {best_score:.4f} after {iteration} iterations")
        print(f"  KM range: {best_metrics.km_range:.1f}, Dead ratio: {best_metrics.dead_km_ratio:.1%}")
        print(f"  KM per bus: {', '.join(f'{k:.1f}' for k in best_metrics.km_per_bus)}")

    return best_buses, best_metrics, best_headway
