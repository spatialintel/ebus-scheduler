"""
network_analyzer.py — Corridor detection and combined frequency analysis.

Identifies routes sharing physical segments, computes effective combined
headway on shared corridors, and flags coordination opportunities.

Usage:
    from src.network_analyzer import analyze_network, CorridorAnalysis
    corridors = analyze_network(city_schedule)
"""

from __future__ import annotations
__version__ = "2026-04-17-p3"

from dataclasses import dataclass, field


@dataclass
class CorridorAnalysis:
    """Analysis of a shared corridor between two routes."""
    route_a: str
    route_b: str
    shared_segments: list[str]      # ["GANGAJALIYA->RTO CIRCLE", ...]
    shared_km: float
    overlap_pct_a: float            # shared_km / route_a total km
    overlap_pct_b: float
    combined_headways: list[dict]   # per band: {band, hw_a, hw_b, hw_combined}
    coordination_flag: bool = False # True if departure offset < 30% of ideal
    ideal_offset_min: float = 0.0
    actual_offset_min: float = 0.0


def analyze_network(city_schedule) -> list[CorridorAnalysis]:
    """
    Detect shared corridors and compute combined frequency.

    For each pair of routes, intersect their segment keys.
    If shared segments exist, compute combined headway per band.
    """
    results = city_schedule.results
    codes = sorted(results.keys())
    corridors: list[CorridorAnalysis] = []

    # Pre-compute segment sets and total km per route
    route_segments: dict[str, set[str]] = {}
    route_km: dict[str, float] = {}
    route_seg_km: dict[str, dict[str, float]] = {}

    for code, r in results.items():
        segs = set()
        seg_km = {}
        for key, km in r.config.segment_distances.items():
            # Normalise: always use alphabetical order for undirected matching
            parts = key.split("->")
            if len(parts) == 2:
                a, b = parts[0].strip(), parts[1].strip()
                # Skip depot segments
                if "DEPOT" in a.upper() or "DEPOT" in b.upper():
                    continue
                norm = f"{min(a,b)}<>{max(a,b)}"
                segs.add(norm)
                seg_km[norm] = km
        route_segments[code] = segs
        route_seg_km[code] = seg_km
        route_km[code] = r.metrics.total_km if r.metrics.total_km > 0 else 1.0

    # Pairwise comparison
    for i, code_a in enumerate(codes):
        for code_b in codes[i+1:]:
            shared = route_segments.get(code_a, set()) & route_segments.get(code_b, set())
            if not shared:
                continue

            # Compute shared distance
            shared_km_a = sum(route_seg_km.get(code_a, {}).get(s, 0) for s in shared)
            shared_km_b = sum(route_seg_km.get(code_b, {}).get(s, 0) for s in shared)
            avg_shared = (shared_km_a + shared_km_b) / 2

            if avg_shared < 1.0:  # less than 1 km overlap — skip
                continue

            overlap_a = avg_shared / max(1.0, route_km[code_a]) * 100
            overlap_b = avg_shared / max(1.0, route_km[code_b]) * 100

            # Combined headway per band
            r_a, r_b = results[code_a], results[code_b]
            combined_hws = _compute_combined_headways(r_a, r_b)

            # Coordination check: are first departures well-offset?
            coord_flag, ideal_off, actual_off = _check_coordination(r_a, r_b)

            shared_labels = [s.replace("<>", " \u2194 ") for s in sorted(shared)]

            corridors.append(CorridorAnalysis(
                route_a=code_a,
                route_b=code_b,
                shared_segments=shared_labels,
                shared_km=round(avg_shared, 1),
                overlap_pct_a=round(overlap_a, 1),
                overlap_pct_b=round(overlap_b, 1),
                combined_headways=combined_hws,
                coordination_flag=coord_flag,
                ideal_offset_min=ideal_off,
                actual_offset_min=actual_off,
            ))

    return corridors


def _compute_combined_headways(r_a, r_b) -> list[dict]:
    """Compute combined headway per time band using H_combined = (Ha*Hb)/(Ha+Hb)."""
    result = []
    try:
        # Build band lists from each route's headway_df
        bands_a = _extract_bands(r_a.headway_df)
        bands_b = _extract_bands(r_b.headway_df)

        # Align on common time grid (use route A's bands as reference)
        for band_start, band_end, hw_a in bands_a:
            # Find matching band in B
            hw_b = _lookup_band(bands_b, band_start)
            if hw_b is None:
                continue

            hw_combined = (hw_a * hw_b) / (hw_a + hw_b) if (hw_a + hw_b) > 0 else 0
            result.append({
                "band": f"{band_start:02d}:00\u2013{band_end:02d}:00",
                "hw_a": int(hw_a),
                "hw_b": int(hw_b),
                "hw_combined": round(hw_combined, 1),
            })
    except Exception:
        pass

    return result


def _extract_bands(headway_df) -> list[tuple[int, int, float]]:
    """Extract (start_hour, end_hour, hw_min) from headway_df."""
    bands = []
    try:
        for _, row in headway_df.iterrows():
            t_from = row["time_from"]
            t_to = row["time_to"]
            hw = float(row["headway_min"])

            if hasattr(t_from, "hour"):
                start_h = t_from.hour
            else:
                start_h = int(str(t_from).split(":")[0])

            if hasattr(t_to, "hour"):
                end_h = t_to.hour
            else:
                end_h = int(str(t_to).split(":")[0])

            bands.append((start_h, end_h, hw))
    except Exception:
        pass
    return bands


def _lookup_band(bands: list[tuple[int, int, float]], hour: int) -> float | None:
    """Find headway for a given hour in a band list."""
    for start_h, end_h, hw in bands:
        if start_h <= hour < end_h:
            return hw
    return bands[-1][2] if bands else None


def _check_coordination(r_a, r_b) -> tuple[bool, float, float]:
    """
    Check if first departures are well-offset for combined frequency.
    Returns (flag, ideal_offset, actual_offset).
    Flag is True if actual_offset < 30% of ideal.
    """
    try:
        # Get first DN departure for each route
        first_a = _first_departure(r_a, "DN")
        first_b = _first_departure(r_b, "DN")

        if first_a is None or first_b is None:
            return False, 0, 0

        # Get peak headway of the route with longer headway
        hw_a = float(r_a.headway_df["headway_min"].min()) if len(r_a.headway_df) > 0 else 30
        hw_b = float(r_b.headway_df["headway_min"].min()) if len(r_b.headway_df) > 0 else 30

        # Ideal offset for equal headways: H/2
        ideal_offset = min(hw_a, hw_b) / 2.0

        # Actual offset
        diff = abs((first_a - first_b).total_seconds()) / 60.0
        # Normalise to within one headway cycle
        cycle = max(hw_a, hw_b)
        actual_offset = diff % cycle if cycle > 0 else diff

        flag = actual_offset < 0.3 * ideal_offset
        return flag, round(ideal_offset, 1), round(actual_offset, 1)
    except Exception:
        return False, 0, 0


def _first_departure(route_result, direction: str):
    """Get the earliest revenue departure in a given direction."""
    deps = []
    for bus in route_result.buses:
        for trip in bus.trips:
            if trip.trip_type == "Revenue" and trip.direction == direction and trip.actual_departure:
                deps.append(trip.actual_departure)
    return min(deps) if deps else None


def corridor_summary_rows(corridors: list[CorridorAnalysis]) -> list[dict]:
    """Format corridors for dashboard table."""
    return [
        {
            "Routes": f"{c.route_a} + {c.route_b}",
            "Shared km": c.shared_km,
            "Overlap A": f"{c.overlap_pct_a:.0f}%",
            "Overlap B": f"{c.overlap_pct_b:.0f}%",
            "Peak combined": f"{min((h['hw_combined'] for h in c.combined_headways), default=0):.0f} min"
                             if c.combined_headways else "\u2014",
            "Coordinated": "No \u2014 offset needed" if c.coordination_flag else "OK",
        }
        for c in corridors
    ]
