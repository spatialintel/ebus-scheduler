"""Tests for distance_engine.py"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from datetime import time
from src.models import RouteConfig
from src.distance_engine import enrich_distances


def _make_config(**overrides):
    """Helper to create a minimal RouteConfig for testing."""
    defaults = dict(
        route_code="R1", route_name="Test",
        depot="DEPOT", start_point="GAN", end_point="ADH",
        intermediates=[],
        fleet_size=3, battery_kwh=210.0, consumption_rate=1.1,
        initial_soc_percent=95.0,
        depot_charger_kw=240.0, depot_charger_efficiency=0.80,
        terminal_charger_kw=0, terminal_charger_efficiency=0,
        trigger_soc_percent=30, target_soc_percent=80,
        min_soc_percent=20, min_charge_duration_min=15,
        operating_start=time(6, 0), operating_end=time(23, 0),
        shift_split=time(13, 0),
        min_layover_min=5, preferred_layover_min=10,
        dead_run_buffer_min=25,
        max_headway_deviation_min=5, km_balance_tolerance_pct=10,
        segment_distances={"DEPOT->GAN": 7.2, "GAN->ADH": 8.2},
        segment_times={"DEPOT->GAN": 25, "GAN->ADH": 28},
        location_coords={},
    )
    defaults.update(overrides)
    return RouteConfig(**defaults)


def test_no_coords_skips_everything():
    """When no coordinates are provided, all segments should be skipped."""
    config = _make_config(location_coords={})
    result = enrich_distances(config)
    assert result["fetched"] == []
    assert len(result["skipped"]) == 2
    assert config.segment_distances["DEPOT->GAN"] == 7.2  # unchanged
    print("  No coords: skipped all, manual values preserved OK")


def test_partial_coords_skips_missing():
    """When only some locations have coords, segments with missing coords are skipped."""
    config = _make_config(
        location_coords={"DEPOT": (23.02, 72.57)},  # GAN and ADH missing
    )
    result = enrich_distances(config)
    assert result["fetched"] == []
    assert "GAN" in result["no_coords"] or "ADH" in result["no_coords"]
    assert config.segment_distances["DEPOT->GAN"] == 7.2  # unchanged
    print("  Partial coords: skipped segments with missing coords OK")


def test_overwrite_false_keeps_existing():
    """When overwrite=False, existing non-zero distances are kept even if coords exist."""
    config = _make_config(
        location_coords={
            "DEPOT": (23.02, 72.57),
            "GAN": (23.03, 72.58),
            "ADH": (23.05, 72.59),
        },
    )
    result = enrich_distances(config, overwrite=False)
    # Both segments have non-zero values, so both should be skipped
    assert "DEPOT->GAN" in result["skipped"]
    assert "GAN->ADH" in result["skipped"]
    assert config.segment_distances["DEPOT->GAN"] == 7.2  # unchanged
    print("  Overwrite=False: kept existing values OK")


def test_zero_distance_triggers_fetch():
    """Segments with distance=0 should attempt OSRM fetch (will fail offline, that's OK)."""
    config = _make_config(
        segment_distances={"DEPOT->GAN": 0, "GAN->ADH": 8.2},
        segment_times={"DEPOT->GAN": 0, "GAN->ADH": 28},
        location_coords={
            "DEPOT": (23.02, 72.57),
            "GAN": (23.03, 72.58),
            "ADH": (23.05, 72.59),
        },
    )
    result = enrich_distances(config, overwrite=False)
    # DEPOT->GAN has distance=0 and coords exist, so it should attempt fetch
    # GAN->ADH has distance=8.2, so it should be skipped
    assert "GAN->ADH" in result["skipped"]
    # DEPOT->GAN is either in fetched (if online) or failed (if offline)
    assert "DEPOT->GAN" in result["fetched"] or "DEPOT->GAN" in result["failed"]
    print("  Zero distance: attempted OSRM fetch OK")


def test_live_osrm_fetch():
    """
    Test actual OSRM call with real Ahmedabad coordinates.
    This test will pass if you have internet, and gracefully skip if you don't.
    """
    config = _make_config(
        segment_distances={"DEPOT->GAN": 0},
        segment_times={"DEPOT->GAN": 0},
        location_coords={
            "DEPOT": (23.0225, 72.5714),
            "GAN": (23.0258, 72.5802),
        },
    )
    result = enrich_distances(config, overwrite=False)

    if result["fetched"]:
        dist = config.segment_distances["DEPOT->GAN"]
        tt = config.segment_times["DEPOT->GAN"]
        assert dist > 0, f"Distance should be > 0, got {dist}"
        assert tt > 0, f"Travel time should be > 0, got {tt}"
        print(f"  Live OSRM: DEPOT->GAN = {dist} km, {tt} min OK")
    elif result["failed"]:
        reason = result["failed"].get("DEPOT->GAN", "unknown")
        print(f"  Live OSRM: skipped (no network: {reason})")
    else:
        print("  Live OSRM: skipped (no network)")


if __name__ == "__main__":
    print("Testing distance_engine...\n")
    test_no_coords_skips_everything()
    test_partial_coords_skips_missing()
    test_overwrite_false_keeps_existing()
    test_zero_distance_triggers_fetch()
    test_live_osrm_fetch()
    print("\nAll tests passed.")
