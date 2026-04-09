"""
city_config_loader.py — Loads multiple route Excel configs into a CityConfig.

Usage:
    from src.city_config_loader import load_city_config, load_city_config_from_files

    # From a folder of Excel files
    city = load_city_config("configs/")

    # From Streamlit uploaded file objects
    city = load_city_config_from_files([uploaded_file_1, uploaded_file_2, ...])
"""

from __future__ import annotations
__version__ = "2026-04-08-p1"

from pathlib import Path
from io import BytesIO
import tempfile
import os

from src.config_loader import load_config, ConfigError
from src.city_models import CityConfig, RouteInput


def load_city_config(folder_path: str | Path) -> CityConfig:
    """
    Scan a folder for .xlsx/.xlsm files, load each as a RouteConfig,
    and return a CityConfig with all routes.

    Raises ConfigError if:
      - No valid Excel files found
      - Duplicate route_codes detected
    """
    folder = Path(folder_path)
    if not folder.is_dir():
        raise ConfigError(f"Not a directory: {folder}")

    excel_files = sorted(
        f for f in folder.iterdir()
        if f.suffix.lower() in (".xlsx", ".xlsm") and not f.name.startswith("~")
    )

    if not excel_files:
        raise ConfigError(f"No Excel files found in {folder}")

    routes: dict[str, RouteInput] = {}
    errors: list[str] = []

    for fp in excel_files:
        try:
            config, headway_df, travel_time_df = load_config(fp)
            code = config.route_code

            if code in routes:
                errors.append(
                    f"Duplicate route_code '{code}' in {fp.name} "
                    f"(already loaded from another file)"
                )
                continue

            routes[code] = RouteInput(
                config=config,
                headway_df=headway_df,
                travel_time_df=travel_time_df,
            )
        except Exception as e:
            errors.append(f"{fp.name}: {e}")

    if errors and not routes:
        raise ConfigError(
            f"All config files failed to load:\n" +
            "\n".join(f"  • {e}" for e in errors)
        )

    # Detect shared depot (Phase 1 assumption: single depot)
    depots = {ri.config.depot for ri in routes.values()}
    depot_name = depots.pop() if len(depots) == 1 else "MULTI_DEPOT"

    return CityConfig(
        routes=routes,
        total_fleet=sum(ri.config.fleet_size for ri in routes.values()),
        depot_name=depot_name,
    )


def load_city_config_from_files(uploaded_files: list) -> tuple[CityConfig, list[str]]:
    """
    Load from Streamlit UploadedFile objects (in-memory).

    Returns:
        city_config: CityConfig
        warnings: list of non-fatal warnings (e.g. parse issues on individual files)
    """
    routes: dict[str, RouteInput] = {}
    warnings: list[str] = []

    for uf in uploaded_files:
        try:
            # Write to temp file (openpyxl needs a file path)
            with tempfile.NamedTemporaryFile(
                suffix=".xlsx", delete=False
            ) as tmp:
                tmp.write(uf.getvalue())
                tmp_path = tmp.name

            config, headway_df, travel_time_df = load_config(tmp_path)
            code = config.route_code

            if code in routes:
                warnings.append(
                    f"⚠ Duplicate route_code '{code}' in {uf.name} — skipped"
                )
                continue

            routes[code] = RouteInput(
                config=config,
                headway_df=headway_df,
                travel_time_df=travel_time_df,
            )

        except Exception as e:
            warnings.append(f"⚠ {uf.name}: {e}")

        finally:
            try:
                os.unlink(tmp_path)
            except Exception:
                pass

    if not routes:
        raise ConfigError("No valid route configs loaded from uploaded files")

    depots = {ri.config.depot for ri in routes.values()}
    depot_name = depots.pop() if len(depots) == 1 else "MULTI_DEPOT"

    city = CityConfig(
        routes=routes,
        total_fleet=sum(ri.config.fleet_size for ri in routes.values()),
        depot_name=depot_name,
    )

    return city, warnings
