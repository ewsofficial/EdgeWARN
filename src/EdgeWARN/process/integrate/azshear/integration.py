import gc
import json
from copy import deepcopy

import numpy as np
import shapely.vectorized as sv
import util.file as fs
import xarray as xr
from util.grib_loader import load_grib_fast

from ..geometry.cell_polygon import StormIntegrationUtils
from .constants import (
    AZSHEAR_BUFFER_KM,
    AZSHEAR_LOW_THRESHOLD,
    AZSHEAR_MID_THRESHOLD,
    empty_azshear_output,
)
from .geometry import (
    buffer_polygon_km,
    grid_spacing_km,
    polygon_area_km2,
    polygon_major_axis_orientation_deg,
)
from .metrics import (
    extract_azshear_candidates,
    summarize_cross_layer_metrics,
    summarize_level_metrics,
)


def _is_grib_path(dataset_path):
    lower_path = str(dataset_path).lower()
    return lower_path.endswith((".grib2", ".grib", ".grb2", ".grb"))


def _open_azshear_dataset(integrator, dataset_path):
    is_grib = _is_grib_path(dataset_path)
    if is_grib:
        try:
            return load_grib_fast(dataset_path), True
        except Exception as exc:
            integrator.io_manager.write_warning(
                f"Fast GRIB loader failed for AzShear file {dataset_path}; falling back to xarray: {exc}"
            )

    return xr.open_dataset(dataset_path, decode_timedelta=True), is_grib


def _safe_float(value, default=0.0):
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _read_recent_history(cell_id, history_cache):
    if cell_id in history_cache:
        return history_cache[cell_id]

    history_file = fs.CELL_DIR / f"{cell_id}.json"
    if not history_file.exists():
        history_cache[cell_id] = []
        return history_cache[cell_id]

    try:
        with open(history_file, "r") as f:
            payload = json.load(f)
        if isinstance(payload, list):
            history_cache[cell_id] = payload[-5:]
        else:
            history_cache[cell_id] = []
    except Exception:
        history_cache[cell_id] = []

    return history_cache[cell_id]


def _history_level_presence_and_peak(entry, level_key):
    props = entry.get("properties", {}) if isinstance(entry, dict) else {}
    azshear = props.get("azshear", {}) if isinstance(props, dict) else {}
    level = azshear.get(level_key)
    if not isinstance(level, dict):
        return False, 0.0

    # New schema
    core = level.get("core_structure")
    if isinstance(core, dict):
        component_count = int(_safe_float(core.get("component_count", 0), 0.0))
        peak = _safe_float(core.get("largest_component_peak_azshear", 0.0), 0.0)
        return component_count > 0, peak

    # Legacy schema fallback
    peak = _safe_float(level.get("peak_value", 0.0), 0.0)
    return bool(level), peak


def _compute_level_persistence(history_entries, level_key, threshold):
    if not history_entries:
        return {
            "dominant_component_persistence": 0.0,
            "peak_persistence": 0.0,
        }

    present_count = 0
    peak_count = 0
    for entry in history_entries:
        present, peak = _history_level_presence_and_peak(entry, level_key)
        if present:
            present_count += 1
        if peak >= threshold:
            peak_count += 1

    denom = max(len(history_entries), 1)
    return {
        "dominant_component_persistence": round(present_count / denom, 3),
        "peak_persistence": round(peak_count / denom, 3),
    }


def _compute_simultaneous_persistence(history_entries):
    if not history_entries:
        return 0.0

    both_count = 0
    for entry in history_entries:
        low_present, _ = _history_level_presence_and_peak(entry, "low")
        mid_present, _ = _history_level_presence_and_peak(entry, "mid")
        if low_present and mid_present:
            both_count += 1

    return round(both_count / max(len(history_entries), 1), 3)


def integrate_azshear_features(integrator, low_dataset_path, mid_dataset_path, storm_cells):
    if not storm_cells or not low_dataset_path or not mid_dataset_path:
        return storm_cells

    zero_output = empty_azshear_output()
    history_cache = {}

    def empty_output():
        return deepcopy(zero_output)

    try:
        low_ds, low_is_grib = _open_azshear_dataset(integrator, low_dataset_path)
        mid_ds, mid_is_grib = _open_azshear_dataset(integrator, mid_dataset_path)
    except Exception as exc:
        integrator.io_manager.write_error(f"Load error for AzShear features: {exc}")
        return storm_cells

    try:
        low_lat_name = "latitude" if "latitude" in low_ds.coords else "lat"
        low_lon_name = "longitude" if "longitude" in low_ds.coords else "lon"
        mid_lat_name = "latitude" if "latitude" in mid_ds.coords else "lat"
        mid_lon_name = "longitude" if "longitude" in mid_ds.coords else "lon"

        low_lat_vals = low_ds[low_lat_name].values
        low_lon_vals = low_ds[low_lon_name].values
        mid_lat_vals = mid_ds[mid_lat_name].values
        mid_lon_vals = mid_ds[mid_lon_name].values

        low_var_name = "unknown" if "unknown" in low_ds.data_vars else list(low_ds.data_vars)[0]
        mid_var_name = "unknown" if "unknown" in mid_ds.data_vars else list(mid_ds.data_vars)[0]
        low_var = low_ds[low_var_name]
        mid_var = mid_ds[mid_var_name]

        low_var_values = low_var.values if low_is_grib else None
        mid_var_values = mid_var.values if mid_is_grib else None

        low_lat_spacing_km, low_lon_spacing_km = grid_spacing_km(low_lat_vals, low_lon_vals)
        mid_lat_spacing_km, mid_lon_spacing_km = grid_spacing_km(mid_lat_vals, mid_lon_vals)
        low_pixel_area_km2 = max(low_lat_spacing_km * low_lon_spacing_km, 0.01)
        mid_pixel_area_km2 = max(mid_lat_spacing_km * mid_lon_spacing_km, 0.01)

        for cell in storm_cells:
            cell.setdefault("properties", {})
            cell["properties"]["azshear"] = empty_output()

            poly = StormIntegrationUtils.create_cell_polygon(cell)
            if poly is None:
                continue

            buffered_poly = buffer_polygon_km(poly, AZSHEAR_BUFFER_KM)
            buffered_area_km2 = max(polygon_area_km2(buffered_poly), 1e-6)
            reflectivity_axis_deg = polygon_major_axis_orientation_deg(poly)

            try:
                low_poly = integrator._polygon_for_dataset(buffered_poly, low_lon_vals)
                mid_poly = integrator._polygon_for_dataset(buffered_poly, mid_lon_vals)

                low_subset, low_lat, low_lon = integrator._extract_spatial_subset(
                    low_ds,
                    low_var,
                    low_is_grib,
                    low_var_values,
                    low_lat_name,
                    low_lon_name,
                    low_lat_vals,
                    low_lon_vals,
                    low_poly,
                )
                mid_subset, mid_lat, mid_lon = integrator._extract_spatial_subset(
                    mid_ds,
                    mid_var,
                    mid_is_grib,
                    mid_var_values,
                    mid_lat_name,
                    mid_lon_name,
                    mid_lat_vals,
                    mid_lon_vals,
                    mid_poly,
                )

                if low_subset is None or mid_subset is None:
                    continue

                low_inside = sv.contains(low_poly.buffer(1e-9), low_lon, low_lat)
                mid_inside = sv.contains(mid_poly.buffer(1e-9), mid_lon, mid_lat)

                low_masked = np.where(low_inside, np.asarray(low_subset), np.nan)
                mid_masked = np.where(mid_inside, np.asarray(mid_subset), np.nan)
                low_masked[low_masked < 0] = np.nan
                mid_masked[mid_masked < 0] = np.nan

                low_candidates = extract_azshear_candidates(
                    low_masked,
                    low_lat,
                    low_lon,
                    AZSHEAR_LOW_THRESHOLD,
                    low_pixel_area_km2,
                    low_lat_spacing_km,
                    low_lon_spacing_km,
                )
                mid_candidates = extract_azshear_candidates(
                    mid_masked,
                    mid_lat,
                    mid_lon,
                    AZSHEAR_MID_THRESHOLD,
                    mid_pixel_area_km2,
                    mid_lat_spacing_km,
                    mid_lon_spacing_km,
                )

                low_summary, low_dominant = summarize_level_metrics(
                    low_candidates,
                    buffered_area_km2,
                    reflectivity_axis_deg,
                )
                mid_summary, mid_dominant = summarize_level_metrics(
                    mid_candidates,
                    buffered_area_km2,
                    reflectivity_axis_deg,
                )

                cell_id = cell.get("id")
                history_entries = _read_recent_history(cell_id, history_cache) if cell_id is not None else []
                low_summary["persistence"] = _compute_level_persistence(
                    history_entries,
                    "low",
                    AZSHEAR_LOW_THRESHOLD,
                )
                mid_summary["persistence"] = _compute_level_persistence(
                    history_entries,
                    "mid",
                    AZSHEAR_MID_THRESHOLD,
                )
                simultaneous_persistence = _compute_simultaneous_persistence(history_entries)

                cross_layer = summarize_cross_layer_metrics(
                    low_dominant,
                    mid_dominant,
                    low_summary,
                    mid_summary,
                    simultaneous_persistence,
                )

                cell["properties"]["azshear"] = {
                    "buffer_km": AZSHEAR_BUFFER_KM,
                    "low": low_summary,
                    "mid": mid_summary,
                    "cross_layer": cross_layer,
                }
            except Exception as exc:
                integrator.io_manager.write_error(f"Process AzShear cell {cell.get('id')}: {exc}")
                cell["properties"]["azshear"] = empty_output()
    finally:
        low_ds.close()
        mid_ds.close()
        gc.collect()

    return storm_cells
