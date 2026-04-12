import re
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import numpy as np

import util.file as fs
from util.grib_loader import load_grib_fast

from . import config as cfg


TIMESTAMP_PATTERNS = (
    re.compile(r"(\d{8}-\d{6})"),
    re.compile(r"(\d{8}_\d{6})"),
)


def _axis_spacing(axis: np.ndarray) -> Optional[float]:
    if len(axis) <= 1:
        return None
    diffs = np.diff(np.asarray(axis, dtype=float))
    finite = diffs[np.isfinite(diffs)]
    if finite.size == 0:
        return None
    return float(np.nanmean(np.abs(finite)))


def _grid_diagnostics(name: str, file_path: str, grid: Dict[str, Any]) -> str:
    latitudes = np.asarray(grid["latitudes"], dtype=float)
    longitudes = np.asarray(grid["longitudes"], dtype=float)
    lat_spacing = _axis_spacing(latitudes)
    lon_spacing = _axis_spacing(longitudes)
    lat_span = (float(latitudes[0]), float(latitudes[-1])) if latitudes.size else (None, None)
    lon_span = (float(longitudes[0]), float(longitudes[-1])) if longitudes.size else (None, None)
    return (
        f"{name}: path={file_path}, shape={grid['values'].shape}, "
        f"lat_span={lat_span}, lon_span={lon_span}, "
        f"lat_spacing={lat_spacing}, lon_spacing={lon_spacing}"
    )


def _extent_tolerance(source_axis: np.ndarray, target_axis: np.ndarray) -> float:
    source_spacing = _axis_spacing(source_axis) or 0.0
    target_spacing = _axis_spacing(target_axis) or 0.0
    return max(source_spacing, target_spacing) * 0.51


def _is_monotonic(axis: np.ndarray) -> bool:
    if len(axis) <= 1:
        return True
    diffs = np.diff(np.asarray(axis, dtype=float))
    return bool(np.all(diffs >= 0) or np.all(diffs <= 0))


def _nearest_axis_indices(source_axis: np.ndarray, target_axis: np.ndarray) -> np.ndarray:
    src = np.asarray(source_axis, dtype=float)
    tgt = np.asarray(target_axis, dtype=float)
    descending = len(src) > 1 and src[0] > src[-1]
    if descending:
        src = src[::-1]

    positions = np.interp(tgt, src, np.arange(len(src), dtype=float))
    indices = np.rint(positions).astype(int)
    indices = np.clip(indices, 0, len(source_axis) - 1)

    if descending:
        indices = (len(source_axis) - 1) - indices

    return indices


def _harmonize_grid(
    grid_name: str,
    grid: Dict[str, Any],
    target_shape: Tuple[int, int],
    target_lats: np.ndarray,
    target_lons: np.ndarray,
) -> Dict[str, Any]:
    values = np.asarray(grid["values"], dtype=float)
    src_lats = np.asarray(grid["latitudes"], dtype=float)
    src_lons = np.asarray(grid["longitudes"], dtype=float)

    if values.shape == target_shape and src_lats.shape == target_lats.shape and src_lons.shape == target_lons.shape:
        return grid

    if values.ndim != 2:
        raise ValueError(f"{grid_name} grid must be 2D, got {values.ndim}D")
    if not _is_monotonic(src_lats) or not _is_monotonic(src_lons):
        raise ValueError(f"{grid_name} coordinates are not monotonic and cannot be harmonized")
    if not _is_monotonic(target_lats) or not _is_monotonic(target_lons):
        raise ValueError(f"target coordinates are not monotonic and cannot be harmonized")

    lat_tol = _extent_tolerance(src_lats, target_lats)
    lon_tol = _extent_tolerance(src_lons, target_lons)
    lat_start_close = np.isclose(src_lats[0], target_lats[0], atol=lat_tol)
    lat_end_close = np.isclose(src_lats[-1], target_lats[-1], atol=lat_tol)
    lon_start_close = np.isclose(src_lons[0], target_lons[0], atol=lon_tol)
    lon_end_close = np.isclose(src_lons[-1], target_lons[-1], atol=lon_tol)
    if not (lat_start_close and lat_end_close and lon_start_close and lon_end_close):
        raise ValueError(
            f"{grid_name} grid extent mismatch: source lat=({src_lats[0]}, {src_lats[-1]}), "
            f"target lat=({target_lats[0]}, {target_lats[-1]}), "
            f"source lon=({src_lons[0]}, {src_lons[-1]}), target lon=({target_lons[0]}, {target_lons[-1]})"
        )

    row_indices = _nearest_axis_indices(src_lats, target_lats)
    col_indices = _nearest_axis_indices(src_lons, target_lons)
    harmonized = values[np.ix_(row_indices, col_indices)]
    if harmonized.shape != target_shape:
        raise ValueError(f"{grid_name} harmonization failed: expected {target_shape}, got {harmonized.shape}")

    print(
        f"[Mesocyclone] Harmonized {grid_name} grid from {values.shape} to {target_shape} "
        f"using nearest-neighbor coordinate mapping"
    )

    updated = dict(grid)
    updated["values"] = harmonized
    updated["latitudes"] = np.asarray(target_lats, dtype=float)
    updated["longitudes"] = np.asarray(target_lons, dtype=float)
    updated["harmonized_from_shape"] = tuple(values.shape)
    return updated


def _validate_grid_alignment(
    grid_name: str,
    grid: Dict[str, Any],
    target_lats: np.ndarray,
    target_lons: np.ndarray,
) -> None:
    src_lats = np.asarray(grid["latitudes"], dtype=float)
    src_lons = np.asarray(grid["longitudes"], dtype=float)

    if not _is_monotonic(src_lats) or not _is_monotonic(src_lons):
        raise ValueError(f"{grid_name} coordinates are not monotonic and cannot be aligned")

    lat_tol = _extent_tolerance(src_lats, target_lats)
    lon_tol = _extent_tolerance(src_lons, target_lons)
    lat_start_close = np.isclose(src_lats[0], target_lats[0], atol=lat_tol)
    lat_end_close = np.isclose(src_lats[-1], target_lats[-1], atol=lat_tol)
    lon_start_close = np.isclose(src_lons[0], target_lons[0], atol=lon_tol)
    lon_end_close = np.isclose(src_lons[-1], target_lons[-1], atol=lon_tol)
    if not (lat_start_close and lat_end_close and lon_start_close and lon_end_close):
        raise ValueError(
            f"{grid_name} grid extent mismatch: source lat=({src_lats[0]}, {src_lats[-1]}), "
            f"target lat=({target_lats[0]}, {target_lats[-1]}), "
            f"source lon=({src_lons[0]}, {src_lons[-1]}), target lon=({target_lons[0]}, {target_lons[-1]})"
        )


def _extract_timestamp_from_name(file_path: str) -> Optional[datetime]:
    name = Path(file_path).name
    for pattern in TIMESTAMP_PATTERNS:
        match = pattern.search(name)
        if not match:
            continue
        stamp = match.group(1)
        for fmt in ("%Y%m%d-%H%M%S", "%Y%m%d_%H%M%S"):
            try:
                return datetime.strptime(stamp, fmt).replace(tzinfo=timezone.utc)
            except ValueError:
                continue
    return None


def _normalize_azshear_units(values: np.ndarray) -> Tuple[np.ndarray, Optional[str]]:
    arr = np.asarray(values, dtype=np.float32)
    if not np.isfinite(arr).any():
        return arr, None

    scale_note = None
    if float(np.nanmax(np.abs(arr))) > cfg.AZSHEAR_UNIT_SCALE_THRESHOLD:
        arr = arr / np.float32(cfg.AZSHEAR_UNIT_DIVISOR)
        scale_note = f"scaled_by_{int(cfg.AZSHEAR_UNIT_DIVISOR)}"

    return arr, scale_note


def _load_grid(file_path: str, normalize_azshear: bool = False) -> Dict[str, Any]:
    ds = load_grib_fast(file_path)
    var_name = "unknown" if "unknown" in ds.data_vars else list(ds.data_vars)[0]
    da = ds[var_name]
    values = np.asarray(da.values, dtype=np.float32)
    scale_note = None
    if normalize_azshear:
        values, scale_note = _normalize_azshear_units(values)

    return {
        "values": values,
        "latitudes": np.asarray(da.coords["latitude"].values, dtype=np.float32),
        "longitudes": np.asarray(da.coords["longitude"].values, dtype=np.float32),
        "scale_note": scale_note,
    }


def load_latest_inputs() -> Dict[str, Any]:
    low_files = fs.latest_files(fs.MRMS_AZSHEARLOW_DIR, 1)
    mid_files = fs.latest_files(fs.MRMS_AZSHEARMID_DIR, 1)
    ref_files = fs.latest_files(fs.MRMS_COMPOSITE_DIR, 1)

    if not low_files or not mid_files or not ref_files:
        raise FileNotFoundError("Required MRMS AzShear/CompRef inputs are unavailable")

    low_path = low_files[-1]
    mid_path = mid_files[-1]
    ref_path = ref_files[-1]

    with ThreadPoolExecutor(max_workers=3) as executor:
        low_future = executor.submit(_load_grid, low_path, True)
        mid_future = executor.submit(_load_grid, mid_path, True)
        ref_future = executor.submit(_load_grid, ref_path, False)
        low_grid = low_future.result()
        mid_grid = mid_future.result()
        ref_grid = ref_future.result()

    print(f"[Mesocyclone] Input diagnostics | {_grid_diagnostics('low', low_path, low_grid)}")
    print(f"[Mesocyclone] Input diagnostics | {_grid_diagnostics('mid', mid_path, mid_grid)}")
    print(f"[Mesocyclone] Input diagnostics | {_grid_diagnostics('reflectivity', ref_path, ref_grid)}")

    ref_shape = low_grid["values"].shape
    ref_lats = low_grid["latitudes"]
    ref_lons = low_grid["longitudes"]
    mid_grid = _harmonize_grid("mid", mid_grid, ref_shape, ref_lats, ref_lons)
    _validate_grid_alignment("reflectivity", ref_grid, ref_lats, ref_lons)

    timestamp = _extract_timestamp_from_name(ref_path)
    if timestamp is None:
        timestamp = datetime.now(timezone.utc)

    lat_spacing_deg = float(np.nanmean(np.abs(np.diff(ref_lats)))) if len(ref_lats) > 1 else cfg.AZSHEAR_GRID_SPACING_DEG
    lon_spacing_deg = float(np.nanmean(np.abs(np.diff(ref_lons)))) if len(ref_lons) > 1 else cfg.AZSHEAR_GRID_SPACING_DEG

    return {
        "timestamp": timestamp,
        "timestamp_iso": timestamp.isoformat(),
        "paths": {
            "low": low_path,
            "mid": mid_path,
            "reflectivity": ref_path,
        },
        "coordinates": {
            "latitudes": ref_lats,
            "longitudes": ref_lons,
            "reflectivity_latitudes": ref_grid["latitudes"],
            "reflectivity_longitudes": ref_grid["longitudes"],
        },
        "grids": {
            "low": low_grid["values"],
            "mid": mid_grid["values"],
            "reflectivity": ref_grid["values"],
        },
        "scale_notes": {
            "low": low_grid.get("scale_note"),
            "mid": mid_grid.get("scale_note"),
        },
        "grid_spacing_deg": {
            "lat": lat_spacing_deg,
            "lon": lon_spacing_deg,
            "expected": cfg.AZSHEAR_GRID_SPACING_DEG,
        },
    }
