import re
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
    finite = np.asarray(values[np.isfinite(values)], dtype=float)
    if finite.size == 0:
        return values.astype(float), None

    scale_note = None
    if float(np.nanmax(np.abs(finite))) > cfg.AZSHEAR_UNIT_SCALE_THRESHOLD:
        values = values / cfg.AZSHEAR_UNIT_DIVISOR
        scale_note = f"scaled_by_{int(cfg.AZSHEAR_UNIT_DIVISOR)}"

    return values.astype(float), scale_note


def _load_grid(file_path: str, normalize_azshear: bool = False) -> Dict[str, Any]:
    ds = load_grib_fast(file_path)
    var_name = "unknown" if "unknown" in ds.data_vars else list(ds.data_vars)[0]
    da = ds[var_name]
    values = np.asarray(da.values, dtype=float)
    scale_note = None
    if normalize_azshear:
        values, scale_note = _normalize_azshear_units(values)

    return {
        "values": values,
        "latitudes": np.asarray(da.coords["latitude"].values, dtype=float),
        "longitudes": np.asarray(da.coords["longitude"].values, dtype=float),
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

    low_grid = _load_grid(low_path, normalize_azshear=True)
    mid_grid = _load_grid(mid_path, normalize_azshear=True)
    ref_grid = _load_grid(ref_path, normalize_azshear=False)

    ref_shape = low_grid["values"].shape
    ref_lats = low_grid["latitudes"]
    ref_lons = low_grid["longitudes"]
    for grid_name, grid in (("mid", mid_grid), ("reflectivity", ref_grid)):
        if grid["values"].shape != ref_shape:
            raise ValueError(f"{grid_name} grid shape mismatch: expected {ref_shape}, got {grid['values'].shape}")
        if grid["latitudes"].shape != ref_lats.shape or grid["longitudes"].shape != ref_lons.shape:
            raise ValueError(f"{grid_name} coordinates do not align with low-level AzShear grid")

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
