import math
from typing import Dict, List, Optional

import numpy as np
from scipy.spatial import cKDTree

from . import config as cfg


def _distance_km(lat_a: float, lon_a: float, lat_b: float, lon_b: float) -> float:
    radius_km = 6371.0
    lat1 = math.radians(lat_a)
    lat2 = math.radians(lat_b)
    dlat = lat2 - lat1
    dlon = math.radians(lon_b - lon_a)
    hav = math.sin(dlat / 2.0) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2.0) ** 2
    return 2.0 * radius_km * math.asin(min(1.0, math.sqrt(max(hav, 0.0))))


def _axis_spacing_km(latitudes: np.ndarray, longitudes: np.ndarray) -> tuple[float, float]:
    lat_spacing_deg = float(np.nanmean(np.abs(np.diff(latitudes)))) if len(latitudes) > 1 else 0.0
    lon_spacing_deg = float(np.nanmean(np.abs(np.diff(longitudes)))) if len(longitudes) > 1 else 0.0
    ref_lat = float(np.nanmean(latitudes)) if len(latitudes) else 35.0
    lat_spacing_km = max(lat_spacing_deg * 111.32, 0.01)
    lon_spacing_km = max(lon_spacing_deg * 111.32 * math.cos(math.radians(ref_lat)), 0.01)
    return lat_spacing_km, lon_spacing_km


def _nearest_axis_indices(source_axis: np.ndarray, target_values: np.ndarray) -> np.ndarray:
    src = np.asarray(source_axis, dtype=float)
    tgt = np.asarray(target_values, dtype=float)
    descending = len(src) > 1 and src[0] > src[-1]
    if descending:
        src = src[::-1]

    positions = np.interp(tgt, src, np.arange(len(src), dtype=float))
    indices = np.rint(positions).astype(int)
    indices = np.clip(indices, 0, len(source_axis) - 1)

    if descending:
        indices = (len(source_axis) - 1) - indices

    return indices


def build_reflectivity_gate_context(
    reflectivity: np.ndarray,
    azshear_latitudes: np.ndarray,
    azshear_longitudes: np.ndarray,
    reflectivity_latitudes: np.ndarray,
    reflectivity_longitudes: np.ndarray,
) -> Dict[str, object]:
    core_mask = np.isfinite(reflectivity) & (reflectivity >= cfg.REFLECTIVITY_THRESHOLD_DBZ)
    lat_spacing_km, lon_spacing_km = _axis_spacing_km(reflectivity_latitudes, reflectivity_longitudes)
    core_tree = None
    if core_mask.size and np.any(core_mask):
        core_rows, core_cols = np.where(core_mask)
        core_points = np.column_stack(
            (
                core_rows.astype(np.float32) * np.float32(lat_spacing_km),
                core_cols.astype(np.float32) * np.float32(lon_spacing_km),
            )
        )
        core_tree = cKDTree(core_points)

    row_map = _nearest_axis_indices(reflectivity_latitudes, azshear_latitudes)
    col_map = _nearest_axis_indices(reflectivity_longitudes, azshear_longitudes)
    return {
        "core_tree": core_tree,
        "row_map": row_map,
        "col_map": col_map,
        "reflectivity_shape": reflectivity.shape,
        "lat_spacing_km": lat_spacing_km,
        "lon_spacing_km": lon_spacing_km,
    }


def apply_reflectivity_gate(
    detections: List[Dict[str, object]],
    reflectivity: np.ndarray,
    latitudes: np.ndarray,
    longitudes: np.ndarray,
    reflectivity_latitudes: np.ndarray,
    reflectivity_longitudes: np.ndarray,
    gate_context: Optional[Dict[str, object]] = None,
) -> List[Dict[str, object]]:
    if gate_context is None:
        gate_context = build_reflectivity_gate_context(
            reflectivity,
            latitudes,
            longitudes,
            reflectivity_latitudes,
            reflectivity_longitudes,
        )

    core_tree = gate_context.get("core_tree")
    row_map = np.asarray(gate_context["row_map"], dtype=int)
    col_map = np.asarray(gate_context["col_map"], dtype=int)
    reflectivity_width = int(gate_context["reflectivity_shape"][1])
    lat_spacing_km = float(gate_context["lat_spacing_km"])
    lon_spacing_km = float(gate_context["lon_spacing_km"])

    gated: List[Dict[str, object]] = []
    for detection in detections:
        rows = np.asarray(detection["pixel_rows"], dtype=int)
        cols = np.asarray(detection["pixel_cols"], dtype=int)
        mapped_rows = row_map[rows]
        mapped_cols = col_map[cols]
        mapped_linear = np.unique(mapped_rows.astype(np.int64) * reflectivity_width + mapped_cols.astype(np.int64))
        mapped_row_points = mapped_linear // reflectivity_width
        mapped_col_points = mapped_linear % reflectivity_width
        reflectivity_values = np.asarray(reflectivity[mapped_row_points, mapped_col_points], dtype=float)
        overlap_count = int(np.count_nonzero(np.isfinite(reflectivity_values) & (reflectivity_values >= cfg.REFLECTIVITY_THRESHOLD_DBZ)))
        reflectivity_max = float(np.nanmax(reflectivity_values)) if reflectivity_values.size else 0.0
        reflectivity_mean = float(np.nanmean(reflectivity_values)) if reflectivity_values.size else 0.0

        min_core_distance_km = None
        if core_tree is not None:
            centroid_row = int(row_map[int(np.clip(np.rint(float(detection["pixel_rows"].mean())), 0, len(row_map) - 1))])
            centroid_col = int(col_map[int(np.clip(np.rint(float(detection["pixel_cols"].mean())), 0, len(col_map) - 1))])
            min_core_distance_km = float(
                core_tree.query((centroid_row * lat_spacing_km, centroid_col * lon_spacing_km))[0]
            )

        passes_gate = overlap_count > 0
        if not passes_gate and min_core_distance_km is not None:
            passes_gate = min_core_distance_km <= cfg.REFLECTIVITY_CORE_DISTANCE_KM

        if not passes_gate:
            continue

        gated_detection = dict(detection)
        gated_detection.update(
            {
                "reflectivity_overlap_pixels": overlap_count,
                "reflectivity_max": round(reflectivity_max, 3),
                "reflectivity_mean": round(reflectivity_mean, 3),
                "distance_to_reflectivity_core_km": None if min_core_distance_km is None else round(min_core_distance_km, 3),
            }
        )
        gated.append(gated_detection)

    return gated
