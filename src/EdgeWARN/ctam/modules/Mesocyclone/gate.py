import math
from typing import Dict, List

import numpy as np
from scipy import ndimage

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


def apply_reflectivity_gate(
    detections: List[Dict[str, object]],
    reflectivity: np.ndarray,
    latitudes: np.ndarray,
    longitudes: np.ndarray,
    reflectivity_latitudes: np.ndarray,
    reflectivity_longitudes: np.ndarray,
) -> List[Dict[str, object]]:
    core_mask = np.isfinite(reflectivity) & (reflectivity >= cfg.REFLECTIVITY_THRESHOLD_DBZ)
    lat_spacing_km, lon_spacing_km = _axis_spacing_km(reflectivity_latitudes, reflectivity_longitudes)
    distance_to_core_km = None
    if core_mask.size and np.any(core_mask):
        distance_to_core_km = ndimage.distance_transform_edt(~core_mask, sampling=(lat_spacing_km, lon_spacing_km))

    gated: List[Dict[str, object]] = []
    for detection in detections:
        rows = np.asarray(detection["pixel_rows"], dtype=int)
        cols = np.asarray(detection["pixel_cols"], dtype=int)
        mapped_rows = _nearest_axis_indices(reflectivity_latitudes, latitudes[rows])
        mapped_cols = _nearest_axis_indices(reflectivity_longitudes, longitudes[cols])
        mapped_points = np.unique(np.column_stack((mapped_rows, mapped_cols)), axis=0)
        reflectivity_values = np.asarray(reflectivity[mapped_points[:, 0], mapped_points[:, 1]], dtype=float)
        overlap_count = int(np.count_nonzero(np.isfinite(reflectivity_values) & (reflectivity_values >= cfg.REFLECTIVITY_THRESHOLD_DBZ)))
        reflectivity_max = float(np.nanmax(reflectivity_values)) if reflectivity_values.size else 0.0
        reflectivity_mean = float(np.nanmean(reflectivity_values)) if reflectivity_values.size else 0.0

        min_core_distance_km = None
        if distance_to_core_km is not None:
            centroid_row = int(_nearest_axis_indices(reflectivity_latitudes, np.array([float(detection["centroid_lat"])]))[0])
            centroid_col = int(_nearest_axis_indices(reflectivity_longitudes, np.array([float(detection["centroid_lon"])]))[0])
            min_core_distance_km = float(distance_to_core_km[centroid_row, centroid_col])

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
