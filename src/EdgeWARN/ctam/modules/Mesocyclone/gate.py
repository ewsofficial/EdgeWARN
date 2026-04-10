import math
from typing import Dict, List

import numpy as np

from . import config as cfg


def _distance_km(lat_a: float, lon_a: float, lat_b: float, lon_b: float) -> float:
    radius_km = 6371.0
    lat1 = math.radians(lat_a)
    lat2 = math.radians(lat_b)
    dlat = lat2 - lat1
    dlon = math.radians(lon_b - lon_a)
    hav = math.sin(dlat / 2.0) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2.0) ** 2
    return 2.0 * radius_km * math.asin(min(1.0, math.sqrt(max(hav, 0.0))))


def apply_reflectivity_gate(
    detections: List[Dict[str, object]],
    reflectivity: np.ndarray,
    latitudes: np.ndarray,
    longitudes: np.ndarray,
) -> List[Dict[str, object]]:
    core_mask = np.isfinite(reflectivity) & (reflectivity >= cfg.REFLECTIVITY_THRESHOLD_DBZ)
    core_rows, core_cols = np.where(core_mask)

    gated: List[Dict[str, object]] = []
    for detection in detections:
        component_mask = detection["component_mask"]
        overlap_count = int(np.count_nonzero(component_mask & core_mask))
        reflectivity_values = np.asarray(reflectivity[component_mask], dtype=float)
        reflectivity_max = float(np.nanmax(reflectivity_values)) if reflectivity_values.size else 0.0
        reflectivity_mean = float(np.nanmean(reflectivity_values)) if reflectivity_values.size else 0.0

        min_core_distance_km = None
        if core_rows.size > 0:
            distances = [
                _distance_km(
                    float(detection["centroid_lat"]),
                    float(detection["centroid_lon"]),
                    float(latitudes[row]),
                    float(longitudes[col]),
                )
                for row, col in zip(core_rows, core_cols)
            ]
            min_core_distance_km = float(min(distances)) if distances else None

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
