import math
from typing import Dict, List, Tuple

import numpy as np
from scipy import ndimage

from . import config as cfg


def _grid_spacing_km(latitudes: np.ndarray, longitudes: np.ndarray) -> Tuple[float, float]:
    if len(latitudes) > 1:
        lat_spacing_deg = float(np.nanmean(np.abs(np.diff(latitudes))))
    else:
        lat_spacing_deg = cfg.AZSHEAR_GRID_SPACING_DEG
    if len(longitudes) > 1:
        lon_spacing_deg = float(np.nanmean(np.abs(np.diff(longitudes))))
    else:
        lon_spacing_deg = cfg.AZSHEAR_GRID_SPACING_DEG

    ref_lat = float(np.nanmean(latitudes)) if len(latitudes) else 35.0
    lat_spacing_km = max(lat_spacing_deg * 111.32, 0.01)
    lon_spacing_km = max(lon_spacing_deg * 111.32 * math.cos(math.radians(ref_lat)), 0.01)
    return lat_spacing_km, lon_spacing_km


def _native_min_area_km2(reference_lat: float) -> float:
    return cfg.MIN_OBJECT_PIXELS * cfg.native_pixel_area_km2(reference_lat)


def _component_perimeter_km(component_mask: np.ndarray, lat_spacing_km: float, lon_spacing_km: float) -> float:
    padded = np.pad(component_mask.astype(bool), 1, mode="constant", constant_values=False)
    center = padded[1:-1, 1:-1]
    north = padded[:-2, 1:-1]
    south = padded[2:, 1:-1]
    west = padded[1:-1, :-2]
    east = padded[1:-1, 2:]

    horizontal_edges = int(np.count_nonzero(center & ~north) + np.count_nonzero(center & ~south))
    vertical_edges = int(np.count_nonzero(center & ~west) + np.count_nonzero(center & ~east))
    return float(horizontal_edges * lon_spacing_km + vertical_edges * lat_spacing_km)


def _component_shape_metrics(pixel_rows: np.ndarray, pixel_cols: np.ndarray) -> Tuple[float, float]:
    if pixel_rows.size < 2:
        return 0.0, 1.0

    coords = np.vstack((pixel_cols.astype(float), pixel_rows.astype(float)))
    cov = np.cov(coords, bias=True)
    eigvals = np.linalg.eigvalsh(cov)
    eigvals = np.maximum(np.sort(eigvals)[::-1], 0.0)
    major = float(eigvals[0]) if eigvals.size else 0.0
    minor = float(eigvals[1]) if eigvals.size > 1 else 0.0
    if major <= 0.0:
        return 0.0, 1.0

    eccentricity = float(min(1.0, math.sqrt(max(0.0, 1.0 - (minor / major)))))
    return eccentricity, (major / max(minor, 1e-6))


def _local_maxima(
    values: np.ndarray,
    component_mask: np.ndarray,
    latitudes: np.ndarray,
    longitudes: np.ndarray,
) -> List[Dict[str, float]]:
    neighborhood = ndimage.maximum_filter(values, size=3, mode="nearest")
    masked = np.where(component_mask, values, -np.inf)
    if not np.isfinite(masked).any():
        return []

    peaks = component_mask & np.isfinite(masked) & (masked == neighborhood)
    rows, cols = np.where(peaks)
    maxima = []
    for row, col in zip(rows, cols):
        maxima.append(
            {
                "lat": float(latitudes[row]),
                "lon": float(longitudes[col]),
                "value": float(values[row, col]),
            }
        )

    maxima.sort(key=lambda item: item["value"], reverse=True)
    return maxima


def detect_layer_objects(values: np.ndarray, latitudes: np.ndarray, longitudes: np.ndarray, layer_name: str) -> List[Dict[str, object]]:
    binary = np.isfinite(values) & (values >= cfg.DETECTION_THRESHOLD)
    if not np.any(binary):
        return []

    labels, count = ndimage.label(binary)
    object_slices = ndimage.find_objects(labels)
    reference_lat = float(np.nanmean(latitudes)) if len(latitudes) else 35.0
    lat_spacing_km, lon_spacing_km = _grid_spacing_km(latitudes, longitudes)
    pixel_area_km2 = lat_spacing_km * lon_spacing_km
    min_area_km2 = _native_min_area_km2(reference_lat)
    detections: List[Dict[str, object]] = []

    for label_idx, obj_slice in enumerate(object_slices, start=1):
        if obj_slice is None:
            continue

        row_slice, col_slice = obj_slice
        label_window = labels[row_slice, col_slice]
        component_mask = label_window == label_idx
        pixel_count = int(np.count_nonzero(component_mask))
        if pixel_count == 0:
            continue

        local_rows, local_cols = np.where(component_mask)
        row_offset = row_slice.start or 0
        col_offset = col_slice.start or 0
        rows = local_rows + row_offset
        cols = local_cols + col_offset
        value_window = values[row_slice, col_slice]
        component_values = np.asarray(value_window[component_mask], dtype=float)
        peak_index = int(np.nanargmax(component_values))
        peak_row = int(rows[peak_index])
        peak_col = int(cols[peak_index])
        perimeter_km = _component_perimeter_km(component_mask, lat_spacing_km, lon_spacing_km)
        area_km2 = float(pixel_count * pixel_area_km2)
        if area_km2 < min_area_km2:
            continue
        compactness = 0.0
        if perimeter_km > 0.0:
            compactness = float(max(0.0, min(1.0, (4.0 * math.pi * area_km2) / (perimeter_km ** 2))))

        eccentricity, aspect_ratio = _component_shape_metrics(rows, cols)
        if aspect_ratio > cfg.MAX_COMPONENT_ASPECT_RATIO:
            continue

        maxima = _local_maxima(
            value_window,
            component_mask,
            latitudes[row_slice],
            longitudes[col_slice],
        )

        detections.append(
            {
                "layer": layer_name,
                "component_id": int(label_idx),
                "pixel_count": pixel_count,
                "area_km2": round(area_km2, 3),
                "native_grid_area_threshold_km2": round(min_area_km2, 3),
                "grid_spacing_deg": {
                    "lat": round(lat_spacing_km / 111.32, 6),
                    "lon": round(lon_spacing_km / max(111.32 * math.cos(math.radians(reference_lat)), 1e-6), 6),
                },
                "centroid_lat": float(np.nanmean(latitudes[rows])),
                "centroid_lon": float(np.nanmean(longitudes[cols])),
                "max_azshear": float(component_values[peak_index]),
                "mean_azshear": float(np.nanmean(component_values)),
                "peak_lat": float(latitudes[peak_row]),
                "peak_lon": float(longitudes[peak_col]),
                "eccentricity": round(eccentricity, 3),
                "compactness": round(compactness, 3),
                "aspect_ratio": round(float(aspect_ratio), 3),
                "maxima": maxima,
                "bbox": {
                    "min_row": int(row_offset + local_rows.min()),
                    "max_row": int(row_offset + local_rows.max()),
                    "min_col": int(col_offset + local_cols.min()),
                    "max_col": int(col_offset + local_cols.max()),
                },
                "pixel_rows": rows.astype(np.int32, copy=False),
                "pixel_cols": cols.astype(np.int32, copy=False),
            }
        )

    detections.sort(key=lambda item: (item["max_azshear"], item["area_km2"]), reverse=True)
    return detections
