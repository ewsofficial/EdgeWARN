import math

import numpy as np
from scipy import ndimage

from .constants import AZSHEAR_MAX_PAIR_SEPARATION_KM, AZSHEAR_MIN_GATE_COUNT
from .geometry import (
    build_component_geometry,
    distance_km,
    midpoint_lon,
    normalize_lon_delta,
    weighted_lon_mean,
)


def compute_component_metrics(component_mask, values, lat_grid, lon_grid, pixel_area_km2, lat_spacing_km, lon_spacing_km):
    rows, cols = np.where(component_mask)
    if rows.size == 0:
        return None

    comp_values = np.asarray(values[component_mask], dtype=float)
    if comp_values.size == 0:
        return None

    finite_mask = np.isfinite(comp_values)
    if not np.any(finite_mask):
        return None

    comp_values = comp_values[finite_mask]

    comp_lats = np.asarray(lat_grid[component_mask], dtype=float)[finite_mask]
    comp_lons = np.asarray(lon_grid[component_mask], dtype=float)[finite_mask]
    peak_index = int(np.nanargmax(comp_values))
    peak_value = float(comp_values[peak_index])
    peak_lat = float(comp_lats[peak_index])
    peak_lon = float(comp_lons[peak_index])

    centroid_lat = float(np.nanmean(comp_lats))
    centroid_lon = float(np.nanmean(comp_lons))

    weights = np.clip(comp_values, 0.0, None)
    weight_sum = float(np.nansum(weights))
    if weight_sum > 0.0:
        weighted_centroid_lat = float(np.average(comp_lats, weights=weights))
        weighted_centroid_lon = weighted_lon_mean(comp_lons, centroid_lon, weights)
    else:
        weighted_centroid_lat = centroid_lat
        weighted_centroid_lon = centroid_lon

    km_per_deg_lat = 111.32
    km_per_deg_lon = km_per_deg_lat * max(math.cos(math.radians(centroid_lat)), 1e-6)

    x = np.array([normalize_lon_delta(float(lon) - centroid_lon) * km_per_deg_lon for lon in comp_lons])
    y = (comp_lats - centroid_lat) * km_per_deg_lat

    if x.size >= 2:
        cov = np.cov(np.vstack((x, y)), bias=True)
        eigvals, eigvecs = np.linalg.eigh(cov)
        eigvals = np.maximum(eigvals, 0.0)
        order = np.argsort(eigvals)[::-1]
        eigvals = eigvals[order]
        eigvecs = eigvecs[:, order]
        major_axis_km = float(4.0 * math.sqrt(eigvals[0])) if eigvals.size else 0.0
        minor_axis_km = float(4.0 * math.sqrt(eigvals[1])) if eigvals.size > 1 else 0.0
        orientation_deg = float((math.degrees(math.atan2(eigvecs[1, 0], eigvecs[0, 0])) + 360.0) % 180.0)
    else:
        major_axis_km = 0.0
        minor_axis_km = 0.0
        orientation_deg = 0.0

    if major_axis_km <= 0.0:
        aspect_ratio = 1.0
        ellipticity = 0.0
    else:
        safe_minor = max(minor_axis_km, 1e-6)
        aspect_ratio = float(major_axis_km / safe_minor)
        ellipticity = float(min(1.0, math.sqrt(max(0.0, 1.0 - (safe_minor / major_axis_km) ** 2))))

    return {
        "pixel_count": int(comp_values.size),
        "area_km2": round(float(comp_values.size * pixel_area_km2), 3),
        "peak_value": round(peak_value, 3),
        "peak_lat": round(peak_lat, 5),
        "peak_lon": round(peak_lon, 5),
        "centroid_lat": round(centroid_lat, 5),
        "centroid_lon": round(centroid_lon, 5),
        "weighted_centroid_lat": round(weighted_centroid_lat, 5),
        "weighted_centroid_lon": round(weighted_centroid_lon, 5),
        "major_axis_km": round(max(major_axis_km, 0.0), 3),
        "minor_axis_km": round(max(minor_axis_km, 0.0), 3),
        "width_km": round(max(minor_axis_km, 0.0), 3),
        "aspect_ratio": round(aspect_ratio, 3),
        "ellipticity": round(ellipticity, 3),
        "orientation_deg": round(orientation_deg, 2),
        "p95_value": round(float(np.nanpercentile(comp_values, 95)), 3),
        "mean_value": round(float(np.nanmean(comp_values)), 3),
        "_component_mask": component_mask,
        "_lat_grid": lat_grid,
        "_lon_grid": lon_grid,
        "_lat_spacing_km": float(lat_spacing_km),
        "_lon_spacing_km": float(lon_spacing_km),
    }


def extract_azshear_candidates(masked_values, lat_grid, lon_grid, threshold, pixel_area_km2, lat_spacing_km, lon_spacing_km):
    binary = np.isfinite(masked_values) & (masked_values >= threshold)
    if not np.any(binary):
        return []

    labels, count = ndimage.label(binary)
    candidates = []
    for label_idx in range(1, count + 1):
        component_mask = labels == label_idx
        metrics = compute_component_metrics(
            component_mask,
            masked_values,
            lat_grid,
            lon_grid,
            pixel_area_km2,
            lat_spacing_km,
            lon_spacing_km,
        )
        if metrics is None:
            continue
        if metrics["pixel_count"] < AZSHEAR_MIN_GATE_COUNT:
            continue
        metrics["component_id"] = int(label_idx)
        candidates.append(metrics)

    candidates.sort(
        key=lambda item: (item["peak_value"], item["p95_value"], item["area_km2"]),
        reverse=True,
    )
    return candidates


def build_overlap_metrics(low_component, mid_component):
    if low_component is None or mid_component is None:
        return {
            "centroid_distance_km": None,
            "overlap_area_km2": None,
            "overlap_ratio": None,
            "low_overlap_fraction": None,
            "mid_overlap_fraction": None,
        }

    low_weighted_lat = float(low_component.get("weighted_centroid_lat", low_component["centroid_lat"]))
    low_weighted_lon = float(low_component.get("weighted_centroid_lon", low_component["centroid_lon"]))
    mid_weighted_lat = float(mid_component.get("weighted_centroid_lat", mid_component["centroid_lat"]))
    mid_weighted_lon = float(mid_component.get("weighted_centroid_lon", mid_component["centroid_lon"]))

    centroid_distance_km = distance_km(
        low_weighted_lat,
        low_weighted_lon,
        mid_weighted_lat,
        mid_weighted_lon,
    )

    ref_lat = (low_weighted_lat + mid_weighted_lat) / 2.0
    ref_lon = midpoint_lon(low_weighted_lon, mid_weighted_lon)
    low_geom = build_component_geometry(low_component, ref_lat, ref_lon)
    mid_geom = build_component_geometry(mid_component, ref_lat, ref_lon)

    if low_geom is None or mid_geom is None:
        return {
            "centroid_distance_km": round(centroid_distance_km, 3),
            "overlap_area_km2": None,
            "overlap_ratio": None,
            "low_overlap_fraction": None,
            "mid_overlap_fraction": None,
        }

    overlap_area_km2 = float(low_geom.intersection(mid_geom).area)
    low_area_km2 = max(float(low_component.get("area_km2", 0.0)), 1e-6)
    mid_area_km2 = max(float(mid_component.get("area_km2", 0.0)), 1e-6)
    union_area_km2 = max(low_area_km2 + mid_area_km2 - overlap_area_km2, 1e-6)

    return {
        "centroid_distance_km": round(centroid_distance_km, 3),
        "overlap_area_km2": round(max(overlap_area_km2, 0.0), 3),
        "overlap_ratio": round(max(overlap_area_km2, 0.0) / union_area_km2, 3),
        "low_overlap_fraction": round(max(overlap_area_km2, 0.0) / low_area_km2, 3),
        "mid_overlap_fraction": round(max(overlap_area_km2, 0.0) / mid_area_km2, 3),
    }


def public_component_metrics(component):
    if component is None:
        return None

    return {
        key: value
        for key, value in component.items()
        if not key.startswith("_")
    }


def build_alignment_metrics(low_component, mid_component):
    if low_component is None or mid_component is None:
        return {
            "paired": False,
            "vertical_centroid_sep_km": None,
            "vertical_peak_sep_km": None,
            "centroid_distance_km": None,
            "orientation_diff_deg": None,
            "width_ratio": None,
            "area_ratio": None,
            "overlap_area_km2": None,
            "overlap_ratio": None,
            "low_overlap_fraction": None,
            "mid_overlap_fraction": None,
            "is_vertically_aligned": False,
        }

    centroid_dlat = (mid_component["centroid_lat"] - low_component["centroid_lat"]) * 111.32
    centroid_dlon = normalize_lon_delta(mid_component["centroid_lon"] - low_component["centroid_lon"]) * 111.32 * max(
        math.cos(math.radians((mid_component["centroid_lat"] + low_component["centroid_lat"]) / 2.0)),
        1e-6,
    )
    peak_dlat = (mid_component["peak_lat"] - low_component["peak_lat"]) * 111.32
    peak_dlon = normalize_lon_delta(mid_component["peak_lon"] - low_component["peak_lon"]) * 111.32 * max(
        math.cos(math.radians((mid_component["peak_lat"] + low_component["peak_lat"]) / 2.0)),
        1e-6,
    )

    centroid_sep_km = math.sqrt(centroid_dlat**2 + centroid_dlon**2)
    peak_sep_km = math.sqrt(peak_dlat**2 + peak_dlon**2)
    orientation_diff = abs(mid_component["orientation_deg"] - low_component["orientation_deg"])
    if orientation_diff > 90.0:
        orientation_diff = 180.0 - orientation_diff

    width_ratio = min(low_component["width_km"], mid_component["width_km"]) / max(
        max(low_component["width_km"], mid_component["width_km"]),
        1e-6,
    )
    area_ratio = min(low_component["area_km2"], mid_component["area_km2"]) / max(
        max(low_component["area_km2"], mid_component["area_km2"]),
        1e-6,
    )
    overlap = build_overlap_metrics(low_component, mid_component)

    return {
        "paired": True,
        "vertical_centroid_sep_km": round(centroid_sep_km, 3),
        "vertical_peak_sep_km": round(peak_sep_km, 3),
        "centroid_distance_km": overlap["centroid_distance_km"],
        "orientation_diff_deg": round(orientation_diff, 2),
        "width_ratio": round(width_ratio, 3),
        "area_ratio": round(area_ratio, 3),
        "overlap_area_km2": overlap["overlap_area_km2"],
        "overlap_ratio": overlap["overlap_ratio"],
        "low_overlap_fraction": overlap["low_overlap_fraction"],
        "mid_overlap_fraction": overlap["mid_overlap_fraction"],
        "is_vertically_aligned": centroid_sep_km <= AZSHEAR_MAX_PAIR_SEPARATION_KM,
    }
