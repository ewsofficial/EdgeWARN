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


def _orientation_diff_deg(a_deg, b_deg):
    diff = abs(float(a_deg) - float(b_deg)) % 180.0
    return 180.0 - diff if diff > 90.0 else diff


def _pca_terms(x_vals, y_vals):
    x = np.asarray(x_vals, dtype=float)
    y = np.asarray(y_vals, dtype=float)
    finite = np.isfinite(x) & np.isfinite(y)
    if np.count_nonzero(finite) < 2:
        return 0.0, 0.0, None

    x = x[finite]
    y = y[finite]
    x = x - np.nanmean(x)
    y = y - np.nanmean(y)

    cov = np.cov(np.vstack((x, y)), bias=True)
    if cov.shape != (2, 2) or not np.all(np.isfinite(cov)):
        return 0.0, 0.0, None

    eigvals, eigvecs = np.linalg.eigh(cov)
    eigvals = np.maximum(eigvals, 0.0)
    order = np.argsort(eigvals)[::-1]
    eigvals = eigvals[order]
    eigvecs = eigvecs[:, order]

    major_var = float(eigvals[0]) if eigvals.size > 0 else 0.0
    minor_var = float(eigvals[1]) if eigvals.size > 1 else 0.0
    if major_var <= 0.0:
        return 0.0, 0.0, None

    orientation_deg = float((math.degrees(math.atan2(eigvecs[1, 0], eigvecs[0, 0])) + 360.0) % 180.0)
    return major_var, minor_var, orientation_deg


def _linearity_ratio_and_score(major_var, minor_var):
    if major_var <= 0.0:
        return 0.0, 0.0

    if minor_var <= 1e-8:
        ratio = 999.0
    else:
        ratio = float(math.sqrt(max(major_var, 0.0) / max(minor_var, 1e-8)))

    score = 0.0 if ratio <= 1.0 else float(1.0 - (1.0 / ratio))
    score = max(0.0, min(1.0, score))
    return ratio, score


def _component_to_local_xy_km(component, lat_values, lon_values):
    lats = np.asarray(lat_values, dtype=float)
    lons = np.asarray(lon_values, dtype=float)
    if lats.size == 0 or lons.size == 0:
        return np.array([], dtype=float), np.array([], dtype=float)

    ref_lat = float(np.nanmean(lats))
    ref_lon = float(np.nanmean(lons))
    km_per_deg_lon = 111.32 * max(math.cos(math.radians(ref_lat)), 1e-6)
    x = np.array([normalize_lon_delta(float(lon) - ref_lon) * km_per_deg_lon for lon in lons], dtype=float)
    y = np.asarray((lats - ref_lat) * 111.32, dtype=float)
    return x, y


def _component_pixel_arrays(component):
    pixel_lats = component.get("_pixel_lats")
    pixel_lons = component.get("_pixel_lons")
    if pixel_lats is not None and pixel_lons is not None:
        comp_lats = np.asarray(pixel_lats, dtype=float)
        comp_lons = np.asarray(pixel_lons, dtype=float)
        finite = np.isfinite(comp_lats) & np.isfinite(comp_lons)
        return comp_lats[finite], comp_lons[finite]

    mask = component.get("_component_mask")
    lat_grid = component.get("_lat_grid")
    lon_grid = component.get("_lon_grid")
    if mask is None or lat_grid is None or lon_grid is None:
        return np.array([], dtype=float), np.array([], dtype=float)

    comp_lats = np.asarray(lat_grid[mask], dtype=float)
    comp_lons = np.asarray(lon_grid[mask], dtype=float)
    finite = np.isfinite(comp_lats) & np.isfinite(comp_lons)
    return comp_lats[finite], comp_lons[finite]


def _component_perimeter_km(component_mask, lat_spacing_km, lon_spacing_km):
    if component_mask.size == 0 or not np.any(component_mask):
        return 0.0

    padded = np.pad(component_mask.astype(bool, copy=False), 1, mode="constant", constant_values=False)
    center = padded[1:-1, 1:-1]
    north = padded[:-2, 1:-1]
    south = padded[2:, 1:-1]
    west = padded[1:-1, :-2]
    east = padded[1:-1, 2:]

    horizontal_edges = int(np.count_nonzero(center & ~north) + np.count_nonzero(center & ~south))
    vertical_edges = int(np.count_nonzero(center & ~west) + np.count_nonzero(center & ~east))
    return float((horizontal_edges * lon_spacing_km) + (vertical_edges * lat_spacing_km))


def _component_pixel_signature(component):
    cached_signature = component.get("_pixel_signature")
    if cached_signature is not None:
        return cached_signature

    comp_lats, comp_lons = _component_pixel_arrays(component)
    if comp_lats.size == 0 or comp_lons.size == 0:
        signature = frozenset()
    else:
        signature = frozenset(zip(np.round(comp_lats, 4).tolist(), np.round(comp_lons, 4).tolist()))

    component["_pixel_signature"] = signature
    return signature


def _largest_component_compactness(component):
    if component is None:
        return 0.0

    perimeter_km = float(component.get("_perimeter_km", 0.0) or 0.0)
    area_km2 = float(max(component.get("area_km2", 0.0), 0.0))
    if perimeter_km > 0.0 and area_km2 > 0.0:
        compactness = (4.0 * math.pi * area_km2) / (perimeter_km**2)
        return float(max(0.0, min(1.0, compactness)))

    ref_lat = float(component.get("centroid_lat", 0.0))
    ref_lon = float(component.get("centroid_lon", 0.0))
    geom = build_component_geometry(component, ref_lat, ref_lon)
    if geom is None:
        return 0.0

    area_km2 = float(max(geom.area, 0.0))
    perimeter_km = float(max(geom.length, 0.0))
    if perimeter_km <= 0.0:
        return 0.0

    compactness = (4.0 * math.pi * area_km2) / (perimeter_km**2)
    return float(max(0.0, min(1.0, compactness)))


def _centroid_line_fit_score(candidates):
    if len(candidates) < 2:
        return 0.0

    centroid_lats = np.array([float(item.get("centroid_lat", np.nan)) for item in candidates], dtype=float)
    centroid_lons = np.array([float(item.get("centroid_lon", np.nan)) for item in candidates], dtype=float)
    finite = np.isfinite(centroid_lats) & np.isfinite(centroid_lons)
    if np.count_nonzero(finite) < 2:
        return 0.0

    x, y = _component_to_local_xy_km(None, centroid_lats[finite], centroid_lons[finite])
    major_var, minor_var, _ = _pca_terms(x, y)
    denom = major_var + minor_var
    if denom <= 0.0:
        return 0.0

    return float(max(0.0, min(1.0, major_var / denom)))


def _level_pixel_orientation_and_linearity(candidates):
    if not candidates:
        return None, 0.0, 0.0

    all_lats = []
    all_lons = []
    for component in candidates:
        comp_lats, comp_lons = _component_pixel_arrays(component)
        if comp_lats.size == 0 or comp_lons.size == 0:
            continue
        all_lats.append(comp_lats)
        all_lons.append(comp_lons)

    if not all_lats:
        return None, 0.0, 0.0

    merged_lats = np.concatenate(all_lats)
    merged_lons = np.concatenate(all_lons)
    x, y = _component_to_local_xy_km(None, merged_lats, merged_lons)
    major_var, minor_var, orientation_deg = _pca_terms(x, y)
    ratio, score = _linearity_ratio_and_score(major_var, minor_var)
    return orientation_deg, ratio, score


def _dominant_component(candidates):
    if not candidates:
        return None

    return max(
        candidates,
        key=lambda item: (float(item.get("area_km2", 0.0)), float(item.get("peak_value", 0.0))),
    )


def _build_overlap_metrics(low_component, mid_component):
    centroid_distance = distance_km(
        float(low_component["centroid_lat"]),
        float(low_component["centroid_lon"]),
        float(mid_component["centroid_lat"]),
        float(mid_component["centroid_lon"]),
    )

    low_signature = _component_pixel_signature(low_component)
    mid_signature = _component_pixel_signature(mid_component)
    if low_signature or mid_signature:
        overlap_pixels = len(low_signature & mid_signature)
        low_area = max(float(low_component.get("area_km2", 0.0)), 1e-6)
        mid_area = max(float(mid_component.get("area_km2", 0.0)), 1e-6)
        low_pixel_count = max(int(low_component.get("pixel_count", 0)), 1)
        mid_pixel_count = max(int(mid_component.get("pixel_count", 0)), 1)
        pixel_area_km2 = min(low_area / low_pixel_count, mid_area / mid_pixel_count)
        overlap_area = float(overlap_pixels * pixel_area_km2)
        union_area = max(low_area + mid_area - overlap_area, 1e-6)
        return centroid_distance, overlap_area, overlap_area / union_area

    ref_lat = (float(low_component["centroid_lat"]) + float(mid_component["centroid_lat"])) / 2.0
    ref_lon = midpoint_lon(float(low_component["centroid_lon"]), float(mid_component["centroid_lon"]))
    low_geom = build_component_geometry(low_component, ref_lat, ref_lon)
    mid_geom = build_component_geometry(mid_component, ref_lat, ref_lon)

    if low_geom is None or mid_geom is None:
        return centroid_distance, 0.0, 0.0

    overlap_area = float(max(low_geom.intersection(mid_geom).area, 0.0))
    low_area = max(float(low_component.get("area_km2", 0.0)), 1e-6)
    mid_area = max(float(mid_component.get("area_km2", 0.0)), 1e-6)
    union_area = max(low_area + mid_area - overlap_area, 1e-6)
    return centroid_distance, overlap_area, overlap_area / union_area


def find_best_cross_layer_pair(low_candidates, mid_candidates):
    if not low_candidates or not mid_candidates:
        return _dominant_component(low_candidates), _dominant_component(mid_candidates), 0

    valid_pairs = []
    for low_component in low_candidates:
        for mid_component in mid_candidates:
            centroid_distance, overlap_area, overlap_ratio = _build_overlap_metrics(low_component, mid_component)
            if centroid_distance > AZSHEAR_MAX_PAIR_SEPARATION_KM:
                continue

            combined_area = float(low_component.get("area_km2", 0.0)) + float(mid_component.get("area_km2", 0.0))
            combined_peak = float(low_component.get("peak_value", 0.0)) + float(mid_component.get("peak_value", 0.0))
            valid_pairs.append(
                (
                    overlap_ratio,
                    -centroid_distance,
                    combined_area,
                    combined_peak,
                    low_component,
                    mid_component,
                )
            )

    if not valid_pairs:
        return _dominant_component(low_candidates), _dominant_component(mid_candidates), 0

    _, _, _, _, best_low, best_mid = max(valid_pairs, key=lambda item: item[:4])
    return best_low, best_mid, len(valid_pairs)


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

    comp_values = np.ascontiguousarray(comp_values[finite_mask])
    comp_lats = np.ascontiguousarray(np.asarray(lat_grid[component_mask], dtype=float)[finite_mask])
    comp_lons = np.ascontiguousarray(np.asarray(lon_grid[component_mask], dtype=float)[finite_mask])

    peak_index = int(np.nanargmax(comp_values))
    peak_value = float(comp_values[peak_index])
    peak_lat = float(comp_lats[peak_index])
    peak_lon = float(comp_lons[peak_index])

    weights = np.clip(comp_values, 0.0, None)
    weight_sum = float(np.nansum(weights))
    ref_lon = float(np.nanmean(comp_lons))
    if weight_sum > 0.0:
        centroid_lat = float(np.average(comp_lats, weights=weights))
        centroid_lon = weighted_lon_mean(comp_lons, ref_lon, weights)
    else:
        centroid_lat = float(np.nanmean(comp_lats))
        centroid_lon = ref_lon

    x, y = _component_to_local_xy_km(None, comp_lats, comp_lons)
    major_var, minor_var, orientation_deg = _pca_terms(x, y)
    major_axis_km = float(4.0 * math.sqrt(max(major_var, 0.0))) if major_var > 0.0 else 0.0
    minor_axis_km = float(4.0 * math.sqrt(max(minor_var, 0.0))) if minor_var > 0.0 else 0.0

    if major_axis_km <= 0.0:
        aspect_ratio = 1.0
        ellipticity = 0.0
    else:
        safe_minor = max(minor_axis_km, 1e-6)
        aspect_ratio = float(major_axis_km / safe_minor)
        ellipticity = float(min(1.0, math.sqrt(max(0.0, 1.0 - (safe_minor / major_axis_km) ** 2))))

    perimeter_km = _component_perimeter_km(component_mask, lat_spacing_km, lon_spacing_km)

    return {
        "pixel_count": int(comp_values.size),
        "area_km2": float(comp_values.size * pixel_area_km2),
        "peak_value": peak_value,
        "peak_lat": peak_lat,
        "peak_lon": peak_lon,
        "centroid_lat": centroid_lat,
        "centroid_lon": centroid_lon,
        "major_axis_km": max(major_axis_km, 0.0),
        "minor_axis_km": max(minor_axis_km, 0.0),
        "width_km": max(minor_axis_km, 0.0),
        "aspect_ratio": aspect_ratio,
        "ellipticity": ellipticity,
        "orientation_deg": float(orientation_deg if orientation_deg is not None else 0.0),
        "p95_value": float(np.nanpercentile(comp_values, 95)),
        "mean_value": float(np.nanmean(comp_values)),
        "_pixel_lats": comp_lats,
        "_pixel_lons": comp_lons,
        "_perimeter_km": perimeter_km,
        "_pixel_bbox": (
            float(np.nanmin(comp_lons)),
            float(np.nanmin(comp_lats)),
            float(np.nanmax(comp_lons)),
            float(np.nanmax(comp_lats)),
        ),
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
        key=lambda item: (float(item.get("area_km2", 0.0)), float(item.get("peak_value", 0.0))),
        reverse=True,
    )
    return candidates


def summarize_level_metrics(candidates, buffered_area_km2, reflectivity_axis_deg):
    total_area = float(sum(float(item.get("area_km2", 0.0)) for item in candidates))
    component_count = len(candidates)
    dominant = _dominant_component(candidates)

    largest_area = float(dominant.get("area_km2", 0.0)) if dominant else 0.0
    secondary_area = float(candidates[1].get("area_km2", 0.0)) if component_count > 1 else 0.0
    dominance_ratio = (largest_area / total_area) if total_area > 0.0 else 0.0
    secondary_core_ratio = (secondary_area / largest_area) if largest_area > 0.0 else 0.0

    compactness = _largest_component_compactness(dominant)

    orientation_deg, linearity_ratio, linearity_score = _level_pixel_orientation_and_linearity(candidates)
    centroid_line_fit_score = _centroid_line_fit_score(candidates)

    if reflectivity_axis_deg is None or orientation_deg is None:
        reflectivity_alignment = 0.0
    else:
        diff = _orientation_diff_deg(orientation_deg, reflectivity_axis_deg)
        reflectivity_alignment = max(0.0, 1.0 - (diff / 90.0))

    coverage_fraction = (total_area / buffered_area_km2) if buffered_area_km2 > 0.0 else 0.0
    fragmentation_index = ((component_count - 1) / (component_count + 1)) if component_count > 0 else 0.0

    return {
        "core_structure": {
            "component_count": component_count,
            "largest_component_area": round(largest_area, 3),
            "largest_component_compactness": round(compactness, 3),
            "largest_component_peak_azshear": round(float(dominant.get("peak_value", 0.0)) if dominant else 0.0, 3),
            "largest_component_mean_azshear": round(float(dominant.get("mean_value", 0.0)) if dominant else 0.0, 3),
        },
        "dominance": {
            "dominance": round(dominance_ratio, 3),
            "dominance_ratio": round(dominance_ratio, 3),
            "secondary_core_ratio": round(secondary_core_ratio, 3),
        },
        "linearity": {
            "linearity": round(linearity_score, 3),
            "centroid_line_fit_score": round(centroid_line_fit_score, 3),
            "linearity_ratio": round(linearity_ratio, 3),
            "alignment_with_reflectivity_axis": round(reflectivity_alignment, 3),
        },
        "distribution": {
            "total_azshear_area": round(total_area, 3),
            "coverage_fraction": round(max(coverage_fraction, 0.0), 3),
            "fragmentation_index": round(max(fragmentation_index, 0.0), 3),
        },
    }, dominant


def summarize_cross_layer_metrics(
    low_component,
    mid_component,
    low_level_summary,
    mid_level_summary,
    simultaneous_persistence,
    mesocyclone_pair_count=0,
):
    if low_component is None or mid_component is None:
        centroid_distance = None
        overlap_area = 0.0
        overlap_ratio = 0.0
        centroid_alignment = 0.0
    else:
        centroid_distance, overlap_area, overlap_ratio = _build_overlap_metrics(low_component, mid_component)
        centroid_alignment = max(0.0, 1.0 - (centroid_distance / AZSHEAR_MAX_PAIR_SEPARATION_KM))

    low_dom = float(low_level_summary.get("dominance", {}).get("dominance_ratio", 0.0))
    mid_dom = float(mid_level_summary.get("dominance", {}).get("dominance_ratio", 0.0))
    if low_component is None or mid_component is None:
        low_peak = float(low_level_summary.get("core_structure", {}).get("largest_component_peak_azshear", 0.0))
        mid_peak = float(mid_level_summary.get("core_structure", {}).get("largest_component_peak_azshear", 0.0))
    else:
        low_peak = float(low_component.get("peak_value", 0.0))
        mid_peak = float(mid_component.get("peak_value", 0.0))

    dominance_ratio_ratio = None if mid_dom <= 0.0 else (low_dom / mid_dom)
    peak_ratio = None if mid_peak <= 0.0 else (low_peak / mid_peak)

    return {
        "dominant_component_overlap_area": round(max(overlap_area, 0.0), 3),
        "dominant_component_overlap_ratio": round(max(overlap_ratio, 0.0), 3),
        "dominant_component_centroid_distance_km": None if centroid_distance is None else round(centroid_distance, 3),
        "dominant_component_centroid_alignment": round(max(centroid_alignment, 0.0), 3),
        "ll_ml_dominance_ratio_ratio": None if dominance_ratio_ratio is None else round(dominance_ratio_ratio, 3),
        "ll_ml_peak_ratio": None if peak_ratio is None else round(peak_ratio, 3),
        "simultaneous_persistence": round(max(float(simultaneous_persistence), 0.0), 3),
        "mesocyclone_pair_count": max(int(mesocyclone_pair_count), 0),
    }
