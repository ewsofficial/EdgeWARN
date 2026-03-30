import gc
import math
from copy import deepcopy

import numpy as np
import shapely.vectorized as sv
import xarray as xr
from scipy import ndimage
from shapely.geometry import box
from shapely.ops import transform, unary_union

from .utils import StormIntegrationUtils

AZSHEAR_BUFFER_KM = 5.0
AZSHEAR_LOW_THRESHOLD = 8.0
AZSHEAR_MID_THRESHOLD = 6.0
AZSHEAR_MAX_PAIR_SEPARATION_KM = 12.0


def _normalize_lon_delta(delta):
    if delta > 180.0:
        return delta - 360.0
    if delta < -180.0:
        return delta + 360.0
    return delta


def _buffer_polygon_km(poly, buffer_km):
    if poly is None or buffer_km <= 0:
        return poly

    centroid = poly.centroid
    ref_lat = centroid.y
    meters_per_deg_lat = 111_320.0
    meters_per_deg_lon = meters_per_deg_lat * max(math.cos(math.radians(ref_lat)), 1e-6)
    buffer_m = buffer_km * 1000.0

    def to_local(x, y, z=None):
        dx = _normalize_lon_delta(x - centroid.x) * meters_per_deg_lon
        dy = (y - centroid.y) * meters_per_deg_lat
        return (dx, dy)

    def to_geo(x, y, z=None):
        lon = centroid.x + (x / meters_per_deg_lon)
        lat = centroid.y + (y / meters_per_deg_lat)
        return (lon, lat)

    local_poly = transform(to_local, poly)
    buffered = local_poly.buffer(buffer_m)
    return transform(to_geo, buffered)


def _distance_km(lat_a, lon_a, lat_b, lon_b):
    ref_lat = (lat_a + lat_b) / 2.0
    dlat = (lat_b - lat_a) * 111.32
    dlon = _normalize_lon_delta(lon_b - lon_a) * 111.32 * max(math.cos(math.radians(ref_lat)), 1e-6)
    return math.sqrt((dlat ** 2) + (dlon ** 2))


def _midpoint_lon(lon_a, lon_b):
    return lon_a + (_normalize_lon_delta(lon_b - lon_a) / 2.0)


def _weighted_lon_mean(lons, ref_lon, weights):
    lon_offsets = np.array([_normalize_lon_delta(float(lon) - ref_lon) for lon in lons], dtype=float)
    return ref_lon + float(np.average(lon_offsets, weights=weights))


def _grid_spacing_km(lat_vals, lon_vals):
    lat_array = np.asarray(lat_vals)
    lon_array = np.asarray(lon_vals)

    if lat_array.ndim == 1 and lat_array.size > 1:
        dlat = float(np.nanmedian(np.abs(np.diff(lat_array))))
        ref_lat = float(np.nanmean(lat_array))
    elif lat_array.ndim == 2 and lat_array.shape[0] > 1:
        dlat = float(np.nanmedian(np.abs(np.diff(lat_array[:, 0]))))
        ref_lat = float(np.nanmean(lat_array))
    else:
        dlat = 0.01
        ref_lat = 35.0

    if lon_array.ndim == 1 and lon_array.size > 1:
        dlon = float(np.nanmedian(np.abs(np.diff(lon_array))))
    elif lon_array.ndim == 2 and lon_array.shape[1] > 1:
        dlon = float(np.nanmedian(np.abs(np.diff(lon_array[0, :]))))
    else:
        dlon = 0.01

    km_per_deg_lat = 111.32
    km_per_deg_lon = km_per_deg_lat * max(math.cos(math.radians(ref_lat)), 1e-6)
    return dlat * km_per_deg_lat, dlon * km_per_deg_lon


def _compute_component_metrics(component_mask, values, lat_grid, lon_grid, pixel_area_km2, lat_spacing_km, lon_spacing_km):
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
        weighted_centroid_lon = _weighted_lon_mean(comp_lons, centroid_lon, weights)
    else:
        weighted_centroid_lat = centroid_lat
        weighted_centroid_lon = centroid_lon

    km_per_deg_lat = 111.32
    km_per_deg_lon = km_per_deg_lat * max(math.cos(math.radians(centroid_lat)), 1e-6)

    x = np.array([_normalize_lon_delta(float(lon) - centroid_lon) * km_per_deg_lon for lon in comp_lons])
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


def _extract_azshear_candidates(masked_values, lat_grid, lon_grid, threshold, pixel_area_km2, lat_spacing_km, lon_spacing_km):
    binary = np.isfinite(masked_values) & (masked_values >= threshold)
    if not np.any(binary):
        return []

    labels, count = ndimage.label(binary)
    candidates = []
    for label_idx in range(1, count + 1):
        component_mask = labels == label_idx
        metrics = _compute_component_metrics(
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
        metrics["component_id"] = int(label_idx)
        candidates.append(metrics)

    candidates.sort(
        key=lambda item: (item["peak_value"], item["p95_value"], item["area_km2"]),
        reverse=True,
    )
    return candidates


def _build_component_geometry(component, ref_lat, ref_lon):
    if component is None:
        return None

    component_mask = component.get("_component_mask")
    lat_grid = component.get("_lat_grid")
    lon_grid = component.get("_lon_grid")
    lat_spacing_km = component.get("_lat_spacing_km")
    lon_spacing_km = component.get("_lon_spacing_km")

    if component_mask is None or lat_grid is None or lon_grid is None:
        return None

    comp_lats = np.asarray(lat_grid[component_mask], dtype=float)
    comp_lons = np.asarray(lon_grid[component_mask], dtype=float)
    if comp_lats.size == 0 or comp_lons.size == 0:
        return None

    lat_half_km = max(float(lat_spacing_km or 0.0) / 2.0, 1e-3)
    lon_half_km = max(float(lon_spacing_km or 0.0) / 2.0, 1e-3)
    km_per_deg_lon = 111.32 * max(math.cos(math.radians(ref_lat)), 1e-6)

    pixel_boxes = []
    for lat, lon in zip(comp_lats, comp_lons):
        x = _normalize_lon_delta(float(lon) - ref_lon) * km_per_deg_lon
        y = (float(lat) - ref_lat) * 111.32
        pixel_boxes.append(box(x - lon_half_km, y - lat_half_km, x + lon_half_km, y + lat_half_km))

    return unary_union(pixel_boxes) if pixel_boxes else None


def _build_overlap_metrics(low_component, mid_component):
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

    centroid_distance_km = _distance_km(
        low_weighted_lat,
        low_weighted_lon,
        mid_weighted_lat,
        mid_weighted_lon,
    )

    ref_lat = (low_weighted_lat + mid_weighted_lat) / 2.0
    ref_lon = _midpoint_lon(low_weighted_lon, mid_weighted_lon)
    low_geom = _build_component_geometry(low_component, ref_lat, ref_lon)
    mid_geom = _build_component_geometry(mid_component, ref_lat, ref_lon)

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


def _public_component_metrics(component):
    if component is None:
        return None

    return {
        key: value
        for key, value in component.items()
        if not key.startswith("_")
    }


def _pair_azshear_components(low_candidates, mid_candidates):
    if not low_candidates or not mid_candidates:
        return None

    best_pair = None
    best_score = None
    for low in low_candidates[:5]:
        for mid in mid_candidates[:5]:
            dlat = (mid["centroid_lat"] - low["centroid_lat"]) * 111.32
            dlon = _normalize_lon_delta(mid["centroid_lon"] - low["centroid_lon"]) * 111.32 * max(
                math.cos(math.radians((mid["centroid_lat"] + low["centroid_lat"]) / 2.0)),
                1e-6,
            )
            centroid_sep_km = math.sqrt(dlat ** 2 + dlon ** 2)
            if centroid_sep_km > AZSHEAR_MAX_PAIR_SEPARATION_KM:
                continue
            score = low["peak_value"] + mid["peak_value"] - 0.2 * centroid_sep_km + 0.05 * min(low["area_km2"], mid["area_km2"])
            if best_score is None or score > best_score:
                best_score = score
                best_pair = (low, mid)

    return best_pair


def _build_alignment_metrics(low_component, mid_component):
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
    centroid_dlon = _normalize_lon_delta(mid_component["centroid_lon"] - low_component["centroid_lon"]) * 111.32 * max(
        math.cos(math.radians((mid_component["centroid_lat"] + low_component["centroid_lat"]) / 2.0)),
        1e-6,
    )
    peak_dlat = (mid_component["peak_lat"] - low_component["peak_lat"]) * 111.32
    peak_dlon = _normalize_lon_delta(mid_component["peak_lon"] - low_component["peak_lon"]) * 111.32 * max(
        math.cos(math.radians((mid_component["peak_lat"] + low_component["peak_lat"]) / 2.0)),
        1e-6,
    )

    centroid_sep_km = math.sqrt(centroid_dlat ** 2 + centroid_dlon ** 2)
    peak_sep_km = math.sqrt(peak_dlat ** 2 + peak_dlon ** 2)
    orientation_diff = abs(mid_component["orientation_deg"] - low_component["orientation_deg"])
    if orientation_diff > 90.0:
        orientation_diff = 180.0 - orientation_diff

    width_ratio = min(low_component["width_km"], mid_component["width_km"]) / max(max(low_component["width_km"], mid_component["width_km"]), 1e-6)
    area_ratio = min(low_component["area_km2"], mid_component["area_km2"]) / max(max(low_component["area_km2"], mid_component["area_km2"]), 1e-6)
    overlap = _build_overlap_metrics(low_component, mid_component)

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


def integrate_azshear_features(integrator, low_dataset_path, mid_dataset_path, storm_cells):
    if not storm_cells or not low_dataset_path or not mid_dataset_path:
        return storm_cells

    zero_output = {
        "buffer_km": AZSHEAR_BUFFER_KM,
        "low": None,
        "mid": None,
        "alignment": {
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
        },
        "low_candidate_count": 0,
        "mid_candidate_count": 0,
    }

    def empty_output():
        return deepcopy(zero_output)

    try:
        low_ds = xr.open_dataset(low_dataset_path, decode_timedelta=True)
        mid_ds = xr.open_dataset(mid_dataset_path, decode_timedelta=True)
    except Exception as e:
        integrator.io_manager.write_error(f"Load error for AzShear features: {e}")
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

        low_lat_spacing_km, low_lon_spacing_km = _grid_spacing_km(low_lat_vals, low_lon_vals)
        mid_lat_spacing_km, mid_lon_spacing_km = _grid_spacing_km(mid_lat_vals, mid_lon_vals)
        low_pixel_area_km2 = max(low_lat_spacing_km * low_lon_spacing_km, 0.01)
        mid_pixel_area_km2 = max(mid_lat_spacing_km * mid_lon_spacing_km, 0.01)

        for cell in storm_cells:
            cell.setdefault("properties", {})
            cell["properties"]["azshear"] = empty_output()

            poly = StormIntegrationUtils.create_cell_polygon(cell)
            if poly is None:
                continue

            buffered_poly = _buffer_polygon_km(poly, AZSHEAR_BUFFER_KM)
            try:
                low_poly = integrator._polygon_for_dataset(buffered_poly, low_lon_vals)
                mid_poly = integrator._polygon_for_dataset(buffered_poly, mid_lon_vals)

                low_subset, low_lat, low_lon = integrator._extract_spatial_subset(
                    low_ds, low_var, False, None, low_lat_name, low_lon_name, low_lat_vals, low_lon_vals, low_poly
                )
                mid_subset, mid_lat, mid_lon = integrator._extract_spatial_subset(
                    mid_ds, mid_var, False, None, mid_lat_name, mid_lon_name, mid_lat_vals, mid_lon_vals, mid_poly
                )

                if low_subset is None or mid_subset is None:
                    continue

                low_inside = sv.contains(low_poly.buffer(1e-9), low_lon, low_lat)
                mid_inside = sv.contains(mid_poly.buffer(1e-9), mid_lon, mid_lat)

                low_masked = np.where(low_inside, np.asarray(low_subset), np.nan)
                mid_masked = np.where(mid_inside, np.asarray(mid_subset), np.nan)
                low_masked[low_masked < 0] = np.nan
                mid_masked[mid_masked < 0] = np.nan

                low_candidates = _extract_azshear_candidates(
                    low_masked,
                    low_lat,
                    low_lon,
                    AZSHEAR_LOW_THRESHOLD,
                    low_pixel_area_km2,
                    low_lat_spacing_km,
                    low_lon_spacing_km,
                )
                mid_candidates = _extract_azshear_candidates(
                    mid_masked,
                    mid_lat,
                    mid_lon,
                    AZSHEAR_MID_THRESHOLD,
                    mid_pixel_area_km2,
                    mid_lat_spacing_km,
                    mid_lon_spacing_km,
                )

                pair = _pair_azshear_components(low_candidates, mid_candidates)
                low_component = pair[0] if pair else (low_candidates[0] if low_candidates else None)
                mid_component = pair[1] if pair else (mid_candidates[0] if mid_candidates else None)
                alignment = _build_alignment_metrics(low_component, mid_component)

                cell["properties"]["azshear"] = {
                    "buffer_km": AZSHEAR_BUFFER_KM,
                    "low": _public_component_metrics(low_component),
                    "mid": _public_component_metrics(mid_component),
                    "alignment": alignment,
                    "low_candidate_count": len(low_candidates),
                    "mid_candidate_count": len(mid_candidates),
                }
            except Exception as e:
                integrator.io_manager.write_error(f"Process AzShear cell {cell.get('id')}: {e}")
                cell["properties"]["azshear"] = empty_output()
    finally:
        low_ds.close()
        mid_ds.close()
        gc.collect()

    return storm_cells
