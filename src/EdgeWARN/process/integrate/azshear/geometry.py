import math

import numpy as np
from shapely.geometry import box
from shapely.ops import transform, unary_union


def normalize_lon_delta(delta):
    if delta > 180.0:
        return delta - 360.0
    if delta < -180.0:
        return delta + 360.0
    return delta


def buffer_polygon_km(poly, buffer_km):
    if poly is None or buffer_km <= 0:
        return poly

    centroid = poly.centroid
    ref_lat = centroid.y
    meters_per_deg_lat = 111_320.0
    meters_per_deg_lon = meters_per_deg_lat * max(math.cos(math.radians(ref_lat)), 1e-6)
    buffer_m = buffer_km * 1000.0

    def to_local(x, y, z=None):
        dx = normalize_lon_delta(x - centroid.x) * meters_per_deg_lon
        dy = (y - centroid.y) * meters_per_deg_lat
        return (dx, dy)

    def to_geo(x, y, z=None):
        lon = centroid.x + (x / meters_per_deg_lon)
        lat = centroid.y + (y / meters_per_deg_lat)
        return (lon, lat)

    local_poly = transform(to_local, poly)
    buffered = local_poly.buffer(buffer_m)
    return transform(to_geo, buffered)


def _polygon_to_local_km(poly):
    if poly is None or poly.is_empty:
        return None, None, None

    centroid = poly.centroid
    ref_lat = float(centroid.y)
    ref_lon = float(centroid.x)
    km_per_deg_lat = 111.32
    km_per_deg_lon = km_per_deg_lat * max(math.cos(math.radians(ref_lat)), 1e-6)

    def to_local(x, y, z=None):
        dx = normalize_lon_delta(float(x) - ref_lon) * km_per_deg_lon
        dy = (float(y) - ref_lat) * km_per_deg_lat
        return (dx, dy)

    return transform(to_local, poly), ref_lat, ref_lon


def polygon_area_km2(poly):
    local_poly, _, _ = _polygon_to_local_km(poly)
    if local_poly is None:
        return 0.0
    return float(max(local_poly.area, 0.0))


def polygon_major_axis_orientation_deg(poly):
    local_poly, _, _ = _polygon_to_local_km(poly)
    if local_poly is None:
        return None

    coords = np.asarray(local_poly.exterior.coords, dtype=float)
    if coords.shape[0] < 3:
        return None

    x = coords[:, 0]
    y = coords[:, 1]
    finite = np.isfinite(x) & np.isfinite(y)
    if np.count_nonzero(finite) < 3:
        return None

    x = x[finite]
    y = y[finite]

    x_centered = x - np.nanmean(x)
    y_centered = y - np.nanmean(y)
    if x_centered.size < 2:
        return None

    cov = np.cov(np.vstack((x_centered, y_centered)), bias=True)
    if cov.shape != (2, 2) or not np.all(np.isfinite(cov)):
        return None

    eigvals, eigvecs = np.linalg.eigh(cov)
    order = np.argsort(eigvals)[::-1]
    eigvals = eigvals[order]
    eigvecs = eigvecs[:, order]

    if eigvals.size == 0 or float(eigvals[0]) <= 0.0:
        return None

    return float((math.degrees(math.atan2(eigvecs[1, 0], eigvecs[0, 0])) + 360.0) % 180.0)


def distance_km(lat_a, lon_a, lat_b, lon_b):
    ref_lat = (lat_a + lat_b) / 2.0
    dlat = (lat_b - lat_a) * 111.32
    dlon = normalize_lon_delta(lon_b - lon_a) * 111.32 * max(math.cos(math.radians(ref_lat)), 1e-6)
    return math.sqrt((dlat**2) + (dlon**2))


def midpoint_lon(lon_a, lon_b):
    return lon_a + (normalize_lon_delta(lon_b - lon_a) / 2.0)


def weighted_lon_mean(lons, ref_lon, weights):
    lon_offsets = np.array([normalize_lon_delta(float(lon) - ref_lon) for lon in lons], dtype=float)
    return ref_lon + float(np.average(lon_offsets, weights=weights))


def grid_spacing_km(lat_vals, lon_vals):
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


def build_component_geometry(component, ref_lat, ref_lon):
    if component is None:
        return None

    lat_spacing_km = component.get("_lat_spacing_km")
    lon_spacing_km = component.get("_lon_spacing_km")

    pixel_lats = component.get("_pixel_lats")
    pixel_lons = component.get("_pixel_lons")
    if pixel_lats is not None and pixel_lons is not None:
        comp_lats = np.asarray(pixel_lats, dtype=float)
        comp_lons = np.asarray(pixel_lons, dtype=float)
    else:
        component_mask = component.get("_component_mask")
        lat_grid = component.get("_lat_grid")
        lon_grid = component.get("_lon_grid")
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
        x = normalize_lon_delta(float(lon) - ref_lon) * km_per_deg_lon
        y = (float(lat) - ref_lat) * 111.32
        pixel_boxes.append(box(x - lon_half_km, y - lat_half_km, x + lon_half_km, y + lat_half_km))

    return unary_union(pixel_boxes) if pixel_boxes else None
