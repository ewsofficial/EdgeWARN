"""
FLOHAR Region Extraction

Thresholds the composite threat grid, applies connected-component
labeling (8-connectivity), filters by area, polygonizes regions,
and computes per-region metadata.

Performance notes:
    - Uses scipy.ndimage.find_objects for bounding-box slicing (no full-grid masks)
    - Vectorized area computation (no Python loops over rows)
    - sum_labels for single-pass pixel counting during area filtering
"""

import numpy as np
from scipy.ndimage import label, find_objects, sum_labels
from typing import List, Dict, Any, Optional, Tuple

from . import config as cfg
from .engine import classify_severity_scalar

# 8-connectivity structuring element — flash flood regions frequently
# connect diagonally and must not be split artificially.
CONNECTIVITY_STRUCTURE = np.ones((3, 3), dtype=int)

# Earth radius in km
_EARTH_RADIUS_KM = 6371.0


# ─────────────────────────────────────────────────────────────────────
# Pixel area calculation (vectorized)
# ─────────────────────────────────────────────────────────────────────

def _pixel_area_km2(lat: float, dlat: float, dlon: float) -> float:
    """
    Approximate area of a single pixel in km² (latitude-dependent).

    Args:
        lat:  Pixel centre latitude (degrees)
        dlat: Pixel height in degrees
        dlon: Pixel width in degrees

    Returns:
        Area in km²
    """
    lat_rad = np.radians(lat)
    height = _EARTH_RADIUS_KM * np.radians(dlat)
    width = _EARTH_RADIUS_KM * np.cos(lat_rad) * np.radians(dlon)
    return abs(height * width)


def _pixel_area_column_km2(
    lat_coords: np.ndarray, dlat: float, dlon: float
) -> np.ndarray:
    """
    Vectorized pixel area for each row (latitude), returned as a 1D array.

    Args:
        lat_coords: 1D array of latitude values
        dlat: Grid spacing in latitude (degrees)
        dlon: Grid spacing in longitude (degrees)

    Returns:
        1D float32 array of pixel areas in km² for each row
    """
    lat_rad = np.radians(lat_coords)
    height = _EARTH_RADIUS_KM * np.radians(dlat)
    width = _EARTH_RADIUS_KM * np.cos(lat_rad) * np.radians(dlon)
    return np.abs(height * width).astype(np.float32)


def _compute_region_area_km2_vectorized(
    region_mask: np.ndarray,
    lat_coords: np.ndarray,
    dlat: float,
    dlon: float,
) -> float:
    """
    Compute total area of a labeled region in km² (fully vectorized).

    Args:
        region_mask: 2D boolean mask of the region (may be a sub-grid slice)
        lat_coords: 1D array of latitude values for each row of the mask
        dlat: Grid spacing in latitude (degrees)
        dlon: Grid spacing in longitude (degrees)

    Returns:
        Total area in km²
    """
    pixels_per_row = region_mask.sum(axis=1)  # vectorized row counts
    pixel_areas = _pixel_area_column_km2(lat_coords, dlat, dlon)
    return float(np.sum(pixels_per_row * pixel_areas))


# ─────────────────────────────────────────────────────────────────────
# Region extraction
# ─────────────────────────────────────────────────────────────────────

def extract_regions(
    threat_grid: np.ndarray,
    lat_coords: np.ndarray,
    lon_coords: np.ndarray,
    pillar_grids: Dict[str, np.ndarray],
    threshold: int = cfg.THREAT_THRESHOLD,
    min_area_km2: float = cfg.MIN_REGION_AREA_KM2,
    water_body_mask: Optional[np.ndarray] = None,
    max_regions: int = cfg.MAX_REGIONS,
    simplify_tolerance: float = cfg.POLYGON_SIMPLIFY_TOLERANCE,
) -> List[Dict[str, Any]]:
    """
    Extract contiguous flood threat regions from the threat grid.

    Pipeline:
        1. Threshold: mask pixels where threat_score >= threshold
        2. Static water body mask (if provided): zero-out permanent water
        3. Connected-component labeling with 8-connectivity
        4. Fast area filtering using sum_labels + pixel area estimates
        5. Precise area calculation with find_objects bounding-box slicing
        6. Cap at max_regions (largest first)
        7. Polygonize and compute metadata

    Args:
        threat_grid:      2D int array of threat scores (0–100)
        lat_coords:       1D array of latitude values
        lon_coords:       1D array of longitude values
        pillar_grids:     Dict with keys 'rainfall', 'hydro', 'ffg' → 2D arrays
        threshold:        Minimum score to include in a region
        min_area_km2:     Minimum region area to keep
        water_body_mask:  Optional boolean mask (True = water body to exclude)
        max_regions:      Maximum number of regions to return
        simplify_tolerance: Polygon simplification tolerance in degrees

    Returns:
        List of region dicts, each containing:
            - geometry: list of [lon, lat] polygon coordinates
            - peak_score, mean_score, severity
            - area_km2, centroid: [lat, lon]
            - pillar_peaks: {rainfall, hydro, ffg}
    """
    # ── Step 1: Threshold ───────────────────────────────────────────
    binary_mask = threat_grid >= threshold

    # ── Step 2: Water body mask ─────────────────────────────────────
    if water_body_mask is not None:
        binary_mask = binary_mask & ~water_body_mask

    # ── Step 3: Connected-component labeling ────────────────────────
    labeled_array, num_features = label(binary_mask, structure=CONNECTIVITY_STRUCTURE)
    del binary_mask

    if num_features == 0:
        return []

    # Grid spacing
    dlat = abs(lat_coords[1] - lat_coords[0]) if len(lat_coords) > 1 else 0.01
    dlon = abs(lon_coords[1] - lon_coords[0]) if len(lon_coords) > 1 else 0.01

    # ── Step 4: Fast pre-filter using sum_labels ────────────────────
    # Count pixels per label in a single vectorized pass
    label_ids = np.arange(1, num_features + 1)
    pixel_counts = sum_labels(
        np.ones(labeled_array.shape, dtype=np.int32),
        labeled_array,
        label_ids,
    )

    # Approximate minimum pixel count from min_area and mid-latitude pixel size
    mid_lat = float(np.median(lat_coords))
    approx_pixel_area = _pixel_area_km2(mid_lat, dlat, dlon)
    if approx_pixel_area > 0:
        # Use a conservative lower bound (80% of mid-lat area) to avoid
        # false rejections at extreme latitudes
        min_pixel_count = int(min_area_km2 / (approx_pixel_area * 1.25))
    else:
        min_pixel_count = 0

    # Pre-filter: skip labels with too few pixels
    candidate_ids = label_ids[pixel_counts >= max(min_pixel_count, 1)]

    if len(candidate_ids) == 0:
        return []

    # ── Step 5: Precise area with find_objects bounding-box slicing ──
    slices = find_objects(labeled_array)

    regions_with_area = []
    for region_id in candidate_ids:
        bbox = slices[region_id - 1]  # find_objects is 0-indexed
        if bbox is None:
            continue

        row_slice, col_slice = bbox

        # Extract sub-grid mask (bounded to bbox)
        label_sub = labeled_array[row_slice, col_slice]
        region_mask_sub = label_sub == region_id

        # Precise area with vectorized computation
        lat_sub = lat_coords[row_slice]
        area_km2 = _compute_region_area_km2_vectorized(region_mask_sub, lat_sub, dlat, dlon)

        if area_km2 >= min_area_km2:
            regions_with_area.append((region_id, bbox, area_km2))

    if not regions_with_area:
        return []

    # ── Step 6: Sort by area (largest first), cap at max ────────────
    regions_with_area.sort(key=lambda x: x[2], reverse=True)
    regions_with_area = regions_with_area[:max_regions]

    # ── Step 7: Polygonize and compute metadata ─────────────────────
    results = []
    for idx, (region_id, bbox, area_km2) in enumerate(regions_with_area, start=1):
        row_slice, col_slice = bbox

        # Re-extract sub-grid mask (small, bounded to bbox)
        label_sub = labeled_array[row_slice, col_slice]
        region_mask_sub = label_sub == region_id

        threat_sub = threat_grid[row_slice, col_slice]
        region_scores = threat_sub[region_mask_sub]
        peak_score = int(np.max(region_scores))
        mean_score = float(np.mean(region_scores))
        severity = classify_severity_scalar(peak_score)

        # Centroid (weighted by score, in full-grid coordinates)
        sub_rows, sub_cols = np.where(region_mask_sub)
        full_rows = sub_rows + row_slice.start
        full_cols = sub_cols + col_slice.start
        weights = threat_grid[full_rows, full_cols].astype(np.float32)
        weight_sum = weights.sum()
        if weight_sum > 0:
            centroid_lat = float(np.average(lat_coords[full_rows], weights=weights))
            centroid_lon = float(np.average(lon_coords[full_cols], weights=weights))
        else:
            centroid_lat = float(np.mean(lat_coords[full_rows]))
            centroid_lon = float(np.mean(lon_coords[full_cols]))

        # Pillar peaks (bounded to bbox)
        pillar_peaks = {}
        for pname, pgrid in pillar_grids.items():
            pillar_sub = pgrid[row_slice, col_slice]
            pillar_peaks[pname] = float(np.max(pillar_sub[region_mask_sub]))

        # Polygon: build from sub-grid mask with sub-grid coordinates
        lat_sub = lat_coords[row_slice]
        lon_sub = lon_coords[col_slice]
        polygon_coords = _mask_to_polygon(
            region_mask_sub, lat_sub, lon_sub, dlat, dlon, simplify_tolerance
        )

        # Skip regions that produced invalid/empty polygons
        if len(polygon_coords) < 3:
            continue

        results.append({
            "region_id": idx,
            "geometry": polygon_coords,
            "peak_score": peak_score,
            "mean_score": round(mean_score, 1),
            "severity": severity,
            "area_km2": round(area_km2, 1),
            "centroid": [round(centroid_lat, 4), round(centroid_lon, 4)],
            "pillar_peaks": {k: round(v, 3) for k, v in pillar_peaks.items()},
        })

    return results


# ─────────────────────────────────────────────────────────────────────
# Polygon extraction
# ─────────────────────────────────────────────────────────────────────

def _mask_to_polygon(
    mask: np.ndarray,
    lat_coords: np.ndarray,
    lon_coords: np.ndarray,
    dlat: float,
    dlon: float,
    simplify_tolerance: float,
) -> List[List[float]]:
    """
    Convert a binary mask to a simplified polygon in [lon, lat] coords.

    Uses rasterio.features.shapes for pixel-to-polygon conversion,
    then simplifies with shapely. The mask should be a sub-grid
    (bounding-box crop) for memory efficiency.

    Falls back to a convex-hull of pixel centres if rasterio is
    not available.

    Args:
        mask:              2D boolean mask (ideally bbox-cropped)
        lat_coords:        1D latitude array for the mask rows
        lon_coords:        1D longitude array for the mask cols
        dlat:              Grid spacing latitude (degrees)
        dlon:              Grid spacing longitude (degrees)
        simplify_tolerance: Simplification tolerance in degrees

    Returns:
        List of [lon, lat] coordinate pairs forming a closed polygon.
        Returns [] if polygon extraction fails.
    """
    try:
        import rasterio.features
        from rasterio.transform import from_bounds
        from shapely.geometry import shape as shapely_shape, Polygon, MultiPolygon

        # Build affine transform for the sub-grid
        nrows, ncols = mask.shape
        west = float(lon_coords[0]) - dlon / 2
        east = float(lon_coords[-1]) + dlon / 2
        # Determine if latitude goes N→S or S→N
        if lat_coords[0] > lat_coords[-1]:
            north = float(lat_coords[0]) + dlat / 2
            south = float(lat_coords[-1]) - dlat / 2
        else:
            north = float(lat_coords[-1]) + dlat / 2
            south = float(lat_coords[0]) - dlat / 2

        transform = from_bounds(west, south, east, north, ncols, nrows)

        # Extract shapes from sub-grid mask
        mask_uint8 = mask.astype(np.uint8)
        shapes = list(rasterio.features.shapes(
            mask_uint8, mask=mask_uint8, transform=transform
        ))

        if not shapes:
            return _fallback_polygon(mask, lat_coords, lon_coords)

        # Take the largest shape (by number of coords)
        largest_geom = max(shapes, key=lambda s: len(str(s[0])))[0]
        poly = shapely_shape(largest_geom)

        if simplify_tolerance > 0:
            poly = poly.simplify(simplify_tolerance)

        # Handle degenerate geometries after simplification
        if poly.is_empty:
            return []

        # Extract exterior from the appropriate geometry type
        if isinstance(poly, Polygon) and poly.exterior is not None:
            coords = list(poly.exterior.coords)
        elif isinstance(poly, MultiPolygon):
            # Take the largest polygon from the MultiPolygon
            largest = max(poly.geoms, key=lambda g: g.area)
            if largest.exterior is not None:
                coords = list(largest.exterior.coords)
            else:
                return []
        else:
            # Point, LineString, GeometryCollection, etc. — not valid
            return []

        return [[round(c[0], 4), round(c[1], 4)] for c in coords]

    except ImportError:
        return _fallback_polygon(mask, lat_coords, lon_coords)


def _fallback_polygon(
    mask: np.ndarray,
    lat_coords: np.ndarray,
    lon_coords: np.ndarray,
) -> List[List[float]]:
    """
    Fallback polygon from convex hull of pixel centres.
    Used when rasterio is not available.
    """
    from shapely.geometry import MultiPoint, Polygon

    rows, cols = np.where(mask)
    if len(rows) == 0:
        return []

    points = [(float(lon_coords[c]), float(lat_coords[r])) for r, c in zip(rows, cols)]
    hull = MultiPoint(points).convex_hull

    if hull.is_empty:
        return []

    # Guard against degenerate hulls (Point, LineString)
    if not isinstance(hull, Polygon) or hull.exterior is None:
        return []

    coords = list(hull.exterior.coords)
    return [[round(c[0], 4), round(c[1], 4)] for c in coords]
