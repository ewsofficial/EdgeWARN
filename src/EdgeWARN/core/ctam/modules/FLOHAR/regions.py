"""
FLOHAR Region Extraction

Thresholds the composite threat grid, applies connected-component
labeling (8-connectivity), filters by area, polygonizes regions,
and computes per-region metadata.

Memory-optimised: uses scipy.ndimage.find_objects for bounding-box
slicing instead of full-grid boolean masks per region.
"""

import numpy as np
from scipy.ndimage import label, find_objects
from typing import List, Dict, Any, Optional, Tuple

from . import config as cfg
from .engine import classify_severity_scalar

# 8-connectivity structuring element — flash flood regions frequently
# connect diagonally and must not be split artificially.
CONNECTIVITY_STRUCTURE = np.ones((3, 3), dtype=int)

# Earth radius in km
_EARTH_RADIUS_KM = 6371.0


# ─────────────────────────────────────────────────────────────────────
# Pixel area calculation
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


def _compute_region_area_km2(
    region_mask: np.ndarray,
    lat_coords: np.ndarray,
    dlat: float,
    dlon: float,
) -> float:
    """
    Compute total area of a labeled region in km².

    Args:
        region_mask: 2D boolean mask of the region (may be a sub-grid slice)
        lat_coords: 1D array of latitude values for each row of the mask
        dlat: Grid spacing in latitude (degrees)
        dlon: Grid spacing in longitude (degrees)

    Returns:
        Total area in km²
    """
    row_indices = np.where(np.any(region_mask, axis=1))[0]
    total_area = 0.0
    for row_idx in row_indices:
        n_pixels = np.sum(region_mask[row_idx])
        pixel_area = _pixel_area_km2(lat_coords[row_idx], dlat, dlon)
        total_area += n_pixels * pixel_area
    return total_area


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
        4. Filter by minimum area using bounding-box slices (memory-efficient)
        5. Cap at max_regions (largest first)
        6. Polygonize and compute metadata

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

    # ── Step 4: Use find_objects for bounding-box slicing ───────────
    # find_objects returns a list of (row_slice, col_slice) per label,
    # avoiding full-grid boolean masks that cause OOM on CONUS grids.
    slices = find_objects(labeled_array)

    regions_with_area = []
    for region_id, bbox in enumerate(slices, start=1):
        if bbox is None:
            continue  # label was removed (e.g. by water body mask)

        row_slice, col_slice = bbox

        # Extract sub-grids for this region's bounding box only
        label_sub = labeled_array[row_slice, col_slice]
        region_mask_sub = label_sub == region_id

        # Compute area using sub-grid lat coords
        lat_sub = lat_coords[row_slice]
        area_km2 = _compute_region_area_km2(region_mask_sub, lat_sub, dlat, dlon)

        if area_km2 >= min_area_km2:
            regions_with_area.append((region_id, bbox, area_km2))

    if not regions_with_area:
        return []

    # ── Step 5: Sort by area (largest first), cap at max ────────────
    regions_with_area.sort(key=lambda x: x[2], reverse=True)
    regions_with_area = regions_with_area[:max_regions]

    # ── Step 6: Polygonize and compute metadata ─────────────────────
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
        weights = threat_grid[full_rows, full_cols].astype(float)
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

        return [[round(c[0], 5), round(c[1], 5)] for c in coords]

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
    return [[round(c[0], 5), round(c[1], 5)] for c in coords]
