"""
Spatial analysis utilities for lineage detection.

This module provides functions for calculating polygon overlap ratios and
building spatial indices for efficient cell matching.
"""

from typing import List, Dict, Tuple, Optional, Any
from shapely.geometry import Polygon
from shapely.validation import make_valid
import numpy as np


def calculate_overlap_ratio(parent_bbox: List[List[float]], 
                           child_bbox: List[List[float]]) -> float:
    """
    Calculate the overlap ratio of parent polygon area covered by child polygon.
    
    This function computes what fraction of the parent's area overlaps with
    the child's area, which is used to determine merge and split events.
    
    Args:
        parent_bbox: List of [lat, lon] coordinate pairs defining the parent polygon.
                     Longitudes should be in 0-360° format (east-positive).
        child_bbox: List of [lat, lon] coordinate pairs defining the child polygon.
                    Longitudes should be in 0-360° format (east-positive).
    
    Returns:
        Float between 0.0 and 1.0 representing the ratio of parent area that
        overlaps with child area. Returns 0.0 if polygons are invalid or disjoint.
    
    Example:
        >>> parent = [[35.1, 262.4], [35.1, 262.6], [35.3, 262.6], [35.3, 262.4]]
        >>> child = [[35.2, 262.5], [35.2, 262.7], [35.4, 262.7], [35.4, 262.5]]
        >>> ratio = calculate_overlap_ratio(parent, child)
        >>> 0.0 <= ratio <= 1.0
        True
    """
    if not parent_bbox or not child_bbox:
        return 0.0
    
    if len(parent_bbox) < 3 or len(child_bbox) < 3:
        # Need at least 3 points for a polygon
        return 0.0
    
    try:
        # Build shapely Polygons from coordinate pair lists
        # Use (lon, lat) ordering for shapely's x,y convention
        parent_coords = [(lon, lat) for lat, lon in parent_bbox]
        child_coords = [(lon, lat) for lat, lon in child_bbox]
        
        parent_poly = Polygon(parent_coords)
        child_poly = Polygon(child_coords)
        
        # Handle invalid polygons (e.g., self-intersecting)
        if not parent_poly.is_valid:
            parent_poly = make_valid(parent_poly)
            if parent_poly.is_empty:
                return 0.0
        
        if not child_poly.is_valid:
            child_poly = make_valid(child_poly)
            if child_poly.is_empty:
                return 0.0
        
        # Check for antimeridian crossing and normalize if needed
        parent_poly = _normalize_antimeridian(parent_poly)
        child_poly = _normalize_antimeridian(child_poly)
        
        if parent_poly.is_empty or child_poly.is_empty:
            return 0.0
        
        intersection = parent_poly.intersection(child_poly)
        
        if intersection.is_empty:
            return 0.0
        
        # Ratio of parent area that overlaps with child (per FR1.2)
        parent_area = parent_poly.area
        if parent_area == 0:
            return 0.0
        
        return intersection.area / parent_area
    
    except Exception:
        # Return 0.0 for any geometry errors
        return 0.0


def _normalize_antimeridian(polygon: Polygon) -> Polygon:
    """
    Normalize polygon coordinates that may cross the antimeridian.
    
    For polygons with coordinates near 360°/0°, this function attempts to
    normalize them to a consistent coordinate system.
    
    Args:
        polygon: Shapely Polygon to normalize
        
    Returns:
        Normalized Polygon, or the original if no normalization needed.
    """
    coords = list(polygon.exterior.coords) if not polygon.is_empty else []
    
    if not coords:
        return polygon
    
    lons = [c[0] for c in coords]
    min_lon = min(lons)
    max_lon = max(lons)
    
    # Check if polygon crosses antimeridian (large span near 360/0 boundary)
    if max_lon > 350 and min_lon < 10 and (max_lon - min_lon) > 180:
        # Shift negative/low longitudes to positive space
        normalized_coords = [
            (lon + 360 if lon < 0 else lon, lat)
            for lon, lat in coords
        ]
        try:
            return Polygon(normalized_coords)
        except Exception:
            return polygon
    
    return polygon


def build_spatial_index(cells: List[Dict[str, Any]]) -> Dict[int, Dict[str, Any]]:
    """
    Build a spatial index for efficient overlap queries.
    
    This function creates a dictionary mapping cell IDs to their bounding
    box extents, enabling quick spatial filtering before expensive polygon
    overlap calculations.
    
    Args:
        cells: List of cell dictionaries, each containing 'id', 'bbox', and
               optionally 'centroid', 'max_refl', 'num_gates'.
    
    Returns:
        Dictionary mapping cell_id to a dict with:
            - 'bbox': The polygon coordinates
            - 'min_lat', 'max_lat', 'min_lon', 'max_lon': Bounding extents
            - 'centroid': Cell centroid [lat, lon]
            - 'max_refl': Maximum reflectivity (for dominant parent selection)
            - 'num_gates': Number of gates (for dominant parent selection)
    """
    index = {}
    
    for cell in cells:
        cell_id = int(cell.get('id', 0))
        bbox = cell.get('bbox', [])
        
        if not bbox or len(bbox) < 3:
            continue
        
        # Calculate bounding extents
        lats = [pt[0] for pt in bbox]
        lons = [pt[1] for pt in bbox]
        
        index[cell_id] = {
            'bbox': bbox,
            'min_lat': min(lats),
            'max_lat': max(lats),
            'min_lon': min(lons),
            'max_lon': max(lons),
            'centroid': cell.get('centroid', [None, None]),
            'max_refl': cell.get('max_refl', 0.0),
            'num_gates': cell.get('num_gates', 0),
        }
    
    return index


def bounds_overlap(bounds1: Dict[str, float], 
                   bounds2: Dict[str, float],
                   buffer: float = 0.0) -> bool:
    """
    Check if two bounding boxes overlap (fast pre-filter).
    
    This is a quick axis-aligned bounding box check to avoid expensive
    polygon intersection calculations for clearly disjoint cells.
    
    Args:
        bounds1: Dict with 'min_lat', 'max_lat', 'min_lon', 'max_lon'
        bounds2: Dict with 'min_lat', 'max_lat', 'min_lon', 'max_lon'
        buffer: Optional buffer distance to expand bounds (degrees)
    
    Returns:
        True if the bounding boxes overlap, False otherwise.
    """
    # Check latitude overlap
    if bounds1['max_lat'] + buffer < bounds2['min_lat']:
        return False
    if bounds1['min_lat'] - buffer > bounds2['max_lat']:
        return False
    
    # Check longitude overlap (accounting for 0-360 format)
    lon1_min = bounds1['min_lon']
    lon1_max = bounds1['max_lon']
    lon2_min = bounds2['min_lon']
    lon2_max = bounds2['max_lon']
    
    # Handle antimeridian crossing
    if lon1_max > 350 and lon1_min < 10:
        # bounds1 crosses antimeridian
        return True
    if lon2_max > 350 and lon2_min < 10:
        # bounds2 crosses antimeridian
        return True
    
    # Normal overlap check
    if lon1_max + buffer < lon2_min:
        return False
    if lon1_min - buffer > lon2_max:
        return False
    
    return True


def find_overlapping_cells(
    target_cell: Dict[str, Any],
    cell_index: Dict[int, Dict[str, Any]],
    overlap_threshold: float = 0.0
) -> List[Tuple[int, float]]:
    """
    Find all cells that overlap with a target cell above a threshold.
    
    This function performs a two-stage overlap detection:
    1. Fast bounding box pre-filter
    2. Precise polygon intersection calculation
    
    Args:
        target_cell: Cell dictionary with 'id' and 'bbox'
        cell_index: Spatial index from build_spatial_index()
        overlap_threshold: Minimum overlap ratio to include (default 0.0)
    
    Returns:
        List of (cell_id, overlap_ratio) tuples, sorted by overlap ratio descending.
    """
    target_id = int(target_cell.get('id', 0))
    target_bbox = target_cell.get('bbox', [])
    
    if not target_bbox:
        return []
    
    # Calculate target bounds
    target_lats = [pt[0] for pt in target_bbox]
    target_lons = [pt[1] for pt in target_bbox]
    target_bounds = {
        'min_lat': min(target_lats),
        'max_lat': max(target_lats),
        'min_lon': min(target_lons),
        'max_lon': max(target_lons),
    }
    
    overlapping = []
    
    for cell_id, cell_data in cell_index.items():
        if cell_id == target_id:
            continue
        
        # Stage 1: Fast bounding box check
        if not bounds_overlap(target_bounds, cell_data):
            continue
        
        # Stage 2: Precise polygon overlap
        overlap_ratio = calculate_overlap_ratio(cell_data['bbox'], target_bbox)
        
        if overlap_ratio >= overlap_threshold:
            overlapping.append((cell_id, overlap_ratio))
    
    # Sort by overlap ratio descending
    overlapping.sort(key=lambda x: x[1], reverse=True)
    
    return overlapping


def select_dominant_parent(
    parent_ids: List[int],
    cell_index: Dict[int, Dict[str, Any]]
) -> int:
    """
    Select the dominant parent cell from a list of merge candidates.
    
    Selection criteria (per PRD FR1.2):
    1. Highest max_refl (maximum reflectivity)
    2. Tiebreaker: largest num_gates
    
    Args:
        parent_ids: List of candidate parent cell IDs
        cell_index: Spatial index containing cell attributes
    
    Returns:
        The ID of the dominant parent cell.
    """
    if not parent_ids:
        raise ValueError("Cannot select dominant parent from empty list")
    
    if len(parent_ids) == 1:
        return parent_ids[0]
    
    best_id = parent_ids[0]
    best_refl = cell_index.get(best_id, {}).get('max_refl', 0.0) or 0.0
    best_gates = cell_index.get(best_id, {}).get('num_gates', 0) or 0
    
    for pid in parent_ids[1:]:
        cell_data = cell_index.get(pid, {})
        refl = cell_data.get('max_refl', 0.0) or 0.0
        gates = cell_data.get('num_gates', 0) or 0
        
        # Compare by max_refl first, then num_gates
        if refl > best_refl or (refl == best_refl and gates > best_gates):
            best_id = pid
            best_refl = refl
            best_gates = gates
    
    return best_id


def select_dominant_child(
    child_ids: List[int],
    cell_index: Dict[int, Dict[str, Any]]
) -> int:
    """
    Select the dominant child cell from a list of split candidates.
    
    Selection criteria (per PRD FR2.2):
    1. Highest max_refl (maximum reflectivity)
    2. Tiebreaker: largest num_gates
    
    The dominant child inherits the parent's ID.
    
    Args:
        child_ids: List of candidate child cell IDs
        cell_index: Spatial index containing cell attributes
    
    Returns:
        The ID of the dominant child cell.
    """
    if not child_ids:
        raise ValueError("Cannot select dominant child from empty list")
    
    if len(child_ids) == 1:
        return child_ids[0]
    
    best_id = child_ids[0]
    best_refl = cell_index.get(best_id, {}).get('max_refl', 0.0) or 0.0
    best_gates = cell_index.get(best_id, {}).get('num_gates', 0) or 0
    
    for cid in child_ids[1:]:
        cell_data = cell_index.get(cid, {})
        refl = cell_data.get('max_refl', 0.0) or 0.0
        gates = cell_data.get('num_gates', 0) or 0
        
        # Compare by max_refl first, then num_gates
        if refl > best_refl or (refl == best_refl and gates > best_gates):
            best_id = cid
            best_refl = refl
            best_gates = gates
    
    return best_id