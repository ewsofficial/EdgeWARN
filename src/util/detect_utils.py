import xarray as xr
import numpy as np
from scipy.ndimage import label, binary_dilation, center_of_mass
import alphashape
from shapely.geometry import Point, Polygon, LineString
import datetime
import math
import matplotlib.pyplot as plt
from scipy.optimize import linear_sum_assignment
from datetime import datetime
from typing import Dict, List, Optional, Tuple
import json


def load_mrms_slice(filepath, lat_limits=None, lon_limits=None):
    ds = xr.open_dataset(filepath)

    # --- Reflectivity ---
    if "reflectivity_combined" in ds:
        refl = ds["reflectivity_combined"]
    elif "unknown" in ds:
        refl = ds["unknown"]
    else:
        raise ValueError("No valid reflectivity data found.")

    # --- Coordinates ---
    if "x" in ds and "y" in ds:
        lat = ds["y"]
        lon = ds["x"]
        lat_dim, lon_dim = "y", "x"
    elif "latitude" in ds and "longitude" in ds:
        lat = ds["latitude"]
        lon = ds["longitude"]
        lat_dim, lon_dim = "latitude", "longitude"
    else:
        raise ValueError("No valid coordinates.")

    # Find index ranges that satisfy bounding box
    if lat_limits is not None:
        lat_mask = (lat >= lat_limits[0]) & (lat <= lat_limits[1])
        y_start, y_end = np.where(lat_mask)[0][[0, -1]] + [0, 1]
    else:
        y_start, y_end = 0, lat.shape[0]

    if lon_limits is not None:
        lon_mask = (lon >= lon_limits[0]) & (lon <= lon_limits[1])
        x_start, x_end = np.where(lon_mask)[0][[0, -1]] + [0, 1]
    else:
        x_start, x_end = 0, lon.shape[0]

    # Handle 1D or 2D lat/lon arrays
    if lat.ndim == 1 and lon.ndim == 1:
        refl_crop = refl.isel({lat_dim: slice(y_start, y_end), lon_dim: slice(x_start, x_end)}).values
        lat_crop = lat.isel({lat_dim: slice(y_start, y_end)}).values
        lon_crop = lon.isel({lon_dim: slice(x_start, x_end)}).values
        # Expand to 2D meshgrid for compatibility
        lon_grid, lat_grid = np.meshgrid(lon_crop, lat_crop)
        ds.close()
        return refl_crop, lat_grid, lon_grid
    else:
        refl_crop = refl.isel({lat_dim: slice(y_start, y_end), lon_dim: slice(x_start, x_end)}).values
        lat_crop = lat.isel({lat_dim: slice(y_start, y_end), lon_dim: slice(x_start, x_end)}).values
        lon_crop = lon.isel({lat_dim: slice(y_start, y_end), lon_dim: slice(x_start, x_end)}).values
        ds.close()
        return refl_crop, lat_crop, lon_crop

from pathlib import Path
import re
import datetime
from datetime import datetime

def extract_timestamp_from_filename(filepath):
    """
    Extract timestamp from MRMS filename with multiple pattern support.
    """
    filename = Path(filepath).name
    print(f"DEBUG: Extracting timestamp from filename: {filename}")
    
    patterns = [
        r'MRMS_MergedReflectivityQC_3D_(\d{8})-(\d{6})',
        r'(\d{8})-(\d{6})_renamed',
        r'(\d{8}-\d{6})',
        r'.*(\d{8})-(\d{6}).*'
    ]
    
    for pattern_idx, pattern in enumerate(patterns):
        match = re.search(pattern, filename)
        if match:
            groups = match.groups()
            print(f"DEBUG: Pattern {pattern_idx+1} matched: {groups}")
            
            if len(groups) == 2:
                date_str, time_str = groups
            else:
                combined = groups[0]
                date_str, time_str = combined[:8], combined[9:]
            
            try:
                formatted_time = (f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}T"
                                 f"{time_str[:2]}:{time_str[2:4]}:{time_str[4:6]}")
                print(f"DEBUG: Extracted timestamp: {formatted_time}")
                return formatted_time
            except (IndexError, ValueError) as e:
                print(f"DEBUG: Error formatting timestamp: {e}")
                continue
    
    fallback = datetime.utcnow().isoformat()
    print(f"DEBUG: Using fallback timestamp: {fallback}")
    return fallback

class StormCellDetector:
    """
    A class to detect and process storm cells from reflectivity data.
    """
    
    def __init__(self, seed_dbz=50, expand_dbz=40, min_gates=25, 
                 max_iterations=100, alpha=0.1, size_ratio_threshold=0.9, 
                 buffer_km=1.0):
        """
        Initialize the StormCellDetector with parameters for cell detection and merging.
        """
        self.seed_dbz = seed_dbz
        self.expand_dbz = expand_dbz
        self.min_gates = min_gates
        self.max_iterations = max_iterations
        self.alpha = alpha
        self.size_ratio_threshold = size_ratio_threshold
        self.buffer_km = buffer_km
    
    @staticmethod
    def bbox_to_points(bbox):
        """
        Convert bounding box dictionary to a list of points representing the polygon.
        """
        if bbox is None:
            return None
        return [
            [bbox["lon_min"], bbox["lat_min"]],  # lower-left
            [bbox["lon_min"], bbox["lat_max"]],  # upper-left
            [bbox["lon_max"], bbox["lat_max"]],  # upper-right
            [bbox["lon_max"], bbox["lat_min"]],  # lower-right
            [bbox["lon_min"], bbox["lat_min"]],  # close polygon to first point
        ]
    
    @staticmethod
    def polygon_to_bbox(polygon):
        """
        Convert a shapely Polygon or similar geometry to a bounding box dictionary.
        Returns None if input is None.
        """
        if polygon is None:
            return None
        minx, miny, maxx, maxy = polygon.bounds  # bounds: (minx, miny, maxx, maxy)
        return {
            "lon_min": minx,
            "lat_min": miny,
            "lon_max": maxx,
            "lat_max": maxy
        }
    
    @staticmethod
    def convert_lon_0_360_to_pm180(lon):
        """
        Helper function: Converts longitude from a 0-360 range to +-180 range
        Parameters:
        - lon: 2D array of longitude values in 0-360 range.
        Returns:
        - lon: 2D array of longitude values converted to -180 to 180 range.
        """
        return np.where(lon > 180, lon - 360, lon)
    
    @staticmethod
    def get_alpha_shape_from_mask(mask, lat_grid, lon_grid, alpha=0.1):
        """
        Generate an alpha shape from a boolean mask.
        """
        if np.sum(mask) == 0:
            return None

        # Ensure we are working with NumPy arrays (like in plotting)
        lat_np = np.array(lat_grid)
        lon_np = np.array(lon_grid)

        points = np.column_stack((lon_np[mask], lat_np[mask]))
        points_list = [tuple(p) for p in points]

        n_points = len(points_list)
        if n_points == 0:
            return None
        elif n_points == 1:
            return Point(points_list[0])
        elif n_points == 2:
            return LineString(points_list)
        else:
            from shapely.geometry import MultiPoint
            mp = MultiPoint(points_list)
            if mp.convex_hull.geom_type in ['Point', 'LineString']:
                return mp.convex_hull
            else:
                import alphashape
                alpha_shape = alphashape.alphashape(points_list, alpha)
                if alpha_shape.geom_type == 'MultiPolygon':
                    alpha_shape = max(alpha_shape.geoms, key=lambda p: p.area)
                return alpha_shape
    
    @staticmethod
    def propagate_cells(reflectivity, lat_grid, lon_grid, seed_dbz=50,
                       expand_dbz=40, min_gates=25, max_iterations=100, alpha=0.1, filepath=None):
        """
        Detect and grow storm cells based on reflectivity thresholds.
        Returns a list of detected cell dictionaries.
        """
        refl_data = np.nan_to_num(reflectivity, nan=-9999)

        # Step 1: Detect seed cells ≥ seed_dbz
        seed_mask = refl_data >= seed_dbz
        labeled_seeds, num_seeds = label(seed_mask)
        print(f"Found {num_seeds} seed cells above {seed_dbz} dBZ")

        # Initialize grown masks with seed masks
        grown_masks = {seed_id: (labeled_seeds == seed_id) for seed_id in range(1, num_seeds + 1)}

        # Initialize global mask of claimed pixels by any cell
        claimed_mask = np.zeros_like(refl_data, dtype=bool)
        for mask in grown_masks.values():
            claimed_mask |= mask

        structure = np.ones((3, 3), dtype=bool)  # connectivity for dilation

        for iteration in range(max_iterations):
            changes = 0
            new_grown_masks = {}

            for seed_id, current_mask in grown_masks.items():
                dilated = binary_dilation(current_mask, structure=structure)
                candidates = np.logical_and(dilated, refl_data >= expand_dbz)
                candidates = np.logical_and(candidates, np.logical_not(current_mask))

                # Only expand into pixels not claimed by other cells
                allowed_candidates = np.logical_and(candidates, np.logical_not(claimed_mask))

                if np.any(allowed_candidates):
                    new_mask = np.logical_or(current_mask, allowed_candidates)
                    new_grown_masks[seed_id] = new_mask
                    changes += 1
                else:
                    new_grown_masks[seed_id] = current_mask

            if changes == 0:
                print(f"No expansions possible after {iteration} iterations.")
                break

            grown_masks = new_grown_masks

            # Update global claimed mask
            claimed_mask = np.zeros_like(refl_data, dtype=bool)
            for mask in grown_masks.values():
                claimed_mask |= mask

        print(f"Expansion completed after {iteration + 1} iterations.")

        # Find Timestamps
        if filepath:
            scan_time = extract_timestamp_from_filename(filepath)
        else:
            scan_time = datetime.utcnow().isoformat()

        # Build detected cell list
        detected_cells = []
        for seed_id, mask in grown_masks.items():
            if np.sum(mask) < min_gates:
                continue
            max_dbz = refl_data[mask].max()

            # centroid of high-reflectivity region if present
            high_refl_mask = np.logical_and(mask, refl_data >= seed_dbz)
            centroid_idx = center_of_mass(high_refl_mask if np.any(high_refl_mask) else mask)
            centroid_lat = lat_grid[int(round(centroid_idx[0])), int(round(centroid_idx[1]))]
            centroid_lon = lon_grid[int(round(centroid_idx[0])), int(round(centroid_idx[1]))]

            # Create alpha shape using only True pixels
            points_idx = np.column_stack(np.where(mask))
            points = [(float(lon_grid[i, j]), float(lat_grid[i, j])) for i, j in points_idx]

            poly = None
            if len(points) == 1:
                poly = Point(points[0])
            elif len(points) == 2:
                poly = LineString(points)
            elif len(points) > 2:
                import alphashape
                poly = alphashape.alphashape(points, alpha)
                if poly.geom_type == 'MultiPolygon':
                    poly = max(poly.geoms, key=lambda p: p.area)

            alpha_shape_coords = [[float(x), float(y)] for x, y in poly.exterior.coords] if poly and hasattr(poly, "exterior") else []

            bbox = StormCellDetector.polygon_to_bbox(poly)

            # Create the cell dict
            cell_dict = {
                "id": int(seed_id),
                "num_gates": int(np.sum(mask)),
                "centroid": [float(centroid_lat), float(centroid_lon)],
                "bbox": bbox,
                "max_reflectivity_dbz": float(max_dbz),
                "alpha_shape": alpha_shape_coords,
                # Add storm history list
                "storm_history": [
                    {
                        "timestamp": scan_time,
                        "max_reflectivity_dbz": float(max_dbz),
                        "num_gates": int(np.sum(mask)),
                        "centroid": [float(centroid_lat), float(centroid_lon)]
                    }
                ]
            }

            detected_cells.append(cell_dict)

        return detected_cells
    
    @staticmethod
    def merge_connected_small_cells(cells, size_ratio_threshold=0.9, buffer_km=1.0, alpha=0.1):
        """
        Merge small storm cells into nearby larger cells if bounding boxes are within ~1 km.

        Parameters:
        - cells: list of cell dicts with keys 'num_gates', 'centroid_latlon', 'alpha_shape', 'bbox'.
        
        Returns:
        - List of merged cells (larger cells updated, small cells merged in and removed).
        """
        from shapely.geometry import Polygon

        def deg_buffer(lat, km):
            """Convert km to approximate degrees latitude/longitude."""
            lat_deg = km / 111.0  # 1° lat ~ 111 km
            lon_deg = km / (111.0 * np.cos(np.radians(lat)))
            return lat_deg, lon_deg

        def bboxes_within_distance(b1, b2, buffer_lat, buffer_lon):
            """Check if bounding boxes are within buffer distance in degrees."""
            return not (
                b1["lon_max"] + buffer_lon < b2["lon_min"] or
                b1["lon_min"] - buffer_lon > b2["lon_max"] or
                b1["lat_max"] + buffer_lat < b2["lat_min"] or
                b1["lat_min"] - buffer_lat > b2["lat_max"]
            )

        def merge_cells(large, small, alpha=0.1):
            """Merge small cell into large cell."""
            # Update num_gates and centroid
            total_gates = large["num_gates"] + small["num_gates"]
            lat1, lon1 = large["centroid"]
            lat2, lon2 = small["centroid"]
            large["centroid"] = [
                (lat1 * large["num_gates"] + lat2 * small["num_gates"]) / total_gates,
                (lon1 * large["num_gates"] + lon2 * small["num_gates"]) / total_gates
            ]
            large["num_gates"] = total_gates

            # Merge alpha shape polygons
            combined_points = []
            if large.get("alpha_shape"):
                combined_points.extend(large["alpha_shape"])
            if small.get("alpha_shape"):
                combined_points.extend(small["alpha_shape"])
            if len(combined_points) >= 3:
                merged_poly = alphashape.alphashape([tuple(p) for p in combined_points], alpha=alpha)
                if merged_poly.geom_type == 'MultiPolygon':
                    merged_poly = max(merged_poly.geoms, key=lambda p: p.area)
                if isinstance(merged_poly, Polygon):
                    large["alpha_shape"] = [[float(x), float(y)] for x, y in merged_poly.exterior.coords]
                else:
                    large["alpha_shape"] = combined_points
            else:
                large["alpha_shape"] = combined_points

            # Update bounding box
            b1 = large["bbox"]
            b2 = small["bbox"]
            large["bbox"] = {
                "lat_min": min(b1["lat_min"], b2["lat_min"]),
                "lat_max": max(b1["lat_max"], b2["lat_max"]),
                "lon_min": min(b1["lon_min"], b2["lon_min"]),
                "lon_max": max(b1["lon_max"], b2["lon_max"])
            }

        # Separate large and small cells
        max_gates = max(cell["num_gates"] for cell in cells)
        large_cells = [c.copy() for c in cells if c["num_gates"] >= max_gates * size_ratio_threshold]
        small_cells = [c.copy() for c in cells if c["num_gates"] < max_gates * size_ratio_threshold]

        merged_any = True
        while merged_any and small_cells:
            merged_any = False
            remaining_small = []

            for s in small_cells:
                lat_c, lon_c = s["centroid"]
                buffer_lat, buffer_lon = deg_buffer(lat_c, buffer_km)

                # Find adjacent large cells within ~1 km
                adjacent_large_cells = [
                    l for l in large_cells
                    if bboxes_within_distance(s["bbox"], l["bbox"], buffer_lat, buffer_lon)
                ]

                if not adjacent_large_cells:
                    remaining_small.append(s)
                    continue

                # Merge into closest by centroid
                def centroid_dist(c1, c2):
                    lat1, lon1 = c1["centroid"]
                    lat2, lon2 = c2["centroid"]
                    return np.hypot(lat1 - lat2, lon1 - lon2)

                closest_large = min(adjacent_large_cells, key=lambda c: centroid_dist(c, s))
                if s["num_gates"] >= closest_large["num_gates"] * size_ratio_threshold:
                    remaining_small.append(s)
                    continue

                merge_cells(closest_large, s, alpha)
                merged_any = True

            small_cells = remaining_small

        # After merging small nearby cells, also merge any overlapping cells (by polygon intersection)
        merged_cells = large_cells + small_cells

        def _cell_to_polygon(cell):
            """Return a shapely Polygon representing the cell: prefer alpha_shape, fall back to bbox box."""
            try:
                if cell.get('alpha_shape') and len(cell['alpha_shape']) >= 3:
                    return Polygon([(p[0], p[1]) for p in cell['alpha_shape']])
                b = cell.get('bbox')
                if b:
                    return Polygon([(b['lon_min'], b['lat_min']), (b['lon_min'], b['lat_max']), (b['lon_max'], b['lat_max']), (b['lon_max'], b['lat_min'])])
            except Exception:
                return None
            return None

        # Iteratively merge overlapping polygons until stable
        overlap_merged = True
        while overlap_merged:
            overlap_merged = False
            n = len(merged_cells)
            i = 0
            while i < n:
                a = merged_cells[i]
                poly_a = _cell_to_polygon(a)
                if poly_a is None:
                    i += 1
                    continue
                j = i + 1
                merged_this_round = False
                while j < n:
                    b = merged_cells[j]
                    poly_b = _cell_to_polygon(b)
                    if poly_b is None:
                        j += 1
                        continue

                    try:
                        if poly_a.intersects(poly_b) and poly_a.intersection(poly_b).area > 0:
                            # Merge the smaller into the larger (by num_gates)
                            if a['num_gates'] >= b['num_gates']:
                                merge_cells(a, b, alpha)
                                del merged_cells[j]
                            else:
                                merge_cells(b, a, alpha)
                                del merged_cells[i]
                            overlap_merged = True
                            merged_this_round = True
                            break
                    except Exception:
                        # If geometry operation failed, skip
                        pass

                    j += 1

                if not merged_this_round:
                    i += 1
                else:
                    # restart since list has changed
                    n = len(merged_cells)

        return merged_cells

# Penalty cost used instead of np.inf for disallowed pairs (keeps cost matrix finite)
PENALTY_COST = 1000.0

class GeoUtils:
    @staticmethod
    def haversine_dist(coord1, coord2):
        R = 6371  # km
        lat1, lon1 = np.radians(coord1)
        lat2, lon2 = np.radians(coord2)
        dlat = lat2 - lat1
        dlon = lon2 - lon1
        a = math.sin(dlat/2)**2 + math.cos(lat1)*math.cos(lat2)*math.sin(dlon/2)**2
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
        return R * c

    @staticmethod
    def polygon_area_km2(latlon_points):
        """
        Approximate polygon area on Earth's surface in km^2 using spherical excess formula.
        Input: list of (lon, lat) tuples
        """
        if not latlon_points or len(latlon_points) < 3:
            return 0.0  # or np.nan, since a polygon with < 3 points has no area
        
        coords = np.radians(np.array(latlon_points))
        if coords.ndim != 2 or coords.shape[1] != 2:
            return 0.0

        lons = coords[:, 0]
        lats = coords[:, 1]
        # Earth's radius in km
        R = 6371.0
        
        # Calculate area using spherical polygon area formula:
        # Reference: https://trs.jpl.nasa.gov/handle/2014/40409 (equation simplified)
        
        # Compute the angles between edges
        def angle_between_vectors(v1, v2):
            return np.arccos(np.clip(np.dot(v1, v2), -1.0, 1.0))
        
        # Convert lat/lon to 3D Cartesian coordinates
        def latlon_to_xyz(lat, lon):
            x = np.cos(lat) * np.cos(lon)
            y = np.cos(lat) * np.sin(lon)
            z = np.sin(lat)
            return np.array([x, y, z])
        
        vertices = [latlon_to_xyz(lat, lon) for lon, lat in zip(lons, lats)]
        
        n = len(vertices)
        angles = 0.0
        for i in range(n):
            v1 = vertices[i - 1]
            v2 = vertices[i]
            v3 = vertices[(i + 1) % n]
            
            a = v1 - v2
            b = v3 - v2
            
            a /= np.linalg.norm(a)
            b /= np.linalg.norm(b)
            
            angle = np.arccos(np.clip(np.dot(a, b), -1.0, 1.0))
            angles += angle
        
        spherical_excess = angles - (n - 2) * np.pi
        area = spherical_excess * R**2
        return abs(area)

class CellProcessor:
    @staticmethod
    def add_area_to_cells(cells):
        """
        Compute area_km2 for each cell based on convex_hull or alpha_shape.
        """
        for cell in cells:
            polygon = cell.get('convex_hull') or cell.get('alpha_shape')
            if polygon and len(polygon) >= 3:
                cell['area_km2'] = GeoUtils.polygon_area_km2(polygon)
            else:
                cell['area_km2'] = 0.0

    @staticmethod
    def normalize_diff(val1, val2, max_val):
        return abs(val1 - val2) / max_val if max_val > 0 else 0

    @staticmethod
    def compute_cost(cellA, cellB, max_vals, weights):
        dist = GeoUtils.haversine_dist(cellA['centroid'], cellB['centroid'])
        if dist > 10:
            # Use a large finite penalty instead of infinity so the Hungarian solver stays feasible
            return PENALTY_COST

        d_num_gates = CellProcessor.normalize_diff(cellA['num_gates'], cellB['num_gates'], max_vals['num_gates'])  # proxy for area
        d_reflect = CellProcessor.normalize_diff(cellA['max_reflectivity_dbz'], cellB['max_reflectivity_dbz'], max_vals['max_reflectivity_dbz'])

        cost = (weights['distance'] * (dist / 10) +
                weights['num_gates'] * d_num_gates +
                weights['max_reflectivity'] * d_reflect)
        return cost

class CellMatcher:
    @staticmethod
    def match_cells(cells0, cells1, weights=None):
        if weights is None:
            weights = {
                'distance': 0.5,
                'num_gates': 0.3,
                'max_reflectivity': 0.2
            }
        # Quick guards for empty inputs
        n0, n1 = len(cells0), len(cells1)
        if n0 == 0 or n1 == 0:
            print(f"DEBUG: No cells to match (n0={n0}, n1={n1})")
            return []

        # Safely compute max values (fall back to 0 if necessary)
        max_num_gates = 0
        max_reflect = 0
        if n0 > 0:
            max_num_gates = max(max_num_gates, max(cell['num_gates'] for cell in cells0))
            max_reflect = max(max_reflect, max(cell['max_reflectivity_dbz'] for cell in cells0))
        if n1 > 0:
            max_num_gates = max(max_num_gates, max(cell['num_gates'] for cell in cells1))
            max_reflect = max(max_reflect, max(cell['max_reflectivity_dbz'] for cell in cells1))

        max_vals = {
            'num_gates': max_num_gates,
            'max_reflectivity_dbz': max_reflect
        }

        # Build cost matrix and check feasibility before assignment
        cost_matrix = np.full((n0, n1), np.inf)
        for i, c0 in enumerate(cells0):
            for j, c1 in enumerate(cells1):
                cost_matrix[i, j] = CellProcessor.compute_cost(c0, c1, max_vals, weights)

        # If there are no costs below the penalty threshold, there are no reasonable matches
        if not (cost_matrix < PENALTY_COST).any():
            print(f"DEBUG: No candidate pairs with cost < PENALTY_COST (n0={n0}, n1={n1}); no feasible matches.")
            return []

        # Try the Hungarian algorithm first; if it fails (infeasible), fall back to a greedy matcher
        try:
            row_ind, col_ind = linear_sum_assignment(cost_matrix)
            matches = []
            for i, j in zip(row_ind, col_ind):
                if np.isfinite(cost_matrix[i, j]):
                    matches.append((i, j, float(cost_matrix[i, j])))
            return matches
        except ValueError as e:
            print(f"DEBUG: linear_sum_assignment failed: {e}; falling back to greedy matching.")
            # Debug: list cost-matrix info before greedy fallback
            try:
                print(f"DEBUG: cost_matrix shape: {cost_matrix.shape}")
                finite_pairs = [(i, j, float(cost_matrix[i, j]))
                                for i in range(n0) for j in range(n1) if np.isfinite(cost_matrix[i, j])]
                print(f"DEBUG: finite pairs found: {len(finite_pairs)}")
                if len(finite_pairs) > 0:
                    # show up to first 30 candidate pairs (sorted by cost) for quick inspection
                    finite_pairs.sort(key=lambda x: x[2])
                    for idx, (i, j, c) in enumerate(finite_pairs[:30]):
                        print(f"DEBUG: candidate {idx+1}: row={i}, col={j}, cost={c:.6f}")
                else:
                    print("DEBUG: No finite pairs found (unexpected, handled earlier).")

                # If the cost matrix is small, print the entire matrix for full visibility
                if cost_matrix.size <= 400:
                    print("DEBUG: full cost_matrix:")
                    print(np.array2string(cost_matrix, precision=6, threshold=1000, suppress_small=True))
                else:
                    # give a brief summary if too large
                    print("DEBUG: cost matrix too large to display, summarizing ...")
                    finite_costs = [c for (_, _, c) in finite_pairs]
                    if finite_costs:
                        print(f"DEBUG: min_cost={min(finite_costs):.6f}, max_cost={max(finite_costs):.6f}")
            except Exception as dbg_e:
                print(f"DEBUG: failed to print cost matrix details: {dbg_e}")

            # Greedy matching: sort all finite pairs by cost and take the lowest-cost disjoint pairs
            # Note: finite_pairs is sorted above when present
            if 'finite_pairs' not in locals():
                finite_pairs = [(i, j, float(cost_matrix[i, j]))
                                for i in range(n0) for j in range(n1) if np.isfinite(cost_matrix[i, j])]
                finite_pairs.sort(key=lambda x: x[2])

            used_rows = set()
            used_cols = set()
            greedy_matches = []
            for i, j, c in finite_pairs:
                if i in used_rows or j in used_cols:
                    continue
                used_rows.add(i)
                used_cols.add(j)
                greedy_matches.append((i, j, c))

            return greedy_matches

class Visualizer:
    @staticmethod
    def plot_radar_and_cells(refl, lat_grid, lon_grid, cells0, cells1, matches):

        # Check if lon is decreasing, then flip arrays to make lon increasing
        if lon_grid[0, 1] < lon_grid[0, 0]:
            lon_grid = np.flip(lon_grid, axis=1)
            refl = np.flip(refl, axis=1)

        fig, ax = plt.subplots(figsize=(12, 10))

        pcm = ax.pcolormesh(lon_grid, lat_grid, refl, cmap='NWSRef', shading='auto', vmin=0, vmax=80)
        fig.colorbar(pcm, ax=ax, label='Reflectivity (dBZ)')

        # Plot cells, same as before
        for cell in cells0:
            polygon = cell.get('convex_hull')
            if not polygon:
                polygon = cell.get('alpha_shape', [])
            if polygon:
                hull_lon, hull_lat = zip(*polygon)
                ax.fill(hull_lon, hull_lat, color='blue', alpha=0.3)
            ax.plot(cell['centroid'][1], cell['centroid'][0], 'bo')

        for cell in cells1:
            polygon = cell.get('convex_hull')
            if not polygon:
                polygon = cell.get('alpha_shape', [])
            if polygon:
                hull_lon, hull_lat = zip(*polygon)
                ax.fill(hull_lon, hull_lat, color='red', alpha=0.3)
            ax.plot(cell['centroid'][1], cell['centroid'][0], 'ro')

        for i, j, cost in matches:
            c0 = cells0[i]['centroid']
            c1 = cells1[j]['centroid']
            ax.plot([c0[1], c1[1]], [c0[0], c1[0]], 'k--', linewidth=1)

        ax.set_xlabel('Longitude')
        ax.set_ylabel('Latitude')
        ax.set_title('Radar Matches')

        plt.show()

class StormCellTracker:
    @staticmethod
    def update_storm_cell_history(existing_cell, new_data):
        """
        Update a storm cell's history, preventing duplicate timestamp entries
        
        Args:
            existing_cell (dict): The existing cell data with history
            new_data (dict): New data to potentially add to history
        
        Returns:
            bool: True if history was updated, False if duplicate was found
        """
        # Extract the timestamp from new data
        new_timestamp = new_data.get('timestamp')
        if not new_timestamp:
            return False  # No timestamp, can't add to history
        
        # Check if this timestamp already exists in history
        for history_entry in existing_cell.get('storm_history', []):
            if history_entry.get('timestamp') == new_timestamp:
                # Update the existing entry with new data instead of adding a duplicate
                history_entry.update(new_data)
                return True  # Entry was updated
        
        # If we get here, this is a new timestamp - add it to history
        if 'storm_history' not in existing_cell:
            existing_cell['storm_history'] = []
        
        existing_cell['storm_history'].append(new_data)
        return True  # New entry was added

    @staticmethod
    def process_matched_cell(existing_cell, new_cell_data, timestamp):
        """
        Process a matched cell: preserve the existing ID and history, 
        update all other properties with new data, and append new scan to history
        """
        # Store the existing ID and history before updating
        existing_id = existing_cell["id"]
        existing_history = existing_cell.get("storm_history", [])
        
        # Update ALL properties except ID and history with new data
        existing_cell.update({
            # DO NOT update the ID - keep the original tracking ID
            "num_gates": new_cell_data["num_gates"],
            "centroid": new_cell_data["centroid"],
            "bbox": new_cell_data.get("bbox", {}),
            "alpha_shape": new_cell_data.get("alpha_shape", []),
            "max_reflectivity_dbz": new_cell_data["max_reflectivity_dbz"],
            # Keep the existing storm_history intact
            "storm_history": existing_history
        })
        
        # Create the new snapshot for this scan
        new_snapshot = {
            "timestamp": timestamp,
            "max_reflectivity_dbz": new_cell_data["max_reflectivity_dbz"],
            "num_gates": new_cell_data["num_gates"],
            "centroid": new_cell_data["centroid"],
        }
        
        # Check if this timestamp already exists in history
        timestamp_exists = False
        for hist_entry in existing_history:
            if hist_entry.get("timestamp") == timestamp:
                # Update the existing entry with new data
                hist_entry.update(new_snapshot)
                timestamp_exists = True
                break
        
        # If timestamp doesn't exist, append new entry
        if not timestamp_exists:
            existing_history.append(new_snapshot)
        
        # Keep history sorted by timestamp
        existing_history.sort(key=lambda h: h["timestamp"])
        
        print(f"DEBUG: Updated cell ID {existing_id} with data from new cell ID {new_cell_data['id']}")
        return True
    
class StormVectorCalculator:
    """
    A class to calculate and manage storm vectors for detected storm cells.
    """
    
    def __init__(self, min_magnitude_m: float = 9000.0):
        """
        Initialize the StormVectorCalculator.
        
        Args:
            min_magnitude_m (float): Minimum magnitude threshold for filtering vectors
        """
        self.min_magnitude_m = min_magnitude_m
    
    def calculate_storm_vectors(self, cells: List[Dict]) -> List[Dict]:
        """
        Calculate vector components for storm cells based on their movement history.
        
        Args:
            cells (list): List of storm cell dictionaries
            
        Returns:
            list: Vector information for each cell with valid history
        """
        vectors = []
        for cell in cells:
            vector_data = self._calculate_cell_vector(cell)
            if vector_data:
                vectors.append(vector_data)
        return vectors
    
    def _calculate_cell_vector(self, cell: Dict) -> Optional[Dict]:
        """
        Calculate vector components for a single storm cell.
        
        Args:
            cell (dict): Storm cell dictionary
            
        Returns:
            dict: Vector information or None if insufficient history
        """
        history = cell.get('storm_history', [])
        if len(history) < 2:
            return None
        
        # Sort history by timestamp (oldest to newest)
        history_sorted = sorted(history, key=lambda x: x['timestamp'])
        h0, h1 = history_sorted[-2], history_sorted[-1]
        
        # Extract centroids and timestamps
        c0 = h0['centroid']
        c1 = h1['centroid']
        t0 = h0['timestamp']
        t1 = h1['timestamp']
        
        # Parse timestamps
        dt0, dt1 = self._parse_timestamps(t0, t1)
        dt_seconds = (dt1 - dt0).total_seconds()
        
        # Calculate movement in meters
        dx, dy = self._calculate_movement(c0, c1, dt_seconds)
        
        # Add dx, dy, dt to the latest (h1) history entry
        h1['dx'] = dx
        h1['dy'] = dy
        h1['dt'] = dt_seconds
        
        return {
            'id': cell['id'],
            'dx': dx,
            'dy': dy,
            'dt': dt_seconds,
            't0': t0,
            't1': t1,
            'c0': c0,
            'c1': c1
        }
    
    def _parse_timestamps(self, t0: str, t1: str) -> Tuple[datetime, datetime]:
        """
        Parse timestamp strings into datetime objects.
        
        Args:
            t0 (str): First timestamp string
            t1 (str): Second timestamp string
            
        Returns:
            tuple: (datetime, datetime) objects for the two timestamps
        """
        try:
            dt0 = datetime.fromisoformat(t0)
            dt1 = datetime.fromisoformat(t1)
        except Exception:
            # Fallback to filename timestamp extraction if needed
            from . import timestamp  # Assuming this module exists
            dt0 = datetime.fromisoformat(timestamp.extract_timestamp_from_filename(t0))
            dt1 = datetime.fromisoformat(timestamp.extract_timestamp_from_filename(t1))
        
        return dt0, dt1
    
    def _calculate_movement(self, c0: List[float], c1: List[float], dt_seconds: float) -> Tuple[float, float]:
        """
        Calculate movement in meters between two points.
        
        Args:
            c0 (list): First centroid [lat, lon]
            c1 (list): Second centroid [lat, lon]
            dt_seconds (float): Time difference in seconds
            
        Returns:
            tuple: (dx, dy) movement in meters
        """
        avg_lat = (c0[0] + c1[0]) / 2
        deg_to_m_lat = 111320.0
        deg_to_m_lon = 111320.0 * math.cos(math.radians(avg_lat))
        
        dx = (c1[1] - c0[1]) * deg_to_m_lon
        dy = (c1[0] - c0[0]) * deg_to_m_lat
        
        return dx, dy
    
    def clean_vectors(self, cells: List[Dict]) -> List[Dict]:
        """
        Remove cells whose latest vector magnitude exceeds the threshold.
        
        Args:
            cells (list): List of cell dicts (modified in-place)
            
        Returns:
            list: Information about removed cells
        """
        removed = []
        kept = []
        
        for cell in cells:
            should_keep, removal_info = self._evaluate_cell(cell)
            if should_keep:
                kept.append(cell)
            elif removal_info:
                removed.append(removal_info)
        
        # Update the original list
        cells.clear()
        cells.extend(kept)
        
        return removed
    
    def _evaluate_cell(self, cell: Dict) -> Tuple[bool, Optional[Dict]]:
        """
        Evaluate whether a cell should be kept based on its vector magnitude.
        
        Args:
            cell (dict): Storm cell dictionary
            
        Returns:
            tuple: (should_keep, removal_info) where removal_info is None if keeping
        """
        hist = cell.get('storm_history', [])
        if not hist:
            return True, None
        
        # Assume history is sorted oldest->newest; take last
        latest = hist[-1]
        dx = latest.get('dx')
        dy = latest.get('dy')
        
        if dx is None or dy is None:
            # No vector recorded, keep the cell
            return True, None
        
        try:
            mag = math.hypot(float(dx), float(dy))
        except Exception:
            return True, None
        
        if mag > self.min_magnitude_m:
            return False, {'id': cell.get('id'), 'magnitude_m': mag}
        else:
            return True, None


def write_vectors():
    """
    Command-line interface function to calculate and write storm vectors.
    """
    import sys
    
    # Default path or from command line
    json_path = sys.argv[1] if len(sys.argv) > 1 else "stormcell_test.json"
    
    # Initialize the vector calculator
    calculator = StormVectorCalculator(min_magnitude_m=9000.0)
    
    # Load cells from JSON file
    with open(json_path, 'r') as f:
        cells = json.load(f)
    
    # Calculate vectors
    vectors = calculator.calculate_storm_vectors(cells)
    
    # Clean vectors with default threshold
    removed_cells = calculator.clean_vectors(cells)
    
    # Write updated cells back to file
    with open(json_path, 'w') as f:
        json.dump(cells, f, indent=4)
    
    # Print vectors
    for v in vectors:
        print(f"id: {v['id']}, dx: {v['dx']:.2f} m, dy: {v['dy']:.2f} m, dt: {v['dt']} s")
    
    # Print removed cells
    if removed_cells:
        print("\nRemoved cells due to high vector magnitude:")
        for r in removed_cells:
            print(f"id: {r['id']}, magnitude: {r['magnitude_m']:.2f} m")