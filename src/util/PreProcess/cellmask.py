import numpy as np
from scipy.ndimage import label, binary_dilation, center_of_mass
import alphashape
from shapely.geometry import Point, LineString
import datetime
from .data_utils import extract_timestamp_from_filename, CellProcessor, PENALTY_COST
from scipy.optimize import linear_sum_assignment

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

        # Pre-Process: Check if there is any reflectivity exceeding the seed dbz
        if not np.any(refl_data >= seed_dbz):
            print("Error: No reflectivity values greater than seed dbz in data")
            return []

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

            if np.any(high_refl_mask):
                # Use reflectivity-weighted centroid for high-reflectivity core
                y_indices, x_indices = np.where(high_refl_mask)
                weights = refl_data[high_refl_mask]
                
                # Calculate weighted mean position
                weighted_y = np.sum(y_indices * weights) / np.sum(weights)
                weighted_x = np.sum(x_indices * weights) / np.sum(weights)
                
                centroid_idx = (weighted_y, weighted_x)
            else:
                # Fall back to binary centroid
                centroid_idx = center_of_mass(mask)

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

        # Pre-Process: Check if cells contains any data
        if not cells or len(cells) == 0:
            print("Error: No cells detected in input")
            return []

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

class CellMatcher:
    @staticmethod

    # TO DO: Need to treat old cells that are >75% covered by a new cell as terminated if not matched to the new cell.

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

        # Safely compute max values (fall back to 1 if necessary to avoid division by zero)
        max_num_gates = 1.0
        max_reflect = 1.0
        
        # Combine both cell sets to find global max values
        all_cells = cells0 + cells1
        if all_cells:
            try:
                max_num_gates = max(max_num_gates, max(cell.get('num_gates', 0) for cell in all_cells))
                max_reflect = max(max_reflect, max(cell.get('max_reflectivity_dbz', 0) for cell in all_cells))
            except (ValueError, KeyError):
                # Handle cases where keys might be missing
                pass

        max_vals = {
            'num_gates': max_num_gates,
            'max_reflectivity_dbz': max_reflect
        }

        # Build cost matrix and check feasibility before assignment
        cost_matrix = np.full((n0, n1), np.inf)
        for i, c0 in enumerate(cells0):
            for j, c1 in enumerate(cells1):
                # Calculate distance between centroids first
                lat1, lon1 = c0.get('centroid', [0, 0])
                lat2, lon2 = c1.get('centroid', [0, 0])
                
                # Calculate dx and dy in km (approximate conversion)
                # 1° latitude ≈ 111 km, 1° longitude ≈ 111 km * cos(latitude)
                dx_km = abs(lon1 - lon2) * 111.0 * np.cos(np.radians((lat1 + lat2) / 2))
                dy_km = abs(lat1 - lat2) * 111.0
                
                # Check if either dx or dy exceeds 10 km
                if dx_km > 10.0 or dy_km > 10.0:
                    cost_matrix[i, j] = np.inf  # Disallow this match
                else:
                    cost_matrix[i, j] = CellMatcher.compute_cost(c0, c1, max_vals, weights)

        # If there are no costs below the penalty threshold, there are no reasonable matches
        if not (cost_matrix < PENALTY_COST).any():
            print(f"DEBUG: No candidate pairs with cost < PENALTY_COST (n0={n0}, n1={n1}); no feasible matches.")
            return []

        # Try the Hungarian algorithm first; if it fails (infeasible), fall back to a greedy matcher
        try:
            row_ind, col_ind = linear_sum_assignment(cost_matrix)
            matches = []
            for i, j in zip(row_ind, col_ind):
                if np.isfinite(cost_matrix[i, j]) and cost_matrix[i, j] < PENALTY_COST:
                    matches.append((i, j, float(cost_matrix[i, j])))
            return matches
        except Exception as e:
            print(f"DEBUG: linear_sum_assignment failed: {e}; falling back to greedy matching.")
            
            # Debug: list cost-matrix info before greedy fallback
            try:
                print(f"DEBUG: cost_matrix shape: {cost_matrix.shape}")
                finite_pairs = [(i, j, float(cost_matrix[i, j]))
                                for i in range(n0) for j in range(n1) 
                                if np.isfinite(cost_matrix[i, j]) and cost_matrix[i, j] < PENALTY_COST]
                print(f"DEBUG: finite pairs found: {len(finite_pairs)}")
                
                if finite_pairs:
                    finite_pairs.sort(key=lambda x: x[2])
                    for idx, (i, j, c) in enumerate(finite_pairs[:30]):
                        print(f"DEBUG: candidate {idx+1}: row={i}, col={j}, cost={c:.6f}")
                else:
                    print("DEBUG: No finite pairs found below penalty threshold.")
                    
            except Exception as dbg_e:
                print(f"DEBUG: failed to print cost matrix details: {dbg_e}")

            # Greedy matching: sort all finite pairs by cost and take the lowest-cost disjoint pairs
            finite_pairs = [(i, j, float(cost_matrix[i, j]))
                            for i in range(n0) for j in range(n1) 
                            if np.isfinite(cost_matrix[i, j]) and cost_matrix[i, j] < PENALTY_COST]
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

    @staticmethod
    def compute_cost(cell0, cell1, max_vals, weights):
        """
        Compute cost between two cells based on distance, num_gates, and reflectivity
        """

        # Guard against empty inputs
        n0, n1 = len(cell0), len(cell1)
        if n0 == 0 or n1 == 0:
            print("Error: No cells detected in input")
            return []
        # Extract values with defaults
        # Calculate distance between centroids
        lat1, lon1 = cell0.get('centroid', [0, 0])
        lat2, lon2 = cell1.get('centroid', [0, 0])
        dist = np.sqrt((lat1 - lat2)**2 + (lon1 - lon2)**2)
        
        num_gates0 = cell0.get('num_gates', 0)
        num_gates1 = cell1.get('num_gates', 0)
        
        reflect0 = cell0.get('max_reflectivity_dbz', 0)
        reflect1 = cell1.get('max_reflectivity_dbz', 0)
        
        # Normalize differences (0-1 range)
        norm_dist = min(dist / 10.0, 1.0)  # Adjust scaling as needed (10 degrees max distance)
        norm_gates_diff = abs(num_gates0 - num_gates1) / max_vals['num_gates']
        norm_reflect_diff = abs(reflect0 - reflect1) / max_vals['max_reflectivity_dbz']
        
        # Weighted cost
        cost = (weights['distance'] * norm_dist +
                weights['num_gates'] * norm_gates_diff +
                weights['max_reflectivity'] * norm_reflect_diff)
        
        return cost