import numpy as np
from scipy.optimize import linear_sum_assignment
import matplotlib.pyplot as plt
import math


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