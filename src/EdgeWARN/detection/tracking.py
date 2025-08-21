from . import detect
import util.file as fs
import numpy as np
from scipy.optimize import linear_sum_assignment
import matplotlib.pyplot as plt
import math
from pathlib import Path
import json

import util.file as fs

def haversine_dist(coord1, coord2):
    R = 6371  # km
    lat1, lon1 = np.radians(coord1)
    lat2, lon2 = np.radians(coord2)
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = math.sin(dlat/2)**2 + math.cos(lat1)*math.cos(lat2)*math.sin(dlon/2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c

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

# Add area calculation to each cell
def add_area_to_cells(cells):
    """
    Compute area_km2 for each cell based on convex_hull or alpha_shape.
    """
    for cell in cells:
        polygon = cell.get('convex_hull') or cell.get('alpha_shape')
        if polygon and len(polygon) >= 3:
            cell['area_km2'] = polygon_area_km2(polygon)
        else:
            cell['area_km2'] = 0.0

def normalize_diff(val1, val2, max_val):
    return abs(val1 - val2) / max_val if max_val > 0 else 0

def compute_cost(cellA, cellB, max_vals, weights):
    dist = haversine_dist(cellA['centroid'], cellB['centroid'])
    if dist > 10:
        return np.inf  # beyond 10 km, discard

    d_num_gates = normalize_diff(cellA['num_gates'], cellB['num_gates'], max_vals['num_gates'])  # proxy for area
    d_reflect = normalize_diff(cellA['max_reflectivity_dbz'], cellB['max_reflectivity_dbz'], max_vals['max_reflectivity_dbz'])

    cost = (weights['distance'] * (dist / 10) +
            weights['num_gates'] * d_num_gates +
            weights['max_reflectivity'] * d_reflect)
    return cost

# --- Matching function stays the same ---
def match_cells(cells0, cells1, weights=None):
    if weights is None:
        weights = {
            'distance': 0.5,
            'num_gates': 0.3,
            'max_reflectivity': 0.2
        }

    max_vals = {
        'num_gates': max(max(cell['num_gates'] for cell in cells0), max(cell['num_gates'] for cell in cells1)),
        'max_reflectivity_dbz': max(max(cell['max_reflectivity_dbz'] for cell in cells0),
                                    max(cell['max_reflectivity_dbz'] for cell in cells1))
    }

    n0, n1 = len(cells0), len(cells1)
    cost_matrix = np.full((n0, n1), np.inf)

    for i, c0 in enumerate(cells0):
        for j, c1 in enumerate(cells1):
            cost_matrix[i, j] = compute_cost(c0, c1, max_vals, weights)

    row_ind, col_ind = linear_sum_assignment(cost_matrix)

    matches = []
    for i, j in zip(row_ind, col_ind):
        if cost_matrix[i, j] != np.inf:
            matches.append((i, j, cost_matrix[i, j]))

    return matches

def plot_radar_and_cells(refl, lat_grid, lon_grid, cells0, cells1, matches):

    # Check if lon is decreasing, then flip arrays to make lon increasing
    if lon_grid[0, 1] < lon_grid[0, 0]:
        lon_grid = np.flip(lon_grid, axis=1)
        refl = np.flip(refl, axis=1)

    fig, ax = plt.subplots(figsize=(12, 10))

    pcm = ax.pcolormesh(lon_grid, lat_grid, refl, cmap='NWSRef', shading='auto', vmin=0, vmax=75)
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

def process_matched_cell(existing_cell, new_cell_data, timestamp):
    """
    Process a matched cell and update its history without duplicates
    
    Args:
        existing_cell (dict): The existing cell to update
        new_cell_data (dict): New cell data from current scan
        timestamp (str): Current timestamp
    
    Returns:
        bool: True if history was updated, False otherwise
    """
    # Create history entry
    history_entry = {
        'timestamp': timestamp,
        'max_reflectivity_dbz': new_cell_data['max_reflectivity_dbz'],
        'num_gates': new_cell_data['num_gates'],
        'centroid': new_cell_data['centroid']
    }
    
    # Update cell properties
    existing_cell.update({
        'num_gates': new_cell_data['num_gates'],
        'centroid': new_cell_data['centroid'],
        'bbox': new_cell_data.get('bbox', existing_cell.get('bbox')),
        'alpha_shape': new_cell_data.get('alpha_shape', existing_cell.get('alpha_shape'))
    })
    
    # Update history without duplicates
    return update_storm_cell_history(existing_cell, history_entry)

def main():
    filepath_old = Path(r"C:\input_data\MRMS_MergedReflectivityQC_3D_20250804-235241_renamed.nc")
    filepath_new = Path(r"C:\input_data\MRMS_MergedReflectivityQC_3D_20250805-000242_renamed.nc")
    storm_json = Path("stormcell_test.json")
    lat_limits = (35, 40)
    lon_limits = (252, 258.5)

    print("=== DEBUG: Starting tracking process ===")
    
    # --- Detect cells from each scan ---
    print("DEBUG: Detecting cells from old scan...")
    cells_old, _ = detect.detect_cells(filepath_old, lat_limits, lon_limits, plot=False)
    print(f"DEBUG: Found {len(cells_old)} cells in old scan: {[cell['id'] for cell in cells_old]}")

    if cells_old:
        print(f"DEBUG: Old scan timestamp: {cells_old[0]['storm_history'][0]['timestamp']}")
    
    print("DEBUG: Detecting cells from new scan...")
    cells_new, _ = detect.detect_cells(filepath_new, lat_limits, lon_limits, plot=False)
    print(f"DEBUG: Found {len(cells_new)} cells in new scan: {[cell['id'] for cell in cells_new]}")

    if cells_new:
        print(f"DEBUG: New scan timestamp: {cells_new[0]['storm_history'][0]['timestamp']}")

    # --- Match cells between scans ---
    print("DEBUG: Matching cells between scans...")
    matches = match_cells(cells_old, cells_new)
    print(f"DEBUG: Found {len(matches)} matches: {matches}")

    # --- Load existing storm data ---
    print("DEBUG: Loading existing storm data...")
    if storm_json.exists():
        with open(storm_json, "r") as f:
            storm_data = json.load(f)
        print(f"DEBUG: Loaded {len(storm_data)} existing cells: {[cell['id'] for cell in storm_data]}")
        
        # CRITICAL FIX: Remove duplicate entries and keep only the most recent version of each cell
        unique_cells = {}
        for cell in storm_data:
            cell_id = cell['id']
            if cell_id not in unique_cells:
                unique_cells[cell_id] = cell
            else:
                # Merge history from both cells while preserving unique timestamps
                existing_cell = unique_cells[cell_id]
                
                # Collect all unique timestamps from both cells
                existing_timestamps = {entry["timestamp"] for entry in existing_cell["storm_history"]}
                new_timestamps = {entry["timestamp"] for entry in cell["storm_history"]}
                
                # If the new cell has unique timestamps, add them to history
                for history_entry in cell["storm_history"]:
                    if history_entry["timestamp"] not in existing_timestamps:
                        existing_cell["storm_history"].append(history_entry)
                
                # Always keep the most recent properties (from whichever cell has the latest timestamp)
                existing_last_time = existing_cell["storm_history"][-1]["timestamp"]
                new_last_time = cell["storm_history"][-1]["timestamp"]
                
                if new_last_time > existing_last_time:
                    # Update current properties with the newer cell's data
                    existing_cell["num_gates"] = cell["num_gates"]
                    existing_cell["centroid"] = cell["centroid"]
                    existing_cell["bbox"] = cell.get("bbox", {})
                    existing_cell["alpha_shape"] = cell.get("alpha_shape", [])
                    existing_cell["max_reflectivity_dbz"] = cell["max_reflectivity_dbz"]

        storm_data = list(unique_cells.values())
        print(f"DEBUG: After deduplication: {len(storm_data)} unique cells: {[cell['id'] for cell in storm_data]}")
    else:
        storm_data = []
        print("DEBUG: No existing storm data found, starting fresh")

    # Create index of existing cells by ID
    existing_cells = {cell['id']: cell for cell in storm_data}
    print(f"DEBUG: Existing cells index: {list(existing_cells.keys())}")

    # Process matched cells
    print("DEBUG: Processing matched cells...")
    for match_idx, (i, j, cost) in enumerate(matches):
        old_cell = cells_old[i]
        new_cell = cells_new[j]
        current_timestamp = new_cell["storm_history"][0]["timestamp"]
        
        print(f"DEBUG: Match {match_idx + 1}: Old cell ID {old_cell['id']} -> New cell ID {new_cell['id']} (cost: {cost:.3f})")
        
        # Check if the NEW cell ID exists in our storm data
        if new_cell['id'] in existing_cells:
            print(f"DEBUG:   Cell ID {new_cell['id']} exists in storm data - updating")
            existing_cell = existing_cells[new_cell['id']]
            
            # Use the new function to update without duplicates
            updated = process_matched_cell(existing_cell, new_cell, current_timestamp)
            
            if updated:
                print(f"DEBUG:   Updated cell properties for ID {new_cell['id']}")
            else:
                print(f"DEBUG:   No update needed for ID {new_cell['id']} (duplicate timestamp)")
        else:
            print(f"DEBUG:   Cell ID {new_cell['id']} is new - adding to storm data")
            # New cell - add it to our data
            storm_data.append(new_cell)
            existing_cells[new_cell['id']] = new_cell
            print(f"DEBUG:   Added new cell ID {new_cell['id']} to storm data")

    # Add unmatched new cells (completely new detections)
    print("DEBUG: Processing unmatched new cells...")
    matched_new_indices = {j for _, j, _ in matches}
    unmatched_count = 0
    for j, new_cell in enumerate(cells_new):
        if j not in matched_new_indices and new_cell['id'] not in existing_cells:
            print(f"DEBUG:   Found unmatched new cell ID {new_cell['id']} - adding")
            storm_data.append(new_cell)
            existing_cells[new_cell['id']] = new_cell
            unmatched_count += 1
    
    print(f"DEBUG: Added {unmatched_count} unmatched new cells")

    # --- Save updated JSON ---
    print("DEBUG: Saving updated JSON...")
    detect.save_cells_to_json(storm_data, storm_json)

    # Debug: Verify final structure
    print("DEBUG: Final storm data structure:")
    for cell in storm_data:
        print(f"  Cell ID {cell['id']}: {len(cell['storm_history'])} history entries")
        for hist_idx, hist_entry in enumerate(cell['storm_history']):
            print(f"    History {hist_idx + 1}: {hist_entry['timestamp']} - {hist_entry['num_gates']} gates")

    print(f"=== DEBUG: Completed tracking process ===")
    print(f"Updated {storm_json} with {len(matches)} matched pairs and {unmatched_count} new cells.")
    print(f"Total cells in database: {len(storm_data)}")

if __name__ == "__main__":
    main()