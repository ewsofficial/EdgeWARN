from . import detect
import util.file as fs
import numpy as np
from scipy.optimize import linear_sum_assignment
import matplotlib.pyplot as plt
import math
from pathlib import Path
import json

import util.file as fs

filepath_old = r"C:\input_data\MRMS_MergedReflectivityQC_3D_20250804-235241_renamed.nc" 
filepath_new = r"C:\input_data\MRMS_MergedReflectivityQC_3D_20250805-000242_renamed.nc"


"""
# Use this code for production models
latest_files = fs.latest_mosaic(2)
filepath_old = latest_files[0]
filepath_new = latest_files[1]
"""

lat_limits = (35, 40)
lon_limits = (252, 258.5)

refl, lat_grid, lon_grid = detect.load_mrms_slice(filepath_old, lat_limits, lon_limits)

cells0 = detect.detect_cells(filepath_old, lat_limits, lon_limits, "stormcell_test.json", plot=True)
cells1 = detect.detect_cells(filepath_new, lat_limits, lon_limits, "stormcell_test.json", plot=True)

print(f"Cells from old scan: {cells0}")
print(f"Cells from new scan: {cells1}")


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
    for cell in cells:
        polygon = cell.get('convex_hull')
        print("WARN:Convex Hull not found within cell dict. Switching to alpha_shape")
        # If convex_hull is missing or empty, try alpha_shape
        if not polygon:
            polygon = cell.get('alpha_shape')

        if polygon and len(polygon) >= 3:
            cell['area_km2'] = polygon_area_km2(polygon)
        else:
            cell['area_km2'] = 0.0


add_area_to_cells(cells0)
add_area_to_cells(cells1)

def normalize_diff(val1, val2, max_val):
    return abs(val1 - val2) / max_val if max_val > 0 else 0

def compute_cost(cellA, cellB, max_vals, weights):
    dist = haversine_dist(cellA['centroid_latlon'], cellB['centroid_latlon'])
    if dist > 10:
        return np.inf  # beyond 10 km, discard

    d_area = normalize_diff(cellA['area_km2'], cellB['area_km2'], max_vals['area_km2'])
    d_num_gates = normalize_diff(cellA['num_gates'], cellB['num_gates'], max_vals['num_gates'])  # proxy for area
    d_reflect = normalize_diff(cellA['max_reflectivity_dbz'], cellB['max_reflectivity_dbz'], max_vals['max_reflectivity_dbz'])

    cost = (weights['distance'] * (dist / 10) +
            weights['area'] * d_area +
            weights['num_gates'] * d_num_gates +
            weights['max_reflectivity'] * d_reflect)
    return cost

def match_cells(cells0, cells1, weights=None):
    if weights is None:
        weights = {
            'distance': 0.4,
            'area': 0.2,
            'num_gates': 0.2,
            'max_reflectivity': 0.2
        }

    max_vals = {
        'area_km2': max(max(cell['area_km2'] for cell in cells0), max(cell['area_km2'] for cell in cells1)),
        'num_gates': max(max(cell['num_gates'] for cell in cells0), max(cell['num_gates'] for cell in cells1)),
        'max_reflectivity_dbz': max(max(cell['max_reflectivity_dbz'] for cell in cells0), max(cell['max_reflectivity_dbz'] for cell in cells1))
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
        ax.plot(cell['centroid_latlon'][1], cell['centroid_latlon'][0], 'bo')

    for cell in cells1:
        polygon = cell.get('convex_hull')
        if not polygon:
            polygon = cell.get('alpha_shape', [])
        if polygon:
            hull_lon, hull_lat = zip(*polygon)
            ax.fill(hull_lon, hull_lat, color='red', alpha=0.3)
        ax.plot(cell['centroid_latlon'][1], cell['centroid_latlon'][0], 'ro')

    for i, j, cost in matches:
        c0 = cells0[i]['centroid_latlon']
        c1 = cells1[j]['centroid_latlon']
        ax.plot([c0[1], c1[1]], [c0[0], c1[0]], 'k--', linewidth=1)

    ax.set_xlabel('Longitude')
    ax.set_ylabel('Latitude')
    ax.set_title('Radar Matches you fat fuck, now lock in')

    plt.show()

matches = match_cells(cells0, cells1)
plot_radar_and_cells(refl, lat_grid, lon_grid, cells0, cells1, matches)

storm_json = Path("stormcell_test.json")

# Load existing storm history
if storm_json.exists():
    with storm_json.open("r") as f:
        storm_data = json.load(f)
else:
    storm_data = []

# Convert to dict by centroid for quick lookup (or use your own unique ID if available)
def cell_key(cell):
    # Round centroid for stability in matching across runs
    lat, lon = cell["centroid_latlon"]
    return f"{round(lat,4)}_{round(lon,4)}"

storm_index = {cell_key(c): idx for idx, c in enumerate(storm_data)}

# Update matched cells
updated = set()
for i, j, cost in matches:
    new_cell = cells1[j]
    key = cell_key(cells0[i])

    if key in storm_index:
        # Replace old with new data
        storm_data[storm_index[key]] = new_cell
        updated.add(j)

# Add unmatched new cells
for j, cell in enumerate(cells1):
    if j not in updated:
        storm_data.append(cell)

# Save back to JSON
with storm_json.open("w") as f:
    json.dump(storm_data, f, indent=2)

print(f"Storm data updated in {storm_json}")