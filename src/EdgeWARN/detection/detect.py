import numpy as np
import xarray as xr
from scipy.ndimage import label, binary_dilation, center_of_mass
import alphashape
from shapely.geometry import Point, Polygon, LineString
import json
import time
import sys
from pathlib import Path
import datetime
from datetime import datetime
import pyart
import re

import matplotlib.pyplot as plt
import matplotlib.cm as cm
import matplotlib.colors as mcolors
import cartopy.crs as ccrs
import cartopy.feature as cfeature

import util.file as fs

"""
"""

print("EdgeWARN Storm Cell Detection")
print("Build: 2025-08-17 Debugged")
print("NOT FOR EXTERNAL USE")
time.sleep(1)

def bbox_to_points(bbox):
    if bbox is None:
        return None
    return [
        [bbox["lon_min"], bbox["lat_min"]],  # lower-left
        [bbox["lon_min"], bbox["lat_max"]],  # upper-left
        [bbox["lon_max"], bbox["lat_max"]],  # upper-right
        [bbox["lon_max"], bbox["lat_min"]],  # lower-right
        [bbox["lon_min"], bbox["lat_min"]],  # close polygon to first point
    ]

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

def convert_lon_0_360_to_pm180(lon):
    """
    Helper function: Converts longitude from a 0-360 range to +-180 range
    Parameters:
    - lon: 2D array of longitude values in 0-360 range.
    Returns:
    - lon: 2D array of longitude values converted to -180 to 180 range.
    """
    return np.where(lon > 180, lon - 360, lon)

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

    # Slice reflectivity and coords
    refl_crop = refl.isel({lat_dim: slice(y_start, y_end), lon_dim: slice(x_start, x_end)}).values
    lat_crop = lat.isel({lat_dim: slice(y_start, y_end), lon_dim: slice(x_start, x_end)}).values
    lon_crop = lon.isel({lat_dim: slice(y_start, y_end), lon_dim: slice(x_start, x_end)}).values

    ds.close()
    return refl_crop, lat_crop, lon_crop

def get_alpha_shape_from_mask(mask, lat_grid, lon_grid, alpha=0.1):
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

    if filepath:
        # Extract timestamp from filename like 20250804-235241
        m = re.search(r'(\d{8}-\d{6})', str(filepath))
        if m:
            timestamp_str = m.group(1)
            scan_time = datetime.strptime(timestamp_str, "%Y%m%d-%H%M%S").isoformat()
        else:
            scan_time = datetime.utcnow().isoformat()
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

        bbox = polygon_to_bbox(poly)

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

def merge_connected_small_cells(cells, size_ratio_threshold=0.35, buffer_km=1.0, alpha=0.1):
    """
    Merge small storm cells into nearby larger cells if bounding boxes are within ~1 km.

    Parameters:
    - cells: list of cell dicts with keys 'num_gates', 'centroid_latlon', 'alpha_shape', 'bbox'.
    - size_ratio_threshold: fraction threshold to define "small" cells.
    - buffer_km: distance in km to consider small and large cells "adjacent".
    - alpha: alpha parameter for alphashape when recalculating merged polygon.

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
        large["centroid"] = (
            (lat1 * large["num_gates"] + lat2 * small["num_gates"]) / total_gates,
            (lon1 * large["num_gates"] + lon2 * small["num_gates"]) / total_gates
        )
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

    return large_cells + small_cells
    
def convert_to_json_serializable(obj, float_precision=6):
    """
    Recursively convert NumPy and float data types to native Python types,
    rounding floats to a fixed number of decimal places.
    """
    if isinstance(obj, dict):
        return {k: convert_to_json_serializable(v, float_precision) for k, v in obj.items()}
    elif isinstance(obj, (list, tuple, np.ndarray)):
        return [convert_to_json_serializable(v, float_precision) for v in obj]
    elif isinstance(obj, (np.float32, np.float64, float)):
        return round(float(obj), float_precision)
    elif isinstance(obj, (np.int32, np.int64, int)):
        return int(obj)
    else:
        return obj

def save_cells_to_json(cells, filepath, float_precision=4):
    """
    Save detected storm cells to a JSON file, keeping bbox, alpha_shape, and storm history.
    Rounds numeric values to the specified float_precision.
    """
    json_data = []
    for cell in cells:
        cell_entry = {
            "id": int(cell["id"]),
            "num_gates": int(cell["num_gates"]),
            "centroid": [round(float(v), float_precision) for v in cell["centroid"]],
            "bbox": cell.get("bbox", {}),
            "alpha_shape": [
                [round(float(x), float_precision), round(float(y), float_precision)]
                for x, y in cell.get("alpha_shape", [])
            ],
            "storm_history": []
        }

        # Add storm history entries if they exist
        for hist_entry in cell.get("storm_history", []):
            cell_entry["storm_history"].append({
                "timestamp": hist_entry.get("timestamp", ""),
                "max_reflectivity_dbz": round(float(hist_entry.get("max_reflectivity_dbz", 0)), float_precision),
                "num_gates": int(hist_entry.get("num_gates", 0)),
                "centroid": [round(float(v), float_precision) for v in hist_entry.get("centroid", [0, 0])],
                "bbox": hist_entry.get("bbox", {})
            })

        json_data.append(cell_entry)

    # Save to file
    with open(filepath, 'w') as f:
        json.dump(json_data, f, indent=4)

    print(f"Saved {len(cells)} cells to {filepath}")

def plot_storm_cells(cells, reflectivity, lat, lon, title="Storm Cell Detection",
                     lat_limits=(38.8, 40.1), lon_limits=(256, 258.5)):
    """
    Plot MRMS reflectivity and storm cells, hardcoding the lat/lon limits.
    lon_limits assumed 0-360.
    """

    import numpy as np
    import matplotlib.pyplot as plt
    import matplotlib.cm as cm
    import matplotlib.colors as mcolors
    import cartopy.crs as ccrs
    import cartopy.feature as cfeature

    # Convert 0–360 lon limits to -180..180
    lon_limits_pm = convert_lon_0_360_to_pm180(np.array(lon_limits))

    # Convert full lon array
    lon_pm = convert_lon_0_360_to_pm180(lon)

    # Create 2D grid if necessary
    if lat.ndim == 1 and lon_pm.ndim == 1:
        lon2d, lat2d = np.meshgrid(lon_pm, lat)
    else:
        lon2d, lat2d = lon_pm, lat

    # Mask reflectivity outside hardcoded limits
    mask = (lat2d >= lat_limits[0]) & (lat2d <= lat_limits[1]) & \
           (lon2d >= lon_limits_pm[0]) & (lon2d <= lon_limits_pm[1])
    refl_masked = np.where(mask, reflectivity, np.nan)
    refl_masked = np.ma.masked_invalid(refl_masked)

    fig, ax = plt.subplots(figsize=(12, 10), subplot_kw={'projection': ccrs.PlateCarree()})
    ax.set_title(title, fontsize=16)

    # Set hardcoded extent
    ax.set_extent([lon_limits_pm[0], lon_limits_pm[1], lat_limits[0], lat_limits[1]], crs=ccrs.PlateCarree())

    # Add map features
    ax.add_feature(cfeature.STATES.with_scale('50m'), edgecolor='gray')
    ax.add_feature(cfeature.COASTLINE)
    ax.add_feature(cfeature.BORDERS, linestyle=':')
    ax.add_feature(cfeature.LAND, facecolor='lightgray')
    ax.add_feature(cfeature.OCEAN, facecolor='lightblue')
    ax.gridlines(draw_labels=True, linewidth=0.5, color='gray', alpha=0.5)

    # Plot reflectivity
    im = ax.pcolormesh(lon2d, lat2d, refl_masked, cmap='NWSRef', vmin=0, vmax=75, shading='auto')

    # Cell colors
    cmap = cm.get_cmap('tab20')
    norm = mcolors.Normalize(vmin=0, vmax=max(1, len(cells) - 1))

    for idx, cell in enumerate(cells):
        lat_c, lon_c = cell["centroid"]
        lon_c = lon_c if lon_c <= 180 else lon_c - 360
        color = cmap(norm(idx))

        # Plot centroid
        ax.scatter(lon_c, lat_c, marker='x', s=100, color=color, zorder=5)

        # Plot bounding box
        bbox = cell.get("bbox")
        if bbox:
            lons = [bbox["lon_min"], bbox["lon_max"], bbox["lon_max"], bbox["lon_min"], bbox["lon_min"]]
            lats = [bbox["lat_min"], bbox["lat_min"], bbox["lat_max"], bbox["lat_max"], bbox["lat_min"]]
            lons = [convert_lon_0_360_to_pm180(lon) for lon in lons]
            ax.plot(lons, lats, linestyle='--', linewidth=2, color=color, alpha=0.7)

        # Plot alpha shape
        alpha_shape = cell.get("alpha_shape", [])
        if len(alpha_shape) >= 3:
            boundary_pts = np.array(alpha_shape)
            boundary_pts[:,0] = convert_lon_0_360_to_pm180(boundary_pts[:,0])
            boundary_pts = np.vstack([boundary_pts, boundary_pts[0]])
            ax.plot(boundary_pts[:, 0], boundary_pts[:, 1], color='black', linewidth=2)

    # Colorbar
    cbar = fig.colorbar(im, ax=ax, orientation='horizontal', pad=0.05)
    cbar.set_label('Reflectivity (dBZ)')

    plt.show()

def detect_cells(filepath, lat_limits, lon_limits, output_json, plot=False):
    print("Loading MRMS data slice...")
    refl, lat, lon = load_mrms_slice(filepath, lat_limits, lon_limits)

    # Extract scan timestamp from file if available
    import datetime
    try:
        import xarray as xr
        ds = xr.open_dataset(filepath)
        scan_time = str(ds.attrs.get('start_date', datetime.datetime.now()))
        ds.close()
    except:
        scan_time = str(datetime.datetime.now())

    print("Running cell propagation...")
    cells = propagate_cells(refl, lat, lon, alpha=0.1, filepath=filepath)

    print("Merging small cells...")
    merged_cells = merge_connected_small_cells(cells)

    # --- Build storm history per cell ---
    storm_history = {}
    for cell in merged_cells:
        cell_id = cell["id"]
        entry = {
            "scan_time": scan_time,
            "max_reflectivity_dbz": cell["max_reflectivity_dbz"],
            "num_gates": cell["num_gates"],
            "centroid": cell["centroid"],
            "bbox": cell["bbox"],
        }
        if cell_id not in storm_history:
            storm_history[cell_id] = []
        storm_history[cell_id].append(entry)

    # Save to JSON
    save_cells_to_json(merged_cells, output_json, float_precision=4)

    if plot:
        print("Plotting detected storm cells before merge ...")
        plot_storm_cells(cells, refl, lat, lon, title="Detected Storm Cells (Initial Pass)")
        print("Plotting final cells ... ")
        plot_storm_cells(merged_cells, refl, lat, lon, title="Detected Storm Cells (Final Pass)")

    print(f"Storm history created for {len(storm_history)} cells.")
    return merged_cells, storm_history

from pathlib import Path
if __name__ == "__main__":
    try:
        filepath = Path(r"C:\input_data\MRMS_MergedReflectivityQC_3D_20250804-235241_renamed.nc")
    except Exception as e:
        print(f"Error finding latest MRMS mosaic: {e}")
        sys.exit(1)
    print(filepath)
    lat_limits = (38.8, 40.1)
    lon_limits = (256, 258.5)  # MRMS longitude is 0-360

    if filepath.exists():
        print("Running detection!")
        detect_cells(filepath, lat_limits, lon_limits, "stormcell_test.json", plot=True)
    else:
        print(f"Filepath: {filepath} does not exist")
