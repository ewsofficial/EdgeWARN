import numpy as np
import xarray as xr
from scipy.ndimage import label, binary_dilation, center_of_mass
import alphashape
from shapely.geometry import Point, Polygon, LineString
import json
import time
import sys
from pathlib import Path

import matplotlib.pyplot as plt
import matplotlib.cm as cm
import matplotlib.colors as mcolors
import cartopy.crs as ccrs
import cartopy.feature as cfeature

import util.file as fs

"""
"""

print("EdgeWARN Storm Cell Detection")
print("Build: 2025-08-17")
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
    """
    Load a slice of MRMS data from a NetCDF file and crop it to specified latitude and longitude limits.

    Parameters:
    - filepath: Path to the MRMS NetCDF file.
    - lat_limits: Tuple of (lat_min, lat_max) to crop latitude.
    - lon_limits: Tuple of (lon_min, lon_max) to crop longitude.

    Returns:
    - refl_crop: 2D array of reflectivity values cropped to the specified limits.
    - lat_crop: 2D array of latitude values corresponding to the cropped reflectivity.
    - lon_crop: 2D array of longitude values corresponding to the cropped reflectivity.
    """
    ds = xr.open_dataset(filepath)

    refl = ds["unknown"]
    lat = ds["latitude"]
    lon = ds["longitude"]

    refl_masked = refl.where(refl > -999.0, np.nan)

    # --- Latitude cropping ---
    if lat_limits:
        lat_min, lat_max = lat_limits
        y_inds = np.where((lat >= lat_min) & (lat <= lat_max))[0]
        y_start, y_end = y_inds[0], y_inds[-1] + 1
    else:
        y_start, y_end = 0, lat.shape[0]

    # --- Longitude cropping ---
    if lon_limits:
        lon_min, lon_max = lon_limits
        x_inds = np.where((lon >= lon_min) & (lon <= lon_max))[0]
        x_start, x_end = x_inds[0], x_inds[-1] + 1
    else:
        x_start, x_end = 0, lon.shape[0]

    # --- Crop reflectivity directly with latitude/longitude ---
    refl_crop = refl_masked.isel(
        latitude=slice(y_start, y_end),
        longitude=slice(x_start, x_end)
    ).values

    # --- Build lat/lon grids ---
    lat_crop, lon_crop = np.meshgrid(
        lat[y_start:y_end].values,
        lon[x_start:x_end].values,
        indexing="ij"
    )

    lon_crop = convert_lon_0_360_to_pm180(lon_crop)

    ds.close()
    return refl_crop, lat_crop, lon_crop


def get_alpha_shape_from_mask(mask, lat_grid, lon_grid, alpha=0.1):
    """
    Generate an alpha shape polygon from the storm cell mask using a fixed alpha.

    Parameters:
    - mask: boolean 2D array where True indicates the cell pixels
    - lat_grid, lon_grid: 2D arrays with lat/lon corresponding to each pixel
    - alpha: float controlling the shape tightness (default 0.1)

    Returns:
    - shapely geometry (Polygon, LineString, or Point) representing the cell boundary
    """
    if np.sum(mask) == 0:
        return None

    points = np.column_stack((lon_grid[mask], lat_grid[mask]))
    points_list = [tuple(p) for p in points]

    n_points = len(points_list)
    if n_points == 0:
        return None
    elif n_points == 1:
        return Point(points_list[0])
    elif n_points == 2:
        return LineString(points_list)
    elif n_points == 3:
        pts = points_list + [points_list[0]]  # close polygon
        return Polygon(pts)
    else:
        alpha_shape = alphashape.alphashape(points_list, alpha)
        if alpha_shape.geom_type == 'MultiPolygon':
            alpha_shape = max(alpha_shape.geoms, key=lambda p: p.area)
        return alpha_shape

def propagate_cells(reflectivity, lat_grid, lon_grid, seed_dbz=50,
                    expand_dbz=40, min_gates=25, max_iterations=100, alpha=0.1):
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

            # Only expand into pixels not claimed by any other cell
            # So exclude pixels in claimed_mask except current_mask pixels
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

    # The rest of your cell output building logic unchanged...
    detected_cells = []
    for seed_id, mask in grown_masks.items():
        if np.sum(mask) < min_gates:
            continue
        max_dbz = refl_data[mask].max()
        high_refl_mask = np.logical_and(mask, refl_data >= 50)
        centroid_idx = center_of_mass(high_refl_mask) if np.any(high_refl_mask) else center_of_mass(mask)
        centroid_lat = lat_grid[int(round(centroid_idx[0])), int(round(centroid_idx[1]))]
        centroid_lon = lon_grid[int(round(centroid_idx[0])), int(round(centroid_idx[1]))]
        poly = get_alpha_shape_from_mask(mask, lat_grid, lon_grid, alpha)
        if poly is not None and hasattr(poly, "exterior"):
            alpha_shape_coords = [[float(x), float(y)] for x, y in poly.exterior.coords]
        else:
            alpha_shape_coords = []
        bbox = polygon_to_bbox(poly)
        detected_cells.append({
            "id": int(seed_id),
            "num_gates": int(np.sum(mask)),
            "centroid_latlon": (float(centroid_lat), float(centroid_lon)),
            "bbox": bbox,
            "max_reflectivity_dbz": float(max_dbz),
            "alpha_shape": alpha_shape_coords
        })

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
        lat1, lon1 = large["centroid_latlon"]
        lat2, lon2 = small["centroid_latlon"]
        large["centroid_latlon"] = (
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
            lat_c, lon_c = s["centroid_latlon"]
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
                lat1, lon1 = c1["centroid_latlon"]
                lat2, lon2 = c2["centroid_latlon"]
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
    Save storm cell data to a JSON file with controlled float precision.

    Parameters:
    - cells: List of detected storm cell dictionaries.
    - filepath: Path to the output JSON file.
    - float_precision: Number of decimal places for float values.
    """
    json_data = []
    for cell in cells:
        bbox = cell.get("bbox")
        bbox_points = bbox_to_points(bbox)
        
        json_data.append({
            "id": cell.get("id"),
            "centroid_latlon": cell.get("centroid_latlon"),
            "convex_hull": cell.get("convex_hull"),
            "bbox": bbox,
            "bbox_points": bbox_points,
            "num_gates": cell.get("num_gates")
        })

    json_data = convert_to_json_serializable(json_data, float_precision=float_precision)

    with open(filepath, 'w') as f:
        json.dump(json_data, f, indent=4)
    print(f"Saved cell data to {filepath}")

def plot_storm_cells(cells, reflectivity, lat_grid, lon_grid, title="Storm Cell Detection"):
    """
    DEBUG: DO NOT CALL IN AN OFFICIAL PRODUCT
    Plots storm cells on a map with reflectivity overlay.
    """

    fig, ax = plt.subplots(figsize=(10, 8), subplot_kw={'projection': ccrs.PlateCarree()})

    ax.set_title(title)
    ax.add_feature(cfeature.STATES.with_scale('50m'), edgecolor='gray')
    ax.add_feature(cfeature.COASTLINE)
    ax.add_feature(cfeature.BORDERS, linestyle=':')
    ax.add_feature(cfeature.LAND)
    ax.add_feature(cfeature.OCEAN)
    ax.add_feature(cfeature.LAKES, alpha=0.5)
    ax.add_feature(cfeature.RIVERS)
    ax.gridlines(draw_labels=True)

    refl_masked = np.ma.masked_invalid(reflectivity)
    im = ax.pcolormesh(lon_grid, lat_grid, refl_masked,
                       cmap='NWSRef', vmin=0, vmax=75, shading='auto')

    cmap = cm.get_cmap('tab20')
    norm = mcolors.Normalize(vmin=0, vmax=max(1, len(cells) - 1))

    for idx, cell in enumerate(cells):
        lat_c, lon_c = cell["centroid_latlon"]
        bbox = cell["bbox"]
        color = cmap(norm(idx))

        ax.scatter(lon_c, lat_c, marker='x', s=100, color=color, label=f'Cell {cell["id"]}', zorder=5)

        # Bounding box
        ax.plot(
            [bbox["lon_min"], bbox["lon_max"], bbox["lon_max"], bbox["lon_min"], bbox["lon_min"]],
            [bbox["lat_min"], bbox["lat_min"], bbox["lat_max"], bbox["lat_max"], bbox["lat_min"]],
            linestyle='--', linewidth=1.5, color=color, alpha=0.6
        )

        # Alpha shape polygon (cell boundary)
        if len(cell.get("alpha_shape", [])) >= 3:
            boundary_pts = np.array(cell["alpha_shape"])
            boundary_pts = np.vstack([boundary_pts, boundary_pts[0]])  # close polygon
            ax.plot(boundary_pts[:, 0], boundary_pts[:, 1], color='black', linewidth=2)

    cbar = fig.colorbar(im, ax=ax, orientation='horizontal', pad=0.05)
    cbar.set_label('Reflectivity (dBZ)')

    ax.legend(loc='upper right')
    plt.show()

def detect_cells(filepath, lat_limits, lon_limits, output_json, plot=False):
    print("Loading MRMS data slice...")
    refl, lat, lon = load_mrms_slice(filepath, lat_limits, lon_limits)

    print("Running cell propagation...")
    cells = propagate_cells(refl, lat, lon, alpha=0.1)

    print("Merging small cells...")
    merged_cells = merge_connected_small_cells(cells)

    print(f"Saving cells to {output_json} ...")
    save_cells_to_json(merged_cells, output_json, float_precision=4)

    if plot:
        print("Plotting detected storm cells before merge ...")
        plot_storm_cells(cells, refl, lat, lon, title="Detected Storm Cells (Initial Pass)")
        print("Plotting final cells ... ")
        plot_storm_cells(merged_cells, refl, lat, lon, title="Detected Storm Cells (Final Pass)")

    print("Done.")

    return merged_cells

from pathlib import Path
if __name__ == "__main__":
    try:
        filepath = Path(fs.latest_mosaic(1)[0])
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
