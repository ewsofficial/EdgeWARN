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
import pyart

import util.file as fs
from ..core.cellmask import StormCellDetector
from ..core.data_utils import load_mrms_slice

"""
"""

print("EdgeWARN Storm Cell Detection")
print("Build: 2025-09-01")
print("Credits: Yuchen Wei")
print("Made by the EWS")
time.sleep(1)

# --- JSON saving function matching detection.py ---
def save_cells_to_json(cells, filepath, float_precision=4):
    """
    Save tracked storm cells to JSON (like detection.py).
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

        for hist_entry in cell.get("storm_history", []):
            cell_entry["storm_history"].append({
                "timestamp": hist_entry.get("timestamp", ""),
                "max_reflectivity_dbz": round(float(hist_entry.get("max_reflectivity_dbz", 0)), float_precision),
                "num_gates": int(hist_entry.get("num_gates", 0)),
                "centroid": [round(float(v), float_precision) for v in hist_entry.get("centroid", [0, 0])]
            })

        json_data.append(cell_entry)

    with open(filepath, 'w') as f:
        json.dump(json_data, f, indent=4)

    print(f"Saved {len(cells)} tracked cells to {filepath}")

### Find a way to get the lat and lon limits to work
def plot_storm_cells(cells, reflectivity, lat, lon, title="Storm Cell Detection",
                     lat_limits=(45.3, 47.3), lon_limits=(256.6, 260.2)):
    """
    Plot MRMS reflectivity and storm cells, hardcoding the lat/lon limits.
    lon_limits assumed 0-360.
    """

    # Convert 0–360 lon limits to -180..180
    lon_limits_pm = StormCellDetector.convert_lon_0_360_to_pm180(np.array(lon_limits))

    # Convert full lon array
    lon_pm = StormCellDetector.convert_lon_0_360_to_pm180(lon)

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
            lons = [StormCellDetector.convert_lon_0_360_to_pm180(lon) for lon in lons]
            ax.plot(lons, lats, linestyle='--', linewidth=2, color=color, alpha=0.7)

        # Plot alpha shape
        alpha_shape = cell.get("alpha_shape", [])
        if len(alpha_shape) >= 3:
            boundary_pts = np.array(alpha_shape)
            boundary_pts[:,0] = StormCellDetector.convert_lon_0_360_to_pm180(boundary_pts[:,0])
            boundary_pts = np.vstack([boundary_pts, boundary_pts[0]])
            ax.plot(boundary_pts[:, 0], boundary_pts[:, 1], color='black', linewidth=2)

    # Colorbar
    cbar = fig.colorbar(im, ax=ax, orientation='horizontal', pad=0.05)
    cbar.set_label('Reflectivity (dBZ)')

    plt.show()

def detect_cells(filepath, lat_limits, lon_limits, plot=False):
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
    cells = StormCellDetector.propagate_cells(refl, lat, lon, alpha=0.1, filepath=filepath)

    print("Merging small cells...")
    merged_cells = StormCellDetector.merge_connected_small_cells(cells)

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

    if plot:
        print("Plotting final cells ... ")
        plot_storm_cells(merged_cells, refl, lat, lon, title="Detected Storm Cells (Final Pass)")

    print(f"Storm history created for {len(storm_history)} cells.")
    return merged_cells, storm_history

from pathlib import Path
if __name__ == "__main__":
    try:
        filepath = Path(r"C:\input_data\nexrad_merged\MRMS_MergedReflectivityQC_max_20250913-002439.nc")
    except Exception as e:
        print(f"Error finding latest MRMS mosaic: {e}")
        sys.exit(1)
    print(filepath)
    lat_limits = (45.3, 47.3)
    lon_limits = (256.6, 260.2)  # MRMS longitude is 0-360

    if filepath.exists():
        print("Running detection!")
        detect_cells(filepath, lat_limits, lon_limits, plot=True)
    else:
        print(f"Filepath: {filepath} does not exist")
