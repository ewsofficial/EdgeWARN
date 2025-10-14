import json
import matplotlib.pyplot as plt
from datetime import datetime
from pathlib import Path
import xarray as xr
import numpy as np
import pyart

# Target timestamp
TARGET_TS = datetime.fromisoformat("2025-10-11T23:22:40")

# Absolute crop bounds
lat_limits = (33.0, 35.0)            # Latitude 33–35°N
lon_limits = (-81.0, -78.0)          # Longitude -81 to -78° (convert to 0–360)

def plot_stormcells_with_ref(json_path: Path, target_ts: datetime, ref_file: Path, output_path: Path = None):
    # Load storm cells
    with open(json_path, "r") as f:
        cells = json.load(f)

    # Load reflectivity
    ds = xr.open_dataset(ref_file)
    var_name = list(ds.data_vars)[0]
    refl = ds[var_name].values

    # Get coordinates
    if 'latitude' in ds and 'longitude' in ds:
        lats = ds['latitude'].values
        lons = ds['longitude'].values
    elif 'lat' in ds and 'lon' in ds:
        lats = ds['lat'].values
        lons = ds['lon'].values
    else:
        raise KeyError("Latitude and longitude not found in dataset")

    # Convert longitudes to 0–360 for MRMS alignment
    lons = np.mod(lons, 360)
    lon_limits_360 = np.mod(np.array(lon_limits), 360)

    # Flip latitude if descending
    if lats[0] > lats[-1]:
        lats = lats[::-1]
        refl = refl[::-1, :]

    # Crop using boolean masks
    lat_mask = (lats >= lat_limits[0]) & (lats <= lat_limits[1])
    lon_mask = (lons >= lon_limits_360[0]) & (lons <= lon_limits_360[1])

    lats_cropped = lats[lat_mask]
    lons_cropped = lons[lon_mask]
    refl_cropped = refl[np.ix_(lat_mask, lon_mask)]

    plt.figure(figsize=(10, 8))

    # Plot reflectivity
    mesh = plt.pcolormesh(lons_cropped, lats_cropped, refl_cropped, cmap="NWSRef", vmin=0, vmax=80)
    plt.colorbar(mesh, label='Reflectivity (dBZ)')

    found_any = False

    # Plot storm cells and their history
    for cell in cells:
        history = cell.get("storm_history", [])
        if not history:
            continue

        # Filter history up to the target timestamp
        history = [h for h in history if datetime.fromisoformat(h["timestamp"]) <= target_ts]
        if not history:
            continue

        found_any = True

        # Normalize alpha fading: oldest = 0.2, newest = 1.0
        num_entries = len(history)
        alphas = np.linspace(0.2, 1.0, num_entries)

        for entry, alpha in zip(history, alphas):
            bbox = entry.get("bbox", [])
            centroid = entry.get("centroid", [])
            cell_id = cell.get("id", "N/A")
            refl_cell = entry.get("max_refl", cell.get("max_refl", None))

            if bbox:
                lats_cell = [p[0] for p in bbox] + [bbox[0][0]]
                lons_cell = [np.mod(p[1], 360) for p in bbox] + [np.mod(bbox[0][1], 360)]
                plt.plot(lons_cell, lats_cell, "k-", linewidth=2, alpha=alpha,
                         label=f"Cell {cell_id} (Refl {refl_cell})" if alpha == 1.0 else None)

            if centroid:
                lon_c = np.mod(centroid[1], 360)
                plt.scatter(lon_c, centroid[0], c="red", s=40, zorder=5, alpha=alpha)
                if alpha == 1.0:
                    plt.text(lon_c, centroid[0], f"{cell_id}", fontsize=8, ha="left", va="bottom")

    if not found_any:
        print(f"No storm entries found for timestamp {target_ts.isoformat()}")

    plt.title(f"Storm Cells with History at {target_ts.isoformat()}")
    plt.xlabel("Longitude (°)")
    plt.ylabel("Latitude (°)")
    plt.legend(loc="best")
    plt.grid(True)
    plt.tight_layout()

    # Save figure as PNG
    if output_path is None:
        output_path = Path(f"storm_cells_history_{target_ts.strftime('%Y%m%d_%H%M%S')}.png")
    plt.savefig(output_path, dpi=300)
    print(f"Plot saved as {output_path}")
    plt.close()

if __name__ == "__main__":
    json_path = Path("stormcell_test.json")
    ref_file = Path(r"C:\EdgeWARN_input\CompRefQC\MRMS_MergedReflectivityQCComposite_00.50_20251011-232240.grib2")
    plot_stormcells_with_ref(json_path, TARGET_TS, ref_file)
