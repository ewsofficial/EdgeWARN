import sys
from pathlib import Path
from ..core.cellmask import StormCellDetector
from ..core.utils import load_mrms_slice
from ..core.visualize import Visualizer

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

    if len(cells) == 0:
        print("Error: No seed cells detected in propagate_cells function")
        return []
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
        Visualizer.plot_storm_cells(merged_cells, refl, lat, lon, title="Detected Storm Cells (Final Pass)")

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
