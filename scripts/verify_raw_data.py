import json
import numpy as np
import xarray as xr
from pathlib import Path

# Paths
prob_path = "/home/yuchenwei/EdgeWARN_input/data/ProbSevere/MRMS_PROBSEVERE_20260212_183634.json"
grib_path = "/home/yuchenwei/EdgeWARN_input/data/CompRefQC/MRMS_MergedReflectivityQCComposite_00.50_20260212-183634.grib2"

def verify_raw_data():
    print(f"Verifying raw data for 2026-02-12T18:36:34...")
    
    # 1. Load ProbSevere JSON to find cell 6656
    with open(prob_path, 'r') as f:
        prob_data = json.load(f)
    
    target_cell = None
    for feature in prob_data['features']:
        props = feature['properties']
        # Check if ID matches (ProbSevere IDs are usually integers)
        # The logs said "Rejecting small expanded clusters: [np.int32(6656)]"
        # This ID might be internal to watershed or from ProbSevere.
        # Let's check for ID 6656.
        if props.get('ID') == 6656:
            target_cell = feature
            break
            
    if not target_cell:
        print("Cell 6656 not found in ProbSevere JSON directly. It likely is a watershed label.")
        print("Checking if any feature has ID close to 6656 or if it's an internal label.")
        # If not found, let's just inspect the GRIB generally for high reflectivity.
    else:
        print(f"Found Cell 6656 in ProbSevere: {target_cell['properties']}")

    # 2. Load GRIB Data
    print(f"Loading GRIB: {grib_path}")
    # Use simpler backend args to avoid some issues
    ds = xr.open_dataset(
        grib_path, 
        engine='cfgrib',
        backend_kwargs={'indexpath': ''},
        chunks={'latitude': 1000, 'longitude': 1000}
    )
    
    # Check variable name, usually unknown or ref
    var_name = list(ds.data_vars)[0]
    ref_data = ds[var_name]
    
    # 3. Analyze High Reflectivity Areas
    # Threshold = 37.5 dBZ
    print("Processing data in chunks to avoid memory issues...")
    
    # Create boolean mask lazily
    mask_da = ref_data >= 37.5
    
    # Compute total count
    count_high_ref = mask_da.sum().compute().item()
    print(f"Total pixels >= 37.5 dBZ in entire CONUS scan: {count_high_ref}")
    
    if count_high_ref == 0:
        print("VERIFIED: No pixels >= 37.5 dBZ found in the raw data.")
        print("This confirms why 0 cells were detected.")
    else:
        print(f"Found {int(count_high_ref)} pixels >= 37.5 dBZ.")
        
        # To analyze clusters, we need to load just the high reflectivity parts or use dask-image
        # Since we can't easily install new packages, let's try to load only a subset if possible,
        # or just trust the count if it's small.
        
        if count_high_ref < 10000:
             # If small enough, load mask into memory
             print("High ref count is small, loading mask for clustering...")
             mask_np = mask_da.values
             
             from scipy.ndimage import label
             labeled_array, num_features = label(mask_np)
             print(f"Found {num_features} contiguous clusters >= 37.5 dBZ.")
             
             valid_clusters = 0
             for i in range(1, num_features + 1):
                size = np.sum(labeled_array == i)
                if size > 5:
                    print(f"Cluster {i}: Size {size} (Valid > 5)")
                    valid_clusters += 1
             
             print(f"\nSummary:")
             print(f"Valid Clusters (> 5 pixels): {valid_clusters}")
             if valid_clusters == 0:
                print("VERIFIED: All high-reflectivity clusters are too small (<= 5 pixels).")
        else:
             print("Too many high ref pixels to cluster in memory safely.")
             print("However, the fact they exist means we might have valid storms.")

if __name__ == "__main__":
    verify_raw_data()
