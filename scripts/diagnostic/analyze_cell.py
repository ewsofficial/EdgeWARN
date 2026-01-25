
import xarray as xr
import numpy as np
import scipy.ndimage
import json

def analyze_cell(json_path, cell_id):
    # We need the expanded_ds. But it's transient in the pipeline.
    # However, we can infer a lot from the JSON count vs bbox.
    with open(json_path, 'r') as f:
        data = json.load(f)
        
    cell = next((c for c in data['features'] if c['id'] == cell_id), None)
    if not cell:
        print(f"Cell {cell_id} not found")
        return
        
    print(f"Analysis for Cell {cell_id}:")
    print(f"  Num Gates: {cell['num_gates']}")
    print(f"  Centroid: {cell['centroid']}")
    
    lats = [p[0] for p in cell['bbox']]
    lons = [p[1] for p in cell['bbox']]
    print(f"  Bbox Lat: {min(lats):.4f} to {max(lats):.4f}")
    print(f"  Bbox Lon: {min(lons):.4f} to {max(lons):.4f}")
    
    # Check if centroid is far from bbox
    d_lat = cell['centroid'][0] - min(lats)
    if d_lat < 0 or d_lat > (max(lats) - min(lats)):
         print(f"  WARNING: Centroid Lat is {d_lat:.4f} degrees outside bbox min")

if __name__ == "__main__":
    import sys
    analyze_cell(sys.argv[1], int(sys.argv[2]))
