import xarray as xr
import sys
from pathlib import Path
import numpy as np

# Add src to path
sys.path.insert(0, str(Path("src").resolve()))

from util.io import IOManager
from util.handler import FileHandler
from EdgeWARN.core.process.detect.tools.gatemapper import GateMapper

io = IOManager("[CheckOverlap]")
handler = FileHandler(io)

# Requested paths from logs
radar_path = "/home/yuchenwei/EdgeWARN_input/data/CompRefQC/MRMS_MergedReflectivityQCComposite_00.50_20260212-152834.grib2"
pt_path = "/home/yuchenwei/EdgeWARN_input/data/PrecipFlag/MRMS_PrecipFlag_00.00_20260212-152800.grib2"
ps_path = "/home/yuchenwei/EdgeWARN_input/data/ProbSevere/MRMS_PROBSEVERE_20260212_152834.json"
lat_limits = (20, 55)
lon_limits = (-130, -60)

radar_ds = handler.load_dataset(radar_path, lat_limits=lat_limits, lon_limits=lon_limits)
pt_ds = handler.load_dataset(pt_path, lat_limits=lat_limits, lon_limits=lon_limits)
ps_ds = handler.load_dataset(ps_path)

if radar_ds is not None and ps_ds is not None and pt_ds is not None:
    # Filter features based on lat/lon bounds (simple check)
    lat_min, lat_max = lat_limits
    lon_min, lon_max = lon_limits
    
    mapper = GateMapper(radar_ds, ps_ds, io, refl_threshold=35.0, min_seed_percentage=0.05)
    
    # Map gates to polygons
    mapped_ds = mapper.map_gates_to_polygons()
    polygon_grid = mapped_ds['PolygonID'].values
    refl_grid = radar_ds['unknown'].values
    pt_grid = pt_ds['unknown'].values if 'unknown' in pt_ds else pt_ds[list(pt_ds.data_vars)[0]].values

    mask = refl_grid >= 35.0
    
    unique_ids = np.unique(polygon_grid)
    unique_ids = unique_ids[unique_ids > 0]
    
    print(f"Unique Polygon IDs in radar grid: {unique_ids}")
    
    for poly_id in unique_ids:
        poly_mask = polygon_grid == poly_id
        pixel_count = np.sum(poly_mask)
        refl_count = np.sum(poly_mask & mask)
        
        # Check PrecipType in this polygon
        pt_vals = pt_grid[poly_mask]
        pt_counts = np.unique(pt_vals, return_counts=True)
        
        ratio = refl_count / pixel_count if pixel_count > 0 else 0
        
        print(f"Polygon ID {poly_id}:")
        print(f"  Total pixels: {pixel_count}")
        print(f"  Pixels >= 35 dBZ: {refl_count}")
        print(f"  PrecipType Distribution: {dict(zip(pt_counts[0], pt_counts[1]))}")
        print(f"  Coverage Ratio: {ratio:.4f}")
        
        if ratio >= 0.05:
             print(f"  -> SHOULD BE DETECTED")
        else:
             print(f"  -> FILTERED OUT (Ratio < 0.05)")

    # Check for gates >= 35 dBZ that do NOT have a polygon ID
    out_of_poly = np.sum(mask & (polygon_grid == 0))
    print(f"High-reflectivity gates outside any ProbSevere polygon: {out_of_poly}")

else:
    print("Failed to load datasets")
