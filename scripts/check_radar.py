import xarray as xr
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path("src").resolve()))

from util.io import IOManager
from util.handler import FileHandler

io = IOManager("[CheckRadar]")
handler = FileHandler(io)

# Requested paths from logs
radar_path = "/home/yuchenwei/EdgeWARN_input/data/CompRefQC/MRMS_MergedReflectivityQCComposite_00.50_20260212-152834.grib2"
lat_limits = (20, 55)
lon_limits = (-130, -60)

ds = handler.load_dataset(radar_path, lat_limits=lat_limits, lon_limits=lon_limits)

if ds is not None:
    refl = ds['unknown'].values
    print(f"Max Reflectivity in subset: {refl.max()}")
    print(f"Min Reflectivity in subset: {refl.min()}")
    print(f"Mean Reflectivity in subset: {refl.mean()}")
    print(f"Number of gates > 40 dBZ: { (refl > 40).sum() }")
    print(f"Number of gates > 35 dBZ: { (refl > 35).sum() }")
    
    # Check coords
    print(f"Lat range: {ds.latitude.min().values} to {ds.latitude.max().values}")
    print(f"Lon range: {ds.longitude.min().values} to {ds.longitude.max().values}")
else:
    print("Failed to load dataset")
