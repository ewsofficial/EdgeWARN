from datetime import datetime, timezone, timedelta

from pathlib import Path
import xarray as xr
import cfgrib
import netCDF4
import json
import re

def extract_timestamp(filepath, use_timezone_utc=False, round_to_minute=False, isoformat=False):
    """
    Compact timestamp extractor for MRMS (YYYYMMDD_HHMMSS) and GOES (sYYYYDDDHHMMSST).
    """
    fname = Path(filepath).name
    dt = None

    # MRMS: YYYYMMDD[-_]HHMMSS
    if m := re.search(r"(\d{8})[-_](\d{6})", fname):
        dt = datetime.strptime(f"{m.group(1)}{m.group(2)}", "%Y%m%d%H%M%S")
    
    # GOES: sYYYYDDDHHMMSST (T = tenths of second, ignored for dt)
    elif m := re.search(r"s(\d{4})(\d{3})(\d{2})(\d{2})(\d{2})(\d{1})", fname):
        y, d, h, mn, s, _ = map(int, m.groups())
        dt = datetime(y, 1, 1, h, mn, s) + timedelta(days=d-1)

    if not dt: return None

    if round_to_minute: dt = dt.replace(second=0, microsecond=0)
    if use_timezone_utc and dt.tzinfo is None: dt = dt.replace(tzinfo=timezone.utc)
    
    return dt.isoformat() if isoformat else dt

class FileHandler:
    def __init__(self, io_manager):
        self.io = io_manager

    def load_dataset(self, filepath, lat_limits=None, lon_limits=None):
        """
        Loads a dataset from filepath, detecting if its a GRIB, netCDF, or JSON file
        Args:
            filepath (str): Filepath to be loaded
            lat_limits (tuple): (min_lat, max_lat)
            lon_limits (tuple): (min_lon, max_lon)
        
        Returns:
            xr.Dataset for GRIB and netCDF
            JSON data for JSON
        """
        
        if filepath.endswith(".json"):
            self.io.write_info(f"Loading JSON file from {filepath}")
            try:
                with open(filepath, 'r') as f:
                    ds = json.load(f)
                    self.io.write_debug(f"Loaded JSON file from {filepath}")
                    return ds

            except Exception as e:
                self.io.write_error(f"Failed to load JSON file: {e}")
                return
        
        ds = None
        if filepath.endswith(".grib2"):
            self.io.write_info(f"Loading GRIB file from {filepath}")
            try:
                ds = xr.open_dataset(filepath, engine="cfgrib", decode_timedelta=True)
                self.io.write_debug(f"Loaded GRIB file from {filepath}")
            except Exception as e:
                self.io.write_error(f"Failed to load GRIB file: {e}")
                return
        
        elif filepath.endswith(".nc"):
            self.io.write_info(f"Loading netCDF file from {filepath}")
            try:
                ds = xr.open_dataset(filepath, engine="netcdf4", decode_timedelta=True)
                self.io.write_debug(f"Loaded netCDF file from {filepath}")
            except Exception as e:
                self.io.write_error(f"Failed to load netCDF file: {e}")
                return

        if ds is not None:
            if lat_limits and lon_limits:
                try:
                    # Handle descending latitude
                    lat_name = "latitude" if "latitude" in ds.coords else "lat"
                    lon_name = "longitude" if "longitude" in ds.coords else "lon"
                    
                    if ds[lat_name][0] > ds[lat_name][-1]:
                        lat_slice = slice(lat_limits[1], lat_limits[0])
                    else:
                        lat_slice = slice(lat_limits[0], lat_limits[1])
                    
                    lon_slice = slice(lon_limits[0], lon_limits[1])
                    
                    ds = ds.sel({lat_name: lat_slice, lon_name: lon_slice})
                    self.io.write_debug(f"Subset dataset to lat: {lat_limits}, lon: {lon_limits}")
                except Exception as e:
                    self.io.write_warning(f"Failed to subset dataset: {e}")
            return ds
