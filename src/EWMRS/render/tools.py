import xarray as xr
import json
from datetime import datetime
from ..util.io import IOManager
from pathlib import Path
import re
import numpy as np
from pyproj import Transformer

try:
    from ..util.eccodes_loader import load_grib_fast
except Exception:
    load_grib_fast = None

io_manager = IOManager("[Transform]")

# Cached transformer for EPSG:4326 to EPSG:3857 (thread-safe per pyproj docs)
_TRANSFORMER_4326_TO_3857 = Transformer.from_crs("EPSG:4326", "EPSG:3857", always_xy=True)

class TransformUtils:
    @staticmethod
    def load_ds(ds_path: Path, lat_limits=None, lon_limits=None):
        """
        Loads .grib2/.nc datasets
        Args:
         - ds_path: Pathlib Path() object of grib2 or netCDF dataset
         - lat_limits, lon_limits: 0-360 format of lat/lon limits (Only works on netCDF)
        
        Returns:
         - ds: Loaded dataset
        """
        
        io_manager.write_debug(f"Opening file: {ds_path} ...")

        try:
            if str(ds_path).endswith(".grib2") or str(ds_path).endswith(".grib"):
                if lat_limits or lon_limits:
                    io_manager.write_warning("lat/lon limits not supported with GRIB files, skipping ... ")

                if load_grib_fast is not None:
                    try:
                        ds = load_grib_fast(str(ds_path))
                        io_manager.write_debug(f"Successfully loaded dataset via eccodes fast loader: {ds_path}")
                        return ds
                    except Exception as e:
                        io_manager.write_warning(f"Fast GRIB loader failed ({e}); falling back to xarray/cfgrib")

                ds = xr.open_dataset(ds_path, decode_timedelta=True)
                io_manager.write_debug(f"Successfully loaded dataset: {ds_path}")
                return ds
        
            if str(ds_path).endswith(".nc"):
                ds = xr.open_dataset(ds_path, decode_timedelta=True)

                if lat_limits and lon_limits:
                    # Latitude/Longitude variables: 'latitude', 'longitude'
                    ds = ds.sel(
                        latitude=slice(lat_limits[0], lat_limits[1]),
                        longitude=slice(lon_limits[0], lon_limits[1])
                    )
                    io_manager.write_debug(f"Loaded dataset subset with lat {lat_limits}, lon {lon_limits}")

                else:
                    io_manager.write_warning("lat/lon coordinates not specified, loading full dataset")

                io_manager.write_debug("Successfully loaded full dataset")
                return ds
        
        except Exception as e:
            io_manager.write_error(f"Failed to load dataset - {e}")
            return
    
    @staticmethod
    def find_timestamp(filepath):
        """
        Finds timestamps in a file based on predetermined patterns
        """
        filename = Path(filepath).name
        io_manager.write_debug(f"Extracting timestamp from filename: {filename}")
        
        patterns = [
            r'MRMS_MergedReflectivityQC_(\d{8})-(\d{6})',
            r'(\d{8})-(\d{6})_renamed',
            r'(\d{8}-\d{6})',
            r'.*(\d{8})-(\d{6}).*',
            r's(\d{4})(\d{3})(\d{2})(\d{2})(\d{2})(\d)'
        ]
        
        for pattern_idx, pattern in enumerate(patterns):
            match = re.search(pattern, filename)
            if match:
                groups = match.groups()
                
                if len(groups) == 2:
                    date_str, time_str = groups
                elif len(groups) == 1 and len(groups[0]) >= 15:  # 'YYYYMMDD-HHMMSS' min length
                    combined = groups[0]
                    date_str, time_str = combined[:8], combined[9:15]
                else:
                    # fallback to next pattern
                    continue

                try:
                    formatted_time = (f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}T"
                                    f"{time_str[:2]}:{time_str[2:4]}:{time_str[4:6]}")
                    io_manager.write_debug(f"Extracted timestamp: {formatted_time}")
                    return formatted_time
                except (IndexError, ValueError) as e:
                    io_manager.write_debug(f"Error formatting timestamp: {e}")
                    continue
        
        fallback = datetime.utcnow().isoformat()
        io_manager.write_debug(f"Using fallback timestamp: {fallback}")
        return fallback
    
    @staticmethod
    def reproject_to_epsg3857(ds):
        """
        [DEPRECATED] Reproject an xarray Dataset from EPSG:4326 (WGS84) to EPSG:3857 (Web Mercator).
        This method is kept for backward compatibility but is no longer used in the main render pipeline.
        """
        io_manager.write_warning("TransformUtils.reproject_to_epsg3857 is deprecated and should not be used.")
        return ds


    # crop_to_reflectivity removed (replaced by efficient downsampling in ewmrs.py)

    
class OverlayManifestUtils:
    """
    Utility class to manage overlay manifest information for render layers.
    Stores details including name, colormap, latest image path, and fixed bounds.
    """

    def __init__(self):
        self.layers = []
        # Fixed bounds in EPSG:3857 (Web Mercator) - in meters
        # Original bounds: 20-55 N, 230-300 E (or -130 to -60 W)
        # Transformed to Web Mercator:
        # West: -130° → ~-14,465,442 m
        # East: -60° → ~-6,679,169 m
        # South: 20° → ~2,273,031 m
        # North: 55° → ~7,361,866 m
        # Precise bounds in EPSG:3857 (Web Mercator) - in meters
        # Exact conversion for: 20-55 N, -130 to -60 W
        self.bounds = {
            'north': 7361866.1,
            'south': 2273030.9,
            'west': -14471533.8,
            'east': -6679169.5
        }

    def validate_bounds(self, bounds):
        """
        Validates that the provided bounds dict matches the format of self.bounds.

        Args:
            bounds (dict): Bounds dict to validate

        Raises:
            ValueError: If bounds is not a dict, missing keys, or values are not numeric
        """
        if not isinstance(bounds, dict):
            raise ValueError("Bounds must be a dictionary")
        required_keys = set(self.bounds.keys())
        if set(bounds.keys()) != required_keys:
            raise ValueError(f"Bounds must have keys: {required_keys}")
        for key, value in bounds.items():
            if not isinstance(value, (int, float)):
                raise ValueError(f"Bounds value for '{key}' must be numeric")

    def add_layer(self, name: str, colormap: str, latest_image: str, timestamp: str, bounds=None):
        """
        Adds a new layer to the manifest.

        Args:
            name (str): Name of the layer
            colormap (str): Colormap key used for the layer
            latest_image (str): Path to the latest image file for the layer
            timestamp (str): Timestamp of the latest file
            bounds (dict, optional): Custom bounds dict. If None, uses self.bounds
        """
        try:
            if bounds is not None:
                self.validate_bounds(bounds)
        except Exception as e:
            io_manager.write_warning(f"Error in validating bounds - {e}; defaulting to default bounds")
        layer = {
            'name': name,
            'colormap': colormap,
            'latest_image': latest_image,
            'timestamp': timestamp,
            'bounds': self.bounds if bounds is None else bounds
        }
        self.layers.append(layer)

    def get_layers(self):
        """
        Returns the list of all stored layers.

        Returns:
            list: List of layer dictionaries
        """
        return self.layers

    def clear_layers(self):
        """
        Clears all stored layers.
        """
        self.layers = []

    def save_to_json(self, filepath: str):
        """
        Saves the layers to a JSON file.

        Args:
            filepath (str): Path to the JSON file to save
        """
        with open(filepath, 'w') as f:
            json.dump(self.layers, f, indent=4)
        io_manager.write_debug(f"Saved overlay manifest to {filepath}")