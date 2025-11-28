import xarray as xr
import json
from datetime import datetime
from util.io import IOManager
from pathlib import Path
import re
import numpy as np
from pyproj import Transformer

io_manager = IOManager("[Transform]")

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
        Reproject an xarray Dataset from EPSG:4326 (WGS84) to EPSG:3857 (Web Mercator).
        
        Args:
            ds (xr.Dataset): Dataset with latitude/longitude coordinates
            
        Returns:
            xr.Dataset: Reprojected dataset with x/y coordinates in EPSG:3857
        """
        # Create transformer from EPSG:4326 to EPSG:3857
        transformer = Transformer.from_crs("EPSG:4326", "EPSG:3857", always_xy=True)
        
        # Get lat/lon coordinate names
        lat_name = "latitude" if "latitude" in ds.coords else "lat"
        lon_name = "longitude" if "longitude" in ds.coords else "lon"
        
        # Extract coordinate values
        lats = ds[lat_name].values
        lons = ds[lon_name].values
        
        # Convert longitude from 0-360 to -180-180 if needed
        lons_converted = np.where(lons > 180, lons - 360, lons)
        
        # Create 2D meshgrids for transformation
        lon_grid, lat_grid = np.meshgrid(lons_converted, lats)
        
        # Transform coordinates
        x_grid, y_grid = transformer.transform(lon_grid, lat_grid)
        
        # Get the 1D x and y arrays (using the first row/column)
        x = x_grid[0, :]  # First row (all x values)
        y = y_grid[:, 0]  # First column (all y values)
        
        # Create new dataset with transformed coordinates
        ds_reprojected = ds.copy()
        
        # Rename coordinates and update values
        ds_reprojected = ds_reprojected.rename({lat_name: 'y', lon_name: 'x'})
        ds_reprojected = ds_reprojected.assign_coords({
            'x': ('x', x),
            'y': ('y', y)
        })
        
        # Add CRS information as attributes
        ds_reprojected.attrs['crs'] = 'EPSG:3857'
        ds_reprojected.attrs['crs_name'] = 'WGS 84 / Pseudo-Mercator'
        
        io_manager.write_debug("Reprojected dataset to EPSG:3857 (Web Mercator)")
        
        return ds_reprojected


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
        self.bounds = {
            'north': 7361866,     # ~55°N in Web Mercator meters
            'south': 2273031,     # ~20°N in Web Mercator meters
            'west': -14465442,    # ~-130°W in Web Mercator meters
            'east': -6679169      # ~-60°W in Web Mercator meters
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
