import xarray as xr
import json
import re
import datetime
from datetime import datetime
from pathlib import Path
from util.io import IOManager
import cfgrib

class DetectionDataHandler:
    def __init__(self, radar_path, ps_path, preciptype_path, io_manager, lat_min, lat_max, lon_min, lon_max):
        """
        Initialize the RadarDataHandler.

        Parameters:
            path (str): Path to MRMS dataset (NetCDF/HDF5 file)
            lat_min, lat_max (float): Latitude bounds for the subset
            lon_min, lon_max (float): Longitude bounds for the subset
        """
        self.radar_path = radar_path
        self.ps_path = ps_path
        self.preciptype_path = preciptype_path
        self.lat_grid = (lat_min, lat_max)
        self.lon_grid = (lon_min, lon_max)
        self.dataset = None
        self.io_manager = io_manager

    def load_subset(self):
        """
        Load the MRMS dataset from file and return a lat/lon subset as xarray.Dataset.
        Only the requested subset is loaded.
        """
        try:
            # Open dataset lazily (do not load full arrays)
            ds = xr.open_dataset(self.radar_path, decode_timedelta=True)

            # Handle descending latitude
            if ds.latitude[0] > ds.latitude[-1]:
                lat_slice = slice(self.lat_grid[1], self.lat_grid[0])
            else:
                lat_slice = slice(self.lat_grid[0], self.lat_grid[1])

            # Longitude slice
            lon_slice = slice(self.lon_grid[0], self.lon_grid[1])

            # Subset dataset (lazy; data not fully loaded yet)
            dataset = ds.sel(latitude=lat_slice, longitude=lon_slice)
            self.io_manager.write_debug(f"Subset prepared for lat: {self.lat_grid}, lon: {self.lon_grid}")

            return dataset

        except Exception as e:
            self.io_manager.write_error(f"Failed to load {self.radar_path}: {e}")
    
    def load_preciptype(self):
        try:
            # Open dataset lazily (do not load full arrays)
            ds = xr.open_dataset(self.preciptype_path, decode_timedelta=True)

            # Handle descending latitude
            if ds.latitude[0] > ds.latitude[-1]:
                lat_slice = slice(self.lat_grid[1], self.lat_grid[0])
            else:
                lat_slice = slice(self.lat_grid[0], self.lat_grid[1])

            # Longitude slice
            lon_slice = slice(self.lon_grid[0], self.lon_grid[1])

            # Subset dataset (lazy; data not fully loaded yet)
            dataset = ds.sel(latitude=lat_slice, longitude=lon_slice)
            self.io_manager.write_debug(f"Subset prepared for lat: {self.lat_grid}, lon: {self.lon_grid}")

            return dataset

        except Exception as e:
            self.io_manager.write_error(f"Failed to load {self.preciptype_path}: {e}")
    
    def load_probsevere(self):
        """
        Load ProbSevere GeoJSON from specified path,
        returning only polygons with at least one vertex in the lat/lon range.
        """
        try:
            with open(self.ps_path, 'r') as f:
                data = json.load(f)
            self.io_manager.write_debug(f"Loaded ProbSevere JSON: {self.ps_path}")

            lat_min, lat_max = self.lat_grid
            lon_min, lon_max = self.lon_grid

            lat_min = (lat_min + 180) % 360 - 180  # -> -77.6
            lat_max = (lat_max + 180) % 360 - 180  # -> -75.2

            lon_min = (lon_min + 180) % 360 - 180  # -> -77.6
            lon_max = (lon_max + 180) % 360 - 180  # -> -75.2

            filtered_features = []

            for feature in data.get('features', []):
                polygon_coords = feature['geometry']['coordinates'][0]  # assuming single polygon

                # Keep feature if any point is within the lat/lon bounds
                if any(lon_min <= lon <= lon_max and lat_min <= lat <= lat_max
                    for lon, lat in polygon_coords):
                    filtered_features.append(feature)

            data['features'] = filtered_features
            return data

        except Exception as e:
            self.io_manager.write_error(f"Failed to load ProbSevere JSON from {self.ps_path}: {e}")
            return []
    
    @staticmethod
    def find_timestamp(filepath):
        """
        Finds timestamps in a file based on predetermined patterns
        """
        filename = Path(filepath).name
        io_manager = IOManager("[CellDetection]")
        io_manager.write_debug(f"Extracting timestamp from filename: {filename}")
        
        patterns = [
            r'MRMS_MergedReflectivityQC_3D_(\d{8})-(\d{6})',
            r'(\d{8})-(\d{6})_renamed',
            r'(\d{8}-\d{6})',
            r'.*(\d{8})-(\d{6}).*',
            r's(\d{4})(\d{3})(\d{2})(\d{2})(\d{2})(\d)'
        ]
        
        for pattern_idx, pattern in enumerate(patterns):
            match = re.search(pattern, filename)
            if match:
                groups = match.groups()
                io_manager.write_debug(f"Pattern {pattern_idx+1} matched: {groups}")
                
                if len(groups) == 2:
                    date_str, time_str = groups
                elif len(groups) == 1 and len(groups[0]) >= 15:  # 'YYYYMMDD-HHMMSS' min length
                    combined = groups[0]
                    date_str, time_str = combined[:8], combined[9:15]
                else:
                    # fallback to next pattern
                    io_manager.write_debug(f"Unexpected group format: {groups}")
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