import re
import datetime
from datetime import datetime
from pathlib import Path
from util.io import IOManager
from util.handler import FileHandler


_TIMESTAMP_PATTERNS = [
    re.compile(r"MRMS_MergedReflectivityQC_3D_(\d{8})-(\d{6})"),
    re.compile(r"(\d{8})-(\d{6})_renamed"),
    re.compile(r"(\d{8}-\d{6})"),
    re.compile(r".*(\d{8})-(\d{6}).*"),
    re.compile(r"s(\d{4})(\d{3})(\d{2})(\d{2})(\d{2})(\d)"),
]

_DETECTION_IO = IOManager("[CellDetection]")

class DetectionDataHandler:
    def __init__(self, radar_path, ps_path, preciptype_path, io_manager, lat_min, lat_max, lon_min, lon_max):
        """
        Initialize the RadarDataHandler.

        Parameters:
            radar_path (str): Path to radar dataset
            ps_path (str): Path to ProbSevere dataset
            preciptype_path (str): Path to precipitation type dataset
            io_manager (IOManager): IO manager instance
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
        self.file_handler = FileHandler(io_manager)

    def load_subset(self):
        """
        Load the MRMS radar dataset from file and return a lat/lon subset as xarray.Dataset.
        Uses the centralized FileHandler.load_dataset method.
        """
        return self.file_handler.load_dataset(
            self.radar_path,
            lat_limits=self.lat_grid,
            lon_limits=self.lon_grid
        )
    
    def load_preciptype(self):
        """
        Load the precipitation type dataset from file and return a lat/lon subset as xarray.Dataset.
        Uses the centralized FileHandler.load_dataset method.
        """
        return self.file_handler.load_dataset(
            self.preciptype_path,
            lat_limits=self.lat_grid,
            lon_limits=self.lon_grid
        )
    
    def load_probsevere(self):
        """
        Load ProbSevere GeoJSON from specified path,
        returning only polygons with at least one vertex in the lat/lon range.
        Uses the centralized FileHandler.load_dataset method for loading.
        """
        # Load the JSON data using FileHandler
        data = self.file_handler.load_dataset(self.ps_path)
        
        if data is None:
            return []
        
        # Filter features based on lat/lon bounds
        lat_min, lat_max = self.lat_grid
        lon_min, lon_max = self.lon_grid

        # Normalize to -180 to 180 range
        lat_min = (lat_min + 180) % 360 - 180
        lat_max = (lat_max + 180) % 360 - 180
        lon_min = (lon_min + 180) % 360 - 180
        lon_max = (lon_max + 180) % 360 - 180

        filtered_features = []

        for feature in data.get('features', []):
            coords = feature['geometry']['coordinates'][0]
            lons, lats = zip(*coords)

            if (
                min(lons) <= lon_max
                and max(lons) >= lon_min
                and min(lats) <= lat_max
                and max(lats) >= lat_min
            ):
                filtered_features.append(feature)

        data['features'] = filtered_features
        return data
    
    @staticmethod
    def find_timestamp(filepath):
        """
        Finds timestamps in a file based on predetermined patterns
        """
        filename = Path(filepath).name
        _DETECTION_IO.write_info(f"Extracting timestamp from filename: {filename}")

        for pattern_idx, pattern in enumerate(_TIMESTAMP_PATTERNS):
            match = pattern.search(filename)
            if match:
                groups = match.groups()
                _DETECTION_IO.write_debug(f"Pattern {pattern_idx+1} matched: {groups}")

                if len(groups) == 2:
                    date_str, time_str = groups
                elif len(groups) == 1 and len(groups[0]) >= 15:  # 'YYYYMMDD-HHMMSS' min length
                    combined = groups[0]
                    date_str, time_str = combined[:8], combined[9:15]
                else:
                    # fallback to next pattern
                    _DETECTION_IO.write_debug(f"Unexpected group format: {groups}")
                    continue

                try:
                    formatted_time = (f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}T"
                                    f"{time_str[:2]}:{time_str[2:4]}:{time_str[4:6]}")
                    _DETECTION_IO.write_debug(f"Extracted timestamp: {formatted_time}")
                    return formatted_time
                except (IndexError, ValueError) as e:
                    _DETECTION_IO.write_warning(f"Error formatting timestamp: {e}")
                    continue

        fallback = datetime.utcnow().isoformat()
        _DETECTION_IO.write_info(f"Using fallback timestamp: {fallback}")
        return fallback