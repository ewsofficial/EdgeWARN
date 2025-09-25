import numpy as np
import xarray as xr
from matplotlib.path import Path
import json
from shapely.geometry import Polygon, Point
from datetime import datetime
import re
from pathlib import Path as PathLibPath

class StatFileHandler:
    def __init__(self):
        """
        Initialize the StatFileLoader for loading data files.
        """
        self.dataset = None
        self.file_path = None

    def convert_lon_to_360(self, lon):
        """
        Convert longitude from -180 to 180 range to 0 to 360 range.
        
        Args:
            lon (array-like): Longitude values in -180 to 180 range
            
        Returns:
            array-like: Longitude values converted to 0 to 360 range
        """
        return np.where(lon < 0, lon + 360, lon)
    
    def convert_lon_to_180(self, lon):
        """
        Convert longitude from 0 to 360 range to -180 to 180 range.
        
        Args:
            lon (array-like): Longitude values in 0 to 360 range
            
        Returns:
            array-like: Longitude values converted to -180 to 180 range
        """
        return np.where(lon > 180, lon - 360, lon)
        
    def load_file(self, file_path):
        """
        Load a radar data file using xarray.
        
        Args:
            file_path (str): Path to the radar data file
            
        Returns:
            xarray.Dataset: Loaded dataset or None if failed
        """
        self.file_path = file_path
        
        try:
            self.dataset = xr.open_dataset(file_path, cache=False)
            print(f"Successfully loaded dataset from {file_path}")
            return self.dataset
        except Exception as e:
            print(f"Error loading file {file_path}: {e}")
            return None
        
    def load_json(self, filepath):
        print(f"DEBUG: Loading JSON file {filepath}")
        with open(filepath, 'r') as f:
            data = json.load(f)
        if not data:
            print(f"ERROR: {filepath} did not have any data")
            return None
        else:
            return data
    
    def write_json(self, data, filepath):
        print(f"DEBUG: Writing to JSON file {filepath}")
        with open(filepath, 'w') as f:
            json.dump(data, f, indent=4)
        print(f"Successfully wrote to JSON file {filepath}")
    
    def find_timestamp(self, filepath):
        """
        Extract timestamp from meteorological file path using common naming patterns.
        
        Args:
            filepath (str): Path to the file
            
        Returns:
            datetime: Extracted timestamp or None if not found
        """
        filename = PathLibPath(filepath).name
        
        # Common timestamp patterns in meteorological files
        patterns = [
            # YYYYMMDD_HHMMSS pattern
            r'(\d{8}[_\.-]\d{6})',
            # YYYYMMDD_HHMM pattern
            r'(\d{8}[_\.-]\d{4})',
            # YYYYMMDD pattern
            r'(\d{8})',
            # Unix timestamp pattern
            r'(\d{10,})'
        ]
        
        for pattern in patterns:
            match = re.search(pattern, filename)
            if match:
                timestamp_str = match.group(1)
                
                try:
                    # Try to parse different timestamp formats
                    if len(timestamp_str) == 15 and ('_' in timestamp_str or '.' in timestamp_str or '-' in timestamp_str):
                        # YYYYMMDD_HHMMSS format
                        date_part, time_part = re.split(r'[_\.-]', timestamp_str)
                        if len(time_part) == 6:
                            return datetime.strptime(f"{date_part}{time_part}", "%Y%m%d%H%M%S")
                        elif len(time_part) == 4:
                            return datetime.strptime(f"{date_part}{time_part}00", "%Y%m%d%H%M%S")
                    
                    elif len(timestamp_str) == 14:
                        # YYYYMMDDHHMMSS format
                        return datetime.strptime(timestamp_str, "%Y%m%d%H%M%S")
                    
                    elif len(timestamp_str) == 12:
                        # YYYYMMDDHHMM format
                        return datetime.strptime(timestamp_str + "00", "%Y%m%d%H%M%S")
                    
                    elif len(timestamp_str) == 8:
                        # YYYYMMDD format
                        return datetime.strptime(timestamp_str + "000000", "%Y%m%d%H%M%S")
                    
                    elif len(timestamp_str) >= 10:
                        # Unix timestamp
                        return datetime.fromtimestamp(int(timestamp_str[:10]))
                        
                except (ValueError, TypeError) as e:
                    print(f"Warning: Could not parse timestamp '{timestamp_str}' from {filename}: {e}")
                    continue
        
        # If no pattern matched, try to extract from dataset if it's loaded
        if self.dataset is not None:
            try:
                # Check for common time coordinate names
                time_coords = ['time', 'valid_time', 'forecast_time', 'reference_time']
                for coord in time_coords:
                    if coord in self.dataset.coords:
                        time_data = self.dataset[coord].values
                        if len(time_data) > 0:
                            if hasattr(time_data[0], 'item'):
                                return datetime.utcfromtimestamp(time_data[0].item() / 1e9)
                            else:
                                return datetime.utcfromtimestamp(time_data[0] / 1e9)
            except Exception as e:
                print(f"Warning: Could not extract time from dataset: {e}")
        
        print(f"Warning: Could not find timestamp in filename: {filename}")
        return None

class StormIntegrationUtils:
    @staticmethod
    def create_coordinate_grids(dataset):
        """
        Extract and create 2D latitude/longitude grids from any dataset.
        """
        # Find latitude and longitude coordinates
        lat_coord = None
        lon_coord = None
        
        for coord_name in dataset.coords:
            if coord_name.lower() in ['lat', 'latitude', 'y']:
                lat_coord = dataset[coord_name].values
            elif coord_name.lower() in ['lon', 'longitude', 'x']:
                lon_coord = dataset[coord_name].values
        
        if lat_coord is None or lon_coord is None:
            raise ValueError("Could not find latitude and longitude coordinates in dataset")
        
        # Create 2D grids if coordinates are 1D
        if lat_coord.ndim == 1 and lon_coord.ndim == 1:
            lon_grid, lat_grid = np.meshgrid(lon_coord, lat_coord)
        else:
            lat_grid, lon_grid = lat_coord, lon_coord
            
        return lat_grid, lon_grid
    
    @staticmethod
    def create_cell_polygon(cell):
        """
        Create a polygon from storm cell data.
        Prioritizes alpha_shape, falls back to bbox, then centroid.
        Returns a Shapely Polygon object.
        """
        
        # Try alpha_shape first (list of [lon, lat] pairs)
        if 'alpha_shape' in cell and cell['alpha_shape']:
            coords = [[point[0], point[1]] for point in cell['alpha_shape']]
            return Polygon(coords)
        
        # Fall back to bounding box
        if 'bbox' in cell:
            bbox = cell['bbox']
            coords = [
                [bbox['lon_min'], bbox['lat_min']],
                [bbox['lon_min'], bbox['lat_max']],
                [bbox['lon_max'], bbox['lat_max']],
                [bbox['lon_max'], bbox['lat_min']]
            ]
            return Polygon(coords)
        
        # Final fallback: small box around centroid
        if 'centroid' in cell and len(cell['centroid']) >= 2:
            lat, lon = cell['centroid'][0], cell['centroid'][1]
            d = 0.01  # ~1km box
            coords = [
                [lon - d, lat - d],
                [lon - d, lat + d],
                [lon + d, lat + d],
                [lon + d, lat - d]
            ]
            return Polygon(coords)
        
        return None
    
    @staticmethod
    def create_polygon_mask(polygon, lat_grid, lon_grid):
        """
        Create a boolean mask using polygon bounds (min/max coordinates).
        Fast and efficient for rectangular approximation.
        """
        if polygon is None:
            return None
            
        # Get the polygon bounds
        minx, miny, maxx, maxy = polygon.bounds
        
        # Create mask based on bounding box
        mask = (lon_grid >= minx) & (lon_grid <= maxx) & (lat_grid >= miny) & (lat_grid <= maxy)
        
        return mask