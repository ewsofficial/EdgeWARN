import xarray as xr
import numpy as np
from pathlib import Path
import re
import datetime
from datetime import datetime
import math

PENALTY_COST = 1000.0

class GeoUtils:
    @staticmethod
    def haversine_dist(coord1, coord2):
        R = 6371  # km
        lat1, lon1 = np.radians(coord1)
        lat2, lon2 = np.radians(coord2)
        dlat = lat2 - lat1
        dlon = lon2 - lon1
        a = math.sin(dlat/2)**2 + math.cos(lat1)*math.cos(lat2)*math.sin(dlon/2)**2
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
        return R * c

    @staticmethod
    def polygon_area_km2(latlon_points):
        """
        Approximate polygon area on Earth's surface in km^2 using spherical excess formula.
        Input: list of (lon, lat) tuples
        """
        if not latlon_points or len(latlon_points) < 3:
            return 0.0  # or np.nan, since a polygon with < 3 points has no area
        
        coords = np.radians(np.array(latlon_points))
        if coords.ndim != 2 or coords.shape[1] != 2:
            return 0.0

        lons = coords[:, 0]
        lats = coords[:, 1]
        # Earth's radius in km
        R = 6371.0
        
        # Calculate area using spherical polygon area formula:
        # Reference: https://trs.jpl.nasa.gov/handle/2014/40409 (equation simplified)
        
        # Compute the angles between edges
        def angle_between_vectors(v1, v2):
            return np.arccos(np.clip(np.dot(v1, v2), -1.0, 1.0))
        
        # Convert lat/lon to 3D Cartesian coordinates
        def latlon_to_xyz(lat, lon):
            x = np.cos(lat) * np.cos(lon)
            y = np.cos(lat) * np.sin(lon)
            z = np.sin(lat)
            return np.array([x, y, z])
        
        vertices = [latlon_to_xyz(lat, lon) for lon, lat in zip(lons, lats)]
        
        n = len(vertices)
        angles = 0.0
        for i in range(n):
            v1 = vertices[i - 1]
            v2 = vertices[i]
            v3 = vertices[(i + 1) % n]
            
            a = v1 - v2
            b = v3 - v2
            
            a /= np.linalg.norm(a)
            b /= np.linalg.norm(b)
            
            angle = np.arccos(np.clip(np.dot(a, b), -1.0, 1.0))
            angles += angle
        
        spherical_excess = angles - (n - 2) * np.pi
        area = spherical_excess * R**2
        return abs(area)
    
class CellProcessor:
    @staticmethod
    def add_area_to_cells(cells):
        """
        Compute area_km2 for each cell based on convex_hull or alpha_shape.
        """
        for cell in cells:
            polygon = cell.get('convex_hull') or cell.get('alpha_shape')
            if polygon and len(polygon) >= 3:
                cell['area_km2'] = GeoUtils.polygon_area_km2(polygon)
            else:
                cell['area_km2'] = 0.0

    @staticmethod
    def normalize_diff(val1, val2, max_val):
        return abs(val1 - val2) / max_val if max_val > 0 else 0

    @staticmethod
    def compute_cost(cellA, cellB, max_vals, weights):
        dist = GeoUtils.haversine_dist(cellA['centroid'], cellB['centroid'])
        if dist > 10:
            # Use a large finite penalty instead of infinity so the Hungarian solver stays feasible
            return PENALTY_COST

        d_num_gates = CellProcessor.normalize_diff(cellA['num_gates'], cellB['num_gates'], max_vals['num_gates'])  # proxy for area
        d_reflect = CellProcessor.normalize_diff(cellA['max_reflectivity_dbz'], cellB['max_reflectivity_dbz'], max_vals['max_reflectivity_dbz'])

        cost = (weights['distance'] * (dist / 10) +
                weights['num_gates'] * d_num_gates +
                weights['max_reflectivity'] * d_reflect)
        return cost

def load_mrms_slice(filepath, lat_limits=None, lon_limits=None):
    ds = xr.open_dataset(filepath)

    # --- Reflectivity ---
    if "reflectivity_combined" in ds:
        refl = ds["reflectivity_combined"]
    elif "unknown" in ds:
        refl = ds["unknown"]
    else:
        raise ValueError("No valid reflectivity data found.")

    # --- Coordinates ---
    if "x" in ds and "y" in ds:
        lat = ds["y"]
        lon = ds["x"]
        lat_dim, lon_dim = "y", "x"
    elif "latitude" in ds and "longitude" in ds:
        lat = ds["latitude"]
        lon = ds["longitude"]
        lat_dim, lon_dim = "latitude", "longitude"
    else:
        raise ValueError("No valid coordinates.")

    # Find index ranges that satisfy bounding box
    if lat_limits is not None:
        lat_mask = (lat >= lat_limits[0]) & (lat <= lat_limits[1])
        y_start, y_end = np.where(lat_mask)[0][[0, -1]] + [0, 1]
    else:
        y_start, y_end = 0, lat.shape[0]

    if lon_limits is not None:
        lon_mask = (lon >= lon_limits[0]) & (lon <= lon_limits[1])
        x_start, x_end = np.where(lon_mask)[0][[0, -1]] + [0, 1]
    else:
        x_start, x_end = 0, lon.shape[0]

    # Handle 1D or 2D lat/lon arrays
    if lat.ndim == 1 and lon.ndim == 1:
        refl_crop = refl.isel({lat_dim: slice(y_start, y_end), lon_dim: slice(x_start, x_end)}).values
        lat_crop = lat.isel({lat_dim: slice(y_start, y_end)}).values
        lon_crop = lon.isel({lon_dim: slice(x_start, x_end)}).values
        # Expand to 2D meshgrid for compatibility
        lon_grid, lat_grid = np.meshgrid(lon_crop, lat_crop)
        ds.close()
        return refl_crop, lat_grid, lon_grid
    else:
        refl_crop = refl.isel({lat_dim: slice(y_start, y_end), lon_dim: slice(x_start, x_end)}).values
        lat_crop = lat.isel({lat_dim: slice(y_start, y_end), lon_dim: slice(x_start, x_end)}).values
        lon_crop = lon.isel({lat_dim: slice(y_start, y_end), lon_dim: slice(x_start, x_end)}).values
        ds.close()
        return refl_crop, lat_crop, lon_crop
    
def extract_timestamp_from_filename(filepath):
    """
    Extract timestamp from MRMS filename with multiple pattern support.
    """
    filename = Path(filepath).name
    print(f"DEBUG: Extracting timestamp from filename: {filename}")
    
    patterns = [
        r'MRMS_MergedReflectivityQC_3D_(\d{8})-(\d{6})',
        r'(\d{8})-(\d{6})_renamed',
        r'(\d{8}-\d{6})',
        r'.*(\d{8})-(\d{6}).*'
    ]
    
    for pattern_idx, pattern in enumerate(patterns):
        match = re.search(pattern, filename)
        if match:
            groups = match.groups()
            print(f"DEBUG: Pattern {pattern_idx+1} matched: {groups}")
            
            if len(groups) == 2:
                date_str, time_str = groups
            else:
                combined = groups[0]
                date_str, time_str = combined[:8], combined[9:]
            
            try:
                formatted_time = (f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}T"
                                 f"{time_str[:2]}:{time_str[2:4]}:{time_str[4:6]}")
                print(f"DEBUG: Extracted timestamp: {formatted_time}")
                return formatted_time
            except (IndexError, ValueError) as e:
                print(f"DEBUG: Error formatting timestamp: {e}")
                continue
    
    fallback = datetime.utcnow().isoformat()
    print(f"DEBUG: Using fallback timestamp: {fallback}")
    return fallback
    