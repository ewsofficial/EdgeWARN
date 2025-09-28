import xarray as xr
import numpy as np
from pathlib import Path
import re
import datetime
from datetime import datetime
import math
from shapely.geometry import Polygon

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
        Calculate polygon area on Earth's surface in km^2 using latitude-corrected shoelace formula.
        Input: list of (lon, lat) tuples in degrees
        """
        if not latlon_points or len(latlon_points) < 3:
            return 0.0
        
        coords = np.array(latlon_points)
        if coords.ndim != 2 or coords.shape[1] != 2:
            return 0.0
        
        lons = coords[:, 0]
        lats = coords[:, 1]
        
        # Earth's radius in km
        R = 6371.0
        
        # Convert to radians and calculate mean latitude for correction
        lat_rad = np.radians(lats)
        mean_lat = np.mean(lat_rad)
        
        # Calculate area using shoelace formula with latitude correction
        area = 0.0
        n = len(lats)
        
        for i in range(n):
            j = (i + 1) % n
            # Convert longitude difference to km at this latitude
            lon_diff_km = np.cos(mean_lat) * R * np.radians(lons[j] - lons[i])
            lat_diff_km = R * np.radians(lats[j] - lats[i])
            
            area += lons[i] * lat_diff_km - lats[i] * lon_diff_km
        
        return abs(area) / 2.0
    
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
    
    @staticmethod
    def calculate_cell_overlap(cell1, cell2):
        """
        Calculate the overlap area between two storm cells in km².
        
        Args:
            cell1 (dict): First storm cell with 'alpha_shape' polygon
            cell2 (dict): Second storm cell with 'alpha_shape' polygon
        
        Returns:
            tuple: (overlap_area_km2, overlap_percentage_cell1, overlap_percentage_cell2)
        """
        # Get polygons from cells
        poly1_points = cell1.get('alpha_shape', [])
        poly2_points = cell2.get('alpha_shape', [])
        
        if len(poly1_points) < 3 or len(poly2_points) < 3:
            return 0.0, 0.0, 0.0
        
        try:
            # Create Shapely Polygon objects
            poly1 = Polygon(poly1_points)
            poly2 = Polygon(poly2_points)
            
            # Fix invalid geometries if needed
            if not poly1.is_valid:
                poly1 = poly1.buffer(0)
            if not poly2.is_valid:
                poly2 = poly2.buffer(0)
            
            # Calculate intersection
            intersection = poly1.intersection(poly2)
            
            if intersection.is_empty:
                return 0.0, 0.0, 0.0
            
            # Calculate areas using our existing method for consistency
            area1 = GeoUtils.polygon_area_km2(poly1_points)
            area2 = GeoUtils.polygon_area_km2(poly2_points)
            intersection_area = GeoUtils.polygon_area_km2(list(intersection.exterior.coords))
            
            # Calculate overlap percentages
            overlap_pct1 = (intersection_area / area1 * 100) if area1 > 0 else 0
            overlap_pct2 = (intersection_area / area2 * 100) if area2 > 0 else 0
            
            return intersection_area, overlap_pct1, overlap_pct2
            
        except Exception as e:
            print(f"Warning: Error calculating cell overlap: {e}")
            return 0.0, 0.0, 0.0

    @staticmethod
    def filter_highly_covered_cells(cells, coverage_threshold=80):
        """
        Filter out cells that are highly covered by larger cells.
        
        Args:
            cells (list): List of storm cell dictionaries
            coverage_threshold (float): Percentage threshold for removal (default: 80%)
        
        Returns:
            list: Filtered list of cells
        """
        if len(cells) <= 1:
            return cells
        
        # Ensure all cells have area calculated
        CellProcessor.add_area_to_cells(cells)
        
        # Sort by area descending (largest first)
        cells_sorted = sorted(cells, key=lambda x: x.get('area_km2', 0), reverse=True)
        
        cells_to_remove = set()
        
        # Compare each cell with all larger cells
        for i, smaller_cell in enumerate(cells_sorted):
            smaller_id = smaller_cell['id']
            if smaller_id in cells_to_remove:
                continue
                
            for larger_cell in cells_sorted[:i]:  # Only check larger cells (earlier in list)
                larger_id = larger_cell['id']
                if larger_id in cells_to_remove:
                    continue
                    
                # Calculate how much the smaller cell is covered by the larger cell
                overlap_area, overlap_pct_smaller, overlap_pct_larger = GeoUtils.calculate_cell_overlap(
                    smaller_cell, larger_cell
                )
                
                # If smaller cell is highly covered by larger cell, mark for removal
                if overlap_pct_smaller > coverage_threshold:
                    cells_to_remove.add(smaller_id)
                    print(f"Removing cell {smaller_id} ({smaller_cell['area_km2']:.1f} km²): "
                          f"{overlap_pct_smaller:.1f}% covered by cell {larger_id} ({larger_cell['area_km2']:.1f} km²)")
                    break  # No need to check other larger cells
        
        # Filter out the cells to remove
        filtered_cells = [cell for cell in cells if cell['id'] not in cells_to_remove]
        
        print(f"Filtered out {len(cells_to_remove)} cells highly covered by larger cells")
        return filtered_cells

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

    # Normalize lat_limits/lon_limits if strings or lists are passed
    def _to_tuple(lim):
        if lim is None:
            return None
        if isinstance(lim, str):
            # Expect formats like "(lat1, lat2)" or "lat1,lat2"
            nums = [float(x) for x in re.findall(r"-?\d+\.?\d*", lim)]
            return (nums[0], nums[1]) if len(nums) >= 2 else None
        if isinstance(lim, (list, tuple)) and len(lim) >= 2:
            return (float(lim[0]), float(lim[1]))
        return None

    lat_limits = _to_tuple(lat_limits)
    lon_limits = _to_tuple(lon_limits)

    # Find index ranges that satisfy bounding box
    if lat_limits is not None:
        lat_mask = (lat >= lat_limits[0]) & (lat <= lat_limits[1])
        idx = np.where(lat_mask)[0]
        if idx.size == 0:
            # No lat points in requested range; fallback to full extent
            y_start, y_end = 0, lat.shape[0]
        else:
            y_start, y_end = idx[[0, -1]] + [0, 1]
    else:
        y_start, y_end = 0, lat.shape[0]

    if lon_limits is not None:
        lon_mask = (lon >= lon_limits[0]) & (lon <= lon_limits[1])
        idx = np.where(lon_mask)[0]
        if idx.size == 0:
            x_start, x_end = 0, lon.shape[0]
        else:
            x_start, x_end = idx[[0, -1]] + [0, 1]
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
        r'.*(\d{8})-(\d{6}).*',
        r's(\d{4})(\d{3})(\d{2})(\d{2})(\d{2})(\d)'
    ]
    
    for pattern_idx, pattern in enumerate(patterns):
        match = re.search(pattern, filename)
        if match:
            groups = match.groups()
            print(f"DEBUG: Pattern {pattern_idx+1} matched: {groups}")
            
            if len(groups) == 2:
                date_str, time_str = groups
            elif len(groups) == 1 and len(groups[0]) >= 15:  # 'YYYYMMDD-HHMMSS' min length
                combined = groups[0]
                date_str, time_str = combined[:8], combined[9:15]
            else:
                # fallback to next pattern
                print(f"DEBUG: Unexpected group format: {groups}")
                continue

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
    