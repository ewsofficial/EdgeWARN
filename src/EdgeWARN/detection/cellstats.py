import xarray as xr
from util import file as fs
import numpy as np
import json
from pathlib import Path as PathLibPath
from matplotlib.path import Path
import re
from datetime import datetime
from typing import Dict, List, Optional

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
            self.dataset = xr.open_dataset(file_path)
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
    
    def parse_probsevere_json(self, json_data: Dict) -> List[Dict]:
        """Parse ProbSevere JSON data and extract storm cell probabilities with coordinates."""
        storm_cells = []
        
        if not isinstance(json_data, dict) or 'features' not in json_data:
            return storm_cells
        
        for feature in json_data['features']:
            if feature.get('type') != 'Feature':
                continue
                
            cell_data = self._extract_probsevere_cell_data(feature)
            if cell_data:
                # Extract coordinates from GeoJSON geometry
                if 'geometry' in feature and feature['geometry']:
                    geometry = feature['geometry']
                    if geometry.get('type') == 'Polygon' and 'coordinates' in geometry:
                        coords = geometry['coordinates']
                        if coords and len(coords) > 0 and len(coords[0]) > 0:
                            # Calculate centroid from polygon coordinates
                            polygon_coords = np.array(coords[0])
                            centroid_lon = np.mean(polygon_coords[:, 0])
                            centroid_lat = np.mean(polygon_coords[:, 1])
                            
                            cell_data['lon'] = float(centroid_lon)
                            cell_data['lat'] = float(centroid_lat)
                            cell_data['polygon_coords'] = coords[0]  # Store full polygon for reference
                
                storm_cells.append(cell_data)
        
        return storm_cells

    def _extract_probsevere_cell_data(self, feature: Dict) -> Optional[Dict]:
        """Extract probability data from a single ProbSevere feature."""
        if not feature or 'properties' not in feature:
            return None
        
        try:
            properties = feature['properties']
            
            cell_data = {
                'id': properties.get('id', 'unknown'),  # Add cell ID if available
                'prob_severe': float(properties.get('ProbSevere', '0')),
                'prob_hail': float(properties.get('ProbHail', '0')),
                'prob_wind': float(properties.get('ProbWind', '0')),
                'prob_tornado': float(properties.get('ProbTor', '0')),
                'mesh': float(properties.get('MESH', '0')),
                'vil': float(properties.get('VIL', '0')),
                'flash_rate': float(properties.get('FLASH_RATE', '0')),
                'mucape': float(properties.get('MUCAPE', '0')),
                'mlcape': float(properties.get('MLCAPE', '0')),
                'mlcin': float(properties.get('MLCIN', '0')),
                'ebshear': float(properties.get('EBSHEAR', '0')),
                'srh_1km': float(properties.get('SRH01KM', '0')),
                'mean_wind_1_3km': float(properties.get('MEANWIND_1-3kmAGL', '0')),
            }
            
            return cell_data
            
        except (ValueError, KeyError, TypeError) as e:
            print(f"Error extracting ProbSevere cell data: {e}")
            return None
    
    def debug_probsevere_structure(self, probsevere_cells):
        """Debug function to understand the structure of ProbSevere data"""
        if not probsevere_cells:
            print("No ProbSevere cells to debug")
            return
        
        print("=== ProbSevere Data Structure Debug ===")
        print(f"Number of ProbSevere cells: {len(probsevere_cells)}")
        
        # Show first few cells
        for i, cell in enumerate(probsevere_cells[:3]):
            print(f"\nCell {i} keys: {list(cell.keys())}")
            for key, value in cell.items():
                if isinstance(value, (int, float, str)) and len(str(value)) < 50:
                    print(f"  {key}: {value}")
                else:
                    print(f"  {key}: {type(value)}")
        
        # Check for coordinates in first cell
        first_cell = probsevere_cells[0]
        coord_sources = ['lat', 'lon', 'latitude', 'longitude', 'centroid', 'geometry']
        found_coords = []
        
        for source in coord_sources:
            if source in first_cell:
                found_coords.append(f"{source}: {first_cell[source]}")
        
        print(f"\nCoordinate sources found: {found_coords}")

class StormIntegrationUtils:
    """
    Utility functions for integrating various datasets with storm cells.
    """
    
    @staticmethod
    def get_nldn_variable_name(nldn_dataset):
        """Get the lightning variable name from NLDN dataset."""
        # Try common NLDN variable names
        possible_vars = ['lightning_flash_rate', 'unknown', 'flash_rate', 'nldn', 'lightning']
        for var_name in possible_vars:
            if var_name in nldn_dataset.data_vars:
                return var_name
        
        # If no common names found, use the first data variable
        return list(nldn_dataset.data_vars.keys())[0]
    
    @staticmethod
    def get_echotop_variable_name(echotop_dataset):
        """Get the echotop variable name from echotop dataset."""
        # Try common echotop variable names
        possible_vars = ['echotop', 'unknown']
        for var_name in possible_vars:
            if var_name in echotop_dataset.data_vars:
                return var_name
        
        # If no common names found, use the first data variable
        return list(echotop_dataset.data_vars.keys())[0]
    
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
        """
        # Try alpha_shape first (list of [lon, lat] pairs)
        if 'alpha_shape' in cell and cell['alpha_shape']:
            return np.array([[point[0], point[1]] for point in cell['alpha_shape']])
        
        # Fall back to bounding box
        if 'bbox' in cell:
            bbox = cell['bbox']
            return np.array([
                [bbox['lon_min'], bbox['lat_min']],
                [bbox['lon_min'], bbox['lat_max']],
                [bbox['lon_max'], bbox['lat_max']],
                [bbox['lon_max'], bbox['lat_min']]
            ])
        
        # Final fallback: small box around centroid
        if 'centroid' in cell and len(cell['centroid']) >= 2:
            lat, lon = cell['centroid'][0], cell['centroid'][1]
            d = 0.01  # ~1km box
            return np.array([
                [lon - d, lat - d],
                [lon - d, lat + d],
                [lon + d, lat + d],
                [lon + d, lat - d]
            ])
        
        return None
    
    @staticmethod
    def create_polygon_mask(polygon, lat_grid, lon_grid):
        """
        Create a boolean mask for points inside the polygon.
        """
        if polygon is None:
            return None
            
        # Flatten coordinate grids and check which points are inside polygon
        points = np.column_stack((lon_grid.ravel(), lat_grid.ravel()))
        path = Path(polygon)  # This now uses matplotlib.path.Path
        mask_flat = path.contains_points(points)
        return mask_flat.reshape(lat_grid.shape)

class StormCellIntegrator:
    """
    Integrates various datasets with storm cells.
    """
    
    def __init__(self):
        """
        Initialize the integrator.
        """
        pass
    
    def find_closest_storm_history_entry(self, storm_history, target_timestamp):
        """
        Find the storm history entry closest to the target timestamp.
        
        Args:
            storm_history: List of storm history entries
            target_timestamp: datetime object to compare against
            
        Returns:
            Index of the closest entry, or None if no entries found
        """
        if not storm_history:
            return None
            
        # Convert target timestamp to datetime object if it's a string
        if isinstance(target_timestamp, str):
            try:
                target_timestamp = datetime.fromisoformat(target_timestamp.replace('Z', '+00:00'))
            except ValueError:
                print(f"Warning: Could not parse target timestamp: {target_timestamp}")
                return None
        
        closest_index = None
        min_time_diff = float('inf')
        
        for i, entry in enumerate(storm_history):
            if 'timestamp' not in entry:
                continue
                
            try:
                # Parse entry timestamp
                entry_time = datetime.fromisoformat(entry['timestamp'].replace('Z', '+00:00'))
                
                # Calculate time difference
                time_diff = abs((entry_time - target_timestamp).total_seconds())
                
                if time_diff < min_time_diff:
                    min_time_diff = time_diff
                    closest_index = i
                    
            except ValueError as e:
                print(f"Warning: Could not parse timestamp in storm history: {entry['timestamp']} - {e}")
                continue
        
        return closest_index
    
    def integrate_nldn(self, nldn_dataset, storm_cells, nldn_timestamp, output_key='max_flash_rate'):
        """
        Integrate NLDN lightning data with storm cells.
        Returns the maximum flash rate for each cell and adds it to the closest temporal entry.
        
        Args:
            nldn_dataset: Loaded xarray Dataset containing NLDN data
            storm_cells: List of storm cell dictionaries
            nldn_timestamp: Timestamp of the NLDN data
            output_key: Key to store the max flash rate under in each cell
            
        Returns:
            List of storm cells with integrated NLDN flash rate data
        """
        # Get lightning variable name
        lightning_var = StormIntegrationUtils.get_nldn_variable_name(nldn_dataset)
        lightning_data = nldn_dataset[lightning_var].values
        
        # Create coordinate grids
        lat_grid, lon_grid = StormIntegrationUtils.create_coordinate_grids(nldn_dataset)
        
        print(f"Integrating NLDN {lightning_var} data for {len(storm_cells)} storm cells...")
        print(f"NLDN timestamp: {nldn_timestamp}")
        
        for cell in storm_cells:
            cell_id = cell.get('id', 'unknown')
            
            # Find the closest storm history entry to the NLDN timestamp
            if 'storm_history' not in cell or not cell['storm_history']:
                print(f"Warning: Cell {cell_id} has no storm history")
                continue
                
            closest_idx = self.find_closest_storm_history_entry(cell['storm_history'], nldn_timestamp)
            
            if closest_idx is None:
                print(f"Warning: Could not find suitable storm history entry for cell {cell_id}")
                continue
            
            # Create polygon and mask for this cell
            polygon = StormIntegrationUtils.create_cell_polygon(cell)
            if polygon is None:
                cell['storm_history'][closest_idx][output_key] = "N/A"
                continue
                
            mask = StormIntegrationUtils.create_polygon_mask(polygon, lat_grid, lon_grid)
            if mask is None or not np.any(mask):
                cell['storm_history'][closest_idx][output_key] = "N/A"
                continue
            
            try:
                # Extract lightning values within the cell polygon
                cell_lightning = lightning_data[mask]
                
                # Filter out negative values (no lightning) and NaN
                valid_lightning = cell_lightning[(cell_lightning >= 0) & (~np.isnan(cell_lightning))]
                
                if valid_lightning.size == 0:
                    cell['storm_history'][closest_idx][output_key] = "N/A"
                else:
                    max_flash_rate = float(np.max(valid_lightning))
                    cell['storm_history'][closest_idx][output_key] = max_flash_rate
                    
                    # Also add timestamp of the NLDN data for reference
                    cell['storm_history'][closest_idx]['nldn_timestamp'] = nldn_timestamp.isoformat() + 'Z'
                    
            except Exception as e:
                print(f"Error processing cell {cell_id}: {e}")
                cell['storm_history'][closest_idx][output_key] = "N/A"
        
        return storm_cells
    
    def integrate_echotop(self, echotop_dataset, storm_cells, echotop_timestamp, output_key='max_echotop_height'):
        """
        Integrate echotop data with storm cells.
        Returns the maximum echotop height for each cell and adds it to the closest temporal entry.
        
        Args:
            echotop_dataset: Loaded xarray Dataset containing echotop data
            storm_cells: List of storm cell dictionaries
            echotop_timestamp: Timestamp of the echotop data
            output_key: Key to store the max echotop height under in each cell
            
        Returns:
            List of storm cells with integrated echotop height data
        """
        # Get echotop variable name
        echotop_var = StormIntegrationUtils.get_echotop_variable_name(echotop_dataset)
        echotop_data = echotop_dataset[echotop_var].values
        
        # Create coordinate grids
        lat_grid, lon_grid = StormIntegrationUtils.create_coordinate_grids(echotop_dataset)
        
        print(f"Integrating echotop {echotop_var} data for {len(storm_cells)} storm cells...")
        print(f"Echotop timestamp: {echotop_timestamp}")
        
        for cell in storm_cells:
            cell_id = cell.get('id', 'unknown')
            
            # Find the closest storm history entry to the echotop timestamp
            if 'storm_history' not in cell or not cell['storm_history']:
                print(f"Warning: Cell {cell_id} has no storm history")
                continue
                
            closest_idx = self.find_closest_storm_history_entry(cell['storm_history'], echotop_timestamp)
            
            if closest_idx is None:
                print(f"Warning: Could not find suitable storm history entry for cell {cell_id}")
                continue
            
            # Create polygon and mask for this cell
            polygon = StormIntegrationUtils.create_cell_polygon(cell)
            if polygon is None:
                cell['storm_history'][closest_idx][output_key] = "N/A"
                continue
                
            mask = StormIntegrationUtils.create_polygon_mask(polygon, lat_grid, lon_grid)
            if mask is None or not np.any(mask):
                cell['storm_history'][closest_idx][output_key] = "N/A"
                continue
            
            try:
                # Extract echotop values within the cell polygon
                cell_echotop = echotop_data[mask]
                
                # Filter out negative values and NaN
                valid_echotop = cell_echotop[(cell_echotop >= 0) & (~np.isnan(cell_echotop))]
                
                if valid_echotop.size == 0:
                    cell['storm_history'][closest_idx][output_key] = "N/A"
                else:
                    max_echotop_height = float(np.max(valid_echotop))
                    cell['storm_history'][closest_idx][output_key] = max_echotop_height
                    
                    # Also add timestamp of the echotop data for reference
                    cell['storm_history'][closest_idx]['echotop_timestamp'] = echotop_timestamp.isoformat() + 'Z'
                    
            except Exception as e:
                print(f"Error processing cell {cell_id}: {e}")
                cell['storm_history'][closest_idx][output_key] = "N/A"
        
        return storm_cells
    
    def integrate_probsevere(self, probsevere_cells, storm_cells, probsevere_timestamp, max_distance_km=20.0):
        """
        Integrate ProbSevere probability data with storm cells by matching based on 
        spatial proximity to cell centroids at the time of each storm history entry.
        """
        print(f"Integrating ProbSevere data for {len(probsevere_cells)} cells with {len(storm_cells)} storm cells...")
        print(f"ProbSevere timestamp: {probsevere_timestamp}")
        
        # Convert max distance from km to degrees (approximate)
        max_distance_deg = max_distance_km / 111.0
        matches_found = 0
        
        # Debug: Print first few ProbSevere cells with their coordinates
        print(f"\nFirst 5 ProbSevere cells with coordinates:")
        for i, cell in enumerate(probsevere_cells[:5]):
            if 'lat' in cell and 'lon' in cell:
                print(f"  Cell {i}: lat={cell['lat']:.4f}, lon={cell['lon']:.4f}")
            else:
                print(f"  Cell {i}: No coordinates found")
        
        for storm_cell in storm_cells:
            cell_id = storm_cell.get('id', 'unknown')
            
            # Skip cells without storm history
            if 'storm_history' not in storm_cell or not storm_cell['storm_history']:
                print(f"  Cell {cell_id}: No storm history")
                continue
                
            # Find the storm history entry closest to the ProbSevere timestamp
            closest_idx = self.find_closest_storm_history_entry(storm_cell['storm_history'], probsevere_timestamp)
            
            if closest_idx is None:
                print(f"  Cell {cell_id}: No history entry close to ProbSevere timestamp")
                continue
                
            entry = storm_cell['storm_history'][closest_idx]
            
            # Get the centroid coordinates from this entry
            if 'centroid' not in entry or len(entry['centroid']) < 2:
                print(f"  Cell {cell_id}: No centroid in history entry")
                continue
                
            storm_lat, storm_lon = entry['centroid'][0], entry['centroid'][1]
            print(f"  Cell {cell_id}: Looking for ProbSevere match near ({storm_lat:.4f}, {storm_lon:.4f})")
            
            # Find the closest ProbSevere cell to this centroid
            closest_probsevere = None
            min_distance = float('inf')
            
            for i, probsevere_cell in enumerate(probsevere_cells):
                # Extract coordinates from ProbSevere cell
                if 'lat' in probsevere_cell and 'lon' in probsevere_cell:
                    ps_lat = probsevere_cell['lat']
                    ps_lon = probsevere_cell['lon']
                    
                    # Convert storm cell longitude from 0-360 to -180-180 range to match ProbSevere
                    storm_lon_converted = storm_lon - 360 if storm_lon > 180 else storm_lon
                    
                    # Calculate distance (simple Euclidean distance in degrees)
                    distance = np.sqrt((storm_lat - ps_lat)**2 + (storm_lon_converted - ps_lon)**2)
                    
                    if distance < min_distance and distance <= max_distance_deg:
                        min_distance = distance
                        closest_probsevere = probsevere_cell
                        closest_idx = i
            
            # If we found a matching ProbSevere cell, integrate the data
            if closest_probsevere is not None:
                # Convert distance back to km for reporting
                distance_km = min_distance * 111.0
                
                entry.update({
                    'prob_severe': closest_probsevere.get('prob_severe', 0),
                    'prob_hail': closest_probsevere.get('prob_hail', 0),
                    'prob_wind': closest_probsevere.get('prob_wind', 0),
                    'prob_tornado': closest_probsevere.get('prob_tornado', 0),
                    'probsevere_mesh': closest_probsevere.get('mesh', 0),
                    'probsevere_vil': closest_probsevere.get('vil', 0),
                    'probsevere_flash_rate': closest_probsevere.get('flash_rate', 0),
                    'probsevere_mucape': closest_probsevere.get('mucape', 0),
                    'probsevere_mlcape': closest_probsevere.get('mlcape', 0),
                    'probsevere_mlcin': closest_probsevere.get('mlcin', 0),
                    'probsevere_ebshear': closest_probsevere.get('ebshear', 0),
                    'probsevere_srh_1km': closest_probsevere.get('srh_1km', 0),
                    'probsevere_mean_wind_1_3km': closest_probsevere.get('mean_wind_1_3km', 0),
                    'probsevere_timestamp': probsevere_timestamp.isoformat() + 'Z',
                    'probsevere_distance_km': round(distance_km, 2),
                    'probsevere_area_sq_km': closest_probsevere.get('area_sq_km', 0)
                })
                
                print(f"  ✓ Matched cell {cell_id} with ProbSevere cell {closest_idx} (distance: {distance_km:.2f} km)")
                matches_found += 1
            else:
                print(f"  ✗ No ProbSevere cell found within {max_distance_km}km of cell {cell_id}")
        
        print(f"ProbSevere integration completed: {matches_found} matches found")
        return storm_cells
    
def main():
    """
    Main function to run integration of all data types with storm cells.
    """
    handler = StatFileHandler()
    
    # Load storm cells
    storm_json_path = "stormcell_test.json"
    print(f"Loading storm cells from {storm_json_path}")
    cells = handler.load_json(storm_json_path)
    if cells is None:
        print("No storm cells loaded; aborting.")
        return
    
    integrator = StormCellIntegrator()
    result_cells = cells
    
    # 1. Integrate NLDN lightning data
    print("\n" + "="*50)
    print("INTEGRATING NLDN LIGHTNING DATA")
    print("="*50)
    nldn_list = fs.latest_nldn(1)
    if nldn_list:
        nldn_path = nldn_list[-1]
        print(f"Using NLDN file: {nldn_path}")
        
        nldn_timestamp = handler.find_timestamp(nldn_path)
        if nldn_timestamp:
            try:
                nldn_ds = xr.open_dataset(nldn_path)
                result_cells = integrator.integrate_nldn(nldn_ds, result_cells, nldn_timestamp)
                print("NLDN integration completed successfully")
            except Exception as e:
                print(f"Failed to integrate NLDN data: {e}")
        else:
            print("Could not determine timestamp from NLDN file")
    else:
        print("No NLDN files found")
    
    # 2. Integrate EchoTop data
    print("\n" + "="*50)
    print("INTEGRATING ECHOTOP DATA")
    print("="*50)
    echotop_list = fs.latest_echotop18(1)  # Assuming you have a similar function for echotop
    if echotop_list:
        echotop_path = echotop_list[-1]
        print(f"Using EchoTop file: {echotop_path}")
        
        echotop_timestamp = handler.find_timestamp(echotop_path)
        if echotop_timestamp:
            try:
                echotop_ds = xr.open_dataset(echotop_path)
                result_cells = integrator.integrate_echotop(echotop_ds, result_cells, echotop_timestamp)
                print("EchoTop integration completed successfully")
            except Exception as e:
                print(f"Failed to integrate EchoTop data: {e}")
        else:
            print("Could not determine timestamp from EchoTop file")
    else:
        print("No EchoTop files found")
    
    # 3. Integrate ProbSevere data
    print("\n" + "="*50)
    print("INTEGRATING PROBSEVERE DATA")
    print("="*50)
    probsevere_list = fs.latest_probsevere(1)  # Assuming you have a function for probsevere files
    if probsevere_list:
        probsevere_path = probsevere_list[-1]
        print(f"Using ProbSevere file: {probsevere_path}")
        
        # Load and parse ProbSevere data
        probsevere_data = handler.load_json(probsevere_path)
        if probsevere_data:
            probsevere_cells = handler.parse_probsevere_json(probsevere_data)
            
            # DEBUG: Print structure of ProbSevere data
            handler.debug_probsevere_structure(probsevere_cells)
            
            # Extract timestamp from ProbSevere data
            probsevere_timestamp_str = probsevere_data.get('validTime', '').replace('_', ' ')
            try:
                probsevere_timestamp = datetime.strptime(probsevere_timestamp_str, "%Y%m%d %H%M%S UTC")
                result_cells = integrator.integrate_probsevere(probsevere_cells, result_cells, probsevere_timestamp)
                print("ProbSevere integration completed successfully")
            except ValueError:
                print(f"Could not parse timestamp from ProbSevere data: {probsevere_timestamp_str}")
        else:
            print("Failed to load ProbSevere JSON data")
    else:
        print("No ProbSevere files found")
    
    # Save final integrated results
    output_json = storm_json_path.replace('.json', '_fully_integrated.json')
    print(f"\nSaving fully integrated results to {output_json}")
    handler.write_json(result_cells, output_json)
    
    # Print comprehensive summary
    print("\n" + "="*60)
    print("INTEGRATION SUMMARY")
    print("="*60)
    
    total_cells = len(result_cells)
    cells_with_nldn = 0
    cells_with_echotop = 0
    cells_with_probsevere = 0
    total_entries = 0
    
    for cell in result_cells:
        if 'storm_history' in cell:
            for entry in cell['storm_history']:
                total_entries += 1
                if isinstance(entry.get('max_flash_rate', "N/A"), (int, float)):
                    cells_with_nldn += 1
                if isinstance(entry.get('max_echotop_height', "N/A"), (int, float)):
                    cells_with_echotop += 1
                if isinstance(entry.get('prob_severe', "N/A"), (int, float)):
                    cells_with_probsevere += 1
    
    print(f"Total storm cells: {total_cells}")
    print(f"Total storm history entries: {total_entries}")
    print(f"Entries with NLDN data: {cells_with_nldn}/{total_entries}")
    print(f"Entries with EchoTop data: {cells_with_echotop}/{total_entries}")
    print(f"Entries with ProbSevere data: {cells_with_probsevere}/{total_entries}")
    print("="*60)
    print(f"Integration complete! Results saved to {output_json}")

if __name__ == "__main__":
    main()