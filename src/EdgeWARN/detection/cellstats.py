import xarray as xr
import util.file as fs
from util.file import StatFileHandler
import numpy as np
from matplotlib.path import Path
from datetime import datetime
from shapely.geometry import shape

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
    
    def integrate_probsevere(self, probsevere_data, storm_cells, probsevere_timestamp, max_distance_km=25.0):
        """
        Integrate ProbSevere probability data with storm cells by matching based on 
        spatial proximity to cell centroids at the time of each storm history entry.
        """
        if not isinstance(probsevere_data, dict) or 'features' not in probsevere_data:
            print("Error: Invalid ProbSevere data format")
            return storm_cells
        
        probsevere_features = probsevere_data['features']
        print(f"Integrating ProbSevere data for {len(probsevere_features)} features with {len(storm_cells)} storm cells...")
        print(f"ProbSevere timestamp: {probsevere_timestamp}")

        # Convert max distance from km to degrees (approximate, at mid-latitudes)
        max_distance_deg = max_distance_km / 111.0
        matches_found = 0

        for storm_cell in storm_cells:
            cell_id = storm_cell.get('id', 'unknown')

            if 'storm_history' not in storm_cell or not storm_cell['storm_history']:
                continue

            closest_idx = self.find_closest_storm_history_entry(storm_cell['storm_history'], probsevere_timestamp)
            if closest_idx is None:
                continue

            entry = storm_cell['storm_history'][closest_idx]

            if 'centroid' not in entry or len(entry['centroid']) < 2:
                continue

            storm_lat, storm_lon = entry['centroid'][0], entry['centroid'][1]
            storm_lon_converted = storm_lon - 360 if storm_lon > 180 else storm_lon

            closest_probsevere = None
            min_distance = float('inf')

            for feature in probsevere_features:
                props = feature.get('properties', {})
                try:
                    geom = feature.get('geometry')
                    if geom:
                        polygon = shape(geom)
                        ps_lon, ps_lat = polygon.centroid.x, polygon.centroid.y
                    else:
                        continue
                except (ValueError, TypeError):
                    continue

                if np.isnan(ps_lat) or np.isnan(ps_lon):
                    continue

                distance = np.sqrt((storm_lat - ps_lat) ** 2 + (storm_lon_converted - ps_lon) ** 2)

                if distance < min_distance and distance <= max_distance_deg:
                    min_distance = distance
                    closest_probsevere = props

            if closest_probsevere:
                distance_km = min_distance * 111.0

                # Core probabilities (flat at top level)
                entry['prob_severe'] = float(closest_probsevere.get('ProbSevere', 0))
                entry['prob_hail']   = float(closest_probsevere.get('ProbHail', 0))
                entry['prob_wind']   = float(closest_probsevere.get('ProbWind', 0))
                entry['prob_tor']    = float(closest_probsevere.get('ProbTor', 0))

                # Nested supporting fields
                entry['probsevere_details'] = {
                    # --- Atmospheric Instability ---
                    'mlcape': float(closest_probsevere.get('MLCAPE', 0)),
                    'mucape': float(closest_probsevere.get('MUCAPE', 0)),
                    'mlcin': float(closest_probsevere.get('MLCIN', 0)),
                    'dcape': float(closest_probsevere.get('DCAPE', 0)),
                    'cape_m10m30': float(closest_probsevere.get('CAPE_M10M30', 0)),
                    'lcl': float(closest_probsevere.get('LCL', 0)),
                    'wetbulb_0c_hgt': float(closest_probsevere.get('WETBULB_0C_HGT', 0)),
                    'lllr': float(closest_probsevere.get('LLLR', 0)),
                    'mllr': float(closest_probsevere.get('MLLR', 0)),

                    # --- Wind / Shear / Rotation ---
                    'ebshear': float(closest_probsevere.get('EBSHEAR', 0)),
                    'srh01km': float(closest_probsevere.get('SRH01KM', 0)),
                    'srw02km': float(closest_probsevere.get('SRW02KM', 0)),
                    'srw46km': float(closest_probsevere.get('SRW46KM', 0)),
                    'meanwind_1_3kmagl': float(closest_probsevere.get('MEANWIND_1-3kmAGL', 0)),
                    'lja': float(closest_probsevere.get('LJA', 0)),

                    # --- Radar / Reflectivity ---
                    'compref': float(closest_probsevere.get('COMPREF', 0)),
                    'ref10': float(closest_probsevere.get('REF10', 0)),
                    'ref20': float(closest_probsevere.get('REF20', 0)),
                    'mesh': float(closest_probsevere.get('MESH', 0)),
                    'h50_above_0c': float(closest_probsevere.get('H50_Above_0C', 0)),
                    'echo_top_50': float(closest_probsevere.get('EchoTop_50', 0)),
                    'vil': float(closest_probsevere.get('VIL', 0)),

                    # --- Lightning / Electrical ---
                    'maxfed': float(closest_probsevere.get('MaxFED', 0)),
                    'maxfcd': float(closest_probsevere.get('MaxFCD', 0)),
                    'accumfcd': float(closest_probsevere.get('AccumFCD', 0)),
                    'minflasharea': float(closest_probsevere.get('MinFlashArea', 0)),
                    'te_at_maxfcd': float(closest_probsevere.get('TE@MaxFCD', 0)),
                    'flash_rate': float(closest_probsevere.get('FLASH_RATE', 0)),
                    'flash_density': float(closest_probsevere.get('FLASH_DENSITY', 0)),
                    'maxllaz': float(closest_probsevere.get('MAXLLAZ', 0)),
                    'p98llaz': float(closest_probsevere.get('P98LLAZ', 0)),
                    'p98mlaz': float(closest_probsevere.get('P98MLAZ', 0)),
                    'maxrc_emiss': float(closest_probsevere.get('MAXRC_EMISS', 0)),
                    'icp': float(closest_probsevere.get('ICP', 0)),

                    # --- Precipitable Water ---
                    'pwat': float(closest_probsevere.get('PWAT', 0)),

                    # --- Probabilities / Hazards ---
                    'probsevere': float(closest_probsevere.get('ProbSevere', 0)),
                    'probhail': float(closest_probsevere.get('ProbHail', 0)),
                    'probwind': float(closest_probsevere.get('ProbWind', 0)),
                    'probtorn': float(closest_probsevere.get('ProbTor', 0)),

                    # --- Storm Motion / Location ---
                    'mlat': float(closest_probsevere.get('MLAT', 0)),
                    'mlon': float(closest_probsevere.get('MLON', 0)),
                    'motion_east': float(closest_probsevere.get('MOTION_EAST', 0)),
                    'motion_south': float(closest_probsevere.get('MOTION_SOUTH', 0)),

                    # --- Storm Size / Geometry ---
                    'size': float(closest_probsevere.get('SIZE', 0)),
                    'avg_beam_hgt': float(closest_probsevere.get('AVG_BEAM_HGT', 0)),
                }


                # Metadata
                entry['probsevere_timestamp'] = probsevere_timestamp.isoformat() + 'Z'
                entry['probsevere_distance_km'] = round(distance_km, 2)

                print(f"  ✓ Matched cell {cell_id} with ProbSevere feature (distance: {distance_km:.2f} km)")
                matches_found += 1

        print(f"ProbSevere integration completed: {matches_found} matches found")
        return storm_cells

    
    def integrate_glm(self, glm_dataset, storm_cells, glm_timestamp, output_key='glm_flashrate_1m'):
        """
        Integrate GLM lightning data with storm cells.
        Counts flashes within each cell's bounding box and calculates flash rate.
        
        Args:
            glm_dataset: Loaded xarray Dataset containing GLM data
            storm_cells: List of storm cell dictionaries
            glm_timestamp: Start timestamp of the GLM data
            output_key: Key to store the flash rate under in each cell
            
        Returns:
            List of storm cells with integrated GLM flash rate data
        """
        # Extract flash locations from GLM dataset
        if 'flash_lat' not in glm_dataset.coords or 'flash_lon' not in glm_dataset.coords:
            print("Error: GLM dataset missing flash_lat or flash_lon coordinates")
            return storm_cells
        
        flash_lats = glm_dataset.flash_lat.values
        flash_lons = glm_dataset.flash_lon.values
        
        print(f"Integrating GLM flash data for {len(storm_cells)} storm cells...")
        print(f"GLM timestamp: {glm_timestamp}")
        print(f"Found {len(flash_lats)} flashes in GLM data")
        
        # Debug: Print first few flash locations
        print(f"First 5 flash locations:")
        for i in range(min(5, len(flash_lats))):
            print(f"  Flash {i}: lat={flash_lats[i]:.4f}, lon={flash_lons[i]:.4f}")
        
        for cell in storm_cells:
            cell_id = cell.get('id', 'unknown')
            
            # Find the closest storm history entry to the GLM timestamp
            if 'storm_history' not in cell or not cell['storm_history']:
                print(f"Warning: Cell {cell_id} has no storm history")
                continue
                
            closest_idx = self.find_closest_storm_history_entry(cell['storm_history'], glm_timestamp)
            
            if closest_idx is None:
                print(f"Warning: Could not find suitable storm history entry for cell {cell_id}")
                continue
            
            # Create polygon for this cell (convert to -180 to 180 longitude range)
            polygon = StormIntegrationUtils.create_cell_polygon(cell)
            if polygon is None:
                cell['storm_history'][closest_idx][output_key] = "N/A"
                continue
            
            try:
                # Convert polygon coordinates from 0-360 to -180-180 range to match GLM
                polygon_180 = []
                for point in polygon:
                    lon = point[0]
                    lat = point[1]
                    # Convert longitude from 0-360 to -180-180
                    if lon > 180:
                        lon -= 360
                    polygon_180.append([lon, lat])
                
                # Count flashes within the cell polygon
                flash_count = 0
                path = Path(polygon_180)
                
                # Check each flash location
                for i in range(len(flash_lats)):
                    if not np.isnan(flash_lats[i]) and not np.isnan(flash_lons[i]):
                        point = [flash_lons[i], flash_lats[i]]
                        if path.contains_point(point):
                            flash_count += 1
                
                # GLM data typically covers 1 minute, so multiply by 3 to get 3-minute equivalent
                # and convert to flashes per minute
                flash_rate_per_min = flash_count * 3  # Convert to minutely flash rate
                
                cell['storm_history'][closest_idx][output_key] = float(flash_rate_per_min)
                
                # Also add timestamp of the GLM data for reference
                cell['storm_history'][closest_idx]['glm_timestamp'] = glm_timestamp.isoformat() + 'Z'
                cell['storm_history'][closest_idx]['glm_flash_count'] = int(flash_count)
                
                # Debug output for cells with flashes
                if flash_count > 0:
                    print(f"Cell {cell_id}: Found {flash_count} flashes (rate: {flash_rate_per_min}/min)")
                
            except Exception as e:
                print(f"Error processing cell {cell_id}: {e}")
                cell['storm_history'][closest_idx][output_key] = "N/A"
        
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
        
        # Load raw ProbSevere JSON
        probsevere_data = handler.load_json(probsevere_path)
        if probsevere_data:
            # DEBUG: print the raw structure
            handler.debug_probsevere_structure(probsevere_data.get("features", []))
            
            # Extract timestamp from ProbSevere data
            probsevere_timestamp_str = probsevere_data.get('validTime', '').replace('_', ' ')
            try:
                probsevere_timestamp = datetime.strptime(probsevere_timestamp_str, "%Y%m%d %H%M%S UTC")
                # Pass the full JSON dict instead of just features
                result_cells = integrator.integrate_probsevere(probsevere_data, result_cells, probsevere_timestamp)
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
    cells_with_glm = 0
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
                if isinstance(entry.get('glm_flashrate_1m', "N/A"), (int, float)):
                    cells_with_glm += 1
    
    print(f"Total storm cells: {total_cells}")
    print(f"Total storm history entries: {total_entries}")
    print(f"Entries with NLDN data: {cells_with_nldn}/{total_entries}")
    print(f"Entries with EchoTop data: {cells_with_echotop}/{total_entries}")
    print(f"Entries with ProbSevere data: {cells_with_probsevere}/{total_entries}")
    print(f"Entries with GLM data: {cells_with_glm}/{total_entries}")
    print("="*60)
    print(f"Integration complete! Results saved to {output_json}")

if __name__ == "__main__":
    main()