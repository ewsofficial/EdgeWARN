from .utils import StormIntegrationUtils
from datetime import datetime
from shapely.geometry import shape
import numpy as np
import xarray as xr
from matplotlib.path import Path
import gc


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
                print(f"[CellIntegration] ERROR: Could not parse target timestamp: {target_timestamp}")
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
                print(f"[CellIntegration] ERROR: Could not parse timestamp in storm history: {entry['timestamp']} - {e}")
                continue
        
        return closest_index
    
    def integrate_ds(self, dataset_path, storm_cells, output_key):
        """
        Integrate dataset with storm cells using lazy loading.
        Returns maximum value in the dataset for each cell and adds it to the latest entry.
        Args:
            dataset_path: Path to the dataset file (lazy loaded)
            storm_cells: List of storm cell dictionaries
            timestamp: Dataset timestamp
            output_key: Key to store data under in each cell

        Returns:
            List of storm cells with integrated dataset data
        """
        print(f"[CellIntegration] DEBUG: Integrating dataset for {len(storm_cells)} storm cells")

        # Lazy load the dataset only when needed
        dataset_loaded = False
        ds_data = None
        lat_grid = None
        lon_grid = None
        
        for i, cell in enumerate(storm_cells):
            cell_id = cell.get('id', f'unknown_{i}')
            
            # Check if storm history exists and has entries
            if 'storm_history' not in cell or not cell['storm_history']:
                continue
                
            # Use the latest storm history entry (last in the list)
            latest_entry = cell['storm_history'][-1]
            
            # Create polygon for this cell
            polygon = StormIntegrationUtils.create_cell_polygon(cell)
            if polygon is None:
                latest_entry[output_key] = "N/A"
                continue
            
            # Lazy load dataset only when we have a valid polygon to process
            if not dataset_loaded:
                try:
                    dataset = xr.open_dataset(dataset_path, decode_timedelta=True)
                    ds_var = list(dataset.data_vars.keys())[0]  # Get first variable
                    ds_data = dataset[ds_var].values
                    lat_grid, lon_grid = StormIntegrationUtils.create_coordinate_grids(dataset)
                    dataset_loaded = True
                    dataset.close()
                except Exception as e:
                    print(f"[CellIntegration] ERROR: Failed to load dataset {dataset_path}: {e}")
                    # Mark all cells with error
                    for temp_cell in storm_cells:
                        if 'storm_history' in temp_cell and temp_cell['storm_history']:
                            temp_cell['storm_history'][-1][output_key] = "DATASET_LOAD_ERROR"
                    return storm_cells
            
            # Create mask for this cell
            mask = StormIntegrationUtils.create_polygon_mask(polygon, lat_grid, lon_grid)
            if mask is None or not np.any(mask):
                latest_entry[output_key] = "N/A"
                continue
            
            try:
                # Extract values within the cell polygon
                cell_data = ds_data[mask]
                
                # Filter out negative values (no data) and NaN
                valid_data = cell_data[(cell_data >= 0) & (~np.isnan(cell_data))]
                
                if valid_data.size == 0:
                    latest_entry[output_key] = "N/A"
                else:
                    max_value = float(np.max(valid_data))
                    latest_entry[output_key] = max_value
                    
            except Exception as e:
                print(f"[CellIntegration] ERROR: Could not process cell {cell_id}: {e}")
                latest_entry[output_key] = "PROCESSING_ERROR"
        
        # Memory cleanup
        try:
            del ds_data
            del lat_grid
            del lon_grid
            gc.collect()
        except NameError:
            pass
                    
        return storm_cells

    def integrate_probsevere(self, probsevere_data, storm_cells, max_distance_km=20.0):
        """
        Integrate ProbSevere probability data with storm cells by matching based on 
        spatial proximity to cell centroids at the time of each storm history entry.
        """
        if not isinstance(probsevere_data, dict) or 'features' not in probsevere_data:
            print("[CellIntegration] ERROR: Invalid ProbSevere data format")
            return storm_cells
        
        probsevere_features = probsevere_data['features']
        print(f"[CellIntegration] DEBUG: Integrating ProbSevere data for {len(probsevere_features)} features with {len(storm_cells)} storm cells...")

        # Convert max distance from km to degrees (approximate, at mid-latitudes)
        max_distance_deg = max_distance_km / 111.0
        matches_found = 0

        for storm_cell in storm_cells:
            cell_id = storm_cell.get('id', 'unknown')

            if 'storm_history' not in storm_cell or not storm_cell['storm_history']:
                continue

            entry = storm_cell['storm_history'][-1]

            if 'centroid' not in entry or len(entry['centroid']) < 2:
                continue

            storm_lat, storm_lon = entry['centroid'][0], entry['centroid'][1]
            storm_lon_converted = storm_lon - 360 if storm_lon > 180 else storm_lon

            # Match by ID instead of distance
            closest_probsevere = None
            for feature in probsevere_features:
                feature_id = feature.get('id') or feature.get('properties', {}).get('ID')
                if feature_id == cell_id:
                    closest_probsevere = feature.get('properties', {})
                    break

            if closest_probsevere:
                distance_km = 0.0  # ID match, distance not used

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
                    # --- Storm Size / Geometry ---
                    'size': float(closest_probsevere.get('SIZE', 0)),
                    'avg_beam_hgt': float(closest_probsevere.get('AVG_BEAM_HGT', 0)),
                }


                # Metadata
                entry['probsevere_distance_km'] = round(distance_km, 2)

                print(f"[CellIntegration] DEBUG: Matched cell {cell_id} with ProbSevere feature (distance: {distance_km:.2f} km)")
                matches_found += 1

        print(f"[CellIntegration] DEBUG: ProbSevere integration completed: {matches_found} matches found")
        return storm_cells