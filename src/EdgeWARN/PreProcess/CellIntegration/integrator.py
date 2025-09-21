from .utils import StormIntegrationUtils
from datetime import datetime
from shapely.geometry import shape
import numpy as np
from matplotlib.path import Path


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
    
    def integrate_ds(self, dataset, storm_cells, timestamp, output_key):
        """
        Integrate dataset with storm cells.
        Returns maximum value in the dataset for each cell and adds it to the closest temporal entry.
        Args:
            dataset: Loaded xarray Dataset containing data
            storm_cells: List of storm cell dictionaries
            timestamp: Dataset timestamp
            output_key: Key to store data under in each cell

        Returns:
            List of storm cells with integrated dataset data
        """
        # Get variable name
        ds_var = 'unknown'
        ds_data = dataset[ds_var].values

        # Create coordinate grids
        lat_grid, lon_grid = StormIntegrationUtils.create_coordinate_grids(dataset)

        print(f"Integrating dataset variable: {ds_var} for {len(storm_cells)} storm cells")
        print(f"Dataset timestamp {timestamp}")

        for cell in storm_cells:
            cell_id = cell.get('id', 'unknown')
            
            # Find the closest storm history entry to the dataset timestamp
            if 'storm_history' not in cell or not cell['storm_history']:
                print(f"Warning: Cell {cell_id} has no storm history")
                continue
                
            closest_idx = self.find_closest_storm_history_entry(cell['storm_history'], timestamp)
            
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
                # Extract values within the cell polygon
                cell_data = ds_data[mask]
                
                # Filter out negative values (no data) and NaN
                valid_data = cell_data[(cell_data >= 0) & (~np.isnan(cell_data))]
                
                if valid_data.size == 0:
                    cell['storm_history'][closest_idx][output_key] = "N/A"
                else:
                    max_flash_rate = float(np.max(valid_data))
                    cell['storm_history'][closest_idx][output_key] = max_flash_rate
                    
                    # Also add timestamp of the data for reference
                    cell['storm_history'][closest_idx]['nldn_timestamp'] = timestamp.isoformat() + 'Z'
                    
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