from util.file import StatFileHandler
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
    
    def integrate_preciprate(self, preciprate_dataset, storm_cells, preciprate_timestamp, output_key='max_precip_rate'):
        """
        Integrate MRMS PrecipRate data with storm cells.
        Returns the maximum precipitation rate for each cell and adds it to the closest temporal entry.
        
        Args:
            preciprate_dataset: Loaded xarray Dataset containing MRMS PrecipRate data
            storm_cells: List of storm cell dictionaries
            preciprate_timestamp: Timestamp of the PrecipRate data
            output_key: Key to store the max precip rate under in each cell
            
        Returns:
            List of storm cells with integrated precip rate data
        """
        # Get precip rate variable name
        preciprate_var = StormIntegrationUtils.get_preciprate_variable_name(preciprate_dataset)
        preciprate_data = preciprate_dataset[preciprate_var].values
        
        # Create coordinate grids
        lat_grid, lon_grid = StormIntegrationUtils.create_coordinate_grids(preciprate_dataset)
        
        print(f"Integrating MRMS PrecipRate {preciprate_var} data for {len(storm_cells)} storm cells...")
        print(f"PrecipRate timestamp: {preciprate_timestamp}")
        
        for cell in storm_cells:
            cell_id = cell.get('id', 'unknown')
            
            # Find the closest storm history entry to the preciprate timestamp
            if 'storm_history' not in cell or not cell['storm_history']:
                print(f"Warning: Cell {cell_id} has no storm history")
                continue
                
            closest_idx = self.find_closest_storm_history_entry(cell['storm_history'], preciprate_timestamp)
            
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
                # Extract precip rate values within the cell polygon
                cell_preciprate = preciprate_data[mask]
                
                # Filter out negative values and NaN
                valid_preciprate = cell_preciprate[(cell_preciprate >= 0) & (~np.isnan(cell_preciprate))]
                
                if valid_preciprate.size == 0:
                    cell['storm_history'][closest_idx][output_key] = "N/A"
                else:
                    max_precip_rate = float(np.max(valid_preciprate))
                    cell['storm_history'][closest_idx][output_key] = max_precip_rate
                    
                    # Also add timestamp of the precip rate data for reference
                    cell['storm_history'][closest_idx]['preciprate_timestamp'] = preciprate_timestamp.isoformat() + 'Z'
                    
            except Exception as e:
                print(f"Error processing cell {cell_id}: {e}")
                cell['storm_history'][closest_idx][output_key] = "N/A"
        
        return storm_cells
    
    def integrate_vil_density(self, vil_density_dataset, storm_cells, vil_density_timestamp, output_key='max_vil_density'):
        """
        Integrate MRMS VIL Density data with storm cells.
        Returns the maximum VIL density for each cell and adds it to the closest temporal entry.
        
        Args:
            vil_density_dataset: Loaded xarray Dataset containing MRMS VIL Density data
            storm_cells: List of storm cell dictionaries
            vil_density_timestamp: Timestamp of the VIL Density data
            output_key: Key to store the max VIL density under in each cell
            
        Returns:
            List of storm cells with integrated VIL density data
        """
        # Get VIL density variable name
        vil_density_var = StormIntegrationUtils.get_vil_density_variable_name(vil_density_dataset)
        vil_density_data = vil_density_dataset[vil_density_var].values
        
        # Create coordinate grids
        lat_grid, lon_grid = StormIntegrationUtils.create_coordinate_grids(vil_density_dataset)
        
        print(f"Integrating MRMS VIL Density {vil_density_var} data for {len(storm_cells)} storm cells...")
        print(f"VIL Density timestamp: {vil_density_timestamp}")
        
        for cell in storm_cells:
            cell_id = cell.get('id', 'unknown')
            
            # Find the closest storm history entry to the vil_density timestamp
            if 'storm_history' not in cell or not cell['storm_history']:
                print(f"Warning: Cell {cell_id} has no storm history")
                continue
                
            closest_idx = self.find_closest_storm_history_entry(cell['storm_history'], vil_density_timestamp)
            
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
                # Extract VIL density values within the cell polygon
                cell_vil_density = vil_density_data[mask]
                
                # Filter out negative values and NaN
                valid_vil_density = cell_vil_density[(cell_vil_density >= 0) & (~np.isnan(cell_vil_density))]
                
                if valid_vil_density.size == 0:
                    cell['storm_history'][closest_idx][output_key] = "N/A"
                else:
                    max_vil_density = float(np.max(valid_vil_density))
                    cell['storm_history'][closest_idx][output_key] = max_vil_density
                    
                    # Also add timestamp of the VIL density data for reference
                    cell['storm_history'][closest_idx]['vil_density_timestamp'] = vil_density_timestamp.isoformat() + 'Z'
                    
            except Exception as e:
                print(f"Error processing cell {cell_id}: {e}")
                cell['storm_history'][closest_idx][output_key] = "N/A"
        
        return storm_cells