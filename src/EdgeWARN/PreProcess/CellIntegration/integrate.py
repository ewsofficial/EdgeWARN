from .utils import StormIntegrationUtils
import xarray as xr
import gc
import numpy as np

class StormCellIntegrator:
    def __init__(self):
        pass

    def integrate_ds(self, dataset_path, storm_cells, output_key):
        print(f"[CellIntegration] DEBUG: Integrating dataset for {len(storm_cells)} storm cells")

        try:
            ds = xr.open_dataset(dataset_path, chunks={"lat": 50, "lon": 50}, decode_timedelta=True)
            var = ds.get("unknown")
            if var is None:
                for cell in storm_cells:
                    if cell.get("storm_history"):
                        cell["storm_history"][-1][output_key] = "MISSING_UNKNOWN_VAR"
                ds.close()
                return storm_cells
        except Exception as e:
            for cell in storm_cells:
                if cell.get("storm_history"):
                    cell["storm_history"][-1][output_key] = "DATASET_LOAD_ERROR"
            return storm_cells

        lat_name = "latitude" if "latitude" in ds.coords else "lat"
        lon_name = "longitude" if "longitude" in ds.coords else "lon"

        for cell in storm_cells:
            if not cell.get("storm_history"):
                continue

            latest = cell["storm_history"][-1]
            poly = StormIntegrationUtils.create_cell_polygon(cell)
            if poly is None:
                latest[output_key] = "N/A"
                continue

            min_lat, min_lon, max_lat, max_lon = poly.bounds

            try:
                # Slice lazily to bounding box
                subset = var.sel(**{lat_name: slice(min_lat, max_lat),
                                    lon_name: slice(min_lon, max_lon)})
                
                # Apply bounding-box mask only
                mask = ((subset[lon_name] >= min_lon) & (subset[lon_name] <= max_lon))
                max_val = subset.where(mask & (subset >= 0)).max().compute()

                latest[output_key] = float(max_val) if not np.isnan(max_val) else "N/A"

            except Exception:
                latest[output_key] = "PROCESSING_ERROR"

            finally:
                del subset, mask, poly
                gc.collect()

        ds.close()
        del var, ds
        gc.collect()
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

        matches_found = 0

        for storm_cell in storm_cells:
            cell_id = storm_cell.get('id', 'unknown')

            if 'storm_history' not in storm_cell or not storm_cell['storm_history']:
                continue

            entry = storm_cell['storm_history'][-1]

            if 'centroid' not in entry or len(entry['centroid']) < 2:
                continue

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
                    'MLCAPE': float(closest_probsevere.get('MLCAPE', 0)),
                    'MUCAPE': float(closest_probsevere.get('MUCAPE', 0)),
                    'MLCIN': float(closest_probsevere.get('MLCIN', 0)),
                    'DCAPE': float(closest_probsevere.get('DCAPE', 0)),
                    'CAPE_M10M30': float(closest_probsevere.get('CAPE_M10M30', 0)),
                    'LCL': float(closest_probsevere.get('LCL', 0)),
                    'Wetbulb_0C_Hgt': float(closest_probsevere.get('WETBULB_0C_HGT', 0)),
                    'LLLR': float(closest_probsevere.get('LLLR', 0)),
                    'MLLR': float(closest_probsevere.get('MLLR', 0)),

                    # --- Kinematics ---
                    'EBShear': float(closest_probsevere.get('EBSHEAR', 0)),
                    'SRH01km': float(closest_probsevere.get('SRH01KM', 0)),
                    'SRH02km': float(closest_probsevere.get('SRW02KM', 0)),
                    'SRW46km': float(closest_probsevere.get('SRW46KM', 0)),
                    'MeanWind_1-3kmAGL': float(closest_probsevere.get('MEANWIND_1-3kmAGL', 0)),
                    'LJA': float(closest_probsevere.get('LJA', 0)),

                    # --- Radar / Reflectivity ---
                    'CompRef': float(closest_probsevere.get('COMPREF', 0)),
                    'Ref10': float(closest_probsevere.get('REF10', 0)),
                    'Ref20': float(closest_probsevere.get('REF20', 0)),
                    'MESH': float(closest_probsevere.get('MESH', 0)),
                    'H50_Above_0C': float(closest_probsevere.get('H50_Above_0C', 0)),
                    'EchoTop50': float(closest_probsevere.get('EchoTop_50', 0)),
                    'VIL': float(closest_probsevere.get('VIL', 0)),

                    # --- Lightning / Electrical ---
                    'MaxFED': float(closest_probsevere.get('MaxFED', 0)),
                    'MaxFCD': float(closest_probsevere.get('MaxFCD', 0)),
                    'AccumFCD': float(closest_probsevere.get('AccumFCD', 0)),
                    'MinFlashArea': float(closest_probsevere.get('MinFlashArea', 0)),
                    'TE@MaxFCD': float(closest_probsevere.get('TE@MaxFCD', 0)),
                    'FlashRate': float(closest_probsevere.get('FLASH_RATE', 0)),
                    'FlashDensity': float(closest_probsevere.get('FLASH_DENSITY', 0)),
                    'MaxLLAz': float(closest_probsevere.get('MAXLLAZ', 0)),
                    'p98LLAz': float(closest_probsevere.get('P98LLAZ', 0)),
                    'p98MLAz': float(closest_probsevere.get('P98MLAZ', 0)),
                    'MaxRC_Emiss': float(closest_probsevere.get('MAXRC_EMISS', 0)),
                    'ICP': float(closest_probsevere.get('ICP', 0)),

                    # --- Precipitable Water ---
                    'PWAT': float(closest_probsevere.get('PWAT', 0)),

                    # --- Beam Height ---
                    'avg_beam_hgt': float(closest_probsevere.get('AVG_BEAM_HGT', 0)),
                }

                print(f"[CellIntegration] DEBUG: Matched cell {cell_id} with ProbSevere feature (distance: {distance_km:.2f} km)")
                matches_found += 1

        print(f"[CellIntegration] DEBUG: ProbSevere integration completed: {matches_found} matches found")
        return storm_cells