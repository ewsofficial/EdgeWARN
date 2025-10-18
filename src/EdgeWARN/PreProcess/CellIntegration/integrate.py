from .utils import StormIntegrationUtils
import xarray as xr
import numpy as np
import gc

class StormCellIntegrator:
    def __init__(self):
        pass

    def integrate_ds(self, dataset_path, storm_cells, output_key):
        """
        Integrate a dataset over storm cells, storing the result in each cell's storm_history.
        Handles both 1D and 2D lat/lon coordinates.
        """
        from shapely.geometry import Polygon

        print(f"[CellIntegration] DEBUG: Integrating dataset for {len(storm_cells)} storm cells")

        # Step 1: Load dataset
        try:
            # Use cfgrib for GRIB2, fallback for NetCDF
            if dataset_path.endswith(".grib2"):
                ds = xr.open_dataset(dataset_path, engine="cfgrib", decode_timedelta=True)
            else:
                ds = xr.open_dataset(dataset_path, decode_timedelta=True)
            ds.load()
        except MemoryError:
            print("[CellIntegration] ERROR: Dataset too large to load into memory")
            for cell in storm_cells:
                if cell.get("storm_history"):
                    cell["storm_history"][-1][output_key] = "MEMORY_ERROR"
            return storm_cells
        except Exception as e:
            print(f"[CellIntegration] ERROR: Failed to load dataset: {e}")
            for cell in storm_cells:
                if cell.get("storm_history"):
                    cell["storm_history"][-1][output_key] = "DATASET_LOAD_ERROR"
            return storm_cells

        var = ds.get("unknown")

        # Step 3: Identify lat/lon coordinate names
        lat_name = "latitude" if "latitude" in ds.coords else "lat"
        lon_name = "longitude" if "longitude" in ds.coords else "lon"

        # Extract coordinate arrays (may be 1D or 2D)
        lat_vals = ds[lat_name].values
        lon_vals = ds[lon_name].values

        # Step 4: Loop over storm cells
        for cell in storm_cells:
            if not cell.get("storm_history"):
                continue

            latest = cell["storm_history"][-1]
            poly = StormIntegrationUtils.create_cell_polygon(cell)
            if poly is None:
                latest[output_key] = "N/A"
                continue

            try:
                # Handle 1D or 2D coordinates
                if lat_vals.ndim == 1 and lon_vals.ndim == 1:
                    # 1D lat/lon
                    mask = np.logical_and.outer(
                        (lat_vals >= poly.bounds[1]) & (lat_vals <= poly.bounds[3]),
                        (lon_vals >= poly.bounds[0]) & (lon_vals <= poly.bounds[2])
                    )
                else:
                    # 2D lat/lon
                    mask = (
                        (lat_vals >= poly.bounds[1]) & (lat_vals <= poly.bounds[3]) &
                        (lon_vals >= poly.bounds[0]) & (lon_vals <= poly.bounds[2])
                    )

                # Apply mask and ignore negative values
                subset = var.where(mask & (var >= 0))

                # Compute max
                max_val = subset.max().item()
                latest[output_key] = float(max_val) if not np.isnan(max_val) else "N/A"

            except Exception as e:
                print(f"[CellIntegration] ERROR: Processing cell {cell.get('id', 'unknown')}: {e}")
                latest[output_key] = "PROCESSING_ERROR"

            finally:
                try:
                    del subset, mask, poly
                except Exception:
                    pass
                gc.collect()

        # Step 5: Cleanup
        ds.close()
        del var, ds
        gc.collect()

        return storm_cells

    def integrate_probsevere(self, probsevere_data, storm_cells):
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

                print(f"[CellIntegration] DEBUG: Matched cell {cell_id}")
                matches_found += 1

        print(f"[CellIntegration] DEBUG: ProbSevere integration completed: {matches_found} matches found")
        return storm_cells