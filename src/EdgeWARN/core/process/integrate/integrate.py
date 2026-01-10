from .utils import StormIntegrationUtils
import xarray as xr
import numpy as np
import shapely.vectorized as sv
import gc

# Suppress cfgrib/xarray compatibility warnings
xr.set_options(use_new_combine_kwarg_defaults=True)

class StormCellIntegrator:
    def __init__(self, io_manager):
        self.io_manager = io_manager

    def integrate_ds_via_max(self, dataset_path, storm_cells, output_key):

        # Load dataset
        try:
            if dataset_path.endswith(".grib2"):
                ds = xr.open_dataset(dataset_path, engine="cfgrib", decode_timedelta=True)
            else:
                ds = xr.open_dataset(dataset_path, decode_timedelta=True)

            # Optimization: REMOVED ds.load() to avoid reading full file into memory
            # ds.load()
        except Exception as e:
            self.io_manager.write_error(f"Load error: {e}")
            return storm_cells


        # Coordinates
        lat_name = "latitude" if "latitude" in ds.coords else "lat"
        lon_name = "longitude" if "longitude" in ds.coords else "lon"

        # Load coordinates (usually small)
        lat_vals = ds[lat_name].values
        lon_vals = ds[lon_name].values

        var = ds.get("unknown")
        if var is None:
            self.io_manager.write_error("Variable 'unknown' not found")
            return storm_cells

        # No need to filter by timestamp or history anymore. Use all cells.
        active_cells = storm_cells
        self.io_manager.write_info(f"Integrating {output_key} data for {len(active_cells)} cells")

        # Optimization: Don't load full var_values
        # var_values = var.values

        is_1d_coords = (lat_vals.ndim == 1)

        for cell in active_cells:
            # Create properties dict if not exists
            if "properties" not in cell:
                cell["properties"] = {}
            
            target = cell["properties"]

            poly = StormIntegrationUtils.create_cell_polygon(cell)
            if poly is None:
                target[output_key] = 0
                continue

            try:
                minx, miny, maxx, maxy = poly.bounds

                if is_1d_coords:
                    # Optimize index finding for 1D coordinates
                    lat_mask = (lat_vals >= miny) & (lat_vals <= maxy)
                    lon_mask = (lon_vals >= minx) & (lon_vals <= maxx)

                    if not np.any(lat_mask) or not np.any(lon_mask):
                        target[output_key] = 0
                        continue

                    # Get indices to slice the dataset
                    lat_indices = np.where(lat_mask)[0]
                    lon_indices = np.where(lon_mask)[0]

                    lat_start, lat_end = lat_indices.min(), lat_indices.max() + 1
                    lon_start, lon_end = lon_indices.min(), lon_indices.max() + 1

                    # Slice the variable (lazy load only this chunk)
                    # Assuming dimensions match lat/lon order. usually (latitude, longitude)
                    sub_var = var[lat_start:lat_end, lon_start:lon_end].values

                    lat_subset = lat_vals[lat_start:lat_end]
                    lon_subset = lon_vals[lon_start:lon_end]

                else:
                    # Fallback for 2D coordinates (slower, requires masking)
                    lat_mask = (lat_vals >= miny) & (lat_vals <= maxy)
                    lon_mask = (lon_vals >= minx) & (lon_vals <= maxx)

                    # Use a bounding box approach on 2D arrays if possible, but simpler to mask
                    # Using xarray selection might be slow if repeated.
                    # Loading the whole variable for a single cell is bad.
                    # But loading it once for all cells is what we removed.

                    # Since we are optimizing for 1D GRIB files (standard MRMS),
                    # we accept a potential performance hit on 2D files if they exist,
                    # or better: we use xarray's .values with boolean mask which will trigger load.
                    # Note: We must ensure we don't load the whole array into memory if possible.
                    # xarray `where` returns a lazy object. `.values` on it loads it.

                    # Correct logic for 2D extraction:
                    mask = lat_mask & lon_mask
                    if not np.any(mask):
                        target[output_key] = 0
                        continue

                    # Extract values directly
                    # If we can't slice, we have to index.
                    # var.values[mask] will load the full array first then index.
                    # To avoid loading full array, we should use xarray's `isel` if we could determine bounds.
                    # Determining bounds in 2D irregular grid is hard.
                    # So we fallback to loading the subset via full mask, accepting I/O hit.
                    # However, since we loop over cells, this repeats I/O.

                    # Recommendation: If using 2D grids, this code path is not optimized for I/O per cell.
                    # But for now, we implement correctness.

                    # Flatten and extract
                    sub_var = var.values[mask] # Triggers load of full array!

                    # We also need lat/lon subsets corresponding to sub_var
                    lat_subset = lat_vals[mask]
                    lon_subset = lon_vals[mask]

                if lat_subset.size == 0 or lon_subset.size == 0:
                    target[output_key] = 0
                    continue

                # Prepare meshgrid for vector containment check
                sub_lon, sub_lat = np.meshgrid(lon_subset, lat_subset)

                inside = sv.contains(poly, sub_lon, sub_lat)

                # Check if any point is inside before masking
                if not np.any(inside):
                     target[output_key] = 0
                     continue

                masked_vals = sub_var[inside]
                masked_vals = masked_vals[masked_vals >= 0]

                if masked_vals.size == 0:
                    target[output_key] = 0
                else:
                    target[output_key] = float(np.nanmax(masked_vals))

            except Exception as e:
                self.io_manager.write_error(f"Process cell {cell.get('id')}: {e}")
                target[output_key] = "PROCESSING_ERROR"

        ds.close()
        return storm_cells

    def integrate_probsevere(self, probsevere_data, storm_cells):
        """
        Integrate ProbSevere probability data with storm cells by matching IDs.
        Flattens all ProbSevere variables directly into each storm history entry.
        """
        if not isinstance(probsevere_data, dict) or 'features' not in probsevere_data:
            self.io_manager.write_error(f"Failed to integrate ProbSevere data - Invalid Data Format")
            return storm_cells

        features = probsevere_data['features']

        # Pre-index features by their ID for O(1) lookups
        feature_lookup = {
            str(f.get('id') or f.get('properties', {}).get('ID')): f.get('properties', {})
            for f in features
        }

        # Variable mappings (key: target name, value: source property)
        field_map = {
            'ProbSevere': 'ProbSevere',
            'ProbWind': 'ProbWind',
            'ProbHail': 'ProbHail',
            'ProbTor': 'ProbTor',
            'MLCAPE': 'MLCAPE',
            'MUCAPE': 'MUCAPE',
            'MLCIN': 'MLCIN',
            'DCAPE': 'DCAPE',
            'CAPE_M10M30': 'CAPE_M10M30',
            'LCL': 'LCL',
            'Wetbulb_0C_Hgt': 'WETBULB_0C_HGT',
            'LLLR': 'LLLR',
            'MLLR': 'MLLR',
            'EBShear': 'EBSHEAR',
            'SRH01km': 'SRH01KM',
            'SRH02km': 'SRW02KM',
            'SRW46km': 'SRW46KM',
            'MeanWind_1-3kmAGL': 'MEANWIND_1-3kmAGL',
            'LJA': 'LJA',
            'CompRef': 'COMPREF',
            'Ref10': 'REF10',
            'Ref20': 'REF20',
            'MESH': 'MESH',
            'H50_Above_0C': 'H50_Above_0C',
            'EchoTop50': 'EchoTop_50',
            'VIL': 'VIL',
            'MaxFED': 'MaxFED',
            'MaxFCD': 'MaxFCD',
            'AccumFCD': 'AccumFCD',
            'MinFlashArea': 'MinFlashArea',
            'TE@MaxFCD': 'TE@MaxFCD',
            'FlashRate': 'FLASH_RATE',
            'FlashDensity': 'FLASH_DENSITY',
            'MaxLLAz': 'MAXLLAZ',
            'p98LLAz': 'P98LLAZ',
            'p98MLAz': 'P98MLAZ',
            'MaxRC_Emiss': 'MAXRC_EMISS',
            'ICP': 'ICP',
            'PWAT': 'PWAT',
            'avg_beam_hgt': 'AVG_BEAM_HGT'
        }

        for cell in storm_cells:
            cell_id = str(cell.get('id'))
            
            # Create properties dict if not exists
            if "properties" not in cell:
                cell["properties"] = {}
                
            match = feature_lookup.get(cell_id)
            if not match:
                continue

            # Flatten values directly into the properties
            for target_key, source_key in field_map.items():
                try:
                    cell["properties"][target_key] = float(match.get(source_key, 0))
                except (TypeError, ValueError):
                    cell["properties"][target_key] = "MATCH_ERROR"

        return storm_cells
