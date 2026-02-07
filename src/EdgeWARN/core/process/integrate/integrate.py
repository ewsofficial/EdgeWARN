from .utils import StormIntegrationUtils
import xarray as xr
import numpy as np
import shapely.vectorized as sv
import gc
from util.grib_loader import load_grib_fast

# Suppress cfgrib/xarray compatibility warnings
xr.set_options(use_new_combine_kwarg_defaults=True)

class StormCellIntegrator:
    def __init__(self, io_manager):
        self.io_manager = io_manager

    @staticmethod
    def _lat_slice_indices(lat_vals, miny, maxy):
        """Return start/end indices for latitude bounds on ascending or descending grids."""
        if lat_vals[0] < lat_vals[-1]:
            start_idx = np.searchsorted(lat_vals, miny)
            end_idx = np.searchsorted(lat_vals, maxy, side='right')
        else:
            lat_reversed = lat_vals[::-1]
            lat_len = len(lat_vals)
            end_idx = lat_len - np.searchsorted(lat_reversed, miny)
            start_idx = lat_len - np.searchsorted(lat_reversed, maxy, side='right')

        return start_idx, end_idx

    def integrate_ds_via_max(self, dataset_path, storm_cells, output_key):

        # Load dataset
        try:
            if dataset_path.endswith(".grib2"):
                # Try fast loader first for GRIB2 files
                try:
                    ds = load_grib_fast(dataset_path)
                except Exception:
                    # Fallback to cfgrib for complex/multi-message files
                    ds = xr.open_dataset(dataset_path, engine="cfgrib", decode_timedelta=True)
                    ds.load()
            else:
                ds = xr.open_dataset(dataset_path, decode_timedelta=True)
                ds.load()
        except Exception as e:
            self.io_manager.write_error(f"Load error: {e}")
            return storm_cells


        # Coordinates
        lat_name = "latitude" if "latitude" in ds.coords else "lat"
        lon_name = "longitude" if "longitude" in ds.coords else "lon"
        lat_vals = ds[lat_name].values
        lon_vals = ds[lon_name].values

        var = ds.get("unknown")
        if var is None:
            self.io_manager.write_error("Variable 'unknown' not found")
            return storm_cells

        # No need to filter by timestamp or history anymore. Use all cells.
        active_cells = storm_cells
        self.io_manager.write_info(f"Integrating {output_key} data for {len(active_cells)} cells")

        var_values = var.values

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

                # Optimization: Use searchsorted for O(logN) slicing instead of O(N) boolean masking
                # This assumes lat_vals and lon_vals are monotonic (standard for GRIB grids)
                
                # Handle Latitude (check if ascending or descending)
                lat_start_idx, lat_end_idx = self._lat_slice_indices(lat_vals, miny, maxy)

                # Handle Longitude (usually ascending 0-360 or -180-180)
                lon_start_idx = np.searchsorted(lon_vals, minx)
                lon_end_idx = np.searchsorted(lon_vals, maxx, side='right')
                
                # Clamp indices
                lat_start_idx = max(0, min(lat_start_idx, len(lat_vals)))
                lat_end_idx = max(0, min(lat_end_idx, len(lat_vals)))
                lon_start_idx = max(0, min(lon_start_idx, len(lon_vals)))
                lon_end_idx = max(0, min(lon_end_idx, len(lon_vals)))

                # Create slices
                lat_subset = lat_vals[lat_start_idx:lat_end_idx]
                lon_subset = lon_vals[lon_start_idx:lon_end_idx]

                if lat_subset.size == 0 or lon_subset.size == 0:
                    target[output_key] = 0
                    continue

                sub_var = var_values[lat_start_idx:lat_end_idx, lon_start_idx:lon_end_idx]
                sub_lon, sub_lat = np.meshgrid(lon_subset, lat_subset)

                if sub_var.size == 0:
                    target[output_key] = 0
                    continue

                inside = sv.contains(poly, sub_lon, sub_lat)

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
        del ds
        gc.collect()
        return storm_cells

    def integrate_multi_stats(self, dataset_path, storm_cells, stats_config_list):
        """
        Integrate a dataset by calculating multiple statistics in a single pass.
        
        Args:
            dataset_path (str): Path to the GRIB/NetCDF file.
            storm_cells (list): List of storm cell dictionaries.
            stats_config_list (list): List of dicts, each containing:
                                      {'key': str, 'method': str, 'percentile': int}
        """
        # Load dataset
        try:
            if dataset_path.endswith(".grib2"):
                try:
                    ds = load_grib_fast(dataset_path)
                except Exception as fast_e:
                    # self.io_manager.write_warning(f"Fast load failed ({fast_e}), falling back")
                    ds = xr.open_dataset(dataset_path, engine="cfgrib", decode_timedelta=True)
                    ds.load()
            else:
                ds = xr.open_dataset(dataset_path, decode_timedelta=True)
                ds.load()

        except Exception as e:
            # unique output keys
            keys = [c['key'] for c in stats_config_list]
            self.io_manager.write_error(f"Load error for {keys}: {e}")
            return storm_cells

        # Coordinates
        lat_name = "latitude" if "latitude" in ds.coords else "lat"
        lon_name = "longitude" if "longitude" in ds.coords else "lon"
        lat_vals = ds[lat_name].values
        lon_vals = ds[lon_name].values
        
        # Assume single variable dataset or 'unknown' for GRIB
        var_name = list(ds.data_vars)[0]
        if "unknown" in ds.data_vars:
            var_name = "unknown"
            
        var = ds.get(var_name)
        if var is None:
            self.io_manager.write_error(f"Variable not found in {dataset_path}")
            return storm_cells

        var_values = var.values

        keys_str = ", ".join([c['key'] for c in stats_config_list])
        self.io_manager.write_info(f"Integrating [{keys_str}] for {len(storm_cells)} cells")

        for cell in storm_cells:
            # Create properties dict if not exists
            if "properties" not in cell:
                cell["properties"] = {}
            
            target = cell["properties"]

            poly = StormIntegrationUtils.create_cell_polygon(cell)
            if poly is None:
                for conf in stats_config_list:
                    target[conf['key']] = 0
                continue

            try:
                minx, miny, maxx, maxy = poly.bounds

                # Optimization: Use searchsorted for O(logN) slicing
                # Handle Latitude
                lat_start_idx, lat_end_idx = self._lat_slice_indices(lat_vals, miny, maxy)

                # Handle Longitude
                lon_start_idx = np.searchsorted(lon_vals, minx)
                lon_end_idx = np.searchsorted(lon_vals, maxx, side='right')
                
                # Clamp indices
                lat_start_idx = max(0, min(lat_start_idx, len(lat_vals)))
                lat_end_idx = max(0, min(lat_end_idx, len(lat_vals)))
                lon_start_idx = max(0, min(lon_start_idx, len(lon_vals)))
                lon_end_idx = max(0, min(lon_end_idx, len(lon_vals)))

                # Create slices
                lat_subset = lat_vals[lat_start_idx:lat_end_idx]
                lon_subset = lon_vals[lon_start_idx:lon_end_idx]

                if lat_subset.size == 0 or lon_subset.size == 0:
                    for conf in stats_config_list:
                        target[conf['key']] = 0
                    continue

                sub_var = var_values[lat_start_idx:lat_end_idx, lon_start_idx:lon_end_idx]
                sub_lon, sub_lat = np.meshgrid(lon_subset, lat_subset)

                if sub_var.size == 0:
                    for conf in stats_config_list:
                        target[conf['key']] = 0
                    continue

                inside = sv.contains(poly, sub_lon, sub_lat)
                masked_vals = sub_var[inside]
                
                # Removing NaNs/negatives if necessary (keeping >=0 for physical quantities)
                masked_vals = masked_vals[~np.isnan(masked_vals)]
                masked_vals = masked_vals[masked_vals >= 0]

                if masked_vals.size == 0:
                    for conf in stats_config_list:
                        target[conf['key']] = 0
                else:
                    percentile_methods = [
                        conf for conf in stats_config_list if conf.get('method') == 'percentile'
                    ]
                    percentile_cache = {}
                    if percentile_methods:
                        unique_percentiles = sorted(
                            {conf.get('percentile', 90) for conf in percentile_methods}
                        )
                        percentile_values = np.percentile(masked_vals, unique_percentiles)
                        percentile_cache = dict(zip(unique_percentiles, percentile_values))

                    for conf in stats_config_list:
                        method = conf.get('method', 'max')
                        percentile = conf.get('percentile', 90)
                        key = conf['key']
                        
                        if method == "max":
                            res = np.max(masked_vals)
                        elif method == "mean":
                            res = np.mean(masked_vals)
                        elif method == "percentile":
                            res = percentile_cache.get(percentile, 0)
                        else:
                            res = 0
                        
                        target[key] = float(res)

            except Exception as e:
                # self.io_manager.write_error(f"Process cell {cell.get('id')}: {e}")
                for conf in stats_config_list:
                    target[conf['key']] = 0 # Default to 0 on error
        
        ds.close()
        del ds
        gc.collect()
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
