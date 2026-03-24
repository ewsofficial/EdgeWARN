from .utils import StormIntegrationUtils
import xarray as xr
import numpy as np
import shapely.vectorized as sv
from shapely.geometry import Polygon
import gc
from util.grib_loader import load_grib_fast

# Suppress cfgrib/xarray compatibility warnings
xr.set_options(use_new_combine_kwarg_defaults=True)

class StormCellIntegrator:
    def __init__(self, io_manager):
        self.io_manager = io_manager

    @staticmethod
    def _axis_slice_indices(coord_vals, min_val, max_val):
        """Return start/end indices for monotonic 1D coordinate bounds."""
        if coord_vals[0] < coord_vals[-1]:
            start_idx = np.searchsorted(coord_vals, min_val)
            end_idx = np.searchsorted(coord_vals, max_val, side='right')
        else:
            reversed_vals = coord_vals[::-1]
            coord_len = len(coord_vals)
            end_idx = coord_len - np.searchsorted(reversed_vals, min_val)
            start_idx = coord_len - np.searchsorted(reversed_vals, max_val, side='right')

        return start_idx, end_idx

    @staticmethod
    def _uses_360_longitude(lon_vals):
        finite_lons = np.asarray(lon_vals)
        finite_lons = finite_lons[np.isfinite(finite_lons)]
        if finite_lons.size == 0:
            return False
        return float(np.min(finite_lons)) >= 0.0 and float(np.max(finite_lons)) > 180.0

    @classmethod
    def _polygon_for_dataset(cls, poly, lon_vals):
        if poly is None or not cls._uses_360_longitude(lon_vals):
            return poly

        return Polygon([
            (lon + 360.0 if lon < 0.0 else lon, lat)
            for lon, lat in poly.exterior.coords
        ])

    def _extract_spatial_subset(self, ds, var, is_grib, var_values, lat_name, lon_name, lat_vals, lon_vals, poly):
        minx, miny, maxx, maxy = poly.bounds

        if lat_vals.ndim == 1 and lon_vals.ndim == 1:
            lat_start_idx, lat_end_idx = self._axis_slice_indices(lat_vals, miny, maxy)
            lon_start_idx, lon_end_idx = self._axis_slice_indices(lon_vals, minx, maxx)

            lat_start_idx = max(0, min(lat_start_idx, len(lat_vals)))
            lat_end_idx = max(0, min(lat_end_idx, len(lat_vals)))
            lon_start_idx = max(0, min(lon_start_idx, len(lon_vals)))
            lon_end_idx = max(0, min(lon_end_idx, len(lon_vals)))

            lat_subset = lat_vals[lat_start_idx:lat_end_idx]
            lon_subset = lon_vals[lon_start_idx:lon_end_idx]
            if lat_subset.size == 0 or lon_subset.size == 0:
                return None, None, None

            if is_grib:
                sub_var = var_values[lat_start_idx:lat_end_idx, lon_start_idx:lon_end_idx]
            else:
                lat_dim = ds[lat_name].dims[0]
                lon_dim = ds[lon_name].dims[0]
                sub_var = var.isel(
                    {lat_dim: slice(lat_start_idx, lat_end_idx), lon_dim: slice(lon_start_idx, lon_end_idx)}
                )
                extra_dims = {
                    dim: 0
                    for dim, size in sub_var.sizes.items()
                    if dim not in (lat_dim, lon_dim)
                }
                for dim, size in sub_var.sizes.items():
                    if dim not in (lat_dim, lon_dim) and size != 1:
                        raise ValueError(f"Non-spatial dimension {dim} has size {size}")
                if extra_dims:
                    sub_var = sub_var.isel(extra_dims, drop=True)
                if sub_var.dims != (lat_dim, lon_dim):
                    sub_var = sub_var.transpose(lat_dim, lon_dim)
                sub_var = sub_var.compute().values

            sub_lon, sub_lat = np.meshgrid(lon_subset, lat_subset)
            return np.asarray(sub_var), sub_lat, sub_lon

        if lat_vals.ndim == 2 and lon_vals.ndim == 2:
            finite_mask = np.isfinite(lat_vals) & np.isfinite(lon_vals)
            bbox_mask = (
                finite_mask
                & (lon_vals >= minx)
                & (lon_vals <= maxx)
                & (lat_vals >= miny)
                & (lat_vals <= maxy)
            )
            if not np.any(bbox_mask):
                return None, None, None

            row_indices, col_indices = np.where(bbox_mask)
            row_slice = slice(int(row_indices.min()), int(row_indices.max()) + 1)
            col_slice = slice(int(col_indices.min()), int(col_indices.max()) + 1)

            if is_grib:
                sub_var = var_values[row_slice, col_slice]
            else:
                spatial_dims = ds[lat_name].dims
                if len(spatial_dims) != 2:
                    raise ValueError(f"Unsupported coordinate dimensions for {lat_name}: {spatial_dims}")
                sub_var = var.isel(
                    {spatial_dims[0]: row_slice, spatial_dims[1]: col_slice}
                )
                extra_dims = {
                    dim: 0
                    for dim, size in sub_var.sizes.items()
                    if dim not in spatial_dims
                }
                for dim, size in sub_var.sizes.items():
                    if dim not in spatial_dims and size != 1:
                        raise ValueError(f"Non-spatial dimension {dim} has size {size}")
                if extra_dims:
                    sub_var = sub_var.isel(extra_dims, drop=True)
                if sub_var.dims != spatial_dims:
                    sub_var = sub_var.transpose(*spatial_dims)
                sub_var = sub_var.compute().values

            sub_var = np.asarray(sub_var)
            sub_lat = lat_vals[row_slice, col_slice]
            sub_lon = lon_vals[row_slice, col_slice]
            if sub_var.ndim != 2 or sub_var.shape != sub_lat.shape:
                raise ValueError(
                    f"Spatial subset shape mismatch: data={sub_var.shape}, lat={sub_lat.shape}, lon={sub_lon.shape}"
                )
            return sub_var, sub_lat, sub_lon

        raise ValueError(
            f"Unsupported coordinate layout: lat.ndim={lat_vals.ndim}, lon.ndim={lon_vals.ndim}"
        )

    def integrate_ds_via_max(self, dataset_path, storm_cells, output_key):
        if not storm_cells:
            return storm_cells

        # Load dataset
        is_grib = dataset_path.endswith(".grib2")
        try:
            if is_grib:
                # Try fast loader first for GRIB2 files
                try:
                    ds = load_grib_fast(dataset_path)
                except Exception:
                    # Fallback to cfgrib for complex/multi-message files
                    ds = xr.open_dataset(dataset_path, engine="cfgrib", decode_timedelta=True)
                    ds.load()
            else:
                # NetCDF: Use lazy loading - do NOT call ds.load()
                ds = xr.open_dataset(dataset_path, decode_timedelta=True)
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

        # For GRIB: load all values at once (already in memory from load_grib_fast)
        # For NetCDF: keep var lazy for subset loading
        var_values = None
        if is_grib:
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
                poly = self._polygon_for_dataset(poly, lon_vals)
                sub_var, sub_lat, sub_lon = self._extract_spatial_subset(
                    ds, var, is_grib, var_values, lat_name, lon_name, lat_vals, lon_vals, poly
                )

                if sub_var is None or sub_var.size == 0:
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
        if not storm_cells:
            return storm_cells

        stats_specs = [
            (conf['key'], conf.get('method', 'max'), conf.get('percentile', 90))
            for conf in stats_config_list
        ]
        zero_results = {key: 0 for key, _, _ in stats_specs}
        unique_percentiles = sorted(
            {
                percentile
                for _, method, percentile in stats_specs
                if method == 'percentile'
            }
        )
        needs_max = any(method == 'max' for _, method, _ in stats_specs)
        needs_mean = any(method == 'mean' for _, method, _ in stats_specs)

        # Load dataset
        is_grib = dataset_path.endswith(".grib2")
        try:
            if is_grib:
                try:
                    ds = load_grib_fast(dataset_path)
                except Exception as fast_e:
                    # self.io_manager.write_warning(f"Fast load failed ({fast_e}), falling back")
                    ds = xr.open_dataset(dataset_path, engine="cfgrib", decode_timedelta=True)
                    ds.load()
            else:
                # NetCDF: Use lazy loading - do NOT call ds.load()
                ds = xr.open_dataset(dataset_path, decode_timedelta=True)

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

        # For GRIB: load all values at once (already in memory from load_grib_fast)
        # For NetCDF: keep var lazy for subset loading
        var_values = None
        if is_grib:
            var_values = var.values

        keys_str = ", ".join([key for key, _, _ in stats_specs])
        self.io_manager.write_info(f"Integrating [{keys_str}] for {len(storm_cells)} cells")

        for cell in storm_cells:
            # Create properties dict if not exists
            if "properties" not in cell:
                cell["properties"] = {}
            
            target = cell["properties"]

            poly = StormIntegrationUtils.create_cell_polygon(cell)
            if poly is None:
                target.update(zero_results)
                continue

            try:
                poly = self._polygon_for_dataset(poly, lon_vals)
                sub_var, sub_lat, sub_lon = self._extract_spatial_subset(
                    ds, var, is_grib, var_values, lat_name, lon_name, lat_vals, lon_vals, poly
                )

                if sub_var is None or sub_var.size == 0:
                    target.update(zero_results)
                    continue

                inside = sv.contains(poly, sub_lon, sub_lat)
                masked_vals = sub_var[inside]
                
                # Removing NaNs/negatives if necessary (keeping >=0 for physical quantities)
                masked_vals = masked_vals[~np.isnan(masked_vals)]
                masked_vals = masked_vals[masked_vals >= 0]

                if masked_vals.size == 0:
                    target.update(zero_results)
                else:
                    percentile_cache = {}
                    # Optimization: precompute config metadata once per dataset so the hot
                    # per-cell loop only performs the NumPy reductions it actually needs.
                    if unique_percentiles:
                        percentile_values = np.percentile(masked_vals, unique_percentiles)
                        percentile_cache = dict(zip(unique_percentiles, percentile_values))

                    max_value = np.max(masked_vals) if needs_max else 0
                    mean_value = np.mean(masked_vals) if needs_mean else 0

                    for key, method, percentile in stats_specs:
                        
                        if method == "max":
                            res = max_value
                        elif method == "mean":
                            res = mean_value
                        elif method == "percentile":
                            res = percentile_cache.get(percentile, 0)
                        else:
                            res = 0
                        
                        target[key] = float(res)

            except Exception as e:
                self.io_manager.write_error(f"Process cell {cell.get('id')}: {e}")
                target.update(zero_results)
        
        ds.close()
        del ds
        gc.collect()
        return storm_cells

    def integrate_probsevere(self, probsevere_data, storm_cells):
        """
        Integrate ProbSevere probability data with storm cells by matching IDs.
        Flattens all ProbSevere variables directly into each storm history entry.
        """
        if not storm_cells:
            return storm_cells

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
