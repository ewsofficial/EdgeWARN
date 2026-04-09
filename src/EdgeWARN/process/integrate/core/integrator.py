import gc

import numpy as np
import shapely.vectorized as sv
import xarray as xr

from util.grib_loader import load_grib_fast
from ..integrate_azshear import integrate_azshear_features as _integrate_azshear_features
from ..utils import StormIntegrationUtils
from .polygon import polygon_for_dataset
from .stats import OUTPUT_DECIMALS, prepare_stats_specs, reduce_stats, sanitize_masked_values
from .subset import axis_slice_indices, extract_spatial_subset

# Suppress cfgrib/xarray compatibility warnings
xr.set_options(use_new_combine_kwarg_defaults=True)


PROBSEVERE_FIELD_MAP = {
    "ProbSevere": "ProbSevere",
    "ProbWind": "ProbWind",
    "ProbHail": "ProbHail",
    "ProbTor": "ProbTor",
    "MLCAPE": "MLCAPE",
    "MUCAPE": "MUCAPE",
    "MLCIN": "MLCIN",
    "DCAPE": "DCAPE",
    "CAPE_M10M30": "CAPE_M10M30",
    "LCL": "LCL",
    "Wetbulb_0C_Hgt": "WETBULB_0C_HGT",
    "LLLR": "LLLR",
    "MLLR": "MLLR",
    "EBShear": "EBSHEAR",
    "SRH01km": "SRH01KM",
    "SRH02km": "SRW02KM",
    "SRW46km": "SRW46KM",
    "MeanWind_1-3kmAGL": "MEANWIND_1-3kmAGL",
    "LJA": "LJA",
    "CompRef": "COMPREF",
    "Ref10": "REF10",
    "Ref20": "REF20",
    "MESH": "MESH",
    "H50_Above_0C": "H50_Above_0C",
    "EchoTop50": "EchoTop_50",
    "VIL": "VIL",
    "MaxFED": "MaxFED",
    "MaxFCD": "MaxFCD",
    "AccumFCD": "AccumFCD",
    "MinFlashArea": "MinFlashArea",
    "TE@MaxFCD": "TE@MaxFCD",
    "FlashRate": "FLASH_RATE",
    "FlashDensity": "FLASH_DENSITY",
    "MaxLLAz": "MAXLLAZ",
    "p98LLAz": "P98LLAZ",
    "p98MLAz": "P98MLAZ",
    "MaxRC_Emiss": "MAXRC_EMISS",
    "ICP": "ICP",
    "PWAT": "PWAT",
    "avg_beam_hgt": "AVG_BEAM_HGT",
}


class StormCellIntegrator:
    def __init__(self, io_manager):
        self.io_manager = io_manager

    @staticmethod
    def _axis_slice_indices(coord_vals, min_val, max_val):
        return axis_slice_indices(coord_vals, min_val, max_val)

    @classmethod
    def _polygon_for_dataset(cls, poly, lon_vals):
        return polygon_for_dataset(poly, lon_vals)

    def integrate_azshear_features(self, low_dataset_path, mid_dataset_path, storm_cells):
        return _integrate_azshear_features(self, low_dataset_path, mid_dataset_path, storm_cells)

    def _extract_spatial_subset(self, ds, var, is_grib, var_values, lat_name, lon_name, lat_vals, lon_vals, poly):
        return extract_spatial_subset(ds, var, is_grib, var_values, lat_name, lon_name, lat_vals, lon_vals, poly)

    def integrate_ds_via_max(self, dataset_path, storm_cells, output_key):
        if not storm_cells:
            return storm_cells

        is_grib = dataset_path.endswith(".grib2")
        try:
            if is_grib:
                try:
                    ds = load_grib_fast(dataset_path)
                except Exception:
                    ds = xr.open_dataset(dataset_path, engine="cfgrib", decode_timedelta=True)
                    ds.load()
            else:
                ds = xr.open_dataset(dataset_path, decode_timedelta=True)
        except Exception as e:
            self.io_manager.write_error(f"Load error: {e}")
            return storm_cells

        lat_name = "latitude" if "latitude" in ds.coords else "lat"
        lon_name = "longitude" if "longitude" in ds.coords else "lon"
        lat_vals = ds[lat_name].values
        lon_vals = ds[lon_name].values

        var = ds.get("unknown")
        if var is None:
            self.io_manager.write_error("Variable 'unknown' not found")
            return storm_cells

        active_cells = storm_cells
        self.io_manager.write_info(f"Integrating {output_key} data for {len(active_cells)} cells")

        var_values = None
        if is_grib:
            var_values = var.values

        for cell in active_cells:
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
                    target[output_key] = round(float(np.nanmax(masked_vals)), OUTPUT_DECIMALS)

            except Exception as e:
                self.io_manager.write_error(f"Process cell {cell.get('id')}: {e}")
                target[output_key] = "PROCESSING_ERROR"

        ds.close()
        del ds
        gc.collect()
        return storm_cells

    def integrate_multi_stats(self, dataset_path, storm_cells, stats_config_list):
        if not storm_cells:
            return storm_cells

        stats_specs, zero_results, unique_percentiles, needs_max, needs_mean = prepare_stats_specs(stats_config_list)

        is_grib = dataset_path.endswith(".grib2")
        try:
            if is_grib:
                try:
                    ds = load_grib_fast(dataset_path)
                except Exception:
                    ds = xr.open_dataset(dataset_path, engine="cfgrib", decode_timedelta=True)
                    ds.load()
            else:
                ds = xr.open_dataset(dataset_path, decode_timedelta=True)

        except Exception as e:
            keys = [c["key"] for c in stats_config_list]
            self.io_manager.write_error(f"Load error for {keys}: {e}")
            return storm_cells

        lat_name = "latitude" if "latitude" in ds.coords else "lat"
        lon_name = "longitude" if "longitude" in ds.coords else "lon"
        lat_vals = ds[lat_name].values
        lon_vals = ds[lon_name].values

        var_name = list(ds.data_vars)[0]
        if "unknown" in ds.data_vars:
            var_name = "unknown"

        var = ds.get(var_name)
        if var is None:
            self.io_manager.write_error(f"Variable not found in {dataset_path}")
            return storm_cells

        var_values = None
        if is_grib:
            var_values = var.values

        keys_str = ", ".join([key for key, _, _ in stats_specs])
        self.io_manager.write_info(f"Integrating [{keys_str}] for {len(storm_cells)} cells")

        for cell in storm_cells:
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
                masked_vals = sanitize_masked_values(masked_vals)

                if masked_vals.size == 0:
                    target.update(zero_results)
                else:
                    target.update(
                        reduce_stats(
                            masked_vals,
                            stats_specs,
                            unique_percentiles,
                            needs_max,
                            needs_mean,
                        )
                    )

            except Exception as e:
                self.io_manager.write_error(f"Process cell {cell.get('id')}: {e}")
                target.update(zero_results)

        ds.close()
        del ds
        gc.collect()
        return storm_cells

    def integrate_probsevere(self, probsevere_data, storm_cells):
        if not storm_cells:
            return storm_cells

        if not isinstance(probsevere_data, dict) or "features" not in probsevere_data:
            self.io_manager.write_error("Failed to integrate ProbSevere data - Invalid Data Format")
            return storm_cells

        features = probsevere_data["features"]

        feature_lookup = {
            str(f.get("id") or f.get("properties", {}).get("ID")): f.get("properties", {})
            for f in features
        }

        for cell in storm_cells:
            cell_id = str(cell.get("id"))

            if "properties" not in cell:
                cell["properties"] = {}

            match = feature_lookup.get(cell_id)
            if not match:
                continue

            for target_key, source_key in PROBSEVERE_FIELD_MAP.items():
                try:
                    cell["properties"][target_key] = float(match.get(source_key, 0))
                except (TypeError, ValueError):
                    cell["properties"][target_key] = "MATCH_ERROR"

        return storm_cells
