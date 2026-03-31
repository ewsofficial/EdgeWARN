import gc
from copy import deepcopy

import numpy as np
import shapely.vectorized as sv
import xarray as xr
from util.grib_loader import load_grib_fast

from ..geometry.cell_polygon import StormIntegrationUtils
from .constants import (
    AZSHEAR_BUFFER_KM,
    AZSHEAR_LOW_THRESHOLD,
    AZSHEAR_MID_THRESHOLD,
    empty_azshear_output,
)
from .geometry import buffer_polygon_km, grid_spacing_km
from .metrics import (
    build_alignment_metrics,
    extract_azshear_candidates,
    public_component_metrics,
)
from .pairing import pair_azshear_components


def _is_grib_path(dataset_path):
    lower_path = str(dataset_path).lower()
    return lower_path.endswith((".grib2", ".grib", ".grb2", ".grb"))


def _open_azshear_dataset(integrator, dataset_path):
    is_grib = _is_grib_path(dataset_path)
    if is_grib:
        try:
            return load_grib_fast(dataset_path), True
        except Exception as exc:
            integrator.io_manager.write_warning(
                f"Fast GRIB loader failed for AzShear file {dataset_path}; falling back to xarray: {exc}"
            )

    return xr.open_dataset(dataset_path, decode_timedelta=True), is_grib


def integrate_azshear_features(integrator, low_dataset_path, mid_dataset_path, storm_cells):
    if not storm_cells or not low_dataset_path or not mid_dataset_path:
        return storm_cells

    zero_output = empty_azshear_output()

    def empty_output():
        return deepcopy(zero_output)

    try:
        low_ds, low_is_grib = _open_azshear_dataset(integrator, low_dataset_path)
        mid_ds, mid_is_grib = _open_azshear_dataset(integrator, mid_dataset_path)
    except Exception as e:
        integrator.io_manager.write_error(f"Load error for AzShear features: {e}")
        return storm_cells

    try:
        low_lat_name = "latitude" if "latitude" in low_ds.coords else "lat"
        low_lon_name = "longitude" if "longitude" in low_ds.coords else "lon"
        mid_lat_name = "latitude" if "latitude" in mid_ds.coords else "lat"
        mid_lon_name = "longitude" if "longitude" in mid_ds.coords else "lon"

        low_lat_vals = low_ds[low_lat_name].values
        low_lon_vals = low_ds[low_lon_name].values
        mid_lat_vals = mid_ds[mid_lat_name].values
        mid_lon_vals = mid_ds[mid_lon_name].values

        low_var_name = "unknown" if "unknown" in low_ds.data_vars else list(low_ds.data_vars)[0]
        mid_var_name = "unknown" if "unknown" in mid_ds.data_vars else list(mid_ds.data_vars)[0]
        low_var = low_ds[low_var_name]
        mid_var = mid_ds[mid_var_name]
        low_var_values = low_var.values if low_is_grib else None
        mid_var_values = mid_var.values if mid_is_grib else None

        low_lat_spacing_km, low_lon_spacing_km = grid_spacing_km(low_lat_vals, low_lon_vals)
        mid_lat_spacing_km, mid_lon_spacing_km = grid_spacing_km(mid_lat_vals, mid_lon_vals)
        low_pixel_area_km2 = max(low_lat_spacing_km * low_lon_spacing_km, 0.01)
        mid_pixel_area_km2 = max(mid_lat_spacing_km * mid_lon_spacing_km, 0.01)

        for cell in storm_cells:
            cell.setdefault("properties", {})
            cell["properties"]["azshear"] = empty_output()

            poly = StormIntegrationUtils.create_cell_polygon(cell)
            if poly is None:
                continue

            buffered_poly = buffer_polygon_km(poly, AZSHEAR_BUFFER_KM)
            try:
                low_poly = integrator._polygon_for_dataset(buffered_poly, low_lon_vals)
                mid_poly = integrator._polygon_for_dataset(buffered_poly, mid_lon_vals)

                low_subset, low_lat, low_lon = integrator._extract_spatial_subset(
                    low_ds,
                    low_var,
                    low_is_grib,
                    low_var_values,
                    low_lat_name,
                    low_lon_name,
                    low_lat_vals,
                    low_lon_vals,
                    low_poly,
                )
                mid_subset, mid_lat, mid_lon = integrator._extract_spatial_subset(
                    mid_ds,
                    mid_var,
                    mid_is_grib,
                    mid_var_values,
                    mid_lat_name,
                    mid_lon_name,
                    mid_lat_vals,
                    mid_lon_vals,
                    mid_poly,
                )

                if low_subset is None or mid_subset is None:
                    continue

                low_inside = sv.contains(low_poly.buffer(1e-9), low_lon, low_lat)
                mid_inside = sv.contains(mid_poly.buffer(1e-9), mid_lon, mid_lat)

                low_masked = np.where(low_inside, np.asarray(low_subset), np.nan)
                mid_masked = np.where(mid_inside, np.asarray(mid_subset), np.nan)
                low_masked[low_masked < 0] = np.nan
                mid_masked[mid_masked < 0] = np.nan

                low_candidates = extract_azshear_candidates(
                    low_masked,
                    low_lat,
                    low_lon,
                    AZSHEAR_LOW_THRESHOLD,
                    low_pixel_area_km2,
                    low_lat_spacing_km,
                    low_lon_spacing_km,
                )
                mid_candidates = extract_azshear_candidates(
                    mid_masked,
                    mid_lat,
                    mid_lon,
                    AZSHEAR_MID_THRESHOLD,
                    mid_pixel_area_km2,
                    mid_lat_spacing_km,
                    mid_lon_spacing_km,
                )

                pair = pair_azshear_components(low_candidates, mid_candidates)
                low_component = pair[0] if pair else (low_candidates[0] if low_candidates else None)
                mid_component = pair[1] if pair else (mid_candidates[0] if mid_candidates else None)
                alignment = build_alignment_metrics(low_component, mid_component)

                cell["properties"]["azshear"] = {
                    "buffer_km": AZSHEAR_BUFFER_KM,
                    "low": public_component_metrics(low_component),
                    "mid": public_component_metrics(mid_component),
                    "alignment": alignment,
                    "low_candidate_count": len(low_candidates),
                    "mid_candidate_count": len(mid_candidates),
                }
            except Exception as e:
                integrator.io_manager.write_error(f"Process AzShear cell {cell.get('id')}: {e}")
                cell["properties"]["azshear"] = empty_output()
    finally:
        low_ds.close()
        mid_ds.close()
        gc.collect()

    return storm_cells
