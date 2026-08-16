from __future__ import annotations

from datetime import timezone
from pathlib import Path
import time
from typing import Optional

import numpy as np
import rasterio.transform
import rioxarray  # noqa: F401  Ensures xarray .rio accessor is registered.
import xarray as xr
from pyproj import CRS
from rasterio.enums import Resampling
from rasterio.warp import reproject

from common.ingest.mrms.utils import extract_timestamp
from EWMRS.render.config import goes_transform_resampling
from util.io import IOManager

io_manager = IOManager("[GOES]")


def extract_goes_timestamp_iso(path: Path | str) -> str:
    """Extract GOES timestamp from filename as an ISO string."""
    timestamp = extract_timestamp(str(path)).astimezone(timezone.utc)
    return timestamp.replace(tzinfo=None).isoformat()


def _select_goes_variable(ds: xr.Dataset, layer_config: dict) -> tuple[Optional[xr.DataArray], Optional[str]]:
    configured_name = layer_config.get("variable_name")
    fallback_names = list(layer_config.get("fallback_variable_names", []))

    candidates = [configured_name, *fallback_names, "CMI", "Rad"]
    seen = set()

    for candidate in candidates:
        if not candidate or candidate in seen:
            continue
        seen.add(candidate)

        if candidate not in ds.data_vars:
            continue

        variable = ds[candidate]
        if "x" in variable.dims and "y" in variable.dims:
            return variable, candidate

    for var_name, variable in ds.data_vars.items():
        if "x" in variable.dims and "y" in variable.dims:
            return variable, var_name

    return None, None


def _select_layer_parameter(layer_config: dict, key: str, channel_id: str | None):
    value = layer_config.get(key)
    if isinstance(value, dict):
        if channel_id in value:
            return value[channel_id]
        if "default" in value:
            return value["default"]
        return None
    return value


def _extract_crs(ds: xr.Dataset) -> tuple[Optional[CRS], Optional[float]]:
    projection_var = ds.get("goes_imager_projection")
    if projection_var is None:
        return None, None

    attrs = dict(projection_var.attrs)

    perspective_height = attrs.get("perspective_point_height")
    if perspective_height is None:
        return None, None

    try:
        crs = CRS.from_cf(attrs)
        return crs, float(perspective_height)
    except Exception:
        try:
            crs = CRS.from_proj4(
                " ".join(
                    [
                        "+proj=geos",
                        f"+h={attrs['perspective_point_height']}",
                        f"+lon_0={attrs['longitude_of_projection_origin']}",
                        f"+a={attrs['semi_major_axis']}",
                        f"+b={attrs['semi_minor_axis']}",
                        f"+sweep={attrs.get('sweep_angle_axis', 'x')}",
                        "+units=m",
                        "+no_defs",
                    ]
                )
            )
            return crs, float(perspective_height)
        except Exception:
            return None, None


def _get_scalar_or_array_parameter(ds: xr.Dataset, data: xr.DataArray, key: str, channel_id: str | None):
    if key in data.attrs:
        return data.attrs[key]

    if key in ds.data_vars:
        value = ds[key]
        if value.size == 1:
            return float(np.asarray(value.values).reshape(-1)[0])

        if "band_id" in value.dims and channel_id:
            try:
                band_num = int(str(channel_id).lstrip("C"))
                selected = value.sel(band_id=band_num)
                if selected.size == 1:
                    return float(np.asarray(selected.values).reshape(-1)[0])
                return selected
            except Exception:
                pass

        if "band" in value.dims and channel_id:
            try:
                band_num = int(str(channel_id).lstrip("C"))
                selected = value.sel(band=band_num)
                if selected.size == 1:
                    return float(np.asarray(selected.values).reshape(-1)[0])
                return selected
            except Exception:
                pass

            try:
                if "band_id" in ds.data_vars and "band" in ds["band_id"].dims:
                    band_ids = np.asarray(ds["band_id"].values).reshape(-1)
                    matches = np.where(band_ids.astype(int) == band_num)[0]
                    if matches.size > 0:
                        selected = value.isel(band=int(matches[0]))
                        if selected.size == 1:
                            return float(np.asarray(selected.values).reshape(-1)[0])
                        return selected
            except Exception:
                pass

        if "channel" in value.dims and channel_id:
            try:
                selected = value.sel(channel=str(channel_id))
                if selected.size == 1:
                    return float(np.asarray(selected.values).reshape(-1)[0])
                return selected
            except Exception:
                pass

        if "x" not in value.dims and "y" not in value.dims:
            return float(np.asarray(value.values).reshape(-1)[0])

        return value

    return None


def _apply_layer_transform(
    ds: xr.Dataset,
    data: xr.DataArray,
    source_variable_name: str,
    layer_config: dict,
) -> xr.DataArray:
    transform_name = layer_config.get("value_transform")
    channel_id = layer_config.get("channel_id")

    if transform_name == "reflectance_from_rad" and source_variable_name == "Rad":
        kappa0 = _get_scalar_or_array_parameter(ds, data, "kappa0", channel_id)
        if kappa0 is not None:
            data = data * np.float32(kappa0)
            data = data.where(~np.isfinite(data) | (data >= 0))

    if transform_name == "brightness_temp_from_rad" and source_variable_name == "Rad":
        fk1 = _get_scalar_or_array_parameter(ds, data, "planck_fk1", channel_id)
        fk2 = _get_scalar_or_array_parameter(ds, data, "planck_fk2", channel_id)
        bc1 = _get_scalar_or_array_parameter(ds, data, "planck_bc1", channel_id)
        bc2 = _get_scalar_or_array_parameter(ds, data, "planck_bc2", channel_id)

        if None not in (fk1, fk2, bc1, bc2):
            rad = data.astype(np.float32)
            rad = rad.where(rad > 0)
            data = ((fk2 / np.log((fk1 / rad) + 1.0)) - bc1) / bc2

    mask_min = _select_layer_parameter(layer_config, "mask_min", channel_id)
    if mask_min is not None:
        data = data.where(data >= float(mask_min))

    mask_max = _select_layer_parameter(layer_config, "mask_max", channel_id)
    if mask_max is not None:
        data = data.where(data <= float(mask_max))

    return data.astype(np.float32)


def _load_goes_abi_render_payload(path: Path, layer_config: dict) -> dict | None:
    """Load GOES ABI source data and metadata into ndarray payload form."""
    channel_id = str(layer_config.get("channel_id", "unknown"))
    stage_start_s = time.perf_counter()
    try:
        with xr.open_dataset(path, decode_timedelta=True) as source_ds:
            if "x" not in source_ds.coords or "y" not in source_ds.coords:
                io_manager.write_error(f"GOES file is missing x/y coordinates: {path}")
                return None

            data_var, source_var_name = _select_goes_variable(source_ds, layer_config)
            if data_var is None or source_var_name is None:
                io_manager.write_error(f"No GOES render variable found in file: {path}")
                return None

            open_select_s = time.perf_counter() - stage_start_s

            data_var = data_var.squeeze(drop=True)
            if "x" not in data_var.dims or "y" not in data_var.dims:
                io_manager.write_error(
                    f"Selected GOES variable '{source_var_name}' does not expose x/y dimensions: {path}"
                )
                return None

            for dim_name in list(data_var.dims):
                if dim_name not in ("x", "y"):
                    data_var = data_var.isel({dim_name: 0}, drop=True)

            data_var = data_var.transpose("y", "x")

            crs, perspective_height = _extract_crs(source_ds)
            if crs is None or perspective_height is None:
                io_manager.write_error(f"Could not determine GOES geostationary CRS metadata for {path}")
                return None

            transform_start_s = time.perf_counter()
            data_var = _apply_layer_transform(source_ds, data_var, source_var_name, layer_config)
            value_transform_s = time.perf_counter() - transform_start_s

            output_start_s = time.perf_counter()
            x_coords = np.asarray(source_ds["x"].values, dtype=np.float64)
            y_coords = np.asarray(source_ds["y"].values, dtype=np.float64)
            if x_coords.ndim != 1 or y_coords.ndim != 1 or x_coords.size < 2 or y_coords.size < 2:
                io_manager.write_error(f"Invalid GOES x/y coordinate arrays for {path}")
                return None

            x_units = str(source_ds["x"].attrs.get("units", "")).lower()
            y_units = str(source_ds["y"].attrs.get("units", "")).lower()
            x_is_radians = "rad" in x_units or np.nanmax(np.abs(x_coords)) <= 2.0
            y_is_radians = "rad" in y_units or np.nanmax(np.abs(y_coords)) <= 2.0
            if x_is_radians:
                x_coords = x_coords * perspective_height
            if y_is_radians:
                y_coords = y_coords * perspective_height

            if x_coords[0] > x_coords[-1]:
                x_coords = x_coords[::-1]
                data_var = data_var.isel(x=slice(None, None, -1))

            if y_coords[0] < y_coords[-1]:
                y_coords = y_coords[::-1]
                data_var = data_var.isel(y=slice(None, None, -1))

            x_resolution = float(np.median(np.diff(x_coords)))
            y_resolution = float(np.median(np.diff(y_coords)))
            if x_resolution <= 0 or y_resolution >= 0:
                io_manager.write_error(f"Unexpected GOES coordinate orientation for {path}")
                return None

            x_res_abs = abs(x_resolution)
            y_res_abs = abs(y_resolution)
            left = float(x_coords[0] - x_res_abs / 2.0)
            top = float(y_coords[0] + y_res_abs / 2.0)
            source_transform = rasterio.transform.from_origin(left, top, x_res_abs, y_res_abs)

            data_values = np.asarray(data_var.values, dtype=np.float32)
            output_extraction_s = time.perf_counter() - output_start_s
            total_s = time.perf_counter() - stage_start_s

            io_manager.write_info(
                f"GOES ABI payload normalized for {channel_id} in {total_s:.3f}s "
                f"(open/select={open_select_s:.3f}s, value_transform={value_transform_s:.3f}s, "
                f"output_extract={output_extraction_s:.3f}s)"
            )

            return {
                "data": data_values,
                "x": x_coords.astype(np.float64),
                "y": y_coords.astype(np.float64),
                "transform": source_transform,
                "crs": crs,
                "source_type": "goes_abi",
                "source_variable": source_var_name,
            }

    except Exception as exc:
        io_manager.write_error(f"Failed to normalize GOES ABI dataset {path}: {exc}")
        return None


def _target_coordinates(shape: tuple[int, int], transform) -> tuple[np.ndarray, np.ndarray]:
    height, width = shape
    x_coords = transform.c + transform.a * (np.arange(width, dtype=np.float64) + 0.5)
    y_coords = transform.f + transform.e * (np.arange(height, dtype=np.float64) + 0.5)
    return x_coords.astype(np.float64), y_coords.astype(np.float64)


def _reproject_goes_payload_to_web_mercator(
    payload: dict,
    *,
    shape: tuple[int, int],
    transform,
    resampling: Resampling,
) -> dict[str, np.ndarray] | None:
    reproject_start_s = time.perf_counter()
    try:
        destination = np.empty(shape, dtype=np.float32)
        destination.fill(np.nan)
        reproject(
            source=np.asarray(payload["data"], dtype=np.float32),
            destination=destination,
            src_transform=payload["transform"],
            src_crs=payload["crs"],
            dst_transform=transform,
            dst_crs="EPSG:3857",
            src_nodata=np.nan,
            dst_nodata=np.nan,
            resampling=resampling,
        )
        reprojection_s = time.perf_counter() - reproject_start_s
        output_start_s = time.perf_counter()
        x_coords, y_coords = _target_coordinates(shape, transform)
        output_extraction_s = time.perf_counter() - output_start_s
        io_manager.write_info(
            f"GOES ABI reprojection completed in {reprojection_s:.3f}s "
            f"(output_extract={output_extraction_s:.3f}s)"
        )
        return {
            "data": destination,
            "x": x_coords,
            "y": y_coords,
        }
    except Exception as exc:
        io_manager.write_error(f"Failed GOES reprojection to EPSG:3857: {exc}")
        return None


def load_goes_abi_render_dataset(path: Path, layer_config: dict) -> xr.Dataset | None:
    """Load ABI fixed-grid data and normalize to the EWMRS renderer contract."""
    payload = _load_goes_abi_render_payload(path, layer_config)
    if payload is None:
        return None

    normalized = xr.Dataset(
        data_vars={"unknown": (("y", "x"), payload["data"])},
        coords={
            "y": payload["y"],
            "x": payload["x"],
        },
        attrs={
            "source_type": str(payload.get("source_type", "goes_abi")),
            "source_variable": str(payload.get("source_variable", "unknown")),
        },
    )
    normalized = normalized.rio.set_spatial_dims(x_dim="x", y_dim="y", inplace=False)
    normalized = normalized.rio.write_crs(payload["crs"], inplace=False)
    normalized = normalized.rio.write_transform(payload["transform"], inplace=False)
    return normalized


def reproject_goes_abi_to_web_mercator(
    ds: xr.Dataset,
    *,
    shape: tuple[int, int],
    transform,
    resampling: Resampling | None = None,
) -> xr.Dataset | None:
    """Reproject normalized GOES dataset to the EWMRS EPSG:3857 target grid.

    ``resampling=None`` defers to ``goes_transform.resampling``. A signature
    default of ``Resampling.bilinear`` bound the value at import time, so the
    catalog key it was transcribed from could never take effect.
    """
    if resampling is None:
        resampling = goes_transform_resampling()
    source_variable = "unknown"
    try:
        source_crs = ds.rio.crs
        source_transform = ds.rio.transform(recalc=False)
        if source_crs is not None:
            source_variable = str(ds.attrs.get("source_variable", "unknown"))
            payload = {
                "data": np.asarray(ds["unknown"].values, dtype=np.float32),
                "transform": source_transform,
                "crs": CRS.from_user_input(source_crs),
            }
            projected = _reproject_goes_payload_to_web_mercator(
                payload,
                shape=shape,
                transform=transform,
                resampling=resampling,
            )
            if projected is not None:
                projected_ds = xr.Dataset(
                    data_vars={"unknown": (("y", "x"), projected["data"])},
                    coords={
                        "y": projected["y"],
                        "x": projected["x"],
                    },
                    attrs={
                        "source_type": "goes_abi",
                        "source_variable": source_variable,
                    },
                )
                projected_ds = projected_ds.rio.set_spatial_dims(x_dim="x", y_dim="y", inplace=False)
                projected_ds = projected_ds.rio.write_crs("EPSG:3857", inplace=False)
                projected_ds = projected_ds.rio.write_transform(transform, inplace=False)
                return projected_ds
    except Exception:
        pass

    try:
        return ds.rio.reproject(
            "EPSG:3857",
            shape=shape,
            transform=transform,
            resampling=resampling,
        )
    except Exception as exc:
        io_manager.write_error(f"Failed GOES reprojection to EPSG:3857: {exc}")
        return None


def load_reproject_goes_abi_render_array(
    path: Path,
    layer_config: dict,
    *,
    shape: tuple[int, int],
    transform,
    resampling: Resampling | None = None,
) -> dict[str, np.ndarray] | None:
    """Load and reproject a GOES ABI channel to a shared array/x/y payload.

    ``None`` is passed straight through to
    :func:`reproject_goes_abi_to_web_mercator`, which owns the resolution, so the
    catalog is read once per call rather than twice.
    """
    channel_id = str(layer_config.get("channel_id", "unknown"))
    total_start_s = time.perf_counter()
    payload = _load_goes_abi_render_payload(path, layer_config)
    if payload is None:
        return None

    projected = _reproject_goes_payload_to_web_mercator(
        payload,
        shape=shape,
        transform=transform,
        resampling=resampling,
    )
    if projected is None:
        return None

    try:
        extract_start_s = time.perf_counter()
        extracted = {
            "data": np.asarray(projected["data"], dtype=np.float32),
            "x": np.asarray(projected["x"], dtype=np.float64),
            "y": np.asarray(projected["y"], dtype=np.float64),
        }
        output_extract_s = time.perf_counter() - extract_start_s
        total_s = time.perf_counter() - total_start_s
        io_manager.write_info(
            f"GOES ABI channel {channel_id} ready for rendering in {total_s:.3f}s "
            f"(final_extract={output_extract_s:.3f}s)"
        )
        return extracted
    except Exception as exc:
        io_manager.write_error(f"Failed to extract GOES reprojection arrays for {path}: {exc}")
        return None
