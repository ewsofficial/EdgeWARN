import json
from dataclasses import asdict
from pathlib import Path

import numpy as np
import xarray as xr

import util.file as fs

IMPORTANT_DATA_VARS = None


def _sanitize_attr_value(value):
    if value is None:
        return None
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, (str, bytes, int, float)):
        return value
    if hasattr(value, "item"):
        try:
            return _sanitize_attr_value(value.item())
        except Exception:
            pass
    if isinstance(value, (list, tuple, dict)):
        return json.dumps(value)
    return str(value)


def _sanitize_attrs(attrs: dict):
    sanitized = {}
    for key, value in (attrs or {}).items():
        sanitized_value = _sanitize_attr_value(value)
        if sanitized_value is not None:
            sanitized[key] = sanitized_value
    return sanitized


def _sanitize_dataset(dataset: xr.Dataset):
    sanitized = dataset.copy(deep=False)
    sanitized.attrs = _sanitize_attrs(dataset.attrs)
    for variable_name in sanitized.variables:
        sanitized[variable_name].attrs = _sanitize_attrs(sanitized[variable_name].attrs)
    return sanitized


def _slim_dataset(dataset: xr.Dataset):
    keep_vars = [name for name in dataset.data_vars if name in IMPORTANT_DATA_VARS]
    slim = dataset[keep_vars] if keep_vars else dataset.drop_vars(list(dataset.data_vars))
    slim = slim.copy(deep=False)
    slim.attrs = {}
    for variable_name in slim.variables:
        slim[variable_name].attrs = {}
    return slim


def _slim_dataset_from_node(node):
    dataset_view = node.ds if hasattr(node, "ds") else node.to_dataset()
    if IMPORTANT_DATA_VARS is None:
        slim = dataset_view
    else:
        keep_vars = [name for name in dataset_view.variables if name in IMPORTANT_DATA_VARS]
        slim = dataset_view[keep_vars] if keep_vars else dataset_view.drop_vars(list(dataset_view.data_vars))
    slim = slim.copy(deep=False)
    slim.attrs = {}
    for variable_name in slim.variables:
        slim[variable_name].attrs = {}
    return slim


def _default_fill_value(dtype):
    dtype = np.dtype(dtype)
    if dtype.kind == "u":
        return np.iinfo(dtype).max
    if dtype.kind == "i":
        return np.iinfo(dtype).min
    if dtype.kind == "f":
        return np.nan
    return None


def _build_variable_encoding(data_array):
    encoding = {}
    source_encoding = data_array.encoding or {}

    for key in ("dtype", "scale_factor", "add_offset", "_FillValue"):
        if key in source_encoding:
            encoding[key] = source_encoding[key]

    target_dtype = encoding.get("dtype")
    if target_dtype is not None and encoding.get("_FillValue") is None:
        default_fill = _default_fill_value(target_dtype)
        if default_fill is not None:
            encoding["_FillValue"] = default_fill

    return encoding


def _dataset_encoding(dataset: xr.Dataset):
    return {
        variable_name: _build_variable_encoding(dataset[variable_name])
        for variable_name in dataset.data_vars
    }


def _empty_root_dataset(attrs: dict):
    return xr.Dataset(attrs=_sanitize_attrs(attrs))


def _write_grouped_netcdf(path: Path, root_attrs: dict, datatree, group_names: list[str]):
    path.parent.mkdir(parents=True, exist_ok=True)
    _empty_root_dataset(root_attrs).to_netcdf(path)
    for group_name in group_names:
        dataset = _sanitize_dataset(_slim_dataset_from_node(datatree[group_name]))
        dataset.to_netcdf(
            path,
            mode="a",
            group=group_name.lstrip("/"),
            encoding=_dataset_encoding(dataset),
        )


def write_outputs(probe, parsed_volume, classified_sweeps, chunks_downloaded, *, base_dir=None):
    if base_dir:
        fs.initialize_filesystem(base_dir)

    low_groups = [sweep.group_name for sweep in classified_sweeps if sweep.bucket == "low"]
    high_groups = [sweep.group_name for sweep in classified_sweeps if sweep.bucket == "high"]

    stem = f"{probe.site}_{probe.volume_id}"
    low_path = fs.NEXRAD_LEVEL2_LOW_DIR / f"{stem}_low.nc"
    high_path = fs.NEXRAD_LEVEL2_HIGH_DIR / f"{stem}_high.nc"
    manifest_path = fs.NEXRAD_LEVEL2_MANIFEST_DIR / f"{stem}.json"

    root_attrs = {
        "site": probe.site,
        "volume_id": probe.volume_id,
        "scan_name": parsed_volume.scan_name,
        "vcp": probe.vcp,
        "dynamic_scan_type": parsed_volume.dynamic_scan_type,
        "source_bucket": parsed_volume.source_bucket,
        "chunks_downloaded": chunks_downloaded,
    }

    if parsed_volume.datatree is not None:
        if low_groups:
            _write_grouped_netcdf(low_path, root_attrs, parsed_volume.datatree, low_groups)
        if high_groups:
            _write_grouped_netcdf(high_path, root_attrs, parsed_volume.datatree, high_groups)

    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_payload = {
        "site": probe.site,
        "volume_id": probe.volume_id,
        "vcp": probe.vcp,
        "scan_name": parsed_volume.scan_name,
        "dynamic_scan_type": parsed_volume.dynamic_scan_type,
        "chunks_downloaded": chunks_downloaded,
        "low_path": str(low_path) if low_groups else None,
        "high_path": str(high_path) if high_groups else None,
        "sweeps": [asdict(sweep) for sweep in classified_sweeps],
    }
    manifest_path.write_text(json.dumps(manifest_payload, indent=2), encoding="utf-8")
    return low_path if low_groups else None, high_path if high_groups else None, manifest_path
