from __future__ import annotations

import gzip
import json
import re
from pathlib import Path

import numpy as np

import util.file as fs


VCP_SWEEP_ELEVATION_LABELS = {
    "VCP-212": {
        0: "0.5",
        1: "0.5",
        2: "0.9",
        3: "0.9",
        4: "1.3",
        5: "1.3",
        6: "1.8",
        7: "2.4",
        8: "3.1",
    },
    "VCP-215": {
        0: "0.5",
        1: "0.5",
        2: "0.9",
        3: "0.9",
        4: "1.2",
        5: "1.2",
        8: "1.8",
        9: "2.4",
        10: "3.1",
    },
    "VCP-12": {
        0: "0.5",
        1: "0.5",
        2: "0.9",
        3: "0.9",
        4: "1.2",
        5: "1.2",
        6: "1.8",
        7: "2.4",
        8: "3.1",
    },
}

OPERATIONAL_ELEVATION_LABELS = frozenset({"0.5", "0.9"})
NEXRAD_FIELD_MAGIC = b"EWFFv1S0"
NEXRAD_VARIABLE_COLORMAP_KEYS = {
    "DBZH": "NWS_Reflectivity",
    "VRADH": "VRADH",
    "WRADH": "WRADH",
    "PHIDP": "PHIDP",
    "RHOHV": "RHOHV",
    "ZDR": "ZDR",
}


def _normalize_scan_name(scan_name: str | None) -> str | None:
    if scan_name is None:
        return None
    text = str(scan_name).strip().upper()
    if not text:
        return None

    match = re.search(r"VCP[-_ ]?(\d+)", text)
    if match:
        return f"VCP-{int(match.group(1))}"

    if text.isdigit():
        return f"VCP-{int(text)}"

    return text


def _canonical_elevation_label(scan_name: str | None, sweep_index: int) -> str | None:
    normalized_scan_name = _normalize_scan_name(scan_name)
    if normalized_scan_name is None:
        return None
    return VCP_SWEEP_ELEVATION_LABELS.get(normalized_scan_name, {}).get(sweep_index)


def _resolve_operational_elevation_label(scan_name: str | None, sweep_index: int) -> str | None:
    elevation_label = _canonical_elevation_label(scan_name, sweep_index)
    if elevation_label in OPERATIONAL_ELEVATION_LABELS:
        return elevation_label
    return None


def nexrad_render_elevation_dir(site: str, scan_timestamp: str, elevation_label: str) -> Path:
    return fs.GUI_NEXRAD_DIR / str(site).upper() / str(scan_timestamp) / str(elevation_label)


def nexrad_render_variable_bin_path(site: str, scan_timestamp: str, elevation_label: str, variable_name: str) -> Path:
    return nexrad_render_elevation_dir(site, scan_timestamp, elevation_label) / f"{variable_name}.bin.gz"


def _should_serialize_variable(sweep, variable_name: str) -> bool:
    waveform = str(getattr(sweep, "waveform", "") or "").lower()
    if waveform == "contiguous_doppler" and variable_name == "DBZH":
        return False
    return True


def _write_nexrad_variable_bin(path: Path, dense_data: np.ndarray, azimuths: np.ndarray, ranges: np.ndarray) -> None:
    data = np.asarray(dense_data, dtype="<f2")
    azimuth_values = np.asarray(azimuths, dtype="<f4")
    range_values = np.asarray(ranges, dtype="<f4")
    counts = np.asarray([azimuth_values.shape[0], range_values.shape[0]], dtype="<u4")

    with gzip.open(path, "wb") as handle:
        handle.write(NEXRAD_FIELD_MAGIC)
        handle.write(counts.tobytes(order="C"))
        handle.write(data.tobytes(order="C"))
        handle.write(azimuth_values.tobytes(order="C"))
        handle.write(range_values.tobytes(order="C"))


def serialize_nexrad_render_intermediate(
    site: str,
    volume_id: str,
    scan_timestamp: str,
    volume_path: Path,
    parsed_volume,
) -> Path:
    scan_dir = Path(volume_path).parent
    render_dir = scan_dir / "render"
    render_dir.mkdir(parents=True, exist_ok=True)

    manifest_layers = []
    datatree = getattr(parsed_volume, "datatree", None)
    scan_name = getattr(parsed_volume, "scan_name", None)
    if datatree is not None:
        for sweep_index, sweep in enumerate(parsed_volume.sweeps):
            canonical_elevation = _resolve_operational_elevation_label(scan_name, sweep_index)
            if canonical_elevation is None:
                continue
            node = datatree[sweep.group_name]
            dataset = node.ds if hasattr(node, "ds") else node.to_dataset()
            if "azimuth" not in dataset.coords or "range" not in dataset.coords:
                continue

            azimuths = np.asarray(dataset["azimuth"].values, dtype=np.float32)
            ranges = np.asarray(dataset["range"].values, dtype=np.float32)
            for variable_name in dataset.data_vars:
                if not _should_serialize_variable(sweep, variable_name):
                    continue
                data_array = dataset[variable_name]
                if tuple(data_array.dims)[:2] != ("azimuth", "range"):
                    continue

                values = np.asarray(data_array.values, dtype=np.float32)
                dense_data = values.T.astype(np.float16, copy=False)

                layer_name = f"NEXRAD_{variable_name}_SWEEP_{sweep_index:02d}"
                elevation_dir = nexrad_render_elevation_dir(site, scan_timestamp, canonical_elevation)
                elevation_dir.mkdir(parents=True, exist_ok=True)
                bin_path = nexrad_render_variable_bin_path(site, scan_timestamp, canonical_elevation, variable_name)
                _write_nexrad_variable_bin(bin_path, dense_data, azimuths, ranges)

                manifest_layers.append(
                    {
                        "name": layer_name,
                        "site": str(site).upper(),
                        "volume_id": str(volume_id),
                        "scan_timestamp": scan_timestamp,
                        "sweep_index": sweep_index,
                        "sweep_group": sweep.group_name,
                        "fixed_angle": float(sweep.fixed_angle),
                        "canonical_elevation": canonical_elevation,
                        "bin_path": str(bin_path),
                        "variable_name": variable_name,
                        "colormap_key": NEXRAD_VARIABLE_COLORMAP_KEYS.get(variable_name),
                        "data_shape": [int(dense_data.shape[0]), int(dense_data.shape[1])],
                        "azimuth_count": int(azimuths.shape[0]),
                        "range_count": int(ranges.shape[0]),
                    }
                )

    manifest_path = render_dir / "manifest.json"
    manifest_payload = {
        "site": str(site).upper(),
        "volume_id": str(volume_id),
        "scan_timestamp": scan_timestamp,
        "volume_path": str(volume_path),
        "scan_name": parsed_volume.scan_name,
        "dynamic_scan_type": parsed_volume.dynamic_scan_type,
        "layers": manifest_layers,
    }
    manifest_path.write_text(json.dumps(manifest_payload, indent=2), encoding="utf-8")
    return manifest_path


def serialize_nexrad_elevation_artifacts(
    site: str,
    volume_id: str,
    scan_timestamp: str,
    elevation_artifacts: list,
) -> Path:
    """Serialize render intermediates from grouped elevation artifacts.

    Reads pre-written elevation NetCDF files and produces GUI bin.gz outputs,
    keeping the GUI output contract unchanged:
        gui/NEXRAD/<SITE>/<SCAN_TIMESTAMP>/<ELEVATION>/<VARIABLE>.bin.gz
    """
    render_dir = fs.GUI_NEXRAD_DIR / str(site).upper() / str(scan_timestamp) / "render"
    render_dir.mkdir(parents=True, exist_ok=True)

    manifest_layers = []
    for artifact in elevation_artifacts:
        elevation_label = artifact.elevation
        if elevation_label not in OPERATIONAL_ELEVATION_LABELS:
            continue

        nc_path = Path(artifact.netcdf_path) if artifact.netcdf_path else None
        if nc_path is None or not nc_path.exists():
            continue

        try:
            import xarray as xr
        except ImportError:
            continue

        try:
            datatree = _open_elevation_datatree(nc_path)
        except Exception:
            continue

        elevation_dir = nexrad_render_elevation_dir(site, scan_timestamp, elevation_label)
        elevation_dir.mkdir(parents=True, exist_ok=True)

        for group_name in sorted(g for g in datatree.groups if g.startswith("/sweep_")):
            node = datatree[group_name]
            dataset = node.ds if hasattr(node, "ds") else node.to_dataset()
            if "azimuth" not in dataset.coords or "range" not in dataset.coords:
                continue

            azimuths = np.asarray(dataset["azimuth"].values, dtype=np.float32)
            ranges = np.asarray(dataset["range"].values, dtype=np.float32)

            sweep_index = int(group_name.split("_")[-1]) if "_" in group_name else 0
            for variable_name in dataset.data_vars:
                data_array = dataset[variable_name]
                if tuple(data_array.dims)[:2] != ("azimuth", "range"):
                    continue

                values = np.asarray(data_array.values, dtype=np.float32)
                dense_data = values.T.astype(np.float16, copy=False)

                layer_name = f"NEXRAD_{variable_name}_SWEEP_{sweep_index:02d}"
                bin_path = nexrad_render_variable_bin_path(site, scan_timestamp, elevation_label, variable_name)
                _write_nexrad_variable_bin(bin_path, dense_data, azimuths, ranges)

                manifest_layers.append(
                    {
                        "name": layer_name,
                        "site": str(site).upper(),
                        "volume_id": str(volume_id),
                        "scan_timestamp": scan_timestamp,
                        "sweep_index": sweep_index,
                        "sweep_group": group_name,
                        "canonical_elevation": elevation_label,
                        "bin_path": str(bin_path),
                        "variable_name": variable_name,
                        "colormap_key": NEXRAD_VARIABLE_COLORMAP_KEYS.get(variable_name),
                        "data_shape": [int(dense_data.shape[0]), int(dense_data.shape[1])],
                        "azimuth_count": int(azimuths.shape[0]),
                        "range_count": int(ranges.shape[0]),
                    }
                )

    manifest_path = render_dir / "manifest.json"
    manifest_payload = {
        "site": str(site).upper(),
        "volume_id": str(volume_id),
        "scan_timestamp": scan_timestamp,
        "source": "elevation_artifacts",
        "layers": manifest_layers,
    }
    manifest_path.write_text(json.dumps(manifest_payload, indent=2), encoding="utf-8")
    return manifest_path


def _open_elevation_datatree(path: Path):
    """Open an elevation NetCDF as a datatree-like structure."""
    import xarray as xr
    import warnings

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        ds = xr.open_dataset(path)

    class _Node:
        def __init__(self, dataset):
            self._ds = dataset

        @property
        def ds(self):
            return self._ds

        def to_dataset(self):
            return self._ds

    class _DataTree:
        def __init__(self, root_ds):
            self._root = root_ds
            self.groups = []

        def __getitem__(self, key):
            return _Node(self._root)

    return _DataTree(ds)
