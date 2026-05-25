from __future__ import annotations

import gzip
import json
import re
from pathlib import Path

import numpy as np

import util.file as fs
from util.nexrad_loader import open_nexrad_artifact_datatree, open_nexrad_level2_datatree


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


def nexrad_render_site_dir(site: str) -> Path:
    return fs.GUI_NEXRAD_DIR / str(site).upper()


def nexrad_render_manifest_dir(site: str) -> Path:
    return nexrad_render_site_dir(site) / "render"


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


def nexrad_render_elevation_dir(site: str, elevation_label: str) -> Path:
    return nexrad_render_site_dir(site) / str(elevation_label)


def nexrad_render_variable_bin_name(site: str, scan_timestamp: str, elevation_label: str, variable_name: str) -> str:
    return f"{str(site).upper()}_{variable_name}_{elevation_label}_{scan_timestamp}.bin.gz"


def nexrad_render_variable_bin_path(site: str, scan_timestamp: str, elevation_label: str, variable_name: str) -> Path:
    return nexrad_render_elevation_dir(site, elevation_label) / nexrad_render_variable_bin_name(
        site,
        scan_timestamp,
        elevation_label,
        variable_name,
    )


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
    render_dir = nexrad_render_manifest_dir(site)
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
                elevation_dir = nexrad_render_elevation_dir(site, canonical_elevation)
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

    manifest_path = render_dir / f"{str(site).upper()}_{scan_timestamp}_{volume_id}.json"
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

    Reads pre-written elevation NetCDF or AR2V files and produces GUI bin.gz outputs:
        gui/NEXRAD/<SITE>/<ELEVATION>/<SITE>_<VARIABLE>_<ELEVATION>_<TIMESTAMP>.bin.gz
    """
    render_dir = nexrad_render_manifest_dir(site)
    render_dir.mkdir(parents=True, exist_ok=True)

    manifest_layers = []
    manifest_timestamp = scan_timestamp
    for artifact in elevation_artifacts:
        elevation_label = artifact.elevation
        if elevation_label not in OPERATIONAL_ELEVATION_LABELS:
            continue

        artifact_timestamp = artifact.elevation_timestamp or artifact.scan_timestamp or scan_timestamp
        if artifact_timestamp is None:
            continue
        manifest_timestamp = manifest_timestamp or artifact_timestamp

        artifact_path = None
        if artifact.netcdf_path:
            artifact_path = Path(artifact.netcdf_path)
        elif getattr(artifact, "ar2v_path", None):
            artifact_path = Path(artifact.ar2v_path)
        if artifact_path is None or not artifact_path.exists():
            continue

        try:
            if artifact_path.suffix == ".ar2v":
                datatree = open_nexrad_artifact_datatree(
                    artifact_path=artifact_path,
                    site=site,
                    volume_id=volume_id,
                )
            else:
                datatree = _open_elevation_datatree(artifact_path)
        except Exception:
            continue

        elevation_dir = nexrad_render_elevation_dir(site, elevation_label)
        elevation_dir.mkdir(parents=True, exist_ok=True)

        for group_name in _iter_artifact_group_names(artifact, datatree):
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
                bin_path = nexrad_render_variable_bin_path(site, artifact_timestamp, elevation_label, variable_name)
                if not bin_path.exists():
                    _write_nexrad_variable_bin(bin_path, dense_data, azimuths, ranges)

                manifest_layers.append(
                    {
                        "name": layer_name,
                        "site": str(site).upper(),
                        "volume_id": str(volume_id),
                        "scan_timestamp": artifact_timestamp,
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

    manifest_path = render_dir / f"{str(site).upper()}_{manifest_timestamp or 'unknown'}_{volume_id}.json"
    manifest_payload = {
        "site": str(site).upper(),
        "volume_id": str(volume_id),
        "scan_timestamp": manifest_timestamp,
        "source": "elevation_artifacts",
        "layers": manifest_layers,
    }
    manifest_path.write_text(json.dumps(manifest_payload, indent=2), encoding="utf-8")
    return manifest_path


def _iter_artifact_group_names(artifact, datatree) -> list[str]:
    available = {str(group_name) for group_name in getattr(datatree, "groups", [])}
    requested = [str(group_name) for group_name in getattr(artifact, "member_group_names", []) if str(group_name) in available]
    if requested:
        return sorted(requested)
    return sorted(g for g in available if g.startswith("/sweep_"))


def _open_elevation_datatree(path: Path):
    """Open an elevation NetCDF or AR2V as a datatree-like structure."""
    if path.suffix == ".ar2v":
        return open_nexrad_level2_datatree(path)

    import netCDF4
    import xarray as xr
    import warnings

    with netCDF4.Dataset(path) as handle:
        group_names = [f"/{name}" for name in handle.groups.keys()]
    if not group_names:
        group_names = ["/sweep_00"]

    class _Node:
        def __init__(self, dataset):
            self._ds = dataset

        @property
        def ds(self):
            return self._ds

        def to_dataset(self):
            return self._ds

    class _DataTree:
        def __init__(self, dataset_path: Path, groups: list[str]):
            self._dataset_path = dataset_path
            self.groups = groups
            self._cache: dict[str, _Node] = {}

        def __getitem__(self, key):
            normalized = str(key).lstrip("/")
            cache_key = normalized or "/"
            cached = self._cache.get(cache_key)
            if cached is not None:
                return cached

            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                if normalized:
                    dataset = xr.open_dataset(self._dataset_path, group=normalized)
                else:
                    dataset = xr.open_dataset(self._dataset_path)
            node = _Node(dataset)
            self._cache[cache_key] = node
            return node

    return _DataTree(path, group_names)
