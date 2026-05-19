import json
import shutil
from dataclasses import asdict
from pathlib import Path

import numpy as np
import xarray as xr

import util.file as fs
from common.ingest.nexrad.s3_chunks import extract_volume_timestamp, required_low_chunks
from common.ingest.nexrad.models import ElevationArtifact, ElevationGroup

IMPORTANT_DATA_VARS = None
NEXRAD_SCAN_DIRS_TO_KEEP = 3
NEXRAD_ELEVATION_DIRS_TO_KEEP = 5


class NexradLocalChunkStore:
    def scan_output_dir(self, site: str, volume_id: str, chunks) -> Path:
        timestamp = extract_volume_timestamp(volume_id, chunks)
        return fs.NEXRAD_LEVEL2_DIR / site.upper() / timestamp

    def chunk_output_dir(self, site: str, volume_id: str, chunks) -> Path:
        return self.scan_output_dir(site, volume_id, chunks) / "chunks"

    def volume_output_path(self, site: str, volume_id: str, chunks) -> Path:
        scan_dir = self.scan_output_dir(site, volume_id, chunks)
        return scan_dir / f"{str(site).upper()}_{scan_dir.name}_{volume_id}.ar2v"

    def local_low_chunks_complete(self, site: str, volume_id: str, chunks) -> bool:
        needed_chunks = required_low_chunks(chunks)
        if not needed_chunks:
            return False

        volume_path = self.volume_output_path(site, volume_id, chunks)
        return volume_path.exists()

    def prune_station_scan_dirs(self, site: str, keep_timestamp: str):
        site_dir = fs.NEXRAD_LEVEL2_DIR / str(site).upper()
        if not site_dir.exists():
            return

        timestamp_dirs = sorted(
            (child for child in site_dir.iterdir() if child.is_dir()),
            key=lambda child: child.name,
            reverse=True,
        )
        keep_dirs = {child.name for child in timestamp_dirs[:NEXRAD_SCAN_DIRS_TO_KEEP]}
        keep_dirs.add(keep_timestamp)

        for child in timestamp_dirs:
            if child.name in keep_dirs:
                continue
            shutil.rmtree(child, ignore_errors=True)


class NexradElevationStore:
    """Manages per-elevation public outputs and internal scratch paths."""

    def elevation_dir(self, site: str, elevation: str) -> Path:
        return fs.NEXRAD_LEVEL2_DIR / str(site).upper() / str(elevation)

    def elevation_netcdf_path(
        self, site: str, elevation: str, elevation_timestamp: str
    ) -> Path:
        ts = elevation_timestamp.replace(":", "-") if elevation_timestamp else "unknown"
        stem = f"{str(site).upper()}_{elevation}_{ts}"
        return self.elevation_dir(site, elevation) / f"{stem}.nc"

    def elevation_manifest_path(
        self, site: str, elevation: str, elevation_timestamp: str
    ) -> Path:
        ts = elevation_timestamp.replace(":", "-") if elevation_timestamp else "unknown"
        stem = f"{str(site).upper()}_{elevation}_{ts}"
        return self.elevation_dir(site, elevation) / f"{stem}.json"

    def runtime_dir(self, site: str) -> Path:
        return fs.NEXRAD_LEVEL2_DIR / ".runtime" / str(site).upper()

    def runtime_scan_path(self, site: str, volume_id: str) -> Path:
        runtime = self.runtime_dir(site)
        runtime.mkdir(parents=True, exist_ok=True)
        return runtime / f"{str(site).upper()}_{volume_id}.ar2v"

    def prune_elevation_artifacts(self, site: str, elevation: str):
        elev_dir = self.elevation_dir(site, elevation)
        if not elev_dir.exists():
            return

        nc_files = sorted(
            (f for f in elev_dir.iterdir() if f.suffix == ".nc"),
            key=lambda f: f.name,
            reverse=True,
        )
        keep = {f.name for f in nc_files[:NEXRAD_ELEVATION_DIRS_TO_KEEP]}

        for f in nc_files:
            if f.name in keep:
                continue
            f.unlink(missing_ok=True)
            json_path = f.with_suffix(".json")
            json_path.unlink(missing_ok=True)


def _elevation_store() -> NexradElevationStore:
    return NexradElevationStore()


def elevation_dir(site: str, elevation: str) -> Path:
    return _elevation_store().elevation_dir(site, elevation)


def elevation_netcdf_path(site: str, elevation: str, elevation_timestamp: str) -> Path:
    return _elevation_store().elevation_netcdf_path(site, elevation, elevation_timestamp)


def elevation_manifest_path(site: str, elevation: str, elevation_timestamp: str) -> Path:
    return _elevation_store().elevation_manifest_path(site, elevation, elevation_timestamp)


def runtime_dir(site: str) -> Path:
    return _elevation_store().runtime_dir(site)


def runtime_scan_path(site: str, volume_id: str) -> Path:
    return _elevation_store().runtime_scan_path(site, volume_id)


def local_elevation_complete(site: str, elevation: str, elevation_timestamp: str) -> bool:
    nc_path = elevation_netcdf_path(site, elevation, elevation_timestamp)
    return nc_path.exists()


def local_scan_elevations_complete(
    site: str,
    required_elevations: list[tuple[str, str]],
) -> bool:
    for elevation, elevation_timestamp in required_elevations:
        if not local_elevation_complete(site, elevation, elevation_timestamp):
            return False
    return True


def prune_elevation_artifacts(site: str, elevation: str):
    _elevation_store().prune_elevation_artifacts(site, elevation)


def chunk_output_dir(site: str, volume_id: str, chunks) -> Path:
    return NexradLocalChunkStore().chunk_output_dir(site, volume_id, chunks)


def volume_output_path(site: str, volume_id: str, chunks) -> Path:
    return NexradLocalChunkStore().volume_output_path(site, volume_id, chunks)


def local_low_chunks_complete(site: str, volume_id: str, chunks) -> bool:
    return NexradLocalChunkStore().local_low_chunks_complete(site, volume_id, chunks)


def prune_station_scan_dirs(site: str, keep_timestamp: str):
    NexradLocalChunkStore().prune_station_scan_dirs(site, keep_timestamp)


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


def _write_elevation_netcdf(
    path: Path,
    root_attrs: dict,
    datatree,
    group_names: list[str],
) -> Path:
    """Write a single elevation NetCDF with all member sweep groups."""
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
    return path


def _write_elevation_manifest(path: Path, artifact: ElevationArtifact) -> Path:
    """Write a per-elevation JSON manifest."""
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "site": artifact.site,
        "volume_id": artifact.volume_id,
        "scan_timestamp": artifact.scan_timestamp,
        "elevation": artifact.elevation,
        "elevation_timestamp": artifact.elevation_timestamp,
        "first_sweep_index": artifact.first_sweep_index,
        "last_sweep_index": artifact.last_sweep_index,
        "member_group_names": artifact.member_group_names,
        "waveforms_present": list(artifact.waveforms_present),
        "supplemental": artifact.supplemental,
        "netcdf_path": artifact.netcdf_path,
    }
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return path


def write_elevation_artifacts(
    group: ElevationGroup,
    datatree,
    *,
    site: str,
    volume_id: str,
    scan_timestamp: str | None,
    elevation_label: str,
    elevation_timestamp: str | None,
    output_root: str | Path | None = None,
) -> list[ElevationArtifact]:
    """Write grouped elevation NetCDF and manifest to the public output tree.

    Returns a list of ElevationArtifact records for the emitted files.
    """
    if output_root:
        fs.initialize_filesystem(output_root)

    ts_for_filename = elevation_timestamp or scan_timestamp or "unknown"
    store = _elevation_store()

    nc_path = store.elevation_netcdf_path(site, elevation_label, ts_for_filename)
    manifest_path = store.elevation_manifest_path(site, elevation_label, ts_for_filename)

    root_attrs = {
        "site": site,
        "volume_id": volume_id,
        "scan_timestamp": scan_timestamp,
        "elevation": elevation_label,
        "elevation_timestamp": elevation_timestamp,
        "first_sweep_index": group.first_sweep_index,
        "last_sweep_index": group.last_sweep_index,
        "supplemental": group.supplemental,
    }

    group_names = [m.group_name for m in group.members]
    _write_elevation_netcdf(nc_path, root_attrs, datatree, group_names)

    artifact = ElevationArtifact(
        site=site,
        volume_id=volume_id,
        scan_timestamp=scan_timestamp,
        elevation=elevation_label,
        elevation_timestamp=elevation_timestamp,
        first_sweep_index=group.first_sweep_index,
        last_sweep_index=group.last_sweep_index,
        member_group_names=group_names,
        waveforms_present=group.waveforms_present,
        supplemental=group.supplemental,
        netcdf_path=str(nc_path),
    )
    _write_elevation_manifest(manifest_path, artifact)

    store.prune_elevation_artifacts(site, elevation_label)

    return [artifact]
