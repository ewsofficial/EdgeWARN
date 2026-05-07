import json
from dataclasses import asdict
from pathlib import Path

import xarray as xr

import util.file as fs


def _empty_root_dataset(attrs: dict):
    return xr.Dataset(attrs=attrs)


def _write_grouped_netcdf(path: Path, root_attrs: dict, datatree, group_names: list[str]):
    path.parent.mkdir(parents=True, exist_ok=True)
    _empty_root_dataset(root_attrs).to_netcdf(path)
    for group_name in group_names:
        dataset = datatree[group_name].to_dataset()
        dataset.to_netcdf(path, mode="a", group=group_name.lstrip("/"))


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
