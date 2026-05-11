from __future__ import annotations

import json
from pathlib import Path

import numpy as np

import util.file as fs

NEXRAD_RENDERABLE_VARS = {
    "DBZH": "NWS_Reflectivity",
    "VRADH": "NEXRAD_Velocity",
    "WRADH": "NEXRAD_SpectrumWidth",
    "RHOHV": "NEXRAD_CorrelationCoefficient",
}


def nexrad_render_output_dir(layer_name: str, site: str) -> Path:
    return fs.GUI_NEXRAD_DIR / layer_name / str(site).upper()


def nexrad_render_timestamp_dir(layer_name: str, site: str, scan_timestamp: str) -> Path:
    return nexrad_render_output_dir(layer_name, site) / str(scan_timestamp)


def _write_float16_file(path: Path, values: np.ndarray) -> None:
    np.asarray(values, dtype=np.float16).tofile(path)


def _write_float32_file(path: Path, values: np.ndarray) -> None:
    np.asarray(values, dtype=np.float32).tofile(path)


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
    if datatree is not None:
        for sweep_index, sweep in enumerate(parsed_volume.sweeps):
            node = datatree[sweep.group_name]
            dataset = node.ds if hasattr(node, "ds") else node.to_dataset()
            if "azimuth" not in dataset.coords or "range" not in dataset.coords:
                continue

            azimuths = np.asarray(dataset["azimuth"].values, dtype=np.float32)
            ranges = np.asarray(dataset["range"].values, dtype=np.float32)
            for variable_name, colormap_key in NEXRAD_RENDERABLE_VARS.items():
                if variable_name not in dataset.data_vars:
                    continue
                data_array = dataset[variable_name]
                if tuple(data_array.dims)[:2] != ("azimuth", "range"):
                    continue

                values = np.asarray(data_array.values, dtype=np.float32)
                dense_data = values.T.astype(np.float16, copy=False)

                layer_name = f"NEXRAD_{variable_name}_SWEEP_{sweep_index:02d}"
                timestamp_dir = nexrad_render_timestamp_dir(layer_name, site, scan_timestamp)
                timestamp_dir.mkdir(parents=True, exist_ok=True)
                azimuths_path = timestamp_dir / "azimuths.f32"
                ranges_path = timestamp_dir / "ranges.f32"
                data_path = timestamp_dir / "data.f16"
                _write_float32_file(azimuths_path, azimuths)
                _write_float32_file(ranges_path, ranges)
                _write_float16_file(data_path, dense_data)

                manifest_layers.append(
                    {
                        "name": layer_name,
                        "site": str(site).upper(),
                        "volume_id": str(volume_id),
                        "scan_timestamp": scan_timestamp,
                        "sweep_index": sweep_index,
                        "sweep_group": sweep.group_name,
                        "fixed_angle": float(sweep.fixed_angle),
                        "variable_name": variable_name,
                        "colormap_key": colormap_key,
                        "azimuths_path": str(azimuths_path),
                        "ranges_path": str(ranges_path),
                        "data_path": str(data_path),
                        "served_dir": str(timestamp_dir),
                        "data_shape": [int(dense_data.shape[0]), int(dense_data.shape[1])],
                        "data_order": "range_azimuth",
                        "azimuth_count": int(azimuths.shape[0]),
                        "range_count": int(ranges.shape[0]),
                        "outdir": str(nexrad_render_output_dir(layer_name, site)),
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
