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


def nexrad_render_variable_dir(site: str, scan_timestamp: str, elevation_label: str, variable_name: str) -> Path:
    return fs.GUI_NEXRAD_DIR / str(site).upper() / str(scan_timestamp) / str(elevation_label) / str(variable_name)


def _should_serialize_variable(sweep, variable_name: str) -> bool:
    waveform = str(getattr(sweep, "waveform", "") or "").lower()
    if waveform == "contiguous_doppler" and variable_name == "DBZH":
        return False
    return True


def _write_float16_gzip_file(path: Path, values: np.ndarray) -> None:
    payload = np.asarray(values, dtype=np.float16).tobytes(order="C")
    with gzip.open(path, "wb") as handle:
        handle.write(payload)


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
                variable_dir = nexrad_render_variable_dir(site, scan_timestamp, canonical_elevation, variable_name)
                variable_dir.mkdir(parents=True, exist_ok=True)
                azimuths_path = variable_dir / "azimuths.f32"
                ranges_path = variable_dir / "ranges.f32"
                data_path = variable_dir / "data.f16.gz"
                _write_float32_file(azimuths_path, azimuths)
                _write_float32_file(ranges_path, ranges)
                _write_float16_gzip_file(data_path, dense_data)

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
                        "variable_dir": str(variable_dir),
                        "variable_name": variable_name,
                        "azimuths_path": str(azimuths_path),
                        "ranges_path": str(ranges_path),
                        "data_path": str(data_path),
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
