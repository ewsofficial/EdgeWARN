import json
from pathlib import Path

import numpy as np

from common.ingest.nexrad.models import NexradCompletionRecord
from EWMRS.pipeline import run_nexrad_render_pipeline
from EWMRS.render.config import get_nexrad_file_list


def test_get_nexrad_file_list_keeps_explicit_manifest_paths(tmp_path):
    manifest = {
        "site": "KTLH",
        "volume_id": "999",
        "scan_timestamp": "20260507-150000",
        "layers": [
            {
                "name": "NEXRAD_DBZH_SWEEP_00",
                "azimuths_path": str(tmp_path / "azimuths.f32"),
                "ranges_path": str(tmp_path / "ranges.f32"),
                "data_path": str(tmp_path / "data.f16"),
                "data_order": "range_azimuth",
                "outdir": str(tmp_path / "gui"),
                "colormap_key": "NWS_Reflectivity",
            }
        ],
    }
    task = NexradCompletionRecord("KTLH", "999", "20260507-150000", None, tmp_path / "manifest.json")

    layers = get_nexrad_file_list(task, manifest)

    assert layers[0]["azimuths_path"] == str(tmp_path / "azimuths.f32")
    assert layers[0]["ranges_path"] == str(tmp_path / "ranges.f32")
    assert layers[0]["data_path"] == str(tmp_path / "data.f16")
    assert layers[0]["outdir"] == str(tmp_path / "gui")
    assert layers[0]["site"] == "KTLH"


def test_run_nexrad_render_pipeline_uses_manifest_payloads_without_latest_file_lookup(monkeypatch, tmp_path):
    azimuths_path = tmp_path / "azimuths.f32"
    ranges_path = tmp_path / "ranges.f32"
    data_path = tmp_path / "data.f16"
    np.array([0.0], dtype=np.float32).tofile(azimuths_path)
    np.array([1000.0], dtype=np.float32).tofile(ranges_path)
    np.array([[5.0]], dtype=np.float16).tofile(data_path)

    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(
        json.dumps(
            {
                "site": "KAAA",
                "volume_id": "111",
                "scan_timestamp": "20260507-150000",
                "layers": [
                    {
                        "name": "NEXRAD_DBZH_SWEEP_00",
                        "azimuths_path": str(azimuths_path),
                        "ranges_path": str(ranges_path),
                        "data_path": str(data_path),
                        "data_order": "range_azimuth",
                        "outdir": str(tmp_path / "gui" / "A"),
                        "colormap_key": "NWS_Reflectivity",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    captured = []

    def _fake_render(layer):
        captured.append((layer["azimuths_path"], layer["ranges_path"], layer["data_path"], layer["data_order"]))
        return layer["name"], [Path(layer["data_path"])]

    monkeypatch.setattr("EWMRS.pipeline._render_nexrad_layer", _fake_render)

    task = NexradCompletionRecord("KAAA", "111", "20260507-150000", None, manifest_path)
    results = run_nexrad_render_pipeline(task)

    assert captured == [(str(azimuths_path), str(ranges_path), str(data_path), "range_azimuth")]
    assert results["NEXRAD_DBZH_SWEEP_00"] == [data_path]


def test_render_nexrad_layer_reconstructs_dense_range_azimuth_grid(monkeypatch, tmp_path):
    from EWMRS.pipeline import _render_nexrad_layer

    azimuths_path = tmp_path / "azimuths.f32"
    ranges_path = tmp_path / "ranges.f32"
    data_path = tmp_path / "data.f16"
    np.array([0.0, 90.0], dtype=np.float32).tofile(azimuths_path)
    np.array([1000.0, 2000.0, 3000.0], dtype=np.float32).tofile(ranges_path)
    np.array([[1.5, 3.5], [np.nan, 4.5], [2.5, 5.5]], dtype=np.float16).tofile(data_path)

    name, result = _render_nexrad_layer(
        {
            "name": "NEXRAD_DBZH_SWEEP_00",
            "azimuths_path": str(azimuths_path),
            "ranges_path": str(ranges_path),
            "data_path": str(data_path),
            "data_order": "range_azimuth",
            "outdir": str(tmp_path / "gui"),
            "colormap_key": "NWS_Reflectivity",
            "scan_timestamp": "2026-05-07T15:00:00",
        }
    )

    assert name == "NEXRAD_DBZH_SWEEP_00"
    assert result == [azimuths_path, ranges_path, data_path]


def test_render_nexrad_layer_returns_none_when_served_artifacts_are_missing(tmp_path):
    from EWMRS.pipeline import _render_nexrad_layer

    name, result = _render_nexrad_layer(
        {
            "name": "NEXRAD_DBZH_SWEEP_00",
            "azimuths_path": str(tmp_path / "missing_azimuths.f32"),
            "ranges_path": str(tmp_path / "missing_ranges.f32"),
            "data_path": str(tmp_path / "missing_data.f16"),
            "data_order": "range_azimuth",
            "outdir": str(tmp_path / "gui"),
            "colormap_key": "NWS_Reflectivity",
            "scan_timestamp": "2026-05-07T15:00:00",
        }
    )

    assert name == "NEXRAD_DBZH_SWEEP_00"
    assert result is None
