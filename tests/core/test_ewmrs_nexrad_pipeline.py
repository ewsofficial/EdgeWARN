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
                "payload_path": str(tmp_path / "payload.npy"),
                "outdir": str(tmp_path / "gui"),
                "colormap_key": "NWS_Reflectivity",
            }
        ],
    }
    task = NexradCompletionRecord("KTLH", "999", "20260507-150000", None, tmp_path / "manifest.json")

    layers = get_nexrad_file_list(task, manifest)

    assert layers[0]["payload_path"] == str(tmp_path / "payload.npy")
    assert layers[0]["outdir"] == str(tmp_path / "gui")
    assert layers[0]["site"] == "KTLH"


def test_run_nexrad_render_pipeline_uses_manifest_payloads_without_latest_file_lookup(monkeypatch, tmp_path):
    payload_a = tmp_path / "site_a.npy"
    np.save(payload_a, np.array([[0.0, 1000.0, 5.0]], dtype=np.float16))

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
                        "payload_path": str(payload_a),
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
        captured.append(layer["payload_path"])
        return layer["name"], [Path(layer["payload_path"])]

    monkeypatch.setattr("EWMRS.pipeline._render_nexrad_layer", _fake_render)

    task = NexradCompletionRecord("KAAA", "111", "20260507-150000", None, manifest_path)
    results = run_nexrad_render_pipeline(task)

    assert captured == [str(payload_a)]
    assert results["NEXRAD_DBZH_SWEEP_00"] == [payload_a]
