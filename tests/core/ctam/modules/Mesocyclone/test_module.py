from datetime import datetime, timezone

import numpy as np

from EdgeWARN.ctam.modules.Mesocyclone import config as cfg
from EdgeWARN.ctam.modules.Mesocyclone.module import MesocycloneModule


def test_mesocyclone_module_writes_sidecar_and_skips_stormcell_attachment(monkeypatch, tmp_path):
    import util.file as fs

    module = MesocycloneModule()
    timestamp = datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc)
    latitudes = np.linspace(35.0, 34.3, 8)
    longitudes = np.linspace(-98.0, -97.3, 8)
    low = np.zeros((8, 8), dtype=float)
    mid = np.zeros((8, 8), dtype=float)
    ref = np.zeros((8, 8), dtype=float)
    low[2:6, 2:6] = 0.03
    mid[2:6, 2:6] = 0.02
    ref[2:6, 2:6] = 50.0

    monkeypatch.setattr(module, "_load_inputs", lambda: {
        "timestamp": timestamp,
        "timestamp_iso": timestamp.isoformat(),
        "paths": {"low": "low.grib2", "mid": "mid.grib2", "reflectivity": "ref.grib2"},
        "coordinates": {"latitudes": latitudes, "longitudes": longitudes},
        "grids": {"low": low, "mid": mid, "reflectivity": ref},
        "scale_notes": {"low": None, "mid": None},
    })
    monkeypatch.setattr(fs, "MESOCYCLONE_DIR", tmp_path)
    monkeypatch.setattr(cfg, "ENABLE_MORPH_CLEANUP", False)

    result = module.run()

    assert result["attach_to_stormcells"] is False
    assert result["metadata"]["detection_count"] == 1
    assert (tmp_path / "mesocyclones_20240101-000000.json").exists()
