from datetime import datetime, timezone

import numpy as np
import pytest

from EdgeWARN.ctam.modules.Mesocyclone import loader


def test_load_latest_inputs_uses_composite_reflectivity_timestamp(monkeypatch):
    monkeypatch.setattr(loader.fs, "MRMS_AZSHEARLOW_DIR", "/tmp/low")
    monkeypatch.setattr(loader.fs, "MRMS_AZSHEARMID_DIR", "/tmp/mid")
    monkeypatch.setattr(loader.fs, "MRMS_COMPOSITE_DIR", "/tmp/ref")

    def fake_latest_files(path, _count):
        if path == "/tmp/low":
            return ["/tmp/low/AzShearLow_20240101-000000.grib2"]
        if path == "/tmp/mid":
            return ["/tmp/mid/AzShearMid_20240101-000200.grib2"]
        return ["/tmp/ref/CompRefQC_20240101-000100.grib2"]

    def fake_load_grid(file_path, normalize_azshear=False):
        return {
            "values": np.zeros((2, 2), dtype=float),
            "latitudes": np.array([35.0, 34.9], dtype=float),
            "longitudes": np.array([-97.0, -96.9], dtype=float),
            "scale_note": None,
        }

    monkeypatch.setattr(loader.fs, "latest_files", fake_latest_files)
    monkeypatch.setattr(loader, "_load_grid", fake_load_grid)

    result = loader.load_latest_inputs()

    assert result["timestamp"] == datetime(2024, 1, 1, 0, 1, tzinfo=timezone.utc)
    assert result["grid_spacing_deg"]["expected"] == 0.005
    assert result["grid_spacing_deg"]["lat"] == pytest.approx(0.1)
