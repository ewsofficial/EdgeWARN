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


def test_load_latest_inputs_harmonizes_coarser_reflectivity_to_azshear_grid(monkeypatch):
    monkeypatch.setattr(loader.fs, "MRMS_AZSHEARLOW_DIR", "/tmp/low")
    monkeypatch.setattr(loader.fs, "MRMS_AZSHEARMID_DIR", "/tmp/mid")
    monkeypatch.setattr(loader.fs, "MRMS_COMPOSITE_DIR", "/tmp/ref")

    def fake_latest_files(path, _count):
        if path == "/tmp/low":
            return ["/tmp/low/AzShearLow_20240101-000000.grib2"]
        if path == "/tmp/mid":
            return ["/tmp/mid/AzShearMid_20240101-000000.grib2"]
        return ["/tmp/ref/CompRefQC_20240101-000000.grib2"]

    def fake_load_grid(file_path, normalize_azshear=False):
        if "CompRefQC" in file_path:
            return {
                "values": np.array([[10.0, 20.0], [30.0, 40.0]], dtype=float),
                "latitudes": np.array([35.0, 34.0], dtype=float),
                "longitudes": np.array([-98.0, -97.0], dtype=float),
                "scale_note": None,
            }

        return {
            "values": np.arange(16, dtype=float).reshape(4, 4),
            "latitudes": np.array([35.0, 34.6666667, 34.3333333, 34.0], dtype=float),
            "longitudes": np.array([-98.0, -97.6666667, -97.3333333, -97.0], dtype=float),
            "scale_note": None,
        }

    monkeypatch.setattr(loader.fs, "latest_files", fake_latest_files)
    monkeypatch.setattr(loader, "_load_grid", fake_load_grid)

    result = loader.load_latest_inputs()

    assert result["grids"]["reflectivity"].shape == (2, 2)
    assert np.array_equal(result["grids"]["reflectivity"], np.array([[10.0, 20.0], [30.0, 40.0]], dtype=float))
    assert np.array_equal(result["coordinates"]["latitudes"], np.array([35.0, 34.6666667, 34.3333333, 34.0], dtype=float))
    assert np.array_equal(result["coordinates"]["longitudes"], np.array([-98.0, -97.6666667, -97.3333333, -97.0], dtype=float))
    assert np.array_equal(result["coordinates"]["reflectivity_latitudes"], np.array([35.0, 34.0], dtype=float))
    assert np.array_equal(result["coordinates"]["reflectivity_longitudes"], np.array([-98.0, -97.0], dtype=float))


def test_load_latest_inputs_harmonizes_half_cell_shifted_reflectivity_grid(monkeypatch):
    monkeypatch.setattr(loader.fs, "MRMS_AZSHEARLOW_DIR", "/tmp/low")
    monkeypatch.setattr(loader.fs, "MRMS_AZSHEARMID_DIR", "/tmp/mid")
    monkeypatch.setattr(loader.fs, "MRMS_COMPOSITE_DIR", "/tmp/ref")

    def fake_latest_files(path, _count):
        if path == "/tmp/low":
            return ["/tmp/low/AzShearLow_20240101-000000.grib2"]
        if path == "/tmp/mid":
            return ["/tmp/mid/AzShearMid_20240101-000000.grib2"]
        return ["/tmp/ref/CompRefQC_20240101-000000.grib2"]

    def fake_load_grid(file_path, normalize_azshear=False):
        if "CompRefQC" in file_path:
            return {
                "values": np.array([[10.0, 20.0], [30.0, 40.0]], dtype=float),
                "latitudes": np.array([34.875, 34.625], dtype=float),
                "longitudes": np.array([-97.875, -97.625], dtype=float),
                "scale_note": None,
            }

        return {
            "values": np.arange(16, dtype=float).reshape(4, 4),
            "latitudes": np.array([34.9375, 34.8125, 34.6875, 34.5625], dtype=float),
            "longitudes": np.array([-97.9375, -97.8125, -97.6875, -97.5625], dtype=float),
            "scale_note": None,
        }

    monkeypatch.setattr(loader.fs, "latest_files", fake_latest_files)
    monkeypatch.setattr(loader, "_load_grid", fake_load_grid)

    result = loader.load_latest_inputs()

    assert np.array_equal(result["grids"]["reflectivity"], np.array([[10.0, 20.0], [30.0, 40.0]], dtype=float))
    assert np.array_equal(result["coordinates"]["reflectivity_latitudes"], np.array([34.875, 34.625], dtype=float))
    assert np.array_equal(result["coordinates"]["reflectivity_longitudes"], np.array([-97.875, -97.625], dtype=float))


def test_load_latest_inputs_rejects_extent_mismatch(monkeypatch):
    monkeypatch.setattr(loader.fs, "MRMS_AZSHEARLOW_DIR", "/tmp/low")
    monkeypatch.setattr(loader.fs, "MRMS_AZSHEARMID_DIR", "/tmp/mid")
    monkeypatch.setattr(loader.fs, "MRMS_COMPOSITE_DIR", "/tmp/ref")

    def fake_latest_files(path, _count):
        if path == "/tmp/low":
            return ["/tmp/low/AzShearLow_20240101-000000.grib2"]
        if path == "/tmp/mid":
            return ["/tmp/mid/AzShearMid_20240101-000000.grib2"]
        return ["/tmp/ref/CompRefQC_20240101-000000.grib2"]

    def fake_load_grid(file_path, normalize_azshear=False):
        if "CompRefQC" in file_path:
            return {
                "values": np.array([[10.0, 20.0], [30.0, 40.0]], dtype=float),
                "latitudes": np.array([36.0, 35.0], dtype=float),
                "longitudes": np.array([-98.0, -97.0], dtype=float),
                "scale_note": None,
            }

        return {
            "values": np.arange(16, dtype=float).reshape(4, 4),
            "latitudes": np.array([35.0, 34.6666667, 34.3333333, 34.0], dtype=float),
            "longitudes": np.array([-98.0, -97.6666667, -97.3333333, -97.0], dtype=float),
            "scale_note": None,
        }

    monkeypatch.setattr(loader.fs, "latest_files", fake_latest_files)
    monkeypatch.setattr(loader, "_load_grid", fake_load_grid)

    with pytest.raises(ValueError, match="reflectivity grid extent mismatch"):
        loader.load_latest_inputs()
