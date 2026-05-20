import json
from pathlib import Path

import pytest

import util.file as fs
from common.ingest.nexrad.writer import (
    NexradElevationStore,
    NexradLocalChunkStore,
    elevation_ar2v_path,
    elevation_dir,
    elevation_netcdf_path,
    elevation_manifest_path,
    runtime_dir,
    runtime_scan_path,
    local_elevation_complete,
    local_scan_elevations_complete,
    prune_elevation_artifacts,
)


def test_elevation_store_paths(tmp_path):
    fs.initialize_filesystem(tmp_path)
    store = NexradElevationStore()

    elev_dir = store.elevation_dir("KTLH", "0.5")
    assert str(elev_dir).endswith("NEXRAD_Level2/KTLH/0.5")

    nc_path = store.elevation_netcdf_path("KTLH", "0.5", "20260507-150000")
    assert "KTLH_0.5_20260507-150000.nc" in str(nc_path)

    ar2v_path = store.elevation_ar2v_path("KTLH", "0.5", "20260507-150000")
    assert "KTLH_0.5_20260507-150000.ar2v" in str(ar2v_path)

    manifest_path = store.elevation_manifest_path("KTLH", "0.5", "20260507-150000")
    assert "KTLH_0.5_20260507-150000.json" in str(manifest_path)

    runtime = store.runtime_dir("KTLH")
    assert str(runtime).endswith("NEXRAD_Level2/.runtime/KTLH")

    scan_path = store.runtime_scan_path("KTLH", "VOL123")
    assert "KTLH_VOL123.ar2v" in str(scan_path)


def test_elevation_dir_helper(tmp_path):
    fs.initialize_filesystem(tmp_path)
    d = elevation_dir("KDDC", "1.3")
    assert "KDDC" in str(d)
    assert "1.3" in str(d)


def test_elevation_netcdf_path_helper(tmp_path):
    fs.initialize_filesystem(tmp_path)
    p = elevation_netcdf_path("KDDC", "1.3", "20260101-120000")
    assert p.suffix == ".nc"
    assert "KDDC" in str(p)


def test_elevation_paths_normalize_iso_timestamps(tmp_path):
    fs.initialize_filesystem(tmp_path)
    store = NexradElevationStore()

    nc_path = store.elevation_netcdf_path("KTLH", "0.5", "2026-05-20T17:45:24Z")
    ar2v_path = store.elevation_ar2v_path("KTLH", "0.5", "2026-05-20T17:45:24Z")
    manifest_path = store.elevation_manifest_path("KTLH", "0.5", "2026-05-20T17:45:24Z")

    assert nc_path.name == "KTLH_0.5_20260520-174524.nc"
    assert ar2v_path.name == "KTLH_0.5_20260520-174524.ar2v"
    assert manifest_path.name == "KTLH_0.5_20260520-174524.json"


def test_elevation_manifest_path_helper(tmp_path):
    fs.initialize_filesystem(tmp_path)
    p = elevation_manifest_path("KDDC", "1.3", "20260101-120000")
    assert p.suffix == ".json"
    assert "KDDC" in str(p)


def test_runtime_dir_helper(tmp_path):
    fs.initialize_filesystem(tmp_path)
    d = runtime_dir("KTLH")
    assert ".runtime" in str(d)
    assert "KTLH" in str(d)


def test_runtime_scan_path_helper(tmp_path):
    fs.initialize_filesystem(tmp_path)
    p = runtime_scan_path("KTLH", "VOL456")
    assert p.suffix == ".ar2v"
    assert "KTLH" in str(p)
    assert "VOL456" in str(p)


def test_local_elevation_complete_false_when_missing(tmp_path):
    fs.initialize_filesystem(tmp_path)
    assert local_elevation_complete("KTLH", "0.5", "20260507-150000") is False


def test_local_elevation_complete_true_when_exists(tmp_path):
    fs.initialize_filesystem(tmp_path)
    nc_path = elevation_netcdf_path("KTLH", "0.5", "20260507-150000")
    nc_path.parent.mkdir(parents=True, exist_ok=True)
    nc_path.write_bytes(b"data")
    assert local_elevation_complete("KTLH", "0.5", "20260507-150000") is True


def test_local_elevation_complete_true_when_ar2v_exists(tmp_path):
    fs.initialize_filesystem(tmp_path)
    ar2v_path = elevation_ar2v_path("KTLH", "0.5", "20260507-150000")
    ar2v_path.parent.mkdir(parents=True, exist_ok=True)
    ar2v_path.write_bytes(b"data")
    assert local_elevation_complete("KTLH", "0.5", "20260507-150000") is True


def test_local_scan_elevations_complete_all_present(tmp_path):
    fs.initialize_filesystem(tmp_path)
    for elev in ("0.5", "0.9"):
        nc_path = elevation_netcdf_path("KTLH", elev, "20260507-150000")
        nc_path.parent.mkdir(parents=True, exist_ok=True)
        nc_path.write_bytes(b"data")

    required = [("0.5", "20260507-150000"), ("0.9", "20260507-150000")]
    assert local_scan_elevations_complete("KTLH", required) is True


def test_local_scan_elevations_complete_missing_one(tmp_path):
    fs.initialize_filesystem(tmp_path)
    nc_path = elevation_netcdf_path("KTLH", "0.5", "20260507-150000")
    nc_path.parent.mkdir(parents=True, exist_ok=True)
    nc_path.write_bytes(b"data")

    required = [("0.5", "20260507-150000"), ("0.9", "20260507-150000")]
    assert local_scan_elevations_complete("KTLH", required) is False


def test_prune_elevation_artifacts_keeps_recent(tmp_path):
    fs.initialize_filesystem(tmp_path)
    elev_dir = elevation_dir("KTLH", "0.5")
    elev_dir.mkdir(parents=True, exist_ok=True)

    for i in range(8):
        nc_file = elev_dir / f"KTLH_0.5_20260507-15000{i}.nc"
        nc_file.write_bytes(b"data")
        json_file = nc_file.with_suffix(".json")
        json_file.write_text("{}")

    prune_elevation_artifacts("KTLH", "0.5")

    remaining_nc = list(elev_dir.glob("*.nc"))
    assert len(remaining_nc) <= 5


def test_prune_station_scan_dirs_ignores_elevation_directories(tmp_path):
    fs.initialize_filesystem(tmp_path)
    site_dir = Path(fs.NEXRAD_LEVEL2_DIR) / "KTLH"
    site_dir.mkdir(parents=True, exist_ok=True)

    for scan_dir_name in ("20260507-150000", "20260507-151000", "20260507-152000", "20260507-153000"):
        (site_dir / scan_dir_name).mkdir()
    for elevation_name in ("0.5", "0.9", "1.3"):
        (site_dir / elevation_name).mkdir()

    NexradLocalChunkStore().prune_station_scan_dirs("KTLH", "20260507-153000")

    assert not (site_dir / "20260507-150000").exists()
    assert (site_dir / "20260507-151000").exists()
    assert (site_dir / "20260507-152000").exists()
    assert (site_dir / "20260507-153000").exists()
    assert (site_dir / "0.5").exists()
    assert (site_dir / "0.9").exists()
    assert (site_dir / "1.3").exists()
