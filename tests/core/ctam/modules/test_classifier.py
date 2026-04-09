import numpy as np
import xarray as xr

from EdgeWARN.ctam.modules.Classifier.classifier import ClassifierModule


def _dataset_from_array(values):
    arr = np.asarray(values, dtype=np.float32)
    return xr.Dataset(
        {
            "unknown": (("latitude", "longitude"), arr),
        },
        coords={
            "latitude": np.arange(arr.shape[0], dtype=np.float32),
            "longitude": np.arange(arr.shape[1], dtype=np.float32),
        },
    )


def _patch_scan(monkeypatch, data_array, cells, path="/tmp/mock.grib2"):
    load_calls = {"count": 0}

    def fake_latest_files(_directory, _count):
        return [path]

    def fake_load_grib(_path):
        load_calls["count"] += 1
        return _dataset_from_array(data_array)

    monkeypatch.setattr("EdgeWARN.ctam.modules.Classifier.classifier.fs.latest_files", fake_latest_files)
    monkeypatch.setattr("EdgeWARN.ctam.modules.Classifier.classifier.load_grib_fast", fake_load_grib)
    monkeypatch.setattr("EdgeWARN.ctam.modules.Classifier.classifier.CTAMJsonManager.load_json", lambda _identifier: {"features": cells})
    return load_calls


def test_classifier_marks_cell_inside_valid_linear_core(monkeypatch):
    data = np.zeros((14, 14), dtype=np.float32)
    data[2:11, 2:12] = 41.0
    data[5:8, 3:11] = 50.0

    cells = [
        {"id": "cell_1", "timestamp": "2026-04-09T22:00:00+00:00", "centroid": [6.0, 6.0], "properties": {}},
    ]
    _patch_scan(monkeypatch, data, cells)

    module = ClassifierModule()
    storm_entry = {"id": "cell_1", "timestamp": "2026-04-09T22:00:00+00:00", "centroid": [6.0, 6.0], "properties": {}}

    module.run(storm_entry)

    assert storm_entry["classification"] == "LINEAR"
    result = storm_entry["modules"]["Classifier"]
    assert result["classification"] == "LINEAR"


def test_classifier_leaves_cell_unclassified_when_outside_linear_core(monkeypatch):
    data = np.zeros((14, 14), dtype=np.float32)
    data[2:11, 2:12] = 41.0
    data[5:8, 3:11] = 50.0

    cells = [
        {"id": "cell_1", "timestamp": "2026-04-09T22:00:00+00:00", "centroid": [9.0, 10.0], "properties": {}},
    ]
    _patch_scan(monkeypatch, data, cells)

    module = ClassifierModule()
    storm_entry = {"id": "cell_1", "timestamp": "2026-04-09T22:00:00+00:00", "centroid": [9.0, 10.0], "properties": {}}

    module.run(storm_entry)

    assert storm_entry["classification"] is None
    result = storm_entry["modules"]["Classifier"]
    assert result["classification"] is None


def test_classifier_discards_unoccupied_40dbz_regions(monkeypatch):
    data = np.zeros((16, 16), dtype=np.float32)
    data[2:11, 2:12] = 41.0
    data[5:8, 3:11] = 50.0
    data[12:15, 12:15] = 50.0

    cells = [
        {"id": "cell_1", "timestamp": "2026-04-09T22:00:00+00:00", "centroid": [6.0, 6.0], "properties": {}},
        {"id": "cell_2", "timestamp": "2026-04-09T22:00:00+00:00", "centroid": [13.0, 13.0], "properties": {}},
    ]
    _patch_scan(monkeypatch, data, [cells[0]])

    module = ClassifierModule()
    storm_entry = dict(cells[1])

    module.run(storm_entry)

    assert storm_entry["classification"] is None
    result = storm_entry["modules"]["Classifier"]
    assert result["classification"] is None


def test_classifier_skips_when_composite_reflectivity_missing(monkeypatch):
    monkeypatch.setattr("EdgeWARN.ctam.modules.Classifier.classifier.fs.latest_files", lambda _directory, _count: None)

    module = ClassifierModule()
    storm_entry = {"id": "cell_1", "timestamp": "2026-04-09T22:00:00+00:00", "centroid": [6.0, 6.0], "properties": {}}

    module.run(storm_entry)

    assert storm_entry["classification"] is None
    result = storm_entry["modules"]["Classifier"]
    assert result["classification"] is None


def test_classifier_reuses_cached_scan_analysis(monkeypatch):
    data = np.zeros((14, 14), dtype=np.float32)
    data[2:11, 2:12] = 41.0
    data[5:8, 3:11] = 50.0

    cells = [
        {"id": "cell_1", "timestamp": "2026-04-09T22:00:00+00:00", "centroid": [6.0, 6.0], "properties": {}},
        {"id": "cell_2", "timestamp": "2026-04-09T22:00:00+00:00", "centroid": [9.0, 10.0], "properties": {}},
    ]
    load_calls = _patch_scan(monkeypatch, data, cells)

    module = ClassifierModule()
    storm_entry_1 = dict(cells[0])
    storm_entry_2 = dict(cells[1])

    module.run(storm_entry_1)
    module.run(storm_entry_2)

    assert load_calls["count"] == 1


def test_classifier_rejects_small_noise_core(monkeypatch):
    data = np.zeros((14, 14), dtype=np.float32)
    data[2:11, 2:12] = 41.0
    data[6:8, 6:8] = 50.0

    cells = [
        {"id": "cell_1", "timestamp": "2026-04-09T22:00:00+00:00", "centroid": [6.0, 6.0], "properties": {}},
    ]
    _patch_scan(monkeypatch, data, cells)

    module = ClassifierModule()
    storm_entry = dict(cells[0])

    module.run(storm_entry)

    assert storm_entry["classification"] is None
    result = storm_entry["modules"]["Classifier"]
    assert result["classification"] is None
