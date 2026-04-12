import numpy as np

from EdgeWARN.ctam.modules.Mesocyclone.detect import detect_layer_objects


def test_detect_layer_objects_returns_component_metrics():
    grid = np.zeros((8, 8), dtype=float)
    grid[2:5, 2:5] = 0.009
    grid[3, 3] = 0.015
    latitudes = 35.0 - (np.arange(8) * 0.005)
    longitudes = -98.0 + (np.arange(8) * 0.005)

    detections = detect_layer_objects(grid, latitudes, longitudes, "low")

    assert len(detections) == 1
    detection = detections[0]
    assert detection["max_azshear"] >= 0.015
    assert detection["pixel_count"] >= 6
    assert detection["area_km2"] > 0.0
    assert detection["eccentricity"] >= 0.0
    assert len(detection["maxima"]) >= 1
    assert detection["grid_spacing_deg"]["lat"] == 0.005


def test_detect_layer_objects_uses_native_area_threshold_for_coarser_grid():
    grid = np.zeros((4, 4), dtype=float)
    grid[0:2, 0:2] = 0.01
    latitudes = 35.0 - (np.arange(4) * 0.01)
    longitudes = -98.0 + (np.arange(4) * 0.01)

    detections = detect_layer_objects(grid, latitudes, longitudes, "low")

    assert len(detections) == 1
    assert detections[0]["pixel_count"] == 4
