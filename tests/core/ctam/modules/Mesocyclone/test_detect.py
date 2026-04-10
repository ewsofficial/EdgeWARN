import numpy as np

from EdgeWARN.ctam.modules.Mesocyclone.detect import detect_layer_objects


def test_detect_layer_objects_returns_component_metrics():
    grid = np.zeros((8, 8), dtype=float)
    grid[2:5, 2:5] = 0.009
    grid[3, 3] = 0.015
    latitudes = np.linspace(35.0, 34.3, 8)
    longitudes = np.linspace(-98.0, -97.3, 8)

    detections = detect_layer_objects(grid, latitudes, longitudes, "low")

    assert len(detections) == 1
    detection = detections[0]
    assert detection["max_azshear"] >= 0.015
    assert detection["pixel_count"] >= 6
    assert detection["area_km2"] > 0.0
    assert detection["eccentricity"] >= 0.0
    assert len(detection["maxima"]) >= 1
