import numpy as np

from EdgeWARN.ctam.modules.Mesocyclone.gate import apply_reflectivity_gate


def test_reflectivity_gate_filters_non_overlapping_detection():
    detection = {
        "component_mask": np.array([[True, False], [False, False]]),
        "centroid_lat": 35.0,
        "centroid_lon": -97.0,
    }
    reflectivity = np.array([[20.0, 20.0], [20.0, 20.0]])
    latitudes = np.array([35.0, 34.9])
    longitudes = np.array([-97.0, -96.9])
    assert apply_reflectivity_gate([detection], reflectivity, latitudes, longitudes) == []


def test_reflectivity_gate_retains_overlapping_detection():
    detection = {
        "component_mask": np.array([[True, False], [False, False]]),
        "centroid_lat": 35.0,
        "centroid_lon": -97.0,
    }
    reflectivity = np.array([[45.0, 20.0], [20.0, 20.0]])
    latitudes = np.array([35.0, 34.9])
    longitudes = np.array([-97.0, -96.9])
    gated = apply_reflectivity_gate([detection], reflectivity, latitudes, longitudes)
    assert len(gated) == 1
    assert gated[0]["reflectivity_max"] == 45.0
