import numpy as np

from EdgeWARN.ctam.modules.Mesocyclone.gate import apply_reflectivity_gate, build_reflectivity_gate_context


def test_reflectivity_gate_filters_non_overlapping_detection():
    detection = {
        "pixel_rows": np.array([0], dtype=int),
        "pixel_cols": np.array([0], dtype=int),
        "centroid_lat": 35.0,
        "centroid_lon": -97.0,
    }
    reflectivity = np.array([[20.0, 20.0], [20.0, 20.0]])
    latitudes = np.array([35.0, 34.9])
    longitudes = np.array([-97.0, -96.9])
    assert apply_reflectivity_gate([detection], reflectivity, latitudes, longitudes, latitudes, longitudes) == []


def test_reflectivity_gate_retains_overlapping_detection():
    detection = {
        "pixel_rows": np.array([0], dtype=int),
        "pixel_cols": np.array([0], dtype=int),
        "centroid_lat": 35.0,
        "centroid_lon": -97.0,
    }
    reflectivity = np.array([[45.0, 20.0], [20.0, 20.0]])
    latitudes = np.array([35.0, 34.9])
    longitudes = np.array([-97.0, -96.9])
    gated = apply_reflectivity_gate([detection], reflectivity, latitudes, longitudes, latitudes, longitudes)
    assert len(gated) == 1
    assert gated[0]["reflectivity_max"] == 45.0


def test_reflectivity_gate_maps_fine_detection_to_native_coarse_reflectivity_grid():
    detection = {
        "pixel_rows": np.array([0, 1, 2, 3], dtype=int),
        "pixel_cols": np.array([0, 1, 2, 3], dtype=int),
        "centroid_lat": 34.75,
        "centroid_lon": -97.75,
    }
    reflectivity = np.array([[45.0, 20.0], [20.0, 20.0]])
    latitudes = np.array([34.9375, 34.8125, 34.6875, 34.5625])
    longitudes = np.array([-97.9375, -97.8125, -97.6875, -97.5625])
    reflectivity_latitudes = np.array([34.875, 34.625])
    reflectivity_longitudes = np.array([-97.875, -97.625])

    gated = apply_reflectivity_gate(
        [detection],
        reflectivity,
        latitudes,
        longitudes,
        reflectivity_latitudes,
        reflectivity_longitudes,
    )

    assert len(gated) == 1
    assert gated[0]["reflectivity_overlap_pixels"] == 1
    assert gated[0]["reflectivity_max"] == 45.0


def test_reflectivity_gate_context_reuse_matches_default_path():
    detection = {
        "pixel_rows": np.array([0, 1, 2, 3], dtype=int),
        "pixel_cols": np.array([0, 1, 2, 3], dtype=int),
        "centroid_lat": 34.75,
        "centroid_lon": -97.75,
    }
    reflectivity = np.array([[45.0, 20.0], [20.0, 20.0]])
    latitudes = np.array([34.9375, 34.8125, 34.6875, 34.5625])
    longitudes = np.array([-97.9375, -97.8125, -97.6875, -97.5625])
    reflectivity_latitudes = np.array([34.875, 34.625])
    reflectivity_longitudes = np.array([-97.875, -97.625])

    gate_context = build_reflectivity_gate_context(
        reflectivity,
        latitudes,
        longitudes,
        reflectivity_latitudes,
        reflectivity_longitudes,
    )

    default_gated = apply_reflectivity_gate(
        [detection],
        reflectivity,
        latitudes,
        longitudes,
        reflectivity_latitudes,
        reflectivity_longitudes,
    )
    reused_gated = apply_reflectivity_gate(
        [detection],
        reflectivity,
        latitudes,
        longitudes,
        reflectivity_latitudes,
        reflectivity_longitudes,
        gate_context,
    )

    assert reused_gated == default_gated
