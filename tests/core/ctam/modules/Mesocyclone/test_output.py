from EdgeWARN.ctam.modules.Mesocyclone.output import build_detection_record


def test_build_detection_record_exports_azshear_keys_in_milli_inverse_seconds():
    record = build_detection_record(
        {
            "id": 7,
            "low": {"centroid_lat": 35.12345, "centroid_lon": -97.54321, "maxima": [{"value": 0.0279}]},
            "mid": {"centroid_lat": 35.12, "centroid_lon": -97.54, "maxima": []},
            "motion_vector": {"u": 1.0, "v": -2.0},
            "max_azshear_low": 0.0279,
            "max_azshear_mid": 0.0194,
            "depth_flag": "deep",
            "reflectivity_max": 51.5,
            "strength_rank": 23,
            "confidence_score": 0.901,
            "area_km2": 9.094,
            "eccentricity": 0.884,
            "compactness": 0.551,
            "strength_label": "violent",
            "association_distance_km": 2.317,
        },
        "2026-04-14T17:00:00+00:00",
    )

    assert record["azshear_low"] == 27.9
    assert record["azshear_mid"] == 19.4
    assert "max_azshear_low" not in record
    assert "max_azshear_mid" not in record
