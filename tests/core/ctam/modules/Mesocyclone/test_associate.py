from EdgeWARN.ctam.modules.Mesocyclone.associate import associate_vertical


def test_associate_vertical_builds_deep_and_single_layer_detections():
    low = [{"centroid_lat": 35.0, "centroid_lon": -97.0, "max_azshear": 0.01}]
    mid = [
        {"centroid_lat": 35.02, "centroid_lon": -97.01, "max_azshear": 0.009},
        {"centroid_lat": 36.0, "centroid_lon": -98.0, "max_azshear": 0.008},
    ]

    associated = associate_vertical(low, mid)

    assert any(item["depth_flag"] == "deep" for item in associated)
    assert any(item["depth_flag"] == "mid-level" for item in associated)
