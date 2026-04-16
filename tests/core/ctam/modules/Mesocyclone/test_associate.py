import numpy as np

from EdgeWARN.ctam.modules.Mesocyclone.associate import associate_vertical


def _build_detection(lat: float, lon: float, max_azshear: float, pixels: list[tuple[int, int]]) -> dict[str, object]:
    rows, cols = zip(*pixels)
    return {
        "centroid_lat": lat,
        "centroid_lon": lon,
        "max_azshear": max_azshear,
        "pixel_rows": np.asarray(rows, dtype=np.int32),
        "pixel_cols": np.asarray(cols, dtype=np.int32),
    }


def test_associate_vertical_requires_footprint_overlap_for_deep_pairing():
    low = [_build_detection(35.0, -97.0, 0.01, [(0, 0), (0, 1), (1, 0), (1, 1), (2, 0), (2, 1)])]
    mid = [_build_detection(35.01, -97.01, 0.009, [(10, 10), (10, 11), (11, 10), (11, 11), (12, 10), (12, 11)])]

    associated = associate_vertical(low, mid)

    assert len(associated) == 2
    assert {item["depth_flag"] for item in associated} == {"shallow", "mid-level"}


def test_associate_vertical_builds_deep_pair_when_footprints_overlap():
    low = [_build_detection(35.0, -97.0, 0.01, [(0, 0), (0, 1), (1, 0), (1, 1), (2, 0), (2, 1)])]
    mid = [_build_detection(35.01, -97.01, 0.009, [(1, 0), (1, 1), (2, 0), (2, 1), (3, 0), (3, 1)])]

    associated = associate_vertical(low, mid)

    assert len(associated) == 1
    assert associated[0]["depth_flag"] == "deep"
    assert associated[0]["association_overlap_pixels"] == 4
    assert associated[0]["association_overlap_ratio"] == 0.667
    assert associated[0]["association_distance_km"] is not None


def test_associate_vertical_rejects_tiny_overlap_below_threshold():
    low_pixels = [(row, col) for row in range(3) for col in range(7)]
    mid_pixels = [(row, col) for row in range(2, 5) for col in range(6, 13)]
    low = [_build_detection(35.0, -97.0, 0.01, low_pixels)]
    mid = [_build_detection(35.01, -97.01, 0.009, mid_pixels)]

    associated = associate_vertical(low, mid)

    assert len(associated) == 2
    assert {item["depth_flag"] for item in associated} == {"shallow", "mid-level"}
