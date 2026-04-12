from EdgeWARN.ctam.modules.Mesocyclone.score import compute_strength_rank, score_detections


def test_compute_strength_rank_is_bounded():
    assert 1 <= compute_strength_rank(0.006) <= 25
    assert compute_strength_rank(0.03) == 25


def test_score_detections_populates_rank_and_confidence():
    scored = score_detections(
        [
            {
                "low": {"max_azshear": 0.012, "reflectivity_max": 45.0, "area_km2": 12.0, "eccentricity": 0.4, "compactness": 0.6},
                "mid": None,
                "depth_flag": "shallow",
                "association_distance_km": None,
            }
        ]
    )
    assert scored[0]["strength_rank"] >= 1
    assert 0.0 <= scored[0]["confidence_score"] <= 1.0
