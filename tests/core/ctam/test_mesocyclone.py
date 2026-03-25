from unittest.mock import MagicMock

from EdgeWARN.ctam.modules.Mesocyclone import MesocycloneModule


def _sample_component(peak, width_km=6.0, ellipticity=0.6, aspect_ratio=1.6):
    return {
        "peak_value": peak,
        "width_km": width_km,
        "ellipticity": ellipticity,
        "aspect_ratio": aspect_ratio,
        "orientation_deg": 20.0,
        "area_km2": 25.0,
        "centroid_lat": 35.0,
        "centroid_lon": -97.0,
        "peak_lat": 35.0,
        "peak_lon": -97.0,
    }


def test_mesocyclone_module_classifies_deep_mesocyclone():
    module = MesocycloneModule()
    storm_entry = {
        "id": 101,
        "tracking_mode": "active",
        "properties": {
            "morphology": {"aspect_ratio": 1.5, "linearity": 0.2},
            "azshear": {
                "low": _sample_component(8.5),
                "mid": _sample_component(5.5, width_km=7.0, ellipticity=0.55, aspect_ratio=1.5),
                "alignment": {
                    "paired": True,
                    "vertical_centroid_sep_km": 2.0,
                    "vertical_peak_sep_km": 1.5,
                    "orientation_diff_deg": 10.0,
                    "width_ratio": 0.85,
                    "area_ratio": 0.8,
                    "is_vertically_aligned": True,
                },
            },
        },
    }
    history_cache = MagicMock()
    history_cache.get.return_value = [
        {
            "properties": {
                "azshear": {
                    "low": _sample_component(7.0),
                    "alignment": {"is_vertically_aligned": True},
                }
            }
        }
    ]

    module.run(storm_entry, history_cache=history_cache)
    result = storm_entry["modules"]["Mesocyclone"]

    assert result["status"] == "success"
    assert result["classification"] == "deep_mesocyclone"
    assert result["confidence"] >= 0.72
    assert "VERTICAL_ALIGNMENT" in result["triggers"]


def test_mesocyclone_module_rejects_broad_misaligned_signature():
    module = MesocycloneModule()
    storm_entry = {
        "id": 102,
        "tracking_mode": "active",
        "properties": {
            "morphology": {"aspect_ratio": 5.0, "linearity": 0.9},
            "azshear": {
                "low": _sample_component(4.5, width_km=24.0, ellipticity=0.05, aspect_ratio=1.0),
                "mid": _sample_component(2.6, width_km=22.0, ellipticity=0.05, aspect_ratio=1.0),
                "alignment": {
                    "paired": True,
                    "vertical_centroid_sep_km": 14.0,
                    "vertical_peak_sep_km": 13.0,
                    "orientation_diff_deg": 80.0,
                    "width_ratio": 0.4,
                    "area_ratio": 0.5,
                    "is_vertically_aligned": False,
                },
            },
        },
    }

    module.run(storm_entry, history_cache=MagicMock())
    result = storm_entry["modules"]["Mesocyclone"]

    assert result["classification"] in {"none", "rotation_candidate"}
    assert result["confidence"] < 0.45


def test_mesocyclone_module_applies_predicted_penalty():
    module = MesocycloneModule()
    storm_entry = {
        "id": 103,
        "tracking_mode": "predicted",
        "properties": {
            "morphology": {"aspect_ratio": 1.4, "linearity": 0.1},
            "azshear": {
                "low": _sample_component(7.0),
                "mid": _sample_component(4.0),
                "alignment": {
                    "paired": True,
                    "vertical_centroid_sep_km": 3.0,
                    "vertical_peak_sep_km": 2.5,
                    "orientation_diff_deg": 15.0,
                    "width_ratio": 0.8,
                    "area_ratio": 0.8,
                    "is_vertically_aligned": True,
                },
            },
        },
    }

    module.run(storm_entry, history_cache=MagicMock())
    result = storm_entry["modules"]["Mesocyclone"]

    assert "PREDICTED_TRACK_PENALTY" in result["triggers"]
    assert result["confidence"] < 0.72
