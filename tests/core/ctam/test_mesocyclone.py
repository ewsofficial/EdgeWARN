from copy import deepcopy
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
                "low": _sample_component(9.0),
                "mid": _sample_component(6.5, width_km=6.0, ellipticity=0.6, aspect_ratio=1.6),
                "alignment": {
                    "paired": True,
                    "vertical_centroid_sep_km": 1.0,
                    "vertical_peak_sep_km": 1.0,
                    "orientation_diff_deg": 5.0,
                    "width_ratio": 0.95,
                    "area_ratio": 0.9,
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


def test_mesocyclone_module_keeps_low_only_rotation_as_candidate():
    module = MesocycloneModule()
    storm_entry = {
        "id": 104,
        "tracking_mode": "active",
        "properties": {
            "morphology": {"aspect_ratio": 1.3, "linearity": 0.1},
            "azshear": {
                "low": _sample_component(7.2),
                "mid": None,
                "alignment": {
                    "paired": False,
                    "vertical_centroid_sep_km": None,
                    "vertical_peak_sep_km": None,
                    "orientation_diff_deg": None,
                    "width_ratio": None,
                    "area_ratio": None,
                    "is_vertically_aligned": False,
                },
            },
        },
    }

    module.run(storm_entry, history_cache=MagicMock())
    result = storm_entry["modules"]["Mesocyclone"]

    assert result["classification"] == "rotation_candidate"
    assert "LOW_LEVEL_ROTATION" in result["triggers"]


def test_mesocyclone_module_skips_when_low_level_signal_missing():
    module = MesocycloneModule()
    storm_entry = {
        "id": 105,
        "tracking_mode": "active",
        "properties": {
            "morphology": {"aspect_ratio": 1.0, "linearity": 0.0},
            "azshear": {
                "low": None,
                "mid": _sample_component(4.0),
                "alignment": {
                    "paired": False,
                    "vertical_centroid_sep_km": None,
                    "vertical_peak_sep_km": None,
                    "orientation_diff_deg": None,
                    "width_ratio": None,
                    "area_ratio": None,
                    "is_vertically_aligned": False,
                },
            },
        },
    }

    module.run(storm_entry, history_cache=MagicMock())
    result = storm_entry["modules"]["Mesocyclone"]

    assert result["status"] == "skipped"
    assert result["classification"] == "none"
    assert result["confidence"] == 0.0


def test_mesocyclone_module_tolerates_malformed_numeric_inputs():
    module = MesocycloneModule()
    storm_entry = {
        "id": 106,
        "tracking_mode": "decaying",
        "properties": {
            "morphology": {"aspect_ratio": "bad", "linearity": None},
            "azshear": {
                "low": _sample_component("7.0", width_km="bad", ellipticity=None, aspect_ratio="1.4"),
                "mid": _sample_component("4.0", width_km=7.5, ellipticity="0.5", aspect_ratio=None),
                "alignment": {
                    "paired": True,
                    "vertical_centroid_sep_km": 3.0,
                    "vertical_peak_sep_km": 2.0,
                    "orientation_diff_deg": 12.0,
                    "width_ratio": 0.8,
                    "area_ratio": 0.75,
                    "is_vertically_aligned": True,
                },
            },
        },
    }

    module.run(storm_entry, history_cache=MagicMock())
    result = storm_entry["modules"]["Mesocyclone"]

    assert result["status"] == "success"
    assert "DECAYING_TRACK_PENALTY" in result["triggers"]
    assert result["classification"] in {"mesocyclone", "rotation_candidate"}


def test_mesocyclone_module_requires_vertical_alignment_for_mesocyclone_classification():
    module = MesocycloneModule()
    storm_entry = {
        "id": 107,
        "tracking_mode": "active",
        "properties": {
            "morphology": {"aspect_ratio": 1.2, "linearity": 0.1},
            "azshear": {
                "low": _sample_component(8.5),
                "mid": _sample_component(6.2, width_km=6.2, ellipticity=0.58, aspect_ratio=1.5),
                "alignment": {
                    "paired": True,
                    "vertical_centroid_sep_km": 11.0,
                    "vertical_peak_sep_km": 10.5,
                    "orientation_diff_deg": 6.0,
                    "width_ratio": 0.92,
                    "area_ratio": 0.88,
                    "is_vertically_aligned": False,
                },
            },
        },
    }

    module.run(storm_entry, history_cache=MagicMock())
    result = storm_entry["modules"]["Mesocyclone"]

    assert "LOW_LEVEL_ROTATION" in result["triggers"]
    assert "MID_LEVEL_ROTATION" in result["triggers"]
    assert "VERTICAL_ALIGNMENT" not in result["triggers"]
    assert result["classification"] == "rotation_candidate"


def test_mesocyclone_persistence_bonus_ignores_low_only_history_but_counts_deep_history():
    module = MesocycloneModule()
    base_storm_entry = {
        "id": 108,
        "tracking_mode": "active",
        "properties": {
            "morphology": {"aspect_ratio": 1.4, "linearity": 0.2},
            "azshear": {
                "low": _sample_component(7.4),
                "mid": _sample_component(4.4, width_km=6.8, ellipticity=0.55, aspect_ratio=1.5),
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

    empty_history = MagicMock()
    empty_history.get.return_value = []

    low_only_history = MagicMock()
    low_only_history.get.return_value = [
        {
            "properties": {
                "azshear": {
                    "low": _sample_component(7.0),
                    "mid": None,
                    "alignment": {"is_vertically_aligned": True},
                }
            }
        }
    ]

    deep_history = MagicMock()
    deep_history.get.return_value = [
        {
            "properties": {
                "azshear": {
                    "low": _sample_component(7.0),
                    "mid": None,
                    "alignment": {"is_vertically_aligned": False},
                }
            },
            "modules": {"Mesocyclone": {"classification": "deep_mesocyclone", "confidence": 0.75}},
        }
    ]

    baseline_entry = deepcopy(base_storm_entry)
    low_only_entry = deepcopy(base_storm_entry)
    deep_history_entry = deepcopy(base_storm_entry)

    module.run(baseline_entry, history_cache=empty_history)
    module.run(low_only_entry, history_cache=low_only_history)
    module.run(deep_history_entry, history_cache=deep_history)

    baseline_confidence = baseline_entry["modules"]["Mesocyclone"]["confidence"]
    low_only_confidence = low_only_entry["modules"]["Mesocyclone"]["confidence"]
    deep_history_confidence = deep_history_entry["modules"]["Mesocyclone"]["confidence"]

    assert low_only_confidence == baseline_confidence
    assert deep_history_confidence > baseline_confidence


def test_mesocyclone_module_does_not_promote_from_prior_plain_classification():
    module = MesocycloneModule()
    storm_entry = {
        "id": 109,
        "tracking_mode": "active",
        "properties": {
            "morphology": {"aspect_ratio": 1.2, "linearity": 0.2},
            "azshear": {
                "low": _sample_component(5.2, width_km=12.0, ellipticity=0.3, aspect_ratio=1.2),
                "mid": None,
                "alignment": {
                    "paired": False,
                    "vertical_centroid_sep_km": None,
                    "vertical_peak_sep_km": None,
                    "orientation_diff_deg": None,
                    "width_ratio": None,
                    "area_ratio": None,
                    "is_vertically_aligned": False,
                },
            },
        },
    }
    history_cache = MagicMock()
    history_cache.get.return_value = [
        {
            "properties": {"azshear": {"low": _sample_component(6.0), "mid": None, "alignment": {"is_vertically_aligned": False}}},
            "modules": {"Mesocyclone": {"classification": "mesocyclone", "confidence": 0.6}},
        }
    ]

    module.run(storm_entry, history_cache=history_cache)
    result = storm_entry["modules"]["Mesocyclone"]

    assert result["classification"] == "rotation_candidate"
