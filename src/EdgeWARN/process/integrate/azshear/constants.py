AZSHEAR_BUFFER_KM = 5.0
AZSHEAR_LOW_THRESHOLD = 8.0
AZSHEAR_MID_THRESHOLD = 6.0
AZSHEAR_MIN_GATE_COUNT = 5
AZSHEAR_MAX_PAIR_SEPARATION_KM = 12.0


def empty_level_output():
    return {
        "core_structure": {
            "component_count": 0,
            "largest_component_area": 0.0,
            "largest_component_compactness": 0.0,
            "largest_component_peak_azshear": 0.0,
            "largest_component_mean_azshear": 0.0,
        },
        "dominance": {
            "dominance": 0.0,
            "dominance_ratio": 0.0,
            "secondary_core_ratio": 0.0,
        },
        "linearity": {
            "linearity": 0.0,
            "centroid_line_fit_score": 0.0,
            "linearity_ratio": 0.0,
            "alignment_with_reflectivity_axis": 0.0,
        },
        "persistence": {
            "dominant_component_persistence": 0.0,
            "peak_persistence": 0.0,
        },
        "distribution": {
            "total_azshear_area": 0.0,
            "coverage_fraction": 0.0,
            "fragmentation_index": 0.0,
        },
    }


def empty_cross_layer_output():
    return {
        "dominant_component_overlap_area": 0.0,
        "dominant_component_overlap_ratio": 0.0,
        "dominant_component_centroid_distance_km": None,
        "dominant_component_centroid_alignment": 0.0,
        "ll_ml_dominance_ratio_ratio": None,
        "ll_ml_peak_ratio": None,
        "simultaneous_persistence": 0.0,
    }


def empty_azshear_output():
    return {
        "buffer_km": AZSHEAR_BUFFER_KM,
        "low": empty_level_output(),
        "mid": empty_level_output(),
        "cross_layer": empty_cross_layer_output(),
    }
