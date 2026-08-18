# This support module is not part of the production pipeline. Keep its fixed
# experimental parameters local instead of exposing inert runtime settings.
_BUFFER_KM = 1.5
_LOW_THRESHOLD = 8.0
_MID_THRESHOLD = 6.0
_MIN_GATE_COUNT = 5
_MAX_PAIR_SEPARATION_KM = 12.0
_HISTORY_WINDOW = 5


def azshear_buffer_km():
    """Radius the cell polygon is buffered by before azshear gates are gathered."""
    return _BUFFER_KM


def azshear_low_threshold():
    """Minimum azshear magnitude counted as a gate in the 0-2 km layer."""
    return _LOW_THRESHOLD


def azshear_mid_threshold():
    """Minimum azshear magnitude counted as a gate in the 3-6 km layer."""
    return _MID_THRESHOLD


def azshear_min_gate_count():
    """Smallest connected component retained as a candidate core."""
    return _MIN_GATE_COUNT


def azshear_max_pair_separation_km():
    """Largest centroid separation a low/mid component pair may have."""
    return _MAX_PAIR_SEPARATION_KM


def azshear_history_window():
    """Number of history samples retained by the experimental support path."""
    return _HISTORY_WINDOW


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
        "pair_count": 0,
    }


def empty_azshear_output():
    return {
        "buffer_km": azshear_buffer_km(),
        "low": empty_level_output(),
        "mid": empty_level_output(),
        "cross_layer": empty_cross_layer_output(),
    }
