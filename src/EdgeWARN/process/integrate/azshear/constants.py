from ..config import section

# Read per call, never at module scope. EdgeWARN/pipeline.py imports this module
# transitively from src/run.py:14, which is 27 lines before get_args() exports
# EDGEWARN_CONFIG_DIR -- a module-scope read here freezes the repo-default
# config directory, and because section() is memoized the poisoned cache entry
# also defeats the correctly-written per-call reads elsewhere in this package.


def azshear_buffer_km():
    """Radius the cell polygon is buffered by before azshear gates are gathered."""
    return section("azshear")["buffer_km"]


def azshear_low_threshold():
    """Minimum azshear magnitude counted as a gate in the 0-2 km layer."""
    return section("azshear")["low_threshold"]


def azshear_mid_threshold():
    """Minimum azshear magnitude counted as a gate in the 3-6 km layer."""
    return section("azshear")["mid_threshold"]


def azshear_min_gate_count():
    """Smallest connected component retained as a candidate core."""
    return section("azshear")["min_gate_count"]


def azshear_max_pair_separation_km():
    """Largest centroid separation a low/mid component pair may have."""
    return section("azshear")["max_pair_separation_km"]


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
