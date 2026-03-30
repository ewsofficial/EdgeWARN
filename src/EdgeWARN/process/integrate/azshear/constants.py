AZSHEAR_BUFFER_KM = 5.0
AZSHEAR_LOW_THRESHOLD = 8.0
AZSHEAR_MID_THRESHOLD = 6.0
AZSHEAR_MAX_PAIR_SEPARATION_KM = 12.0


def empty_alignment_output():
    return {
        "paired": False,
        "vertical_centroid_sep_km": None,
        "vertical_peak_sep_km": None,
        "centroid_distance_km": None,
        "orientation_diff_deg": None,
        "width_ratio": None,
        "area_ratio": None,
        "overlap_area_km2": None,
        "overlap_ratio": None,
        "low_overlap_fraction": None,
        "mid_overlap_fraction": None,
        "is_vertically_aligned": False,
    }


def empty_azshear_output():
    return {
        "buffer_km": AZSHEAR_BUFFER_KM,
        "low": None,
        "mid": None,
        "alignment": empty_alignment_output(),
        "low_candidate_count": 0,
        "mid_candidate_count": 0,
    }
