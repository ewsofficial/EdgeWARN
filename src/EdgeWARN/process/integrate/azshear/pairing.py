import math

from .constants import AZSHEAR_MAX_PAIR_SEPARATION_KM
from .geometry import normalize_lon_delta


def pair_azshear_components(low_candidates, mid_candidates):
    if not low_candidates or not mid_candidates:
        return None

    best_pair = None
    best_score = None
    for low in low_candidates[:5]:
        for mid in mid_candidates[:5]:
            dlat = (mid["centroid_lat"] - low["centroid_lat"]) * 111.32
            dlon = normalize_lon_delta(mid["centroid_lon"] - low["centroid_lon"]) * 111.32 * max(
                math.cos(math.radians((mid["centroid_lat"] + low["centroid_lat"]) / 2.0)),
                1e-6,
            )
            centroid_sep_km = math.sqrt(dlat**2 + dlon**2)
            if centroid_sep_km > AZSHEAR_MAX_PAIR_SEPARATION_KM:
                continue
            score = low["peak_value"] + mid["peak_value"] - 0.2 * centroid_sep_km + 0.05 * min(
                low["area_km2"],
                mid["area_km2"],
            )
            if best_score is None or score > best_score:
                best_score = score
                best_pair = (low, mid)

    return best_pair
