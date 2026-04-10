import math
from typing import Dict, List

from . import config as cfg


def _distance_km(lat_a: float, lon_a: float, lat_b: float, lon_b: float) -> float:
    radius_km = 6371.0
    lat1 = math.radians(lat_a)
    lat2 = math.radians(lat_b)
    dlat = lat2 - lat1
    dlon = math.radians(lon_b - lon_a)
    hav = math.sin(dlat / 2.0) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2.0) ** 2
    return 2.0 * radius_km * math.asin(min(1.0, math.sqrt(max(hav, 0.0))))


def associate_vertical(low_detections: List[Dict[str, object]], mid_detections: List[Dict[str, object]]) -> List[Dict[str, object]]:
    pairs = []
    used_low = set()
    used_mid = set()

    candidate_pairs = []
    for low_index, low_detection in enumerate(low_detections):
        for mid_index, mid_detection in enumerate(mid_detections):
            distance_km = _distance_km(
                float(low_detection["centroid_lat"]),
                float(low_detection["centroid_lon"]),
                float(mid_detection["centroid_lat"]),
                float(mid_detection["centroid_lon"]),
            )
            if distance_km <= cfg.VERTICAL_ASSOCIATION_DISTANCE_KM:
                candidate_pairs.append((distance_km, -max(float(low_detection["max_azshear"]), float(mid_detection["max_azshear"])), low_index, mid_index))

    candidate_pairs.sort()
    for distance_km, _, low_index, mid_index in candidate_pairs:
        if low_index in used_low or mid_index in used_mid:
            continue
        used_low.add(low_index)
        used_mid.add(mid_index)
        low_detection = low_detections[low_index]
        mid_detection = mid_detections[mid_index]
        pairs.append(
            {
                "low": low_detection,
                "mid": mid_detection,
                "depth_flag": "deep",
                "association_distance_km": round(distance_km, 3),
            }
        )

    for low_index, low_detection in enumerate(low_detections):
        if low_index in used_low:
            continue
        pairs.append(
            {
                "low": low_detection,
                "mid": None,
                "depth_flag": "shallow",
                "association_distance_km": None,
            }
        )

    for mid_index, mid_detection in enumerate(mid_detections):
        if mid_index in used_mid:
            continue
        pairs.append(
            {
                "low": None,
                "mid": mid_detection,
                "depth_flag": "mid-level",
                "association_distance_km": None,
            }
        )

    pairs.sort(
        key=lambda item: max(
            float(item["low"]["max_azshear"]) if item["low"] is not None else 0.0,
            float(item["mid"]["max_azshear"]) if item["mid"] is not None else 0.0,
        ),
        reverse=True,
    )
    return pairs
