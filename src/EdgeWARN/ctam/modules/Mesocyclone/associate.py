import math
from typing import Dict, List

import numpy as np

from . import config as cfg


def _distance_km(lat_a: float, lon_a: float, lat_b: float, lon_b: float) -> float:
    radius_km = 6371.0
    lat1 = math.radians(lat_a)
    lat2 = math.radians(lat_b)
    dlat = lat2 - lat1
    dlon = math.radians(lon_b - lon_a)
    hav = math.sin(dlat / 2.0) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2.0) ** 2
    return 2.0 * radius_km * math.asin(min(1.0, math.sqrt(max(hav, 0.0))))


def _linearized_pixels(detection: Dict[str, object]) -> set[int]:
    rows = detection.get("pixel_rows")
    cols = detection.get("pixel_cols")
    if rows is None or cols is None:
        return set()

    rows_array = np.asarray(rows)
    cols_array = np.asarray(cols)
    if rows_array.ndim != 1 or cols_array.ndim != 1 or rows_array.size != cols_array.size or rows_array.size == 0:
        return set()

    linearized = (rows_array.astype(np.int64) << 32) | (cols_array.astype(np.int64) & np.int64(0xFFFFFFFF))
    return set(linearized.tolist())


def _footprint_overlap(low_pixels: set[int], mid_pixels: set[int]) -> tuple[int, float]:
    if not low_pixels or not mid_pixels:
        return 0, 0.0

    overlap_pixels = len(low_pixels & mid_pixels)
    if overlap_pixels <= 0:
        return 0, 0.0

    overlap_ratio = overlap_pixels / min(len(low_pixels), len(mid_pixels))
    return overlap_pixels, overlap_ratio


def associate_vertical(low_detections: List[Dict[str, object]], mid_detections: List[Dict[str, object]]) -> List[Dict[str, object]]:
    pairs = []
    used_low = set()
    used_mid = set()
    low_footprints = [_linearized_pixels(detection) for detection in low_detections]
    mid_footprints = [_linearized_pixels(detection) for detection in mid_detections]

    candidate_pairs = []
    for low_index, low_detection in enumerate(low_detections):
        for mid_index, mid_detection in enumerate(mid_detections):
            distance_km = _distance_km(
                float(low_detection["centroid_lat"]),
                float(low_detection["centroid_lon"]),
                float(mid_detection["centroid_lat"]),
                float(mid_detection["centroid_lon"]),
            )
            if distance_km > cfg.VERTICAL_ASSOCIATION_DISTANCE_KM:
                continue

            overlap_pixels, overlap_ratio = _footprint_overlap(low_footprints[low_index], mid_footprints[mid_index])
            if overlap_pixels <= 0 or overlap_ratio < cfg.VERTICAL_ASSOCIATION_MIN_OVERLAP_RATIO:
                continue

            candidate_pairs.append(
                (
                    -overlap_ratio,
                    distance_km,
                    -max(float(low_detection["max_azshear"]), float(mid_detection["max_azshear"])),
                    low_index,
                    mid_index,
                    overlap_pixels,
                )
            )

    candidate_pairs.sort()
    for negative_overlap_ratio, distance_km, _, low_index, mid_index, overlap_pixels in candidate_pairs:
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
                "association_overlap_pixels": overlap_pixels,
                "association_overlap_ratio": round(-negative_overlap_ratio, 3),
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
