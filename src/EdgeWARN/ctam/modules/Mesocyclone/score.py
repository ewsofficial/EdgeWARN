from typing import Dict, List

from . import config as cfg


def classify_strength(max_azshear: float) -> str:
    for threshold, label in cfg.STRENGTH_BINS:
        if max_azshear >= threshold:
            return label
    return "weak"


def compute_strength_rank(max_azshear: float) -> int:
    if max_azshear <= cfg.RANK_MIN:
        return 1
    if max_azshear >= cfg.RANK_MAX:
        return cfg.MAX_STRENGTH_RANK

    scaled = (max_azshear - cfg.RANK_MIN) / max(cfg.RANK_MAX - cfg.RANK_MIN, 1e-6)
    return max(1, min(cfg.MAX_STRENGTH_RANK, int(round(1 + scaled * (cfg.MAX_STRENGTH_RANK - 1)))))


def score_detections(associated_detections: List[Dict[str, object]]) -> List[Dict[str, object]]:
    scored = []
    for detection in associated_detections:
        low = detection.get("low")
        mid = detection.get("mid")
        max_azshear_low = float(low.get("max_azshear", 0.0)) if low else 0.0
        max_azshear_mid = float(mid.get("max_azshear", 0.0)) if mid else 0.0
        max_azshear = max(max_azshear_low, max_azshear_mid)
        reflectivity_max = max(
            float(low.get("reflectivity_max", 0.0)) if low else 0.0,
            float(mid.get("reflectivity_max", 0.0)) if mid else 0.0,
        )
        depth_bonus = 1.0 if detection["depth_flag"] == "deep" else 0.55
        az_component = min(1.0, max_azshear / max(cfg.RANK_MAX, 1e-6))
        ref_component = min(1.0, reflectivity_max / 60.0)
        confidence = (
            cfg.CONFIDENCE_WEIGHTS["azshear"] * az_component
            + cfg.CONFIDENCE_WEIGHTS["depth"] * depth_bonus
            + cfg.CONFIDENCE_WEIGHTS["reflectivity"] * ref_component
        )

        merged = dict(detection)
        merged.update(
            {
                "strength_label": classify_strength(max_azshear),
                "strength_rank": compute_strength_rank(max_azshear),
                "confidence_score": round(min(1.0, max(0.0, confidence)), 3),
                "max_azshear_low": round(max_azshear_low, 4),
                "max_azshear_mid": round(max_azshear_mid, 4),
                "reflectivity_max": round(reflectivity_max, 3),
                "area_km2": round(
                    max(float(low.get("area_km2", 0.0)) if low else 0.0, float(mid.get("area_km2", 0.0)) if mid else 0.0),
                    3,
                ),
                "eccentricity": round(
                    max(float(low.get("eccentricity", 0.0)) if low else 0.0, float(mid.get("eccentricity", 0.0)) if mid else 0.0),
                    3,
                ),
                "compactness": round(
                    max(float(low.get("compactness", 0.0)) if low else 0.0, float(mid.get("compactness", 0.0)) if mid else 0.0),
                    3,
                ),
            }
        )
        scored.append(merged)

    return scored
