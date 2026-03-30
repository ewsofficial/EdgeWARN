from typing import Any, Dict, Optional

from ...interface import AnalysisModule
from . import config as cfg


class MesocycloneModule(AnalysisModule):
    @property
    def name(self) -> str:
        return "Mesocyclone"

    @staticmethod
    def _clamp(value: float, lower: float = 0.0, upper: float = 1.0) -> float:
        return max(lower, min(upper, value))

    @staticmethod
    def _safe_float(value: Any, default: float = 0.0) -> float:
        try:
            if value is None:
                return default
            return float(value)
        except (TypeError, ValueError):
            return default

    def _score_strength(self, low: Optional[Dict[str, Any]], mid: Optional[Dict[str, Any]]) -> float:
        if not low:
            return 0.0

        low_peak = self._safe_float(low.get("peak_value", 0.0))
        low_score = self._clamp((low_peak - cfg.LOW_PEAK_STRONG) / max(cfg.LOW_PEAK_EXTREME - cfg.LOW_PEAK_STRONG, 1e-6))

        if not mid:
            return 0.65 * low_score

        mid_peak = self._safe_float(mid.get("peak_value", 0.0))
        mid_score = self._clamp((mid_peak - cfg.MID_PEAK_STRONG) / max(cfg.MID_PEAK_EXTREME - cfg.MID_PEAK_STRONG, 1e-6))
        return (0.65 * low_score) + (0.35 * mid_score)

    def _score_alignment(self, alignment: Dict[str, Any]) -> float:
        if not alignment.get("paired"):
            return 0.0

        centroid_sep = alignment.get("vertical_centroid_sep_km")
        peak_sep = alignment.get("vertical_peak_sep_km")
        orientation_diff = alignment.get("orientation_diff_deg")
        width_ratio = alignment.get("width_ratio")

        centroid_score = 0.0 if centroid_sep is None else self._clamp(1.0 - (centroid_sep / cfg.MAX_VERTICAL_SEPARATION_KM))
        peak_score = 0.0 if peak_sep is None else self._clamp(1.0 - (peak_sep / cfg.MAX_PEAK_SEPARATION_KM))
        orient_score = 0.0 if orientation_diff is None else self._clamp(1.0 - (orientation_diff / 90.0))
        width_score = 0.0 if width_ratio is None else self._clamp(width_ratio)
        return (0.4 * centroid_score) + (0.3 * peak_score) + (0.15 * orient_score) + (0.15 * width_score)

    def _score_morphology(self, low: Optional[Dict[str, Any]], mid: Optional[Dict[str, Any]], storm_morphology: Dict[str, Any]) -> float:
        if not low:
            return 0.0

        candidates = [low]
        if mid:
            candidates.append(mid)

        widths = [self._safe_float(item.get("width_km", 0.0)) for item in candidates]
        ellipticities = [self._safe_float(item.get("ellipticity", 0.0)) for item in candidates]
        aspect_ratios = [self._safe_float(item.get("aspect_ratio", 1.0), 1.0) for item in candidates]

        width_score = self._clamp(1.0 - (max(widths) / cfg.MAX_WIDTH_KM))
        ellipticity_score = self._clamp((max(ellipticities) - cfg.MIN_ELLIPTICITY) / max(1.0 - cfg.MIN_ELLIPTICITY, 1e-6))
        aspect_score = self._clamp((max(aspect_ratios) - cfg.MIN_ASPECT_RATIO) / 2.0)

        reflectivity_aspect = self._safe_float(storm_morphology.get("aspect_ratio", 1.0), 1.0)
        reflectivity_linearity = self._safe_float(storm_morphology.get("linearity", 0.0))
        reflectivity_penalty = 0.0
        if reflectivity_aspect > 4.0 or reflectivity_linearity > 0.75:
            reflectivity_penalty = 0.15

        return self._clamp((0.45 * width_score) + (0.3 * ellipticity_score) + (0.25 * aspect_score) - reflectivity_penalty)

    def _persistence_bonus(self, storm_entry: Dict[str, Any], history_cache: Optional[Any]) -> float:
        if history_cache is None:
            return 0.0

        cell_id = storm_entry.get("id")
        if cell_id is None:
            return 0.0

        try:
            history = history_cache.get(cell_id, limit=cfg.PERSISTENCE_HISTORY_LIMIT)
        except Exception:
            return 0.0

        qualifying = 0
        for entry in history:
            props = entry.get("properties", {})
            azshear = props.get("azshear", {})
            alignment = azshear.get("alignment", {})
            low = azshear.get("low")
            mid = azshear.get("mid")
            if low and mid and alignment.get("is_vertically_aligned"):
                qualifying += 1
                continue

            prior_result = entry.get("modules", {}).get(self.name, {})
            if prior_result.get("classification") == "deep_mesocyclone":
                qualifying += 1

        return min(cfg.PERSISTENCE_CAP, qualifying * cfg.PERSISTENCE_STEP)

    def run(self, storm_entry: Dict[str, Any], environment: Optional[Dict[str, Any]] = None, history_cache: Optional[Any] = None) -> None:
        props = storm_entry.get("properties", {})
        azshear = props.get("azshear", {})
        low = azshear.get("low")
        mid = azshear.get("mid")
        alignment = azshear.get("alignment", {})
        storm_morphology = props.get("morphology", {})
        is_aligned = bool(alignment.get("is_vertically_aligned", False))
        low_peak = None if not low else self._safe_float(low.get("peak_value"), None)
        mid_peak = None if not mid else self._safe_float(mid.get("peak_value"), None)

        triggers = []
        strength_score = self._score_strength(low, mid)
        alignment_score = self._score_alignment(alignment)
        morphology_score = self._score_morphology(low, mid, storm_morphology)
        confidence = (
            cfg.STRENGTH_WEIGHT * strength_score
            + cfg.ALIGNMENT_WEIGHT * alignment_score
            + cfg.MORPHOLOGY_WEIGHT * morphology_score
        )

        if low_peak is not None and low_peak >= cfg.LOW_PEAK_STRONG:
            triggers.append("LOW_LEVEL_ROTATION")
        if mid_peak is not None and mid_peak >= cfg.MID_PEAK_STRONG:
            triggers.append("MID_LEVEL_ROTATION")
        if is_aligned:
            triggers.append("VERTICAL_ALIGNMENT")
        if low and self._safe_float(low.get("ellipticity", 0.0)) >= cfg.MIN_ELLIPTICITY:
            triggers.append("ELLIPTICAL_SIGNATURE")
        if low and self._safe_float(low.get("width_km", cfg.MAX_WIDTH_KM + 1.0), cfg.MAX_WIDTH_KM + 1.0) <= cfg.MAX_WIDTH_KM:
            triggers.append("COMPACT_WIDTH")

        confidence += self._persistence_bonus(storm_entry, history_cache)

        tracking_mode = storm_entry.get("tracking_mode")
        if tracking_mode == "predicted":
            confidence -= cfg.PREDICTED_TRACK_PENALTY
            triggers.append("PREDICTED_TRACK_PENALTY")
        elif tracking_mode == "decaying":
            confidence -= cfg.DECAYING_TRACK_PENALTY
            triggers.append("DECAYING_TRACK_PENALTY")

        confidence = round(self._clamp(confidence), 3)
        classification = "none"
        if confidence >= cfg.DEEP_CONFIDENCE and is_aligned and mid_peak is not None and mid_peak >= cfg.MID_PEAK_STRONG:
            classification = "deep_mesocyclone"
        elif confidence >= cfg.MIN_CLASSIFY_CONFIDENCE and low and is_aligned and mid:
            classification = "mesocyclone"
        elif low:
            classification = "rotation_candidate"

        storm_entry.setdefault("modules", {})
        storm_entry["modules"][self.name] = {
            "status": "success" if low else "skipped",
            "classification": classification,
            "confidence": confidence,
            "triggers": triggers,
            "diagnostics": {
                "strength_score": round(strength_score, 3),
                "alignment_score": round(alignment_score, 3),
                "morphology_score": round(morphology_score, 3),
                "aligned": is_aligned,
                "low_peak": low_peak,
                "mid_peak": mid_peak,
                "vertical_centroid_sep_km": alignment.get("vertical_centroid_sep_km"),
                "vertical_peak_sep_km": alignment.get("vertical_peak_sep_km"),
            },
        }
