from datetime import timezone
from typing import Any, Dict

from EdgeWARN.ctam.interface import GridAnalysisModule

from .associate import associate_vertical
from .detect import detect_layer_objects
from .gate import apply_reflectivity_gate
from .loader import load_latest_inputs
from .output import build_detection_record, build_payload, save_mesocyclone_output
from .preprocess import preprocess_inputs
from .score import score_detections
from .track import MesocycloneTracker


class MesocycloneModule(GridAnalysisModule):
    def __init__(self):
        self._tracker = MesocycloneTracker()

    @property
    def name(self) -> str:
        return "Mesocyclone"

    def _load_inputs(self) -> Dict[str, Any]:
        return load_latest_inputs()

    def run(self) -> Dict[str, Any]:
        try:
            inputs = self._load_inputs()
        except Exception as exc:
            print(f"[Mesocyclone] Input load failed: {exc}")
            return {
                "features": {"type": "FeatureCollection", "features": []},
                "metadata": {"error": str(exc), "detection_count": 0},
                "timestamp": None,
                "attach_to_stormcells": False,
            }

        preprocessed = preprocess_inputs(inputs["grids"])
        latitudes = inputs["coordinates"]["latitudes"]
        longitudes = inputs["coordinates"]["longitudes"]

        low_objects = detect_layer_objects(preprocessed["low"], latitudes, longitudes, "low")
        mid_objects = detect_layer_objects(preprocessed["mid"], latitudes, longitudes, "mid")

        low_gated = apply_reflectivity_gate(low_objects, preprocessed["reflectivity"], latitudes, longitudes)
        mid_gated = apply_reflectivity_gate(mid_objects, preprocessed["reflectivity"], latitudes, longitudes)

        associated = associate_vertical(low_gated, mid_gated)
        scored = score_detections(associated)

        timestamp = inputs["timestamp"]
        timestamp = timestamp.astimezone(timezone.utc)
        tracked = self._tracker.update(scored, timestamp)
        timestamp_iso = timestamp.isoformat()
        timestamp_token = timestamp.strftime("%Y%m%d-%H%M%S")

        detection_records = [build_detection_record(detection, timestamp_iso) for detection in tracked]
        metadata = {
            "detection_count": len(detection_records),
            "low_candidate_count": len(low_objects),
            "mid_candidate_count": len(mid_objects),
            "low_gated_count": len(low_gated),
            "mid_gated_count": len(mid_gated),
            "input_paths": inputs["paths"],
            "scale_notes": inputs.get("scale_notes", {}),
            "grid_spacing_deg": inputs.get("grid_spacing_deg", {}),
        }
        payload = build_payload(timestamp_iso, metadata, detection_records)
        output_path = save_mesocyclone_output(timestamp_token, payload)

        print(f"[Mesocyclone] Persisted {len(detection_records)} detection(s) to {output_path}")

        return {
            "features": {"type": "FeatureCollection", "features": []},
            "metadata": metadata,
            "timestamp": timestamp_iso,
            "output_path": str(output_path),
            "attach_to_stormcells": False,
        }
