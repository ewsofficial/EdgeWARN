from concurrent.futures import ThreadPoolExecutor
from datetime import timezone
from time import perf_counter
from typing import Any, Dict

from EdgeWARN.ctam.interface import GridAnalysisModule

from .associate import associate_vertical
from .detect import detect_layer_objects
from .gate import apply_reflectivity_gate, build_reflectivity_gate_context
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
        started_at = perf_counter()
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
        print(f"[Mesocyclone] Input load completed in {perf_counter() - started_at:.3f}s")

        stage_started_at = perf_counter()
        preprocessed = preprocess_inputs(inputs["grids"])
        print(f"[Mesocyclone] Preprocess completed in {perf_counter() - stage_started_at:.3f}s")
        latitudes = inputs["coordinates"]["latitudes"]
        longitudes = inputs["coordinates"]["longitudes"]
        reflectivity_latitudes = inputs["coordinates"].get("reflectivity_latitudes", latitudes)
        reflectivity_longitudes = inputs["coordinates"].get("reflectivity_longitudes", longitudes)

        stage_started_at = perf_counter()
        with ThreadPoolExecutor(max_workers=2) as executor:
            low_future = executor.submit(detect_layer_objects, preprocessed["low"], latitudes, longitudes, "low")
            mid_future = executor.submit(detect_layer_objects, preprocessed["mid"], latitudes, longitudes, "mid")
            low_objects = low_future.result()
            mid_objects = mid_future.result()
        print(
            f"[Mesocyclone] Detection completed in {perf_counter() - stage_started_at:.3f}s "
            f"(low={len(low_objects)}, mid={len(mid_objects)})"
        )

        stage_started_at = perf_counter()
        gate_context = build_reflectivity_gate_context(
            preprocessed["reflectivity"],
            latitudes,
            longitudes,
            reflectivity_latitudes,
            reflectivity_longitudes,
        )
        with ThreadPoolExecutor(max_workers=2) as executor:
            low_future = executor.submit(
                apply_reflectivity_gate,
                low_objects,
                preprocessed["reflectivity"],
                latitudes,
                longitudes,
                reflectivity_latitudes,
                reflectivity_longitudes,
                gate_context,
            )
            mid_future = executor.submit(
                apply_reflectivity_gate,
                mid_objects,
                preprocessed["reflectivity"],
                latitudes,
                longitudes,
                reflectivity_latitudes,
                reflectivity_longitudes,
                gate_context,
            )
            low_gated = low_future.result()
            mid_gated = mid_future.result()
        print(
            f"[Mesocyclone] Reflectivity gating completed in {perf_counter() - stage_started_at:.3f}s "
            f"(low={len(low_gated)}, mid={len(mid_gated)})"
        )

        stage_started_at = perf_counter()
        associated = associate_vertical(low_gated, mid_gated)
        scored = score_detections(associated)
        print(f"[Mesocyclone] Association/scoring completed in {perf_counter() - stage_started_at:.3f}s")

        timestamp = inputs["timestamp"]
        timestamp = timestamp.astimezone(timezone.utc)
        stage_started_at = perf_counter()
        tracked = self._tracker.update(scored, timestamp)
        print(f"[Mesocyclone] Tracking completed in {perf_counter() - stage_started_at:.3f}s")
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

        print(
            f"[Mesocyclone] Persisted {len(detection_records)} detection(s) to {output_path} "
            f"in {perf_counter() - started_at:.3f}s total"
        )

        return {
            "features": {"type": "FeatureCollection", "features": []},
            "metadata": metadata,
            "timestamp": timestamp_iso,
            "output_path": str(output_path),
            "attach_to_stormcells": False,
        }
