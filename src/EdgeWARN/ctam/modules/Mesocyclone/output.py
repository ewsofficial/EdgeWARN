import json
from pathlib import Path
from typing import Dict, List

import util.file as fs


def build_detection_record(detection: Dict[str, object], timestamp_iso: str) -> Dict[str, object]:
    low = detection.get("low")
    mid = detection.get("mid")
    primary = low if low is not None else mid
    return {
        "id": int(detection["id"]),
        "time": timestamp_iso,
        "lat": round(float(primary["centroid_lat"]), 5),
        "lon": round(float(primary["centroid_lon"]), 5),
        "motion_vector": detection.get("motion_vector", {"u": 0.0, "v": 0.0}),
        "max_azshear_low": float(detection.get("max_azshear_low", 0.0)),
        "max_azshear_mid": float(detection.get("max_azshear_mid", 0.0)),
        "depth_flag": detection["depth_flag"],
        "reflectivity_max": float(detection.get("reflectivity_max", 0.0)),
        "strength_rank": int(detection.get("strength_rank", 1)),
        "confidence_score": float(detection.get("confidence_score", 0.0)),
        "area": float(detection.get("area_km2", 0.0)),
        "eccentricity": float(detection.get("eccentricity", 0.0)),
        "compactness": float(detection.get("compactness", 0.0)),
        "strength_label": detection.get("strength_label", "weak"),
        "multi_peak_count_low": len(low.get("maxima", [])) if low is not None else 0,
        "multi_peak_count_mid": len(mid.get("maxima", [])) if mid is not None else 0,
        "association_distance_km": detection.get("association_distance_km"),
    }


def save_mesocyclone_output(timestamp_token: str, payload: Dict[str, object]) -> Path:
    fs.MESOCYCLONE_DIR.mkdir(parents=True, exist_ok=True)
    output_path = fs.MESOCYCLONE_DIR / f"mesocyclones_{timestamp_token}.json"
    with open(output_path, "w") as file_handle:
        json.dump(payload, file_handle, indent=2)
    return output_path


def build_payload(timestamp_iso: str, metadata: Dict[str, object], detections: List[Dict[str, object]]) -> Dict[str, object]:
    return {
        "type": "MesocycloneDetectionCollection",
        "source": "Mesocyclone",
        "timestamp": timestamp_iso,
        "metadata": metadata,
        "detections": detections,
    }
