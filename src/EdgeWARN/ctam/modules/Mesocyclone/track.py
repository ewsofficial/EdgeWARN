import math
from datetime import datetime
from typing import Dict, List

from scipy.optimize import linear_sum_assignment

from . import config as cfg


def _distance_km(lat_a: float, lon_a: float, lat_b: float, lon_b: float) -> float:
    radius_km = 6371.0
    lat1 = math.radians(lat_a)
    lat2 = math.radians(lat_b)
    dlat = lat2 - lat1
    dlon = math.radians(lon_b - lon_a)
    hav = math.sin(dlat / 2.0) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2.0) ** 2
    return 2.0 * radius_km * math.asin(min(1.0, math.sqrt(max(hav, 0.0))))


def _motion_vector(prev_track: Dict[str, object], lat: float, lon: float, timestamp: datetime) -> Dict[str, float]:
    prev_time = prev_track["time"]
    delta_seconds = max((timestamp - prev_time).total_seconds(), 1.0)
    delta_lat_km = (lat - float(prev_track["lat"])) * 111.32
    delta_lon_km = (lon - float(prev_track["lon"])) * 111.32 * math.cos(math.radians(lat))
    return {
        "u": round((delta_lon_km * 1000.0) / delta_seconds, 3),
        "v": round((delta_lat_km * 1000.0) / delta_seconds, 3),
    }


class MesocycloneTracker:
    def __init__(self):
        self._tracks: Dict[int, Dict[str, object]] = {}
        self._next_track_id = 1

    def _expire(self, timestamp: datetime) -> None:
        expired_ids = []
        for track_id, track in self._tracks.items():
            if timestamp - track["time"] > cfg.TRACK_MEMORY:
                expired_ids.append(track_id)
        for track_id in expired_ids:
            del self._tracks[track_id]

    def update(self, detections: List[Dict[str, object]], timestamp: datetime) -> List[Dict[str, object]]:
        self._expire(timestamp)
        if not detections:
            return []

        track_ids = list(self._tracks.keys())
        unmatched_detection_indices = set(range(len(detections)))
        assignments = {}

        if track_ids:
            cost_matrix = []
            for track_id in track_ids:
                row = []
                track = self._tracks[track_id]
                for detection in detections:
                    det_lat = self._detection_lat(detection)
                    det_lon = self._detection_lon(detection)
                    distance_km = _distance_km(float(track["lat"]), float(track["lon"]), det_lat, det_lon)
                    row.append(distance_km)
                cost_matrix.append(row)

            row_ind, col_ind = linear_sum_assignment(cost_matrix)
            for row_idx, col_idx in zip(row_ind.tolist(), col_ind.tolist()):
                distance_km = cost_matrix[row_idx][col_idx]
                if distance_km > cfg.TRACK_MATCH_DISTANCE_KM:
                    continue
                assignments[track_ids[row_idx]] = col_idx
                unmatched_detection_indices.discard(col_idx)

        results = []
        for track_id, detection_index in assignments.items():
            detection = dict(detections[detection_index])
            det_lat = self._detection_lat(detection)
            det_lon = self._detection_lon(detection)
            track = self._tracks[track_id]
            motion_vector = _motion_vector(track, det_lat, det_lon, timestamp)
            history = list(track.get("history", []))
            history.append({"time": track["time"].isoformat(), "lat": track["lat"], "lon": track["lon"]})
            self._tracks[track_id] = {
                "lat": det_lat,
                "lon": det_lon,
                "time": timestamp,
                "history": history[-10:],
                "motion_vector": motion_vector,
            }
            detection.update({"id": track_id, "motion_vector": motion_vector, "track_history_length": len(history)})
            results.append(detection)

        for detection_index in sorted(unmatched_detection_indices):
            detection = dict(detections[detection_index])
            track_id = self._next_track_id
            self._next_track_id += 1
            det_lat = self._detection_lat(detection)
            det_lon = self._detection_lon(detection)
            motion_vector = {"u": 0.0, "v": 0.0}
            self._tracks[track_id] = {
                "lat": det_lat,
                "lon": det_lon,
                "time": timestamp,
                "history": [],
                "motion_vector": motion_vector,
            }
            detection.update({"id": track_id, "motion_vector": motion_vector, "track_history_length": 0})
            results.append(detection)

        results.sort(key=lambda item: int(item["id"]))
        return results

    @staticmethod
    def _detection_lat(detection: Dict[str, object]) -> float:
        if detection.get("low") is not None:
            return float(detection["low"]["centroid_lat"])
        return float(detection["mid"]["centroid_lat"])

    @staticmethod
    def _detection_lon(detection: Dict[str, object]) -> float:
        if detection.get("low") is not None:
            return float(detection["low"]["centroid_lon"])
        return float(detection["mid"]["centroid_lon"])
