from datetime import datetime, timedelta, timezone

from EdgeWARN.ctam.modules.Mesocyclone.track import MesocycloneTracker


def test_tracker_reuses_id_for_nearby_detection():
    tracker = MesocycloneTracker()
    t0 = datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc)
    t1 = t0 + timedelta(minutes=5)
    first = tracker.update([
        {"low": {"centroid_lat": 35.0, "centroid_lon": -97.0}, "mid": None, "depth_flag": "shallow"}
    ], t0)
    second = tracker.update([
        {"low": {"centroid_lat": 35.02, "centroid_lon": -97.01}, "mid": None, "depth_flag": "shallow"}
    ], t1)

    assert first[0]["id"] == second[0]["id"]


def test_tracker_expires_old_tracks():
    tracker = MesocycloneTracker()
    t0 = datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc)
    t1 = t0 + timedelta(minutes=11)
    first = tracker.update([
        {"low": {"centroid_lat": 35.0, "centroid_lon": -97.0}, "mid": None, "depth_flag": "shallow"}
    ], t0)
    second = tracker.update([
        {"low": {"centroid_lat": 35.0, "centroid_lon": -97.0}, "mid": None, "depth_flag": "shallow"}
    ], t1)

    assert first[0]["id"] != second[0]["id"]
