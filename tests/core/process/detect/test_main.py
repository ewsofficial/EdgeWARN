import os
import json
from datetime import datetime, timedelta
from unittest.mock import patch

import util.file as fs
from EdgeWARN.process.detect import main as detect_main


def test_detect_main_preserves_stormcell_dir_when_cleanup_disabled(tmp_path):
    fs._define_paths(tmp_path)
    stormcell_file = fs.STORMCELL_DIR / "stormcells_20240101-120000.json"
    fs.STORMCELL_DIR.mkdir(parents=True, exist_ok=True)
    stormcell_file.write_text("{}")

    old_time = datetime.now() - timedelta(hours=3)
    os.utime(stormcell_file, (old_time.timestamp(), old_time.timestamp()))

    detect_main.main(
        None,
        None,
        None,
        None,
        None,
        None,
        (20, 55),
        (-130, -60),
        tmp_path / "stormcell_test.json",
        cleanup_stormcells=False,
    )

    assert stormcell_file.exists()


def test_detect_main_cleans_stormcell_dir_when_cleanup_enabled(tmp_path):
    fs._define_paths(tmp_path)
    stormcell_file = fs.STORMCELL_DIR / "stormcells_20240101-120000.json"
    fs.STORMCELL_DIR.mkdir(parents=True, exist_ok=True)
    stormcell_file.write_text("{}")

    old_time = datetime.now() - timedelta(hours=3)
    os.utime(stormcell_file, (old_time.timestamp(), old_time.timestamp()))

    detect_main.main(
        None,
        None,
        None,
        None,
        None,
        None,
        (20, 55),
        (-130, -60),
        tmp_path / "stormcell_test.json",
        cleanup_stormcells=True,
    )

    assert not stormcell_file.exists()


def test_detect_main_single_frame_generates_vectors_from_history(tmp_path):
    fs._define_paths(tmp_path)
    fs.CELL_DIR.mkdir(parents=True, exist_ok=True)

    radar_file = tmp_path / "radar.grib2"
    radar_file.write_text("stub")

    history_path = fs.CELL_DIR / "1.json"
    history_path.write_text(json.dumps([
        {
            "timestamp": "2024-01-01T11:55:00",
            "centroid": [35.0, -97.0],
        }
    ]))

    with patch.object(detect_main.DetectionDataHandler, "find_timestamp", return_value="2024-01-01T12:00:00"), \
         patch.object(detect_main, "_detect_with_optional_probsevere", return_value=([
             {"id": 1, "centroid": [35.1, -96.9]}
         ], None)), \
         patch.object(detect_main, "match_alerts_to_cells", side_effect=lambda entries, *_args, **_kwargs: entries):
        output_file, _ = detect_main.main(
            radar_file,
            None,
            None,
            None,
            None,
            None,
            (20, 55),
            (-130, -60),
            tmp_path / "stormcell_test.json",
            cleanup_stormcells=False,
        )

    saved = json.loads(output_file.read_text())
    feature = saved["features"][0]

    assert feature["timestamp"] == "2024-01-01T12:00:00"
    assert feature["dt"] == 300.0
    assert "dx" in feature
    assert "dy" in feature
