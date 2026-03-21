import os
from datetime import datetime, timedelta

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
