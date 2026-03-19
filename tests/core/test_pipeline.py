from unittest.mock import patch

from EdgeWARN import pipeline


def test_historical_cleanup_skips_cells_and_stormcells(tmp_path):
    data_dir = tmp_path / "data"
    rap_dir = data_dir / "RAP"
    composite_dir = data_dir / "CompRefQC"
    cell_dir = data_dir / "cells"
    stormcell_dir = data_dir / "stormcells"

    for directory in (rap_dir, composite_dir, cell_dir, stormcell_dir):
        directory.mkdir(parents=True)

    with patch.object(pipeline.fs, "CELL_DIR", cell_dir), \
         patch.object(pipeline.fs, "STORMCELL_DIR", stormcell_dir), \
         patch.object(pipeline.fs, "RAP_DIR", rap_dir), \
         patch.object(
             pipeline,
             "get_output_dirs",
             return_value=[composite_dir, cell_dir, stormcell_dir, composite_dir],
         ), \
         patch.object(pipeline.fs, "clean_old_files") as mock_clean:
        pipeline._cleanup_historical_data_dirs(pipeline.IOManager("[TestPipeline]"))

    cleaned_dirs = [call.args[0] for call in mock_clean.call_args_list]
    cleaned_max_files = [call.kwargs["max_files"] for call in mock_clean.call_args_list]

    assert composite_dir in cleaned_dirs
    assert rap_dir in cleaned_dirs
    assert cell_dir not in cleaned_dirs
    assert stormcell_dir not in cleaned_dirs
    assert cleaned_dirs.count(composite_dir) == 1
    assert all(max_files == 5 for max_files in cleaned_max_files)
