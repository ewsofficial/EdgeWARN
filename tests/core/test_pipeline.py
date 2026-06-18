from unittest.mock import patch
from types import SimpleNamespace

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


def test_historical_pipeline_preserves_cell_and_stormcell_dirs():
    with patch.object(pipeline, "_cleanup_historical_data_dirs"), \
         patch.object(
             pipeline,
             "run_tandem_ingest_cycle",
             return_value=SimpleNamespace(detection_inputs_ready=True, errors={}),
         ) as mock_ingest, \
         patch.object(pipeline, "run_edgewarn_detection_phase", return_value="generated.json") as mock_detect, \
         patch.object(pipeline, "run_edgewarn_integration_phase", return_value=True) as mock_integrate:
        generated_file, _ = pipeline.historical_pipeline(
            dt=pipeline.datetime(2024, 1, 1, 12, 0, tzinfo=pipeline.timezone.utc),
            lat_limits=(20, 55),
            lon_limits=(-130, -60),
            json_output="stormcell_test.json",
        )

    assert generated_file == "generated.json"
    assert mock_ingest.call_args.kwargs["include_goes"] is False
    assert mock_ingest.call_args.kwargs["include_ewmrs"] is False
    assert mock_detect.call_args.kwargs["cleanup_stormcells"] is False
    assert mock_integrate.call_args.kwargs["remove_old_cells"] is False


def test_historical_pipeline_skips_integration_when_realtime_staged_inputs_are_incomplete():
    with patch.object(pipeline, "_cleanup_historical_data_dirs"), \
         patch.object(
             pipeline,
             "run_tandem_ingest_cycle",
             return_value=SimpleNamespace(
                 detection_inputs_ready=True,
                 errors={"rap_ingest": "RAP inputs unavailable"},
             ),
         ), \
         patch.object(pipeline, "run_edgewarn_detection_phase", return_value="generated.json"), \
         patch.object(pipeline, "run_edgewarn_integration_phase") as mock_integrate:
        generated_file, _ = pipeline.historical_pipeline(
            dt=pipeline.datetime(2024, 1, 1, 12, 0, tzinfo=pipeline.timezone.utc),
            lat_limits=(20, 55),
            lon_limits=(-130, -60),
            json_output="stormcell_test.json",
        )

    assert generated_file == "generated.json"
    mock_integrate.assert_not_called()
