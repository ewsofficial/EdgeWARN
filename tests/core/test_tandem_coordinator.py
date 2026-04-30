import asyncio
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import AsyncMock, patch

from common.pipeline import coordinator


def _run_cycle(logs, **kwargs):
    return asyncio.run(
        coordinator.run_tandem_ingest_cycle(
            datetime(2026, 4, 27, 13, 0, tzinfo=timezone.utc),
            logs.append,
            include_goes=False,
            **kwargs,
        )
    )


def test_tandem_cycle_runs_rap_uint16_conversion_when_rap_ingest_succeeds(tmp_path):
    logs = []
    rap_file = tmp_path / "RAP.20260427-13z.awp130pgrbf00.grib2"
    rap_file.write_bytes(b"grib")

    with patch.object(coordinator.mrms_ingest, "download_detection_files_async", new=AsyncMock(return_value=None)), \
         patch.object(coordinator.mrms_ingest, "download_integration_files_async", new=AsyncMock(return_value=None)), \
         patch.object(coordinator, "download_rap_async", new=AsyncMock(return_value=rap_file)), \
         patch("EWMRS.pipeline.run_rap_uint16_pipeline", return_value={"RAP_TestLayer": Path("data.u16")}) as mock_convert:
        state = _run_cycle(logs)

    mock_convert.assert_called_once_with(rap_file, datetime(2026, 4, 27, 13, 0, tzinfo=timezone.utc))
    assert "rap_ingest" not in state.errors
    assert "ewmrs_rap_uint16" not in state.errors


def test_tandem_cycle_skips_rap_uint16_conversion_when_rap_ingest_fails():
    logs = []

    with patch.object(coordinator.mrms_ingest, "download_detection_files_async", new=AsyncMock(return_value=None)), \
         patch.object(coordinator.mrms_ingest, "download_integration_files_async", new=AsyncMock(return_value=None)), \
         patch.object(coordinator, "download_rap_async", new=AsyncMock(return_value=None)), \
         patch.object(coordinator, "download_rap", return_value=None), \
         patch("EWMRS.pipeline.run_rap_uint16_pipeline") as mock_convert:
        state = _run_cycle(logs)

    mock_convert.assert_not_called()
    assert state.errors["rap_ingest"] == "RAP inputs unavailable"


def test_tandem_cycle_keeps_rap_ready_when_rap_uint16_conversion_fails(tmp_path):
    logs = []
    rap_file = tmp_path / "RAP.20260427-13z.awp130pgrbf00.grib2"
    rap_file.write_bytes(b"grib")

    with patch.object(coordinator.mrms_ingest, "download_detection_files_async", new=AsyncMock(return_value=None)), \
         patch.object(coordinator.mrms_ingest, "download_integration_files_async", new=AsyncMock(return_value=None)), \
         patch.object(coordinator, "download_rap_async", new=AsyncMock(return_value=rap_file)), \
         patch("EWMRS.pipeline.run_rap_uint16_pipeline", side_effect=RuntimeError("conversion failed")):
        state = _run_cycle(logs)

    assert "rap_ingest" not in state.errors
    assert state.errors["ewmrs_rap_uint16"] == "EWMRS RAP Uint16Array conversion failed"


def test_tandem_cycle_skips_rap_uint16_conversion_when_ewmrs_disabled(tmp_path):
    logs = []
    rap_file = tmp_path / "RAP.20260427-13z.awp130pgrbf00.grib2"
    rap_file.write_bytes(b"grib")

    with patch.object(coordinator.mrms_ingest, "download_detection_files_async", new=AsyncMock(return_value=None)), \
         patch.object(coordinator.mrms_ingest, "download_integration_files_async", new=AsyncMock(return_value=None)), \
         patch.object(coordinator, "download_rap_async", new=AsyncMock(return_value=rap_file)), \
         patch("EWMRS.pipeline.run_rap_uint16_pipeline") as mock_convert:
        state = _run_cycle(logs, include_ewmrs=False)

    mock_convert.assert_not_called()
    assert "rap_ingest" not in state.errors
    assert "ewmrs_rap_uint16" not in state.errors
    assert state.ewmrs_mrms_inputs_ready is False
    assert state.ewmrs_goes_inputs_ready is False
