import asyncio
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import AsyncMock, patch

from common.pipeline import coordinator
from common.ingest.mrms.downloader import DownloadBatchResult
from common.ingest.manifest import staged_input_from_path


def _run_cycle(logs, **kwargs):
    return asyncio.run(
        coordinator.run_tandem_ingest_cycle(
            datetime(2026, 4, 27, 13, 0, tzinfo=timezone.utc),
            logs.append,
            include_goes=False,
            **kwargs,
        )
    )


def _record(tmp_path, product, timestamp):
    path = tmp_path / f"MRMS_{product}_{timestamp:%Y%m%d-%H%M%S}.grib2"
    path.write_bytes(b"data")
    return staged_input_from_path(
        product,
        path,
        source="test",
        family="mrms",
    )


def _complete_batch(tmp_path, product, timestamp):
    return DownloadBatchResult(
        attempted=(product,),
        downloaded=(_record(tmp_path, product, timestamp),),
        failed=(),
    )


def test_tandem_cycle_runs_rap_uint16_conversion_when_rap_ingest_succeeds(tmp_path):
    logs = []
    rap_file = tmp_path / "RAP.20260427-13z.awp130pgrbf00.grib2"
    rap_file.write_bytes(b"grib")

    timestamp = datetime(2026, 4, 27, 13, 0, tzinfo=timezone.utc)
    with patch.object(coordinator.mrms_ingest, "download_detection_files_async", new=AsyncMock(return_value=_complete_batch(tmp_path, "Detection", timestamp))), \
         patch.object(coordinator.mrms_ingest, "download_integration_files_async", new=AsyncMock(return_value=_complete_batch(tmp_path, "Integration", timestamp))), \
         patch.object(coordinator, "download_rap_async", new=AsyncMock(return_value=rap_file)), \
         patch("EWMRS.pipeline.run_rap_uint16_pipeline", return_value={"RAP_TestLayer": Path("data.u16")}) as mock_convert:
        state = _run_cycle(logs)

    mock_convert.assert_called_once_with(rap_file, datetime(2026, 4, 27, 13, 0, tzinfo=timezone.utc))
    assert state.input_manifest.latest_for_product("RAP").local_path == rap_file
    assert "rap_ingest" not in state.errors
    assert "ewmrs_rap_uint16" not in state.errors


def test_tandem_cycle_skips_rap_uint16_conversion_when_rap_ingest_fails(tmp_path):
    logs = []
    rap_ingest = AsyncMock(
        side_effect=RuntimeError(
            "RAP unavailable within 180-minute analysis-age limit; "
            "checked: rap.20260726/rap.t13z=not_found"
        )
    )

    timestamp = datetime(2026, 4, 27, 13, 0, tzinfo=timezone.utc)
    with patch.object(coordinator.mrms_ingest, "download_detection_files_async", new=AsyncMock(return_value=_complete_batch(tmp_path, "Detection", timestamp))), \
         patch.object(coordinator.mrms_ingest, "download_integration_files_async", new=AsyncMock(return_value=_complete_batch(tmp_path, "Integration", timestamp))), \
         patch.object(coordinator, "download_rap_async", new=rap_ingest), \
         patch("EWMRS.pipeline.run_rap_uint16_pipeline") as mock_convert:
        state = _run_cycle(logs)

    mock_convert.assert_not_called()
    rap_ingest.assert_awaited_once()
    assert "180-minute analysis-age limit" in state.errors["rap_ingest"]
    assert sum("RAP ingestion failed" in message for message in logs) == 1


def test_tandem_cycle_keeps_rap_ready_when_rap_uint16_conversion_fails(tmp_path):
    logs = []
    rap_file = tmp_path / "RAP.20260427-13z.awp130pgrbf00.grib2"
    rap_file.write_bytes(b"grib")

    timestamp = datetime(2026, 4, 27, 13, 0, tzinfo=timezone.utc)
    with patch.object(coordinator.mrms_ingest, "download_detection_files_async", new=AsyncMock(return_value=_complete_batch(tmp_path, "Detection", timestamp))), \
         patch.object(coordinator.mrms_ingest, "download_integration_files_async", new=AsyncMock(return_value=_complete_batch(tmp_path, "Integration", timestamp))), \
         patch.object(coordinator, "download_rap_async", new=AsyncMock(return_value=rap_file)), \
         patch("EWMRS.pipeline.run_rap_uint16_pipeline", side_effect=RuntimeError("conversion failed")):
        state = _run_cycle(logs)

    assert "rap_ingest" not in state.errors
    assert state.errors["ewmrs_rap_uint16"] == "EWMRS RAP Uint16Array conversion failed"


def test_rap_uint16_conversion_requires_at_least_one_complete_layer(
    monkeypatch,
):
    monkeypatch.setattr(
        "EWMRS.pipeline.run_rap_uint16_pipeline",
        lambda *_args, **_kwargs: {},
    )

    result = asyncio.run(
        coordinator._run_rap_uint16_conversion(
            Path("rap.grib2"),
            datetime(2026, 4, 27, 13, 0, tzinfo=timezone.utc),
            lambda _message: None,
        )
    )

    assert result is False


def test_tandem_cycle_skips_rap_uint16_conversion_when_ewmrs_disabled(tmp_path):
    logs = []
    rap_file = tmp_path / "RAP.20260427-13z.awp130pgrbf00.grib2"
    rap_file.write_bytes(b"grib")

    timestamp = datetime(2026, 4, 27, 13, 0, tzinfo=timezone.utc)
    with patch.object(coordinator.mrms_ingest, "download_detection_files_async", new=AsyncMock(return_value=_complete_batch(tmp_path, "Detection", timestamp))), \
         patch.object(coordinator.mrms_ingest, "download_integration_files_async", new=AsyncMock(return_value=_complete_batch(tmp_path, "Integration", timestamp))), \
         patch.object(coordinator, "download_rap_async", new=AsyncMock(return_value=rap_file)), \
         patch("EWMRS.pipeline.run_rap_uint16_pipeline") as mock_convert:
        state = _run_cycle(logs, include_ewmrs=False)

    mock_convert.assert_not_called()
    assert "rap_ingest" not in state.errors
    assert "ewmrs_rap_uint16" not in state.errors
    assert state.ewmrs_mrms_inputs_ready is False
    assert state.ewmrs_goes_inputs_ready is False


def test_tandem_cycle_rejects_partial_detection_batch(tmp_path):
    logs = []
    rap_file = tmp_path / "RAP.20260427-13z.awp130pgrbf00.grib2"
    rap_file.write_bytes(b"grib")
    partial = DownloadBatchResult(
        attempted=("Composite", "PrecipFlag"),
        downloaded=(
            _record(
                tmp_path,
                "Composite",
                datetime(2026, 4, 27, 13, 0, tzinfo=timezone.utc),
            ),
        ),
        failed=("PrecipFlag",),
    )
    complete = DownloadBatchResult(
        attempted=("EchoTop",),
        downloaded=(
            _record(
                tmp_path,
                "EchoTop",
                datetime(2026, 4, 27, 13, 0, tzinfo=timezone.utc),
            ),
        ),
        failed=(),
    )

    with patch.object(coordinator.mrms_ingest, "download_detection_files_async", new=AsyncMock(return_value=partial)), \
         patch.object(coordinator.mrms_ingest, "download_detection_files", return_value=partial), \
         patch.object(coordinator.mrms_ingest, "download_integration_files_async", new=AsyncMock(return_value=complete)), \
         patch.object(coordinator, "download_rap_async", new=AsyncMock(return_value=rap_file)), \
         patch("EWMRS.pipeline.run_rap_uint16_pipeline", return_value={}):
        state = _run_cycle(logs)

    assert state.detection_inputs_ready is False
    assert state.errors["detection_ingest"] == "Detection inputs unavailable"
    assert state.ewmrs_mrms_inputs_ready is False
