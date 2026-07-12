from datetime import datetime, timezone
from pathlib import Path

import pytest

import common.ingest.synoptic.downloader as synoptic_downloader


@pytest.mark.asyncio
async def test_download_synoptic_logs_one_attempt_per_url(monkeypatch, mock_io_manager, tmp_path):
    dt = datetime(2026, 7, 8, 12, 0, tzinfo=timezone.utc)

    async def fake_async(*_args, **_kwargs):
        return None

    sync_calls = {"count": 0}

    def fake_sync(*_args, **_kwargs):
        sync_calls["count"] += 1
        if sync_calls["count"] == 1:
            return None
        return tmp_path / "RAP.20260708-11z.awp130pgrbf00.grib2"

    monkeypatch.setattr(synoptic_downloader, "io_manager", mock_io_manager)
    monkeypatch.setattr(synoptic_downloader, "download_synoptic_async", fake_async)
    monkeypatch.setattr(synoptic_downloader, "download_synoptic_sync", fake_sync)

    result = await synoptic_downloader.download_synoptic(
        dt,
        "bucket-name",
        "rap.{hour:02d}.grib2",
        "rap/{date}",
        Path(tmp_path),
        dataset_name="RAP",
    )

    assert result == tmp_path / "RAP.20260708-11z.awp130pgrbf00.grib2"
    assert mock_io_manager.write_info.call_args_list[0].args[0] == (
        "Attempting RAP download: s3://bucket-name/rap/20260708/rap.12.grib2"
    )
    assert mock_io_manager.write_info.call_args_list[1].args[0] == (
        "Attempting RAP fallback to previous hour: 2026-07-08 11:00:00+00:00 "
        "(s3://bucket-name/rap/20260708/rap.11.grib2)"
    )


@pytest.mark.asyncio
async def test_download_synoptic_logs_single_404_before_hour_fallback(monkeypatch, mock_io_manager, tmp_path):
    dt = datetime(2026, 7, 8, 12, 0, tzinfo=timezone.utc)

    async def fake_async(*_args, **_kwargs):
        raise FileNotFoundError("s3://bucket-name/rap/20260708/rap.12.grib2")

    def fake_sync(*_args, **_kwargs):
        raise AssertionError("sync fallback should not run after async 404")

    monkeypatch.setattr(synoptic_downloader, "io_manager", mock_io_manager)
    monkeypatch.setattr(synoptic_downloader, "download_synoptic_async", fake_async)
    monkeypatch.setattr(synoptic_downloader, "download_synoptic_sync", fake_sync)

    result = await synoptic_downloader.download_synoptic(
        dt,
        "bucket-name",
        "rap.{hour:02d}.grib2",
        "rap/{date}",
        Path(tmp_path),
        dataset_name="RAP",
    )

    assert result is None
    assert mock_io_manager.write_warning.call_args_list[0].args[0] == (
        "Synoptic file not found on S3 (404): s3://bucket-name/rap/20260708/rap.12.grib2"
    )
