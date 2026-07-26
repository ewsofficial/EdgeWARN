from datetime import datetime, timezone
from pathlib import Path

import pytest

import common.ingest.synoptic.downloader as synoptic_downloader


DT = datetime(2026, 7, 26, 13, 6, tzinfo=timezone.utc)
FILE_PATTERN = "rap.t{hour:02d}z.awp130pgrbf00.grib2"
DIR_PATTERN = "rap.{date}"


async def _download(tmp_path, **kwargs):
    return await synoptic_downloader.download_synoptic(
        DT,
        "bucket-name",
        FILE_PATTERN,
        DIR_PATTERN,
        Path(tmp_path),
        dataset_name="RAP",
        max_age_minutes=kwargs.pop("max_age_minutes", 180),
        **kwargs,
    )


@pytest.mark.asyncio
async def test_current_and_previous_missing_selects_second_previous(
    monkeypatch, mock_io_manager, tmp_path
):
    async_calls = []

    async def fake_async(current_dt, *_args):
        async_calls.append(current_dt)
        if current_dt.hour != 11:
            raise FileNotFoundError("missing")
        _, local_path = synoptic_downloader._build_synoptic_s3_params(
            current_dt, FILE_PATTERN, DIR_PATTERN, tmp_path
        )
        local_path.write_bytes(b"grib")
        return local_path

    def fake_sync(*_args):
        raise AssertionError("definitive async 404 must not receive a sync retry")

    monkeypatch.setattr(synoptic_downloader, "io_manager", mock_io_manager)
    monkeypatch.setattr(synoptic_downloader, "download_synoptic_async", fake_async)
    monkeypatch.setattr(synoptic_downloader, "download_synoptic_sync", fake_sync)

    result = await _download(tmp_path)

    assert result.name == "RAP.20260726-11z.awp130pgrbf00.grib2"
    assert [candidate.hour for candidate in async_calls] == [13, 12, 11]
    selection_log = mock_io_manager.write_info.call_args_list[-1].args[0]
    assert "analysis=2026-07-26T11:00:00+00:00" in selection_log
    assert "age_minutes=126" in selection_log


@pytest.mark.asyncio
async def test_valid_local_fallback_avoids_network(
    monkeypatch, mock_io_manager, tmp_path
):
    local_path = tmp_path / "RAP.20260726-12z.awp130pgrbf00.grib2"
    local_path.write_bytes(b"cached-grib")
    async_calls = []

    async def fake_async(current_dt, *_args):
        async_calls.append(current_dt)
        raise FileNotFoundError("missing")

    monkeypatch.setattr(synoptic_downloader, "io_manager", mock_io_manager)
    monkeypatch.setattr(synoptic_downloader, "download_synoptic_async", fake_async)
    monkeypatch.setattr(
        synoptic_downloader,
        "download_synoptic_sync",
        lambda *_args: pytest.fail("sync should not run"),
    )

    result = await _download(tmp_path)

    assert result == local_path
    assert [candidate.hour for candidate in async_calls] == [13]
    assert "source=local" in mock_io_manager.write_info.call_args_list[-1].args[0]


@pytest.mark.asyncio
async def test_invalid_local_file_proceeds_to_remote(
    monkeypatch, mock_io_manager, tmp_path
):
    local_path = tmp_path / "RAP.20260726-13z.awp130pgrbf00.grib2"
    local_path.write_bytes(b"")

    async def fake_async(current_dt, *_args):
        assert current_dt.hour == 13
        local_path.write_bytes(b"downloaded")
        return local_path

    monkeypatch.setattr(synoptic_downloader, "io_manager", mock_io_manager)
    monkeypatch.setattr(synoptic_downloader, "download_synoptic_async", fake_async)

    result = await _download(tmp_path)

    assert result == local_path
    assert local_path.read_bytes() == b"downloaded"
    assert "Ignoring invalid local RAP file" in (
        mock_io_manager.write_warning.call_args_list[0].args[0]
    )


@pytest.mark.asyncio
async def test_exhausted_404_search_attempts_each_key_once(
    monkeypatch, mock_io_manager, tmp_path
):
    keys = []

    async def fake_async(current_dt, *_args):
        key, _ = synoptic_downloader._build_synoptic_s3_params(
            current_dt, FILE_PATTERN, DIR_PATTERN, tmp_path
        )
        keys.append(key)
        raise FileNotFoundError(key)

    monkeypatch.setattr(synoptic_downloader, "io_manager", mock_io_manager)
    monkeypatch.setattr(synoptic_downloader, "download_synoptic_async", fake_async)
    monkeypatch.setattr(
        synoptic_downloader,
        "download_synoptic_sync",
        lambda *_args: pytest.fail("sync should not run after 404"),
    )

    with pytest.raises(synoptic_downloader.SynopticUnavailableError) as exc_info:
        await _download(tmp_path)

    assert keys == [
        "rap.20260726/rap.t13z.awp130pgrbf00.grib2",
        "rap.20260726/rap.t12z.awp130pgrbf00.grib2",
        "rap.20260726/rap.t11z.awp130pgrbf00.grib2",
    ]
    assert all(attempt.failure == "not_found" for attempt in exc_info.value.attempts)
    assert "180-minute analysis-age limit" in str(exc_info.value)


@pytest.mark.asyncio
async def test_async_transport_failure_uses_sync_once(
    monkeypatch, mock_io_manager, tmp_path
):
    sync_calls = []

    async def fake_async(*_args):
        raise RuntimeError("connection reset")

    def fake_sync(current_dt, *_args):
        sync_calls.append(current_dt)
        _, local_path = synoptic_downloader._build_synoptic_s3_params(
            current_dt, FILE_PATTERN, DIR_PATTERN, tmp_path
        )
        local_path.write_bytes(b"grib")
        return local_path

    monkeypatch.setattr(synoptic_downloader, "io_manager", mock_io_manager)
    monkeypatch.setattr(synoptic_downloader, "download_synoptic_async", fake_async)
    monkeypatch.setattr(synoptic_downloader, "download_synoptic_sync", fake_sync)

    result = await _download(tmp_path)

    assert result.name == "RAP.20260726-13z.awp130pgrbf00.grib2"
    assert len(sync_calls) == 1
    assert "source=s3_sync" in mock_io_manager.write_info.call_args_list[-1].args[0]


@pytest.mark.asyncio
async def test_authentication_failure_is_distinguished(
    monkeypatch, mock_io_manager, tmp_path
):
    async def fake_async(*_args):
        raise RuntimeError("AccessDenied: invalid credential signature")

    def fake_sync(*_args):
        raise RuntimeError("Forbidden")

    monkeypatch.setattr(synoptic_downloader, "io_manager", mock_io_manager)
    monkeypatch.setattr(synoptic_downloader, "download_synoptic_async", fake_async)
    monkeypatch.setattr(synoptic_downloader, "download_synoptic_sync", fake_sync)

    with pytest.raises(synoptic_downloader.SynopticUnavailableError) as exc_info:
        await _download(tmp_path, max_age_minutes=6)

    assert len(exc_info.value.attempts) == 1
    assert exc_info.value.attempts[0].failure == "authentication"
    assert "=authentication" in str(exc_info.value)


def test_candidate_age_boundary_is_inclusive():
    dt = datetime(2026, 7, 26, 13, 6, tzinfo=timezone.utc)

    within_126 = list(synoptic_downloader._eligible_analysis_times(dt, 126))
    within_125 = list(synoptic_downloader._eligible_analysis_times(dt, 125))

    assert [candidate.hour for candidate in within_126] == [13, 12, 11]
    assert [candidate.hour for candidate in within_125] == [13, 12]


@pytest.mark.asyncio
async def test_fallback_builds_correct_month_rollover_key(
    monkeypatch, mock_io_manager, tmp_path
):
    dt = datetime(2026, 3, 1, 0, 5, tzinfo=timezone.utc)
    keys = []

    async def fake_async(current_dt, *_args):
        key, local_path = synoptic_downloader._build_synoptic_s3_params(
            current_dt, FILE_PATTERN, DIR_PATTERN, tmp_path
        )
        keys.append(key)
        if current_dt.hour == 23:
            local_path.write_bytes(b"grib")
            return local_path
        raise FileNotFoundError(key)

    monkeypatch.setattr(synoptic_downloader, "io_manager", mock_io_manager)
    monkeypatch.setattr(synoptic_downloader, "download_synoptic_async", fake_async)

    result = await synoptic_downloader.download_synoptic(
        dt,
        "bucket-name",
        FILE_PATTERN,
        DIR_PATTERN,
        tmp_path,
        dataset_name="RAP",
        max_age_minutes=180,
    )

    assert result.name == "RAP.20260228-23z.awp130pgrbf00.grib2"
    assert keys == [
        "rap.20260301/rap.t00z.awp130pgrbf00.grib2",
        "rap.20260228/rap.t23z.awp130pgrbf00.grib2",
    ]


def test_naive_and_non_utc_times_normalize_to_utc():
    naive = datetime(2026, 7, 26, 13, 6)
    offset = datetime.fromisoformat("2026-07-26T09:06:00-04:00")

    assert synoptic_downloader._as_utc(naive) == DT
    assert synoptic_downloader._as_utc(offset) == DT
