from pathlib import Path

import pytest

import util.file as fs
from common.ingest.nexrad.coordinator import NexradScanCoordinator
from common.ingest.nexrad.main import chunk_output_dir
from common.ingest.nexrad.models import ChunkKey, NexradIngestResult, RadarStationVcp


def _station(site="KTLH", *, vcp=212, latest_scan_time="2026-05-07T15:00:00Z"):
    return RadarStationVcp(
        site=site,
        vcp=vcp,
        raw_vcp=f"R{vcp}" if vcp is not None else None,
        rda_timestamp="2026-05-07T14:59:00Z",
        level_two_last_received_time=latest_scan_time,
        properties={},
    )


def _chunks(site="KTLH", volume_id="999", stamp="20260507-150000", last_number=25):
    chunks = []
    for number in range(1, last_number + 1):
        chunk_type = "S" if number == 1 else "I"
        chunks.append(
            ChunkKey(
                site=site,
                volume_id=volume_id,
                chunk_number=number,
                chunk_type=chunk_type,
                key=f"{site}/{volume_id}/{stamp}-{number:03d}-{chunk_type}",
            )
        )
    return chunks


@pytest.mark.asyncio
async def test_coordinator_downloads_matching_latest_scan_and_fetches_station_catalog_once(tmp_path):
    fs.initialize_filesystem(tmp_path)
    station_calls = []
    ingest_calls = []
    station = _station()
    remote_chunks = _chunks()

    def _station_fetcher(*, session=None):
        station_calls.append(session)
        return {"KTLH": station}

    async def _volume_lister(site, limit=1, **_kwargs):
        assert site == "KTLH"
        assert limit == 3
        return ["999"]

    async def _chunk_lister(_site, _volume_id, **_kwargs):
        return remote_chunks

    async def _ingest_trigger(site, volume_id, **kwargs):
        ingest_calls.append((site, volume_id, kwargs.get("station_vcp")))
        return NexradIngestResult(site=site, volume_id=volume_id, vcp=212, dynamic_scan_type=None, low_path=None, high_path=None, manifest_path=None, chunks_downloaded=25, complete=True)

    coordinator = NexradScanCoordinator(
        station_fetcher=_station_fetcher,
        async_volume_lister=_volume_lister,
        async_chunk_lister=_chunk_lister,
        async_ingest_trigger=_ingest_trigger,
    )

    results = await coordinator.ingest_latest_station_scans_async(base_dir=tmp_path)

    assert station_calls == [None]
    assert ingest_calls == [("KTLH", "999", station)]
    assert len(results) == 1
    assert results[0].action == "downloaded"
    assert results[0].chunks_downloaded == 25


@pytest.mark.asyncio
async def test_coordinator_skips_disallowed_vcp_without_listing_s3(tmp_path):
    fs.initialize_filesystem(tmp_path)
    listed = []

    async def _volume_lister(*_args, **_kwargs):
        listed.append(True)
        return []

    coordinator = NexradScanCoordinator(
        station_fetcher=lambda **_kwargs: {"KTLX": _station(site="KTLX", vcp=35)},
        async_volume_lister=_volume_lister,
    )

    results = await coordinator.ingest_latest_station_scans_async(base_dir=tmp_path)

    assert [result.action for result in results] == ["skipped_invalid_vcp"]
    assert listed == []


@pytest.mark.asyncio
async def test_coordinator_skips_missing_latest_scan_timestamp(tmp_path):
    fs.initialize_filesystem(tmp_path)
    listed = []

    async def _volume_lister(*_args, **_kwargs):
        listed.append(True)
        return []

    coordinator = NexradScanCoordinator(
        station_fetcher=lambda **_kwargs: {"KTLH": _station(latest_scan_time=None)},
        async_volume_lister=_volume_lister,
    )

    results = await coordinator.ingest_latest_station_scans_async(base_dir=tmp_path)

    assert [result.action for result in results] == ["skipped_missing_timestamp"]
    assert listed == []


@pytest.mark.asyncio
async def test_coordinator_skips_when_latest_scan_is_already_downloaded(tmp_path):
    fs.initialize_filesystem(tmp_path)
    remote_chunks = _chunks()
    outdir = chunk_output_dir("KTLH", "999", remote_chunks)
    outdir.mkdir(parents=True, exist_ok=True)
    for chunk in remote_chunks:
        (outdir / Path(chunk.key).name).write_bytes(b"x")
    ingest_calls = []

    async def _ingest_trigger(*_args, **_kwargs):
        ingest_calls.append(True)
        return None

    coordinator = NexradScanCoordinator(
        station_fetcher=lambda **_kwargs: {"KTLH": _station()},
        async_volume_lister=lambda *_args, **_kwargs: _return(["999"]),
        async_chunk_lister=lambda *_args, **_kwargs: _return(remote_chunks),
        async_ingest_trigger=_ingest_trigger,
    )

    results = await coordinator.ingest_latest_station_scans_async(base_dir=tmp_path)

    assert [result.action for result in results] == ["skipped_already_downloaded"]
    assert ingest_calls == []


@pytest.mark.asyncio
async def test_coordinator_skips_when_remote_low_chunks_are_incomplete(tmp_path):
    fs.initialize_filesystem(tmp_path)
    incomplete_chunks = _chunks(last_number=24)
    ingest_calls = []

    async def _ingest_trigger(*_args, **_kwargs):
        ingest_calls.append(True)
        return None

    coordinator = NexradScanCoordinator(
        station_fetcher=lambda **_kwargs: {"KTLH": _station()},
        async_volume_lister=lambda *_args, **_kwargs: _return(["999"]),
        async_chunk_lister=lambda *_args, **_kwargs: _return(incomplete_chunks),
        async_ingest_trigger=_ingest_trigger,
    )

    results = await coordinator.ingest_latest_station_scans_async(base_dir=tmp_path)

    assert [result.action for result in results] == ["skipped_incomplete_remote"]
    assert ingest_calls == []


@pytest.mark.asyncio
async def test_coordinator_skips_when_no_recent_volume_matches_latest_scan(tmp_path):
    fs.initialize_filesystem(tmp_path)
    coordinator = NexradScanCoordinator(
        station_fetcher=lambda **_kwargs: {"KTLH": _station()},
        async_volume_lister=lambda *_args, **_kwargs: _return(["999"]),
        async_chunk_lister=lambda *_args, **_kwargs: _return(_chunks(stamp="20260507-145500")),
    )

    results = await coordinator.ingest_latest_station_scans_async(base_dir=tmp_path)

    assert [result.action for result in results] == ["skipped_no_matching_volume"]


@pytest.mark.asyncio
async def test_coordinator_retries_partial_local_scan_download(tmp_path):
    fs.initialize_filesystem(tmp_path)
    remote_chunks = _chunks()
    outdir = chunk_output_dir("KTLH", "999", remote_chunks)
    outdir.mkdir(parents=True, exist_ok=True)
    for chunk in remote_chunks[:-1]:
        (outdir / Path(chunk.key).name).write_bytes(b"x")
    ingest_calls = []

    async def _ingest_trigger(site, volume_id, **_kwargs):
        ingest_calls.append((site, volume_id))
        return NexradIngestResult(site=site, volume_id=volume_id, vcp=212, dynamic_scan_type=None, low_path=None, high_path=None, manifest_path=None, chunks_downloaded=1, complete=True)

    coordinator = NexradScanCoordinator(
        station_fetcher=lambda **_kwargs: {"KTLH": _station()},
        async_volume_lister=lambda *_args, **_kwargs: _return(["999"]),
        async_chunk_lister=lambda *_args, **_kwargs: _return(remote_chunks),
        async_ingest_trigger=_ingest_trigger,
    )

    results = await coordinator.ingest_latest_station_scans_async(base_dir=tmp_path)

    assert ingest_calls == [("KTLH", "999")]
    assert [result.action for result in results] == ["downloaded"]


@pytest.mark.asyncio
async def test_coordinator_keeps_site_isolation_on_listing_error(tmp_path):
    fs.initialize_filesystem(tmp_path)

    async def _chunk_lister(site, _volume_id, **_kwargs):
        if site == "KTLH":
            raise RuntimeError("boom")
        return _chunks(site="KDGX", volume_id="123")

    async def _ingest_trigger(site, volume_id, **_kwargs):
        return NexradIngestResult(site=site, volume_id=volume_id, vcp=212, dynamic_scan_type=None, low_path=None, high_path=None, manifest_path=None, chunks_downloaded=25, complete=True)

    coordinator = NexradScanCoordinator(
        station_fetcher=lambda **_kwargs: {"KTLH": _station(site="KTLH"), "KDGX": _station(site="KDGX")},
        async_volume_lister=lambda site, *_args, **_kwargs: _return(["999"] if site == "KTLH" else ["123"]),
        async_chunk_lister=_chunk_lister,
        async_ingest_trigger=_ingest_trigger,
    )

    results = await coordinator.ingest_latest_station_scans_async(base_dir=tmp_path)

    assert [result.site for result in results] == ["KDGX", "KTLH"]
    assert [result.action for result in results] == ["downloaded", "site_error"]


async def _return(value):
    return value
