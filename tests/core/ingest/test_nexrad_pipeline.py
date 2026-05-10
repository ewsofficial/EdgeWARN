from pathlib import Path

import pytest

import util.file as fs
from common.ingest.nexrad.models import ChunkKey, NexradCompletionRecord, NexradIngestResult, RadarStationVcp
from common.ingest.nexrad.pipeline import NexradRealtimeIngestionPipeline
from common.ingest.nexrad.pipeline.models import PendingVolume
from common.ingest.nexrad.writer import volume_output_path


def _station(site="KTLH", *, vcp=212):
    return RadarStationVcp(
        site=site,
        vcp=vcp,
        raw_vcp=f"R{vcp}" if vcp is not None else None,
        rda_timestamp="2026-05-07T14:59:00Z",
        level_two_last_received_time="2026-05-07T15:00:00Z",
        properties={},
    )


def _chunks(site="KTLH", volume_id="999", stamp="20260507-150000", last_number=25):
    return [
        ChunkKey(
            site=site,
            volume_id=volume_id,
            chunk_number=number,
            chunk_type="S" if number == 1 else "I",
            key=f"{site}/{volume_id}/{stamp}-{number:03d}-{'S' if number == 1 else 'I'}",
        )
        for number in range(1, last_number + 1)
    ]


def _record(site, volume_id, scan_timestamp):
    return NexradCompletionRecord(
        site=site,
        volume_id=volume_id,
        scan_timestamp=scan_timestamp,
        volume_path=None,
        manifest_path=None,
    )


@pytest.mark.asyncio
async def test_pipeline_filters_stations_before_any_s3_listing(tmp_path):
    fs.initialize_filesystem(tmp_path)
    listed = []

    async def _volume_lister(site, **_kwargs):
        listed.append(site)
        return ["999"]

    pipeline = NexradRealtimeIngestionPipeline(
        base_dir=tmp_path,
        sites=["ktlh", "wxyz", "ktlx", "kmia"],
        station_fetcher=lambda **_kwargs: {
            "KTLH": _station("KTLH", vcp=212),
            "WXYZ": _station("WXYZ", vcp=212),
            "KTLX": _station("KTLX", vcp=35),
            "KMIA": _station("KMIA", vcp=12),
        },
        async_volume_lister=_volume_lister,
        async_chunk_lister=lambda *_args, **_kwargs: _return(_chunks(site="KMIA")),
        async_ingest_trigger=lambda *_args, **_kwargs: _return(None),
    )

    await pipeline.scan_for_new_volumes_once()

    assert listed == ["KMIA", "KTLH"]


@pytest.mark.asyncio
async def test_pipeline_downloads_complete_volume_and_emits_after_ingest(tmp_path):
    fs.initialize_filesystem(tmp_path)
    events = []

    async def _ingest_trigger(site, volume_id, **_kwargs):
        events.append(("ingest", site, volume_id))
        return NexradIngestResult(site=site, volume_id=volume_id, vcp=212, dynamic_scan_type=None, volume_path=None, scan_timestamp="20260507-150000", low_path=None, high_path=None, manifest_path=None, chunks_downloaded=25, complete=True)

    pipeline = NexradRealtimeIngestionPipeline(
        base_dir=tmp_path,
        station_fetcher=lambda **_kwargs: {"KTLH": _station()},
        async_volume_lister=lambda *_args, **_kwargs: _return(["999"]),
        async_chunk_lister=lambda *_args, **_kwargs: _return(_chunks()),
        async_ingest_trigger=_ingest_trigger,
        download_emitter=lambda records: events.append(("emit", tuple(records))),
    )

    downloaded = await pipeline.scan_for_new_volumes_once()

    assert downloaded == [_record("KTLH", "999", "20260507-150000")]
    assert events == [
        ("ingest", "KTLH", "999"),
        ("emit", (_record("KTLH", "999", "20260507-150000"),)),
    ]


@pytest.mark.asyncio
async def test_pipeline_adds_incomplete_volume_to_pending_without_emitting(tmp_path):
    fs.initialize_filesystem(tmp_path)
    emitted = []
    ingested = []
    pipeline = NexradRealtimeIngestionPipeline(
        base_dir=tmp_path,
        station_fetcher=lambda **_kwargs: {"KTLH": _station()},
        async_volume_lister=lambda *_args, **_kwargs: _return(["999"]),
        async_chunk_lister=lambda *_args, **_kwargs: _return(_chunks(last_number=24)),
        async_ingest_trigger=lambda *_args, **_kwargs: _track_call(ingested),
        download_emitter=lambda records: emitted.append(tuple(records)),
    )

    downloaded = await pipeline.scan_for_new_volumes_once()

    assert downloaded == []
    assert list(pipeline.pending_tracker.pending) == [("KTLH", "999")]
    assert ingested == []
    assert emitted == []


@pytest.mark.asyncio
async def test_pipeline_rechecks_pending_and_downloads_when_chunks_complete(tmp_path):
    fs.initialize_filesystem(tmp_path)
    emitted = []
    ingested = []
    chunk_calls = []

    async def _chunk_lister(site, volume_id, **_kwargs):
        chunk_calls.append((site, volume_id))
        if len(chunk_calls) == 1:
            return _chunks(last_number=24)
        return _chunks()

    async def _ingest_trigger(site, volume_id, **_kwargs):
        ingested.append((site, volume_id))
        return NexradIngestResult(site=site, volume_id=volume_id, vcp=212, dynamic_scan_type=None, volume_path=None, scan_timestamp="20260507-150000", low_path=None, high_path=None, manifest_path=None, chunks_downloaded=25, complete=True)

    pipeline = NexradRealtimeIngestionPipeline(
        base_dir=tmp_path,
        station_fetcher=lambda **_kwargs: {"KTLH": _station()},
        async_volume_lister=lambda *_args, **_kwargs: _return(["999"]),
        async_chunk_lister=_chunk_lister,
        async_ingest_trigger=_ingest_trigger,
        download_emitter=lambda sites: emitted.append(tuple(sites)),
    )

    await pipeline.scan_for_new_volumes_once()
    downloaded = await pipeline.check_pending_once()

    assert downloaded == [_record("KTLH", "999", "20260507-150000")]
    assert ingested == [("KTLH", "999")]
    assert emitted == [(_record("KTLH", "999", "20260507-150000"),)]
    assert pipeline.pending_tracker.pending == {}


@pytest.mark.asyncio
async def test_pipeline_emits_downloaded_sites_for_multiple_scan_completions(tmp_path):
    fs.initialize_filesystem(tmp_path)
    emitted = []

    async def _ingest_trigger(site, volume_id, **_kwargs):
        return NexradIngestResult(site=site, volume_id=volume_id, vcp=212, dynamic_scan_type=None, volume_path=None, scan_timestamp="20260507-150000" if site == "KTLH" else "20260507-150100", low_path=None, high_path=None, manifest_path=None, chunks_downloaded=25, complete=True)

    pipeline = NexradRealtimeIngestionPipeline(
        base_dir=tmp_path,
        station_fetcher=lambda **_kwargs: {"KTLH": _station("KTLH"), "KDGX": _station("KDGX")},
        async_volume_lister=lambda site, **_kwargs: _return(["999"] if site == "KTLH" else ["123"]),
        async_chunk_lister=lambda site, volume_id, **_kwargs: _return(_chunks(site=site, volume_id=volume_id)),
        async_ingest_trigger=_ingest_trigger,
        download_emitter=lambda records: emitted.append(tuple(records)),
    )

    downloaded = await pipeline.scan_for_new_volumes_once()

    assert downloaded == [
        _record("KDGX", "123", "20260507-150100"),
        _record("KTLH", "999", "20260507-150000"),
    ]
    assert emitted == [tuple(downloaded)]


@pytest.mark.asyncio
async def test_pipeline_drops_stale_pending_when_newer_volume_is_seen(tmp_path):
    fs.initialize_filesystem(tmp_path)
    volume_ids = iter([["999"], ["1000"]])
    chunk_map = {
        "999": _chunks(volume_id="999", last_number=24),
        "1000": _chunks(volume_id="1000"),
    }
    ingested = []

    async def _ingest_trigger(site, volume_id, **_kwargs):
        ingested.append((site, volume_id))
        return NexradIngestResult(site=site, volume_id=volume_id, vcp=212, dynamic_scan_type=None, volume_path=None, scan_timestamp="20260507-150000", low_path=None, high_path=None, manifest_path=None, chunks_downloaded=25, complete=True)

    pipeline = NexradRealtimeIngestionPipeline(
        base_dir=tmp_path,
        station_fetcher=lambda **_kwargs: {"KTLH": _station()},
        async_volume_lister=lambda *_args, **_kwargs: _return(next(volume_ids)),
        async_chunk_lister=lambda _site, volume_id, **_kwargs: _return(chunk_map[volume_id]),
        async_ingest_trigger=_ingest_trigger,
    )

    await pipeline.scan_for_new_volumes_once()
    assert list(pipeline.pending_tracker.pending) == [("KTLH", "999")]

    await pipeline.scan_for_new_volumes_once()

    assert ("KTLH", "999") not in pipeline.pending_tracker.pending
    assert ingested == [("KTLH", "1000")]


@pytest.mark.asyncio
async def test_pipeline_removes_local_complete_pending_without_duplicate_ingest(tmp_path):
    fs.initialize_filesystem(tmp_path)
    chunks = _chunks()
    volume_path = volume_output_path("KTLH", "999", chunks)
    volume_path.parent.mkdir(parents=True, exist_ok=True)
    volume_path.write_bytes(b"volume")
    ingested = []
    pipeline = NexradRealtimeIngestionPipeline(
        base_dir=tmp_path,
        station_fetcher=lambda **_kwargs: {"KTLH": _station()},
        async_volume_lister=lambda *_args, **_kwargs: _return(["999"]),
        async_chunk_lister=lambda *_args, **_kwargs: _return(chunks),
        async_ingest_trigger=lambda *_args, **_kwargs: _track_call(ingested),
    )
    pipeline.pending_tracker.upsert(
        PendingVolume(
            site="KTLH",
            volume_id="999",
            station=_station(),
            latest_scan_time="20260507-150000",
        )
    )
    pipeline.last_seen_by_site["KTLH"] = "999"

    downloaded = await pipeline.check_pending_once()

    assert downloaded == []
    assert ingested == []
    assert pipeline.pending_tracker.pending == {}


@pytest.mark.asyncio
async def test_pipeline_does_not_emit_for_failed_ingest(tmp_path):
    fs.initialize_filesystem(tmp_path)
    emitted = []
    pipeline = NexradRealtimeIngestionPipeline(
        base_dir=tmp_path,
        station_fetcher=lambda **_kwargs: {"KTLH": _station()},
        async_volume_lister=lambda *_args, **_kwargs: _return(["999"]),
        async_chunk_lister=lambda *_args, **_kwargs: _return(_chunks()),
        async_ingest_trigger=lambda *_args, **_kwargs: _return(None),
        download_emitter=lambda sites: emitted.append(tuple(sites)),
    )

    downloaded = await pipeline.scan_for_new_volumes_once()

    assert downloaded == []
    assert emitted == []


@pytest.mark.asyncio
async def test_pipeline_run_forever_uses_scan_and_pending_cadence(monkeypatch):
    class StopLoop(Exception):
        pass

    now = {"value": 0.0}
    scans = []
    pendings = []

    async def _sleep(delay):
        now["value"] += max(delay, 0)
        if now["value"] >= 60:
            raise StopLoop()

    pipeline = NexradRealtimeIngestionPipeline(
        scan_interval_seconds=60,
        completion_interval_seconds=20,
        sleeper=_sleep,
        monotonic=lambda: now["value"],
    )

    async def _scan_once(**_kwargs):
        scans.append(now["value"])

    async def _pending_once(**_kwargs):
        pendings.append(now["value"])

    monkeypatch.setattr(pipeline, "scan_for_new_volumes_once", _scan_once)
    monkeypatch.setattr(pipeline, "check_pending_once", _pending_once)

    with pytest.raises(StopLoop):
        await pipeline.run_forever()

    assert scans == [0.0]
    assert pendings == [0.0, 20.0, 40.0]


async def _return(value):
    return value


async def _track_call(calls):
    calls.append(True)
    return None
