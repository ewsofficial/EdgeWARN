import json

import pytest

import util.file as fs
import common.ingest.nexrad.service as nexrad_service_module
from common.ingest.nexrad.grouping import INGEST_READINESS_ELEVATION_IDS
from common.ingest.nexrad.models import ChunkKey
from common.ingest.nexrad.service import NexradIngestService
from common.ingest.nexrad.writer import (
    elevation_manifest_path,
    elevation_netcdf_path,
    runtime_scan_path,
    site_manifest_path,
)


def _chunks(site="KTLH", volume_id="999", stamp="20260507-150000", last_number=2):
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


@pytest.mark.asyncio
async def test_stream_ingest_async_only_downloads_new_chunks_for_partial_volume(tmp_path, monkeypatch):
    fs.initialize_filesystem(tmp_path)
    fetch_calls = []

    async def _async_chunk_fetcher(chunk, **_kwargs):
        fetch_calls.append(chunk.chunk_number)
        return f"chunk-{chunk.chunk_number}".encode("ascii")

    service = NexradIngestService(async_chunk_fetcher=_async_chunk_fetcher)
    monkeypatch.setattr(
        nexrad_service_module,
        "_required_elevation_paths_complete",
        lambda *_args, **_kwargs: False,
    )
    monkeypatch.setattr(
        service,
        "_run_worker_parse",
        lambda state, site, volume_id, scan_timestamp, seen_elevation_keys, first_elevation_timestamp, **_kwargs: first_elevation_timestamp,
    )

    first_result = await service._stream_ingest_volume_async(
        "KTLH",
        "999",
        _chunks(last_number=2),
        s3_client=object(),
        base_dir=tmp_path,
    )
    second_result = await service._stream_ingest_volume_async(
        "KTLH",
        "999",
        _chunks(last_number=4),
        s3_client=object(),
        base_dir=tmp_path,
    )

    runtime_path = runtime_scan_path("KTLH", "999")
    state_path = service._runtime_state_path("KTLH", "999")
    state = json.loads(state_path.read_text(encoding="utf-8"))

    assert first_result.chunks_downloaded == 2
    assert second_result.chunks_downloaded == 2
    assert fetch_calls == [1, 2, 3, 4]
    assert runtime_path.read_bytes() == b"chunk-1chunk-2chunk-3chunk-4"
    assert len(state["downloaded_chunk_keys"]) == 4


@pytest.mark.asyncio
async def test_stream_ingest_async_clears_runtime_state_after_completion(tmp_path, monkeypatch):
    fs.initialize_filesystem(tmp_path)
    completion_states = iter([False, True])

    async def _async_chunk_fetcher(chunk, **_kwargs):
        return f"chunk-{chunk.chunk_number}".encode("ascii")

    service = NexradIngestService(async_chunk_fetcher=_async_chunk_fetcher)
    monkeypatch.setattr(
        nexrad_service_module,
        "_required_elevation_paths_complete",
        lambda *_args, **_kwargs: next(completion_states),
    )
    monkeypatch.setattr(
        service,
        "_run_worker_parse",
        lambda state, site, volume_id, scan_timestamp, seen_elevation_keys, first_elevation_timestamp, **_kwargs: first_elevation_timestamp,
    )

    await service._stream_ingest_volume_async(
        "KTLH",
        "999",
        _chunks(last_number=2),
        s3_client=object(),
        base_dir=tmp_path,
    )

    runtime_path = runtime_scan_path("KTLH", "999")
    state_path = service._runtime_state_path("KTLH", "999")
    assert runtime_path.exists()
    assert state_path.exists()

    result = await service._stream_ingest_volume_async(
        "KTLH",
        "999",
        _chunks(last_number=3),
        s3_client=object(),
        base_dir=tmp_path,
    )

    assert result.complete is True
    assert result.chunks_downloaded == 1
    assert not runtime_path.exists()
    assert not state_path.exists()


def _write_completion_sidecars(site, volume_id, scan_timestamp):
    for index, elev in enumerate(INGEST_READINESS_ELEVATION_IDS):
        elevation_timestamp = f"{scan_timestamp[:-2]}{index:02d}"
        nc_path = elevation_netcdf_path(site, elev, elevation_timestamp)
        nc_path.parent.mkdir(parents=True, exist_ok=True)
        nc_path.write_bytes(b"ok")
        manifest_path = elevation_manifest_path(site, elev, elevation_timestamp)
        manifest_path.write_text(
            json.dumps(
                {
                    "site": site,
                    "volume_id": volume_id,
                    "volume_timestamp": scan_timestamp,
                    "scan_timestamp": scan_timestamp,
                    "elevation": elev,
                    "elevation_timestamp": elevation_timestamp,
                    "first_sweep_timestamp": elevation_timestamp,
                    "last_sweep_timestamp": elevation_timestamp,
                    "member_sweeps": [
                        {
                            "group_name": f"{elev}-0",
                            "sweep_index": 0,
                            "fixed_angle": float(elev),
                            "elevation_number": None,
                            "waveform": "batch",
                            "timestamp": elevation_timestamp,
                        }
                    ],
                    "netcdf_path": str(nc_path),
                }
            ),
            encoding="utf-8",
        )


@pytest.mark.asyncio
async def test_stream_ingest_async_writes_site_manifest_only_after_completion(tmp_path, monkeypatch):
    fs.initialize_filesystem(tmp_path)
    completion_states = iter([False, True])

    async def _async_chunk_fetcher(chunk, **_kwargs):
        return f"chunk-{chunk.chunk_number}".encode("ascii")

    service = NexradIngestService(async_chunk_fetcher=_async_chunk_fetcher)
    monkeypatch.setattr(
        nexrad_service_module,
        "_required_elevation_paths_complete",
        lambda *_args, **_kwargs: next(completion_states),
    )

    def _fake_run_worker_parse(state, site, volume_id, scan_timestamp, seen_elevation_keys, first_elevation_timestamp, **_kwargs):
        _write_completion_sidecars(site, volume_id, scan_timestamp)
        return first_elevation_timestamp

    monkeypatch.setattr(service, "_run_worker_parse", _fake_run_worker_parse)

    await service._stream_ingest_volume_async(
        "KTLH",
        "999",
        _chunks(last_number=2),
        s3_client=object(),
        base_dir=tmp_path,
    )
    assert not site_manifest_path("KTLH").exists()

    await service._stream_ingest_volume_async(
        "KTLH",
        "999",
        _chunks(last_number=3),
        s3_client=object(),
        base_dir=tmp_path,
    )

    manifest = json.loads(site_manifest_path("KTLH").read_text(encoding="utf-8"))
    assert manifest["volumes"][0]["volume_id"] == "999"
    assert manifest["volumes"][0]["volume_timestamp"] == "20260507-150000"
    assert manifest["volumes"][0]["sweeps"] == [
        {
            "sweep_index": 0,
            "group_name": "0.5-0",
            "elevation": 0.5,
            "timestamp": "20260507-150000",
            "waveform": "batch",
        },
        {
            "sweep_index": 0,
            "group_name": "0.9-0",
            "elevation": 0.9,
            "timestamp": "20260507-150001",
            "waveform": "batch",
        },
        {
            "sweep_index": 0,
            "group_name": "1.3-0",
            "elevation": 1.3,
            "timestamp": "20260507-150002",
            "waveform": "batch",
        },
        {
            "sweep_index": 0,
            "group_name": "1.8-0",
            "elevation": 1.8,
            "timestamp": "20260507-150003",
            "waveform": "batch",
        },
        {
            "sweep_index": 0,
            "group_name": "2.4-0",
            "elevation": 2.4,
            "timestamp": "20260507-150004",
            "waveform": "batch",
        },
        {
            "sweep_index": 0,
            "group_name": "3.1-0",
            "elevation": 3.1,
            "timestamp": "20260507-150005",
            "waveform": "batch",
        },
        {
            "sweep_index": 0,
            "group_name": "4.0-0",
            "elevation": 4.0,
            "timestamp": "20260507-150006",
            "waveform": "batch",
        },
    ]
