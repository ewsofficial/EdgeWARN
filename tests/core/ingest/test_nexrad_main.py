from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pytest

import util.file as fs
import common.ingest.nexrad.main as nexrad_main
from common.ingest.nexrad.models import ChunkKey
from common.ingest.nexrad.main import NexradIngestService, ingest_allowed_vcp_volume, list_allowed_vcp_sites


def test_ingest_downloads_chunks_to_timestamped_site_chunks_dir(tmp_path):
    fs.initialize_filesystem(tmp_path)

    chunks = [
        ChunkKey(
            "KTLH",
            "999",
            number,
            "S" if number == 1 else "I",
            f"KTLH/999/20260507-150000-{number:03d}-{'S' if number == 1 else 'I'}",
        )
        for number in range(1, 31)
    ]

    def _chunk_bytes(chunk, **_kwargs):
        return f"chunk{chunk.chunk_number}".encode("utf-8")

    with patch("common.ingest.nexrad.main.probe_volume_vcp", return_value=type("Probe", (), {
        "accepted": True,
        "site": "KTLH",
        "volume_id": "999",
        "vcp": 212,
    })()), \
         patch("common.ingest.nexrad.main.list_volume_chunks", return_value=chunks), \
         patch("common.ingest.nexrad.main.get_chunk_bytes", side_effect=_chunk_bytes):
        result = ingest_allowed_vcp_volume("KTLH", "999", base_dir=tmp_path, s3_client=object())

    assert result.site == "KTLH"
    assert result.volume_id == "999"
    assert result.vcp == 212
    assert result.chunks_downloaded == 25
    assert result.complete is True
    assert result.low_path is None
    assert result.high_path is None
    assert result.manifest_path is None
    outdir = Path(tmp_path) / "data" / "NEXRAD_Level2" / "KTLH" / "20260507-150000" / "chunks"
    assert (outdir / "20260507-150000-001-S").read_bytes() == b"chunk1"
    assert (outdir / "20260507-150000-025-I").read_bytes() == b"chunk25"
    assert not (outdir / "20260507-150000-026-I").exists()


def test_list_allowed_vcp_sites_filters_and_sorts():
    station = lambda vcp: type("Station", (), {"vcp": vcp})()
    stations = {
        "KBBB": station(99),
        "KCCC": station(212),
        "KAAA": station(12),
        "KDDD": station(None),
        "WXYZ": station(212),
    }

    with patch("common.ingest.nexrad.main.fetch_radar_station_vcps", return_value=stations):
        sites = list_allowed_vcp_sites()

    # WXYZ should be excluded because it does not start with 'K'
    assert sites == ["KAAA", "KCCC"]


def test_ingest_allowed_vcp_volume_uses_provided_station_vcp(tmp_path):
    fs.initialize_filesystem(tmp_path)
    chunks = [
        ChunkKey("KTLH", "999", number, "I", f"KTLH/999/20260507-150000-{number:03d}-I")
        for number in range(1, 26)
    ]
    station = type("Station", (), {"vcp": 212})()

    def _chunk_bytes(chunk, **_kwargs):
        return f"chunk{chunk.chunk_number}".encode("utf-8")

    with patch("common.ingest.nexrad.main.probe_volume_vcp") as probe_mock, \
         patch("common.ingest.nexrad.main.list_volume_chunks", return_value=chunks), \
         patch("common.ingest.nexrad.main.get_chunk_bytes", side_effect=_chunk_bytes):
        result = ingest_allowed_vcp_volume("KTLH", "999", base_dir=tmp_path, s3_client=object(), station_vcp=station)

    probe_mock.assert_not_called()
    assert result.site == "KTLH"
    assert result.volume_id == "999"
    assert result.vcp == 212


def test_ingest_keeps_latest_three_station_scan_dirs(tmp_path):
    fs.initialize_filesystem(tmp_path)
    oldest_outdir = Path(tmp_path) / "data" / "NEXRAD_Level2" / "KTLH" / "20260507-145000" / "chunks"
    old_outdir = Path(tmp_path) / "data" / "NEXRAD_Level2" / "KTLH" / "20260507-145500" / "chunks"
    newer_outdir = Path(tmp_path) / "data" / "NEXRAD_Level2" / "KTLH" / "20260507-145900" / "chunks"
    oldest_outdir.mkdir(parents=True, exist_ok=True)
    old_outdir.mkdir(parents=True, exist_ok=True)
    newer_outdir.mkdir(parents=True, exist_ok=True)
    (oldest_outdir / "stale").write_bytes(b"oldest")
    chunks = [
        ChunkKey("KTLH", "999", number, "I", f"KTLH/999/20260507-150000-{number:03d}-I")
        for number in range(1, 26)
    ]
    station = type("Station", (), {"vcp": 212})()

    def _chunk_bytes(chunk, **_kwargs):
        return f"chunk{chunk.chunk_number}".encode("utf-8")

    with patch("common.ingest.nexrad.main.list_volume_chunks", return_value=chunks), \
         patch("common.ingest.nexrad.main.get_chunk_bytes", side_effect=_chunk_bytes):
        ingest_allowed_vcp_volume("KTLH", "999", base_dir=tmp_path, s3_client=object(), station_vcp=station)

    assert not oldest_outdir.parent.exists()
    assert old_outdir.parent.exists()
    assert newer_outdir.parent.exists()
    assert (Path(tmp_path) / "data" / "NEXRAD_Level2" / "KTLH" / "20260507-150000" / "chunks").exists()


class _AsyncBody:
    def __init__(self, payload):
        self.payload = payload

    async def iter_chunks(self):
        yield self.payload


class _AsyncS3Client:
    def __init__(self, payloads=None):
        self.payloads = payloads or {}

    async def get_object(self, *, Bucket, Key):
        return {"Body": _AsyncBody(self.payloads[(Bucket, Key)])}


def _make_low_chunks(site="KTLH", volume_id="999"):
    return [
        ChunkKey(site, volume_id, number, "S" if number == 1 else "I", f"{site}/{volume_id}/20260507-150000-{number:03d}-{'S' if number == 1 else 'I'}")
        for number in range(1, 26)
    ]


@pytest.mark.asyncio
async def test_ingest_allowed_vcp_volume_async_downloads_chunks_to_timestamped_site_chunks_dir(tmp_path):
    fs.initialize_filesystem(tmp_path)
    chunks = _make_low_chunks()
    station = type("Station", (), {"vcp": 212})()

    async def _chunk_bytes(chunk, **_kwargs):
        return f"chunk{chunk.chunk_number}".encode("utf-8")

    service = NexradIngestService(async_chunk_lister=lambda *_args, **_kwargs: None)
    service.async_chunk_lister = lambda *_args, **_kwargs: _return_chunks(chunks)
    service.async_chunk_fetcher = _chunk_bytes
    service._stream_chunk_downloads = False

    result = await service.ingest_allowed_vcp_volume_async(
        "KTLH",
        "999",
        base_dir=tmp_path,
        s3_client=object(),
        station_vcp=station,
    )

    assert result.site == "KTLH"
    assert result.volume_id == "999"
    assert result.vcp == 212
    assert result.chunks_downloaded == 25
    outdir = Path(tmp_path) / "data" / "NEXRAD_Level2" / "KTLH" / "20260507-150000" / "chunks"
    assert (outdir / "20260507-150000-001-S").read_bytes() == b"chunk1"
    assert (outdir / "20260507-150000-025-I").read_bytes() == b"chunk25"


@pytest.mark.asyncio
async def test_ingest_allowed_vcp_volume_async_skips_existing_files(tmp_path):
    fs.initialize_filesystem(tmp_path)
    chunks = _make_low_chunks()
    outdir = Path(tmp_path) / "data" / "NEXRAD_Level2" / "KTLH" / "20260507-150000" / "chunks"
    outdir.mkdir(parents=True, exist_ok=True)
    (outdir / "20260507-150000-001-S").write_bytes(b"existing")
    station = type("Station", (), {"vcp": 212})()
    fetched = []

    async def _chunk_bytes(chunk, **_kwargs):
        fetched.append(chunk.chunk_number)
        return f"chunk{chunk.chunk_number}".encode("utf-8")

    service = NexradIngestService(async_chunk_lister=lambda *_args, **_kwargs: None)
    service.async_chunk_lister = lambda *_args, **_kwargs: _return_chunks(chunks)
    service.async_chunk_fetcher = _chunk_bytes
    service._stream_chunk_downloads = False

    await service.ingest_allowed_vcp_volume_async(
        "KTLH",
        "999",
        base_dir=tmp_path,
        s3_client=object(),
        station_vcp=station,
    )

    assert 1 not in fetched
    assert len(fetched) == 24
    assert (outdir / "20260507-150000-001-S").read_bytes() == b"existing"


@pytest.mark.asyncio
async def test_ingest_allowed_vcp_volume_async_keeps_latest_three_station_scan_dirs(tmp_path):
    fs.initialize_filesystem(tmp_path)
    oldest_outdir = Path(tmp_path) / "data" / "NEXRAD_Level2" / "KTLH" / "20260507-145000" / "chunks"
    old_outdir = Path(tmp_path) / "data" / "NEXRAD_Level2" / "KTLH" / "20260507-145500" / "chunks"
    newer_outdir = Path(tmp_path) / "data" / "NEXRAD_Level2" / "KTLH" / "20260507-145900" / "chunks"
    oldest_outdir.mkdir(parents=True, exist_ok=True)
    old_outdir.mkdir(parents=True, exist_ok=True)
    newer_outdir.mkdir(parents=True, exist_ok=True)
    (oldest_outdir / "stale").write_bytes(b"oldest")
    chunks = _make_low_chunks()
    station = type("Station", (), {"vcp": 212})()

    async def _chunk_bytes(chunk, **_kwargs):
        return f"chunk{chunk.chunk_number}".encode("utf-8")

    service = NexradIngestService(async_chunk_lister=lambda *_args, **_kwargs: None)
    service.async_chunk_lister = lambda *_args, **_kwargs: _return_chunks(chunks)
    service.async_chunk_fetcher = _chunk_bytes
    service._stream_chunk_downloads = False

    await service.ingest_allowed_vcp_volume_async(
        "KTLH",
        "999",
        base_dir=tmp_path,
        s3_client=object(),
        station_vcp=station,
    )

    assert not oldest_outdir.parent.exists()
    assert old_outdir.parent.exists()
    assert newer_outdir.parent.exists()
    assert (Path(tmp_path) / "data" / "NEXRAD_Level2" / "KTLH" / "20260507-150000" / "chunks").exists()


@pytest.mark.asyncio
async def test_ingest_latest_allowed_vcp_scans_async_fetches_station_catalog_once(tmp_path):
    fs.initialize_filesystem(tmp_path)
    chunks = _make_low_chunks()
    station = type("Station", (), {"vcp": 212})()
    station_fetch_calls = []
    volume_probe_calls = []

    def _station_fetcher(*, session=None):
        station_fetch_calls.append(session)
        return {"KTLH": station, "KDGX": station}

    async def _volume_lister(site, limit=1, **_kwargs):
        return [f"{site}-vol-{index}" for index in range(limit)]

    async def _chunk_lister(_site, _volume_id, **_kwargs):
        return chunks

    async def _chunk_fetcher(chunk, **_kwargs):
        return str(chunk.chunk_number).encode("utf-8")

    def _volume_prober(*args, **kwargs):
        volume_probe_calls.append((args, kwargs))
        raise AssertionError("volume_prober should not be used when station_vcps are shared")

    service = NexradIngestService(
        station_fetcher=_station_fetcher,
        volume_prober=_volume_prober,
        async_volume_lister=_volume_lister,
        async_chunk_lister=_chunk_lister,
        async_chunk_fetcher=_chunk_fetcher,
    )
    service._stream_chunk_downloads = False

    results = await service.ingest_latest_allowed_vcp_scans_async(
        ["KTLH", "KDGX"],
        max_volumes_per_site=2,
        base_dir=tmp_path,
        s3_client=object(),
    )

    assert len(results) == 4
    assert station_fetch_calls == [None]
    assert volume_probe_calls == []


@pytest.mark.asyncio
async def test_ingest_latest_allowed_vcp_scans_async_uses_shared_station_mapping_without_fetch_or_probe(tmp_path):
    fs.initialize_filesystem(tmp_path)
    chunks = _make_low_chunks(site="KDGX", volume_id="123")
    station = type("Station", (), {"vcp": 212})()

    async def _volume_lister(_site, limit=1, **_kwargs):
        return ["123"]

    async def _chunk_lister(_site, _volume_id, **_kwargs):
        return chunks

    async def _chunk_fetcher(chunk, **_kwargs):
        return f"chunk{chunk.chunk_number}".encode("utf-8")

    service = NexradIngestService(
        station_fetcher=lambda **_kwargs: (_ for _ in ()).throw(AssertionError("station_fetcher should not be used")),
        volume_prober=lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("volume_prober should not be used")),
        async_volume_lister=_volume_lister,
        async_chunk_lister=_chunk_lister,
        async_chunk_fetcher=_chunk_fetcher,
    )
    service._stream_chunk_downloads = False

    results = await service.ingest_latest_allowed_vcp_scans_async(
        ["KDGX"],
        max_volumes_per_site=1,
        base_dir=tmp_path,
        s3_client=object(),
        station_vcps={"KDGX": station},
    )

    assert len(results) == 1
    assert results[0].site == "KDGX"


@pytest.mark.asyncio
async def test_ingest_allowed_vcp_volume_async_rejects_disallowed_station_without_listing_chunks(tmp_path):
    fs.initialize_filesystem(tmp_path)
    station = type("Station", (), {"vcp": 35})()
    listed = []

    async def _chunk_lister(*_args, **_kwargs):
        listed.append(True)
        return []

    service = NexradIngestService(async_chunk_lister=_chunk_lister)

    result = await service.ingest_allowed_vcp_volume_async(
        "KTLX",
        "999",
        base_dir=tmp_path,
        s3_client=object(),
        station_vcp=station,
    )

    assert result is None
    assert listed == []


@pytest.mark.asyncio
async def test_ingest_latest_allowed_vcp_scans_async_skips_per_volume_exceptions(tmp_path):
    fs.initialize_filesystem(tmp_path)
    chunks = _make_low_chunks(site="KTLH", volume_id="ok")
    station = type("Station", (), {"vcp": 212})()

    async def _volume_lister(_site, limit=1, **_kwargs):
        return ["bad", "ok"]

    async def _chunk_lister(_site, volume_id, **_kwargs):
        if volume_id == "bad":
            raise RuntimeError("boom")
        return chunks

    async def _chunk_fetcher(chunk, **_kwargs):
        return f"chunk{chunk.chunk_number}".encode("utf-8")

    service = NexradIngestService(
        async_volume_lister=_volume_lister,
        async_chunk_lister=_chunk_lister,
        async_chunk_fetcher=_chunk_fetcher,
    )
    service._stream_chunk_downloads = False

    results = await service.ingest_latest_allowed_vcp_scans_async(
        ["KTLH"],
        base_dir=tmp_path,
        s3_client=object(),
        station_vcps={"KTLH": station},
    )

    assert len(results) == 1
    assert results[0].volume_id == "ok"


async def _return_chunks(chunks):
    return chunks


@pytest.mark.asyncio
async def test_ingest_latest_station_scans_async_forwards_to_coordinator():
    captured = {}

    async def _impl(sites=None, **kwargs):
        captured["sites"] = sites
        captured.update(kwargs)
        return ["ok"]

    with patch("common.ingest.nexrad.coordinator.ingest_latest_station_scans_async", side_effect=_impl):
        result = await nexrad_main.ingest_latest_station_scans_async(
            ["KTLH"],
            base_dir="/tmp/base",
            max_candidate_volumes_per_site=5,
        )

    assert result == ["ok"]
    assert captured == {
        "sites": ["KTLH"],
        "base_dir": "/tmp/base",
        "s3_client": None,
        "weather_session": None,
        "max_candidate_volumes_per_site": 5,
    }


def test_main_uses_latest_scan_coordinator_for_default_path():
    args = SimpleNamespace(
        site=None,
        volume_id=None,
        base_dir=None,
        max_volumes_per_site=1,
        max_candidate_volumes_per_site=4,
    )
    parsed_sites = []

    async def _ingest_latest_station_scans_async(sites=None, **kwargs):
        parsed_sites.append((sites, kwargs))
        return [
            SimpleNamespace(
                site="KTLH",
                action="skipped_already_downloaded",
                latest_scan_time="20260507-150000",
                volume_id="999",
                vcp=212,
                chunks_downloaded=0,
            )
        ]

    parser = SimpleNamespace(parse_args=lambda: args)

    with patch.object(nexrad_main, "_build_parser", return_value=parser), \
         patch.object(nexrad_main, "ingest_latest_station_scans_async", side_effect=_ingest_latest_station_scans_async), \
         patch.object(nexrad_main.io_manager, "write_info") as write_info, \
         patch.object(nexrad_main.io_manager, "write_error") as write_error:
        nexrad_main.main()

    assert parsed_sites == [(None, {"base_dir": None, "max_candidate_volumes_per_site": 4})]
    write_error.assert_not_called()
    assert any("action=skipped_already_downloaded" in call.args[0] for call in write_info.call_args_list)
