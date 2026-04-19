import asyncio
from datetime import datetime, timezone

import common.ingest.mrms.downloader as ingest_downloader
from common.ingest.mrms.config import GoesIngestSpec


def test_download_goes_product_filters_to_requested_abi_channel(monkeypatch, tmp_path):
    dt = datetime(2026, 4, 19, 0, 10, tzinfo=timezone.utc)
    spec = GoesIngestSpec(
        product="ABI-L1b-RadC",
        outdir=tmp_path / "VisibleRed",
        channel_id="C02",
        channel_name="visible_red",
        filename_matcher=r"(?:_|-)M\dC02_",
    )
    captured = {}

    class DummyFinder:
        def __init__(self, *args, **kwargs):
            pass

        def lookup_files(self, prefixes):
            return [
                ("ABI-L1b-RadC/2026/109/00/OR_ABI-L1b-RadC-M6C05_G19_s20261090008000.nc", dt.replace(minute=8)),
                ("ABI-L1b-RadC/2026/109/00/OR_ABI-L1b-RadC-M6C02_G19_s20261090010000.nc", dt),
                ("ABI-L1b-RadC/2026/109/00/OR_ABI-L1b-RadC-M6C02_G19_s20261090000000.nc", dt.replace(minute=0)),
            ]

    class DummyDownloader:
        def __init__(self, *args, **kwargs):
            pass

        def download_matching(self, file_list, outdir):
            captured["file_list"] = file_list
            captured["outdir"] = outdir
            outdir.mkdir(parents=True, exist_ok=True)
            target = outdir / "OR_ABI-L1b-RadC-M6C02_G19_s20261090010000.nc"
            target.write_text("abi")
            return target

        def download_all_matching(self, file_list, outdir):
            raise AssertionError("ABI channel ingest should select one best-match file")

        def decompress_file(self, gz_path):
            return gz_path

    monkeypatch.setattr(ingest_downloader, "FileFinder", DummyFinder)
    monkeypatch.setattr(ingest_downloader, "FileDownloader", DummyDownloader)
    monkeypatch.setattr(ingest_downloader, "_cleanup_goes_outdir_sync", lambda *args, **kwargs: None)

    result = ingest_downloader.download_goes_product(spec, dt)

    assert [path for path, _ in captured["file_list"]] == [
        "ABI-L1b-RadC/2026/109/00/OR_ABI-L1b-RadC-M6C02_G19_s20261090010000.nc",
        "ABI-L1b-RadC/2026/109/00/OR_ABI-L1b-RadC-M6C02_G19_s20261090000000.nc",
    ]
    assert captured["outdir"] == spec.outdir
    assert result == [spec.outdir / "OR_ABI-L1b-RadC-M6C02_G19_s20261090010000.nc"]


def test_download_all_goes_files_async_iterates_glm_and_abi_specs(monkeypatch, tmp_path):
    dt = datetime(2026, 4, 19, 0, 10, tzinfo=timezone.utc)
    specs = [
        GoesIngestSpec("GLM-L2-LCFA", tmp_path / "GLM"),
        GoesIngestSpec(
            product="ABI-L1b-RadC",
            outdir=tmp_path / "VisibleRed",
            channel_id="C02",
            channel_name="visible_red",
            filename_matcher=r"(?:_|-)M\dC02_",
        ),
    ]
    seen = []

    class DummyClientContext:
        async def __aenter__(self):
            return object()

        async def __aexit__(self, exc_type, exc, tb):
            return False

    class DummySession:
        def client(self, *args, **kwargs):
            return DummyClientContext()

    class DummyAsyncFinder:
        def __init__(self, *args, **kwargs):
            pass

        async def async_lookup_files(self, prefixes):
            return []

    async def fake_download(
        goes_spec,
        dt_arg,
        max_entries,
        hour_lookback,
        s3_client,
        parent_trace_id=None,
        preloaded_files=None,
    ):
        seen.append((goes_spec.product, goes_spec.channel_id, dt_arg, max_entries, hour_lookback))
        return []

    monkeypatch.setattr(ingest_downloader, "get_goes_modifiers", lambda: specs)
    monkeypatch.setattr(ingest_downloader.aioboto3, "Session", lambda: DummySession())
    monkeypatch.setattr(ingest_downloader, "AsyncFileFinder", DummyAsyncFinder)
    monkeypatch.setattr(ingest_downloader, "_download_goes_product_async", fake_download)

    asyncio.run(ingest_downloader.download_all_goes_files_async(dt, max_entries=7, hour_lookback=2))

    assert seen == [
        ("GLM-L2-LCFA", None, dt, 7, 2),
        ("ABI-L1b-RadC", "C02", dt, 7, 2),
    ]


def test_download_all_goes_files_async_uses_shared_abi_lookup(monkeypatch, tmp_path):
    dt = datetime(2026, 4, 19, 0, 10, tzinfo=timezone.utc)
    specs = [
        GoesIngestSpec("GLM-L2-LCFA", tmp_path / "GLM"),
        GoesIngestSpec(
            product="ABI-L1b-RadC",
            outdir=tmp_path / "VisibleBlue",
            channel_id="C01",
            channel_name="visible_blue",
            filename_matcher=r"(?:_|-)M\dC01_",
        ),
        GoesIngestSpec(
            product="ABI-L1b-RadC",
            outdir=tmp_path / "VisibleRed",
            channel_id="C02",
            channel_name="visible_red",
            filename_matcher=r"(?:_|-)M\dC02_",
        ),
    ]

    shared_lookup_result = [("ABI-L1b-RadC/2026/109/00/fake.nc", dt)]
    lookup_calls = []
    seen_preloaded = []

    class DummyClientContext:
        async def __aenter__(self):
            return object()

        async def __aexit__(self, exc_type, exc, tb):
            return False

    class DummySession:
        def client(self, *args, **kwargs):
            return DummyClientContext()

    class DummyAsyncFinder:
        def __init__(self, dt_arg, bucket_arg, max_entries, io_manager, s3_client=None):
            lookup_calls.append(("init", max_entries))

        async def async_lookup_files(self, prefixes):
            lookup_calls.append(("lookup", tuple(prefixes)))
            return shared_lookup_result

    async def fake_download(
        goes_spec,
        dt_arg,
        max_entries,
        hour_lookback,
        s3_client,
        parent_trace_id=None,
        preloaded_files=None,
    ):
        seen_preloaded.append((goes_spec.channel_id, preloaded_files))
        return []

    monkeypatch.setattr(ingest_downloader, "get_goes_modifiers", lambda: specs)
    monkeypatch.setattr(ingest_downloader.aioboto3, "Session", lambda: DummySession())
    monkeypatch.setattr(ingest_downloader, "AsyncFileFinder", DummyAsyncFinder)
    monkeypatch.setattr(ingest_downloader, "_download_goes_product_async", fake_download)

    asyncio.run(ingest_downloader.download_all_goes_files_async(dt, max_entries=7, hour_lookback=2))

    assert lookup_calls == [
        ("init", 96),
        ("lookup", ("ABI-L1b-RadC/2026/109/00/", "ABI-L1b-RadC/2026/108/23/")),
    ]
    assert (None, None) in seen_preloaded
    assert ("C01", shared_lookup_result) in seen_preloaded
    assert ("C02", shared_lookup_result) in seen_preloaded
    assert len(seen_preloaded) == 3


def test_download_all_goes_files_sync_uses_shared_abi_lookup(monkeypatch, tmp_path):
    dt = datetime(2026, 4, 19, 0, 10, tzinfo=timezone.utc)
    specs = [
        GoesIngestSpec("GLM-L2-LCFA", tmp_path / "GLM"),
        GoesIngestSpec(
            product="ABI-L1b-RadC",
            outdir=tmp_path / "VisibleBlue",
            channel_id="C01",
            channel_name="visible_blue",
            filename_matcher=r"(?:_|-)M\dC01_",
        ),
        GoesIngestSpec(
            product="ABI-L1b-RadC",
            outdir=tmp_path / "VisibleRed",
            channel_id="C02",
            channel_name="visible_red",
            filename_matcher=r"(?:_|-)M\dC02_",
        ),
    ]

    shared_lookup_result = [("ABI-L1b-RadC/2026/109/00/fake.nc", dt)]
    lookup_calls = []
    seen_preloaded = []

    class DummyFinder:
        def __init__(self, dt_arg, bucket_arg, max_entries, io_manager):
            lookup_calls.append(("init", max_entries))

        def lookup_files(self, prefixes):
            lookup_calls.append(("lookup", tuple(prefixes)))
            return shared_lookup_result

    def fake_download(goes_spec, dt_arg, max_entries, hour_lookback, preloaded_files=None):
        seen_preloaded.append((goes_spec.channel_id, preloaded_files))
        return []

    monkeypatch.setattr(ingest_downloader, "get_goes_modifiers", lambda: specs)
    monkeypatch.setattr(ingest_downloader, "FileFinder", DummyFinder)
    monkeypatch.setattr(ingest_downloader, "download_goes_product", fake_download)

    ingest_downloader.download_all_goes_files(dt, max_entries=7, hour_lookback=2)

    assert lookup_calls == [
        ("init", 96),
        ("lookup", ("ABI-L1b-RadC/2026/109/00/", "ABI-L1b-RadC/2026/108/23/")),
    ]
    assert seen_preloaded == [
        (None, None),
        ("C01", shared_lookup_result),
        ("C02", shared_lookup_result),
    ]


def test_goes_search_max_entries_fixed_to_96(tmp_path):
    abi_spec = GoesIngestSpec(
        product="ABI-L1b-RadC",
        outdir=tmp_path / "VisibleBlue",
        channel_id="C01",
        channel_name="visible_blue",
        filename_matcher=r"(?:_|-)M\dC01_",
    )
    glm_spec = GoesIngestSpec("GLM-L2-LCFA", tmp_path / "GLM")

    assert ingest_downloader._get_goes_search_max_entries(abi_spec, 10) == 96
    assert ingest_downloader._get_goes_search_max_entries(abi_spec, 1000) == 96
    assert ingest_downloader._get_goes_search_max_entries(glm_spec, 10) == 96
