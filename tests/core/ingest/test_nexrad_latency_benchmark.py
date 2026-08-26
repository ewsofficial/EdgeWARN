"""Focused tests for the NEXRAD pipeline latency benchmark.

Covers timestamp arithmetic, writer/service persistence of parse-completion
timestamps, manifest collection/deduplication/staleness, atomic per-site output,
and the bounded subprocess runner.  None of these require live AWS/weather.gov
access.
"""

from __future__ import annotations

import importlib.util
import json
import signal
import subprocess
import time as time_module
from datetime import datetime, timezone
from pathlib import Path

import pytest

import util.file as fs
import common.ingest.nexrad.service as nexrad_service_module
from common.ingest.nexrad.models import ChunkKey, ElevationGroup, SweepRecord
from common.ingest.nexrad.service import NexradIngestService
from common.ingest.nexrad.writer import (
    elevation_manifest_path,
    elevation_netcdf_path,
    site_manifest_path,
    write_elevation_artifacts,
    write_site_manifest,
)

_BENCHMARK_PATH = Path(__file__).resolve().parents[2] / "benchmarks" / "benchmark_nexrad_pipeline_latency.py"


def _load_benchmark_module():
    import sys

    spec = importlib.util.spec_from_file_location("benchmark_nexrad_pipeline_latency", _BENCHMARK_PATH)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


bm = _load_benchmark_module()


def _fixed_started():
    return bm._parse_iso_utc("2026-08-13T17:00:00.000Z")


def _write_elev_manifest(
    tmp_path,
    *,
    site="KTLH",
    volume_id="999",
    elevation="0.5",
    elevation_timestamp="20260813-170001",
    parse_finished_at="2026-08-13T17:01:12.482Z",
):
    directory = Path(tmp_path) / "data" / "NEXRAD_Level2" / site
    elev_dir = directory / elevation
    elev_dir.mkdir(parents=True, exist_ok=True)
    path = elev_dir / f"{site}_{elevation}_{elevation_timestamp}.json"
    path.write_text(
        json.dumps(
            {
                "site": site,
                "volume_id": volume_id,
                "volume_timestamp": "20260813-170000",
                "scan_timestamp": "20260813-170000",
                "elevation": elevation,
                "elevation_timestamp": elevation_timestamp,
                "parse_finished_at": parse_finished_at,
            }
        ),
        encoding="utf-8",
    )
    return path


def _write_site_manifest(tmp_path, *, site="KTLH", volumes):
    directory = Path(tmp_path) / "data" / "NEXRAD_Level2" / site
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / "manifest.json"
    path.write_text(json.dumps({"site": site, "volumes": volumes}), encoding="utf-8")
    return path


# --------------------------------------------------------------------------
# Timestamp tests
# --------------------------------------------------------------------------


def test_parse_iso_utc_preserves_fractional_seconds():
    parsed = bm._parse_iso_utc("2026-08-13T17:42:31.482Z")
    assert parsed is not None
    assert parsed.tzinfo is not None
    assert parsed.utcoffset().total_seconds() == 0
    assert parsed.microsecond == 482000


def test_parse_iso_utc_rejects_malformed():
    assert bm._parse_iso_utc("not-a-timestamp") is None
    assert bm._parse_iso_utc("") is None
    assert bm._parse_iso_utc(None) is None


def test_parse_reference_nexrad_format_parses_as_utc():
    parsed = bm._parse_reference_timestamp("20260813-170231")
    assert parsed == datetime(2026, 8, 13, 17, 2, 31, tzinfo=timezone.utc)


def test_parse_reference_accepts_iso_fallback():
    parsed = bm._parse_reference_timestamp("2026-08-13T17:02:31.000Z")
    assert parsed is not None
    assert parsed.microsecond == 0


def test_latency_requires_finish_at_or_after_benchmark_start(tmp_path):
    collector = bm.LatencyCollector(tmp_path, started_at=_fixed_started())
    latency, reason = collector._latency_and_reason("2026-08-13T17:03:12.482Z", "20260813-170231")
    assert latency == pytest.approx(41.482)
    assert reason is None


def test_volume_latency_subtraction(tmp_path):
    collector = bm.LatencyCollector(tmp_path, started_at=bm._parse_iso_utc("2026-08-13T15:00:00.000Z"))
    latency, reason = collector._latency_and_reason("2026-08-13T15:05:09.214Z", "20260813-150000")
    assert latency == pytest.approx(309.214)
    assert reason is None


def test_missing_or_malformed_reference_is_excluded(tmp_path):
    collector = bm.LatencyCollector(tmp_path, started_at=_fixed_started())
    latency, reason = collector._latency_and_reason("2026-08-13T17:03:12.482Z", None)
    assert latency is None
    assert reason == "missing_or_invalid_reference_timestamp"

    latency, reason = collector._latency_and_reason("2026-08-13T17:03:12.482Z", "2026-02-30T00:00:00Z")
    assert latency is None
    assert reason == "missing_or_invalid_reference_timestamp"


def test_negative_unreasonable_reference_is_excluded(tmp_path):
    collector = bm.LatencyCollector(tmp_path, started_at=_fixed_started())
    latency, reason = collector._latency_and_reason("2026-08-13T17:01:00.000Z", "20260813-170231")
    assert latency is None
    assert reason == "reference_after_parse_finish"


def test_stale_finish_before_benchmark_start_is_marked_stale(tmp_path):
    collector = bm.LatencyCollector(tmp_path, started_at=_fixed_started())
    latency, reason = collector._latency_and_reason("2026-08-13T16:59:59.999Z", "20260813-150001")
    assert latency is None
    assert reason == "stale"


# --------------------------------------------------------------------------
# Writer / service persistence tests
# --------------------------------------------------------------------------


class _FakeRawSweep:
    def __init__(self, group_name, waveform):
        self.group_name = group_name
        self.waveform = waveform
        self.record_ranges = [(0, 6)]


class _FakeRawVolume:
    volume_header = b"header"
    record_buffer = b"record"
    metadata_ranges = []
    trailing_bytes = b""

    def __init__(self, sweeps):
        self.sweeps = sweeps


def _write_test_artifact(site, volume_id, scan_timestamp, elevation, member_specs):
    group = ElevationGroup(
        elevation_id=elevation,
        canonical_angle_deg=float(elevation),
        members=[
            SweepRecord(
                index=index,
                group_name=group_name,
                fixed_angle=float(elevation),
                waveform=waveform,
                timestamp=timestamp,
                azimuth_count=360,
            )
            for index, (group_name, waveform, timestamp) in enumerate(member_specs)
        ],
        waveforms_present={waveform for _group_name, waveform, _timestamp in member_specs},
        first_sweep_index=0,
        last_sweep_index=len(member_specs) - 1,
        first_timestamp=member_specs[0][2],
        last_timestamp=member_specs[-1][2],
        complete=True,
    )
    source = _FakeRawVolume([_FakeRawSweep(group_name, waveform) for group_name, waveform, _timestamp in member_specs])
    return write_elevation_artifacts(
        group,
        source,
        site=site,
        volume_id=volume_id,
        scan_timestamp=scan_timestamp,
        download_started_at="2026-05-07T15:00:00Z",
        elevation_label=elevation,
        elevation_timestamp=member_specs[0][2],
    )


def test_write_elevation_artifacts_persists_parse_finished_at(tmp_path):
    fs.initialize_filesystem(tmp_path)
    artifacts = _write_test_artifact(
        "KTLH",
        "VOL-001",
        "20260507-150000",
        "0.5",
        [("sweep_0", "contiguous_surveillance", "20260507-150001")],
    )

    assert len(artifacts) == 1
    assert artifacts[0].parse_finished_at is not None

    manifest = json.loads(elevation_manifest_path("KTLH", "0.5", "20260507-150001").read_text(encoding="utf-8"))
    assert manifest["parse_finished_at"] == artifacts[0].parse_finished_at
    assert artifacts[0].ingest_status == "success"
    assert manifest["ingest_status"] == "success"
    assert manifest["ingest_error"] is None
    assert manifest["file_written_at"].endswith("Z")

    parsed = bm._parse_iso_utc(manifest["parse_finished_at"])
    assert parsed is not None
    assert (parsed - bm._parse_reference_timestamp("20260507-150001")).total_seconds() >= 0


def test_write_elevation_artifacts_does_not_persist_on_export_failure(tmp_path, monkeypatch):
    import common.ingest.nexrad.writer as writer_module

    fs.initialize_filesystem(tmp_path)

    def _fail(*_args, **_kwargs):
        raise IOError("simulated export failure")

    monkeypatch.setattr(writer_module, "_write_elevation_ar2v", _fail)

    with pytest.raises(IOError):
        _write_test_artifact(
            "KTLH",
            "VOL-001",
            "20260507-150000",
            "0.5",
            [("sweep_0", "batch", "20260507-150001")],
        )

    manifest = json.loads(elevation_manifest_path("KTLH", "0.5", "20260507-150001").read_text(encoding="utf-8"))
    assert manifest["ingest_status"] == "failed"
    assert manifest["ingest_error"].startswith("OSError: simulated export failure")


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


def _write_completion_sidecars(site, volume_id, scan_timestamp):
    from common.ingest.nexrad.grouping import INGEST_READINESS_ELEVATION_IDS

    for index, elevation in enumerate(INGEST_READINESS_ELEVATION_IDS):
        elevation_timestamp = f"{scan_timestamp[:-2]}{index:02d}"
        _write_test_artifact(
            site,
            volume_id,
            scan_timestamp,
            elevation,
            [(f"readiness_{elevation}", "batch", elevation_timestamp)],
        )


def test_stream_ingest_sync_records_volume_parse_finished_at_on_completion(tmp_path, monkeypatch):
    fs.initialize_filesystem(tmp_path)
    _write_completion_sidecars("KTLH", "999", "20260507-150000")
    service = NexradIngestService(chunk_fetcher=lambda *_args, **_kwargs: b"x")
    monkeypatch.setattr(
        nexrad_service_module,
        "_required_elevation_paths_complete",
        lambda *_args, **_kwargs: True,
    )
    monkeypatch.setattr(
        service,
        "_run_worker_parse",
        lambda *_args, **_kwargs: None,
    )

    result = service._stream_ingest_volume(
        "KTLH",
        "999",
        _chunks(),
        s3_client=object(),
        base_dir=tmp_path,
    )

    assert result.complete is True
    manifest = json.loads(site_manifest_path("KTLH").read_text(encoding="utf-8"))
    assert manifest["volumes"][0]["volume_id"] == "999"
    assert manifest["current_volume_parse_finished_at"]
    assert manifest["volumes"][0]["volume_parse_finished_at"] == manifest["current_volume_parse_finished_at"]
    assert bm._parse_iso_utc(manifest["volumes"][0]["volume_parse_finished_at"]) is not None


@pytest.mark.asyncio
async def test_stream_ingest_async_records_volume_parse_finished_at_on_completion(tmp_path, monkeypatch):
    fs.initialize_filesystem(tmp_path)
    _write_completion_sidecars("KTLH", "999", "20260507-150000")

    async def _async_chunk_fetcher(_chunk, **_kwargs):
        return b"x"

    service = NexradIngestService(async_chunk_fetcher=_async_chunk_fetcher)
    monkeypatch.setattr(
        nexrad_service_module,
        "_required_elevation_paths_complete",
        lambda *_args, **_kwargs: True,
    )
    monkeypatch.setattr(
        service,
        "_run_worker_parse",
        lambda *_args, **_kwargs: None,
    )

    result = await service._stream_ingest_volume_async(
        "KTLH",
        "999",
        _chunks(),
        s3_client=object(),
        base_dir=tmp_path,
    )

    assert result.complete is True
    manifest = json.loads(site_manifest_path("KTLH").read_text(encoding="utf-8"))
    assert manifest["volumes"][0]["volume_id"] == "999"
    assert manifest["current_volume_parse_finished_at"]
    assert manifest["volumes"][0]["volume_parse_finished_at"] == manifest["current_volume_parse_finished_at"]


@pytest.mark.asyncio
async def test_stream_ingest_async_skips_completion_timestamp_on_partial_volume(tmp_path, monkeypatch):
    fs.initialize_filesystem(tmp_path)

    async def _async_chunk_fetcher(_chunk, **_kwargs):
        return b"x"

    service = NexradIngestService(async_chunk_fetcher=_async_chunk_fetcher)
    monkeypatch.setattr(
        nexrad_service_module,
        "_required_elevation_paths_complete",
        lambda *_args, **_kwargs: False,
    )
    monkeypatch.setattr(
        service,
        "_run_worker_parse",
        lambda *_args, **_kwargs: None,
    )

    result = await service._stream_ingest_volume_async(
        "KTLH",
        "999",
        _chunks(),
        s3_client=object(),
        base_dir=tmp_path,
    )

    assert result.complete is False
    if site_manifest_path("KTLH").exists():
        manifest = json.loads(site_manifest_path("KTLH").read_text(encoding="utf-8"))
        assert manifest.get("current_volume_parse_finished_at") is None


def test_site_manifest_preserves_volume_parse_finished_at_across_rebuilds(tmp_path):
    fs.initialize_filesystem(tmp_path)

    _write_test_artifact("KTLH", "VOL-E", "20260507-154000", "0.5", [("e0", "batch", "20260507-154001")])
    write_site_manifest(
        "KTLH",
        current_volume_id="VOL-E",
        current_volume_timestamp="20260507-154000",
        current_volume_parse_finished_at="2026-08-13T15:10:00.123Z",
    )

    _write_test_artifact("KTLH", "VOL-F", "20260507-155000", "0.5", [("f0", "batch", "20260507-155001")])
    _write_test_artifact("KTLH", "VOL-F", "20260507-155000", "0.9", [("f1", "batch", "20260507-155002")])
    write_site_manifest(
        "KTLH",
        current_volume_id="VOL-F",
        current_volume_timestamp="20260507-155000",
        current_volume_parse_finished_at="2026-08-13T15:20:00.456Z",
    )

    manifest = json.loads(site_manifest_path("KTLH").read_text(encoding="utf-8"))
    by_id = {volume["volume_id"]: volume for volume in manifest["volumes"]}
    assert manifest["current_volume_parse_finished_at"] == "2026-08-13T15:20:00.456Z"
    assert by_id["VOL-F"]["volume_parse_finished_at"] == "2026-08-13T15:20:00.456Z"
    assert by_id["VOL-E"]["volume_parse_finished_at"] == "2026-08-13T15:10:00.123Z"


# --------------------------------------------------------------------------
# Collector tests
# --------------------------------------------------------------------------


def test_collector_deduplicates_elevation_records(tmp_path):
    collector = bm.LatencyCollector(tmp_path, started_at=_fixed_started())
    _write_elev_manifest(tmp_path)

    collector.poll_once()
    collector.poll_once()

    results = collector.results_by_site()
    assert results["KTLH"]["summary"]["elevation_count"] == 1
    assert len(results["KTLH"]["elevations"]) == 1


def test_collector_filters_stale_events_before_run_start(tmp_path):
    collector = bm.LatencyCollector(tmp_path, started_at=_fixed_started())
    _write_elev_manifest(tmp_path, parse_finished_at="2026-08-13T16:59:00.000Z")

    collector.poll_once()

    results = collector.results_by_site()
    assert results["KTLH"]["summary"]["elevation_count"] == 0
    assert results["KTLH"]["summary"]["excluded_elevation_count"] == 0


def test_collector_retries_malformed_manifest_on_next_poll(tmp_path):
    collector = bm.LatencyCollector(tmp_path, started_at=_fixed_started())
    path = _write_elev_manifest(tmp_path)
    path.write_text("{not valid json", encoding="utf-8")

    collector.poll_once()
    assert collector.results_by_site()["KTLH"]["summary"]["elevation_count"] == 0

    _write_elev_manifest(tmp_path)
    collector.poll_once()
    assert collector.results_by_site()["KTLH"]["summary"]["elevation_count"] == 1


def test_collector_attributes_records_to_site_in_manifest(tmp_path):
    collector = bm.LatencyCollector(tmp_path, started_at=_fixed_started())
    _write_elev_manifest(tmp_path, site="KTLA", volume_id="888")

    collector.poll_once()

    results = collector.results_by_site()
    assert results["KTLA"]["summary"]["elevation_count"] == 1
    assert results.get("KTLH", {}).get("summary", {}).get("elevation_count", 0) == 0


def test_collector_excludes_elevation_with_missing_reference(tmp_path):
    collector = bm.LatencyCollector(tmp_path, started_at=_fixed_started())
    directory = Path(tmp_path) / "data" / "NEXRAD_Level2" / "KTLH" / "0.5"
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / "KTLH_0.5_20260813-170001.json"
    path.write_text(
        json.dumps(
            {
                "site": "KTLH",
                "volume_id": "999",
                "scan_timestamp": "20260813-170000",
                "elevation": "0.5",
                "parse_finished_at": "2026-08-13T17:01:12.482Z",
            }
        ),
        encoding="utf-8",
    )

    collector.poll_once()

    results = collector.results_by_site()["KTLH"]
    assert results["summary"]["elevation_count"] == 1
    assert results["summary"]["excluded_elevation_count"] == 1
    record = results["elevations"][0]
    assert record["latency_seconds"] is None
    assert record["exclusion_reason"] == "missing_or_invalid_reference_timestamp"
    assert results["exclusions"][0]["kind"] == "elevation"


def test_collector_reads_volume_records_from_site_manifest(tmp_path):
    collector = bm.LatencyCollector(tmp_path, started_at=_fixed_started())
    _write_site_manifest(
        tmp_path,
        volumes=[
            {
                "volume_id": "999",
                "volume_timestamp": "20260813-170000",
                "scan_timestamp": "20260813-170000",
                "volume_parse_finished_at": "2026-08-13T17:05:09.214Z",
            }
        ],
    )

    collector.poll_once()

    results = collector.results_by_site()["KTLH"]
    assert results["summary"]["volume_count"] == 1
    assert results["volumes"][0]["latency_seconds"] == pytest.approx(309.214)
    assert results["volumes"][0]["readiness_elevations"] == list(bm.INGEST_READINESS_ELEVATION_IDS)


def test_collector_normalizes_requested_sites_to_uppercase(tmp_path):
    collector = bm.LatencyCollector(tmp_path, sites=["ktlh"], started_at=_fixed_started())

    collector.poll_once()

    assert collector.results_by_site() == {"KTLH": {
        "elevations": [],
        "volumes": [],
        "exclusions": [],
        "summary": {
            "elevation_count": 0,
            "volume_count": 0,
            "excluded_elevation_count": 0,
            "excluded_volume_count": 0,
            "elevation_latency_seconds": {"min": None, "p50": None, "p95": None, "max": None},
            "volume_latency_seconds": {"min": None, "p50": None, "p95": None, "max": None},
        },
    }}


def test_percentile_summary_empty_uses_nulls():
    assert bm._percentile_summary([]) == {"min": None, "p50": None, "p95": None, "max": None}


def test_percentile_summary_computes_interpolated_percentiles():
    summary = bm._percentile_summary([0.0, 10.0, 20.0])
    assert summary["min"] == 0.0
    assert summary["max"] == 20.0
    assert summary["p50"] == pytest.approx(10.0)
    assert summary["p95"] == pytest.approx(19.0)


# --------------------------------------------------------------------------
# Persistence / runner tests
# --------------------------------------------------------------------------


def test_persist_site_result_atomic(tmp_path):
    doc = {"schema_version": 1, "site": "KTLH"}
    output = tmp_path / "KTLH.json"
    bm._persist_site_result(output, doc)

    assert json.loads(output.read_text(encoding="utf-8")) == doc
    assert not list(tmp_path.glob("*.tmp"))


def test_persist_site_result_append_retains_prior_runs(tmp_path):
    output = tmp_path / "KTLH.json"
    bm._persist_site_result(output, {"run": 1}, append=True)
    bm._persist_site_result(output, {"run": 2}, append=True)

    assert json.loads(output.read_text(encoding="utf-8")) == [{"run": 1}, {"run": 2}]


def test_checkpoint_results_writes_checkpointed_doc(tmp_path):
    (tmp_path / "data" / "NEXRAD_Level2" / "KTLH").mkdir(parents=True, exist_ok=True)
    collector = bm.LatencyCollector(tmp_path, started_at=_fixed_started())
    _write_elev_manifest(tmp_path)
    collector.poll_once()

    config = _bench_config(tmp_path, sites=None)
    started_at = _fixed_started()
    output_dir = tmp_path / "out"
    bm._checkpoint_results(config, collector, started_at=started_at, output_dir=output_dir)

    out_path = output_dir / "KTLH.json"
    assert out_path.exists()
    doc = json.loads(out_path.read_text(encoding="utf-8"))
    assert doc["checkpointed"] is True
    assert doc["benchmark"]["pipeline_exit"] is None
    assert doc["summary"]["elevation_count"] == 1
    assert not list(output_dir.glob("*.tmp"))


class _FakePipeline:
    def __init__(self, running=True):
        self.started = 0
        self.stopped = False
        self.stop_kwargs = None
        self._running = running
        self._exit_info = {"signal": "SIGINT", "return_code": 0}

    def set_exit_info(self, info):
        self._exit_info = info

    def start(self):
        self.started += 1

    @property
    def is_running(self):
        return self._running

    @property
    def returncode(self):
        return None

    def tail_lines(self):
        return ["pipeline line"]


    def stop_gracefully(self, **kwargs):
        self.stopped = True
        self.stop_kwargs = kwargs
        self._running = False
        return self._exit_info


def _bench_config(tmp_path, **overrides):
    values = {
        "duration_seconds": 0.05,
        "sites": ["KTLH"],
        "base_dir": str(tmp_path),
        "output_dir": str(tmp_path / "out"),
        "poll_interval_seconds": 0.01,
    }
    values.update(overrides)
    return bm.BenchmarkConfig(**values)


def test_run_benchmark_waits_configured_duration_and_records_metadata(tmp_path):
    fake = _FakePipeline()
    docs = bm.run_benchmark(_bench_config(tmp_path), pipeline=fake)

    assert fake.started == 1
    assert fake.stopped is True

    assert len(docs) == 1
    doc = docs[0]
    assert doc["site"] == "KTLH"
    assert doc["benchmark"]["requested_duration_seconds"] == 0.05
    assert doc["benchmark"]["actual_duration_seconds"] >= 0.04
    assert doc["benchmark"]["pipeline_exit"] == {"signal": "SIGINT", "return_code": 0}
    assert doc["schema_version"] == 1
    assert doc["percentile_method"] == "linear_interpolation"
    assert doc["summary"]["elevation_count"] == 0
    assert doc["pipeline_output_tail"] == ["pipeline line"]

    out_path = tmp_path / "out" / "KTLH.json"
    assert out_path.exists()
    assert json.loads(out_path.read_text(encoding="utf-8")) == doc


def test_run_benchmark_uses_uppercase_site_filename(tmp_path):
    fake = _FakePipeline(running=False)
    bm.run_benchmark(_bench_config(tmp_path, sites=["ktlh"]), pipeline=fake)

    assert (tmp_path / "out" / "KTLH.json").exists()


def test_run_benchmark_all_sites_records_sites_none(tmp_path):
    (tmp_path / "data" / "NEXRAD_Level2" / "KTLH").mkdir(parents=True, exist_ok=True)
    fake = _FakePipeline(running=False)
    docs = bm.run_benchmark(_bench_config(tmp_path, sites=None), pipeline=fake)

    assert fake.stopped is True
    assert docs[0]["configuration"]["sites"] is None


def test_build_pipeline_argv_omits_sites_for_all_sites(tmp_path):
    argv = bm._build_pipeline_argv(_bench_config(tmp_path, sites=None))
    assert argv[1] == "-c"
    assert "run_realtime_ingestion_pipeline" in argv[2]
    assert "sites" not in json.loads(argv[4])


def test_build_pipeline_argv_includes_sites_when_requested(tmp_path):
    argv = bm._build_pipeline_argv(_bench_config(tmp_path, sites=["ktlh"]))
    assert json.loads(argv[4])["sites"] == ["ktlh"]


def test_run_benchmark_records_early_exit_without_draining_duration(tmp_path):
    fake = _FakePipeline(running=False)
    started = time_module.monotonic()
    docs = bm.run_benchmark(_bench_config(tmp_path), pipeline=fake)
    elapsed = time_module.monotonic() - started

    assert fake.stopped is True
    assert elapsed < 0.03
    assert docs[0]["benchmark"]["pipeline_exit"] == {"signal": "SIGINT", "return_code": 0}


class _FakeProc:
    def __init__(self, wait_outcomes, *, already_exited=False):
        self._outcomes = list(wait_outcomes)
        self.returncode = 0 if already_exited else None
        self.already_exited = already_exited
        self.sent = []

    def poll(self):
        return self.returncode if self.already_exited else None

    def send_signal(self, sig):
        self.sent.append(sig)

    def wait(self, timeout=None):
        if not self._outcomes:
            return self.returncode
        outcome = self._outcomes.pop(0)
        if outcome == "timeout":
            raise subprocess.TimeoutExpired(cmd="fake", timeout=timeout)
        self.returncode = outcome
        return outcome

    def kill(self):
        self.sent.append("SIGKILL")


def test_pipeline_process_stop_sends_sigint_first(tmp_path):
    pipeline = bm.PipelineProcess(argv=[], cwd=tmp_path)
    pipeline._proc = _FakeProc(wait_outcomes=[0])

    exit_info = pipeline.stop_gracefully(grace_seconds=1, term_seconds=1, kill_seconds=1)

    assert pipeline._proc.sent == [signal.SIGINT]
    assert exit_info == {"signal": "SIGINT", "return_code": 0}


def test_pipeline_process_stop_falls_back_to_sigterm(tmp_path):
    pipeline = bm.PipelineProcess(argv=[], cwd=tmp_path)
    pipeline._proc = _FakeProc(wait_outcomes=["timeout", 0])

    exit_info = pipeline.stop_gracefully(grace_seconds=1, term_seconds=1, kill_seconds=1)

    assert pipeline._proc.sent == [signal.SIGINT, signal.SIGTERM]
    assert exit_info == {"signal": "SIGTERM", "return_code": 0}


def test_pipeline_process_stop_escalates_to_sigkill(tmp_path):
    pipeline = bm.PipelineProcess(argv=[], cwd=tmp_path)
    pipeline._proc = _FakeProc(wait_outcomes=["timeout", "timeout", 0])

    exit_info = pipeline.stop_gracefully(grace_seconds=1, term_seconds=1, kill_seconds=1)

    assert pipeline._proc.sent == [signal.SIGINT, signal.SIGTERM, "SIGKILL"]
    assert exit_info == {"signal": "SIGKILL", "return_code": 0}


def test_pipeline_process_stop_skips_signals_for_exited_child(tmp_path):
    pipeline = bm.PipelineProcess(argv=[], cwd=tmp_path)
    pipeline._proc = _FakeProc(wait_outcomes=[], already_exited=True)

    exit_info = pipeline.stop_gracefully(grace_seconds=1, term_seconds=1, kill_seconds=1)

    assert pipeline._proc.sent == []
    assert exit_info == {"signal": None, "return_code": 0}
    assert pipeline.terminated_by_benchmark is False
