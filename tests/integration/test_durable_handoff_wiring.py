"""Phase 2 wiring test: durable handoff published by the tandem cycle.

Drives ``run_tandem_cycle_once`` with stubbed workers and monkeypatched
downloaders, asserting that mrms-ready.json and rap-ready.json are committed
alongside the in-memory release events, that a failed phase publishes nothing,
and that republication of the same cycle is idempotent.
"""

import multiprocessing
import sys
import types
from datetime import datetime, timezone

import pytest

import common.pipeline.coordinator as coordinator
from common.ingest.manifest import staged_input_from_path
from common.ingest.mrms.downloader import DownloadBatchResult
from util.runtime.cycle import TandemCycleConfig, run_tandem_cycle_once
from util.runtime.handoff import (
    canonical_cycle_id,
    iter_committed_records,
    phase_record_path,
    read_phase_record,
    shadow_validate_phase_record,
)


DT = datetime(2026, 3, 17, 20, 0, tzinfo=timezone.utc)


def _batch(tmp_path, timestamp, product):
    path = tmp_path / "staged" / product / f"MRMS_{product}_{timestamp:%Y%m%d-%H%M%S}.grib2"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"data")
    return DownloadBatchResult(
        attempted=(product,),
        downloaded=(staged_input_from_path(product, path, source="test", family="mrms"),),
        failed=(),
    )


def _stub_worker(*args, **kwargs):
    return None


@pytest.fixture()
def stubbed_workers(monkeypatch):
    import util.runtime.cycle as cycle_module

    monkeypatch.setattr(cycle_module, "edgewarn_tandem_worker", _stub_worker)
    fake_ewmrs = types.ModuleType("EWMRS.pipeline")
    fake_ewmrs.ewmrs_tandem_worker = _stub_worker
    monkeypatch.setitem(sys.modules, "EWMRS.pipeline", fake_ewmrs)


def _config(tmp_path, *, handoff_enabled=True):
    return TandemCycleConfig(
        lat_limits=(20.0, 55.0),
        lon_limits=(230.0, 300.0),
        profile=False,
        disable_ctam=False,
        disable_ctam_modules=False,
        disable_tracking=False,
        disable_polygon_expansion=False,
        refl_threshold=20.0,
        min_seed_percentage=10.0,
        drop_offset=0.0,
        config_dir=None,
        ewmrs_enabled=True,
        goes_enabled=False,
        mrms_core_only=False,
        goes_render_wait_seconds=1.0,
        goes_render_wait_interval_seconds=0.1,
        base_dir=str(tmp_path),
        handoff_enabled=handoff_enabled,
    )


def _patch_downloaders(monkeypatch, tmp_path):
    async def fake_detection(dt, max_entries=10, remove_old_files=True):
        return _batch(tmp_path, dt, "Detection")

    def sync_detection(*args, **kwargs):
        raise RuntimeError("sync fallback must not run when async succeeded")

    async def fake_mrms_integration(dt, max_entries=10, remove_old_files=True):
        return _batch(tmp_path, dt, "Integration")

    async def fake_rap(dt):
        rap_path = tmp_path / "rap" / f"RAP.{dt:%Y%m%d-%H}z.awp130pgrbf00.grib2"
        rap_path.parent.mkdir(parents=True, exist_ok=True)
        rap_path.write_bytes(b"grib")
        return rap_path

    async def fake_uint16(rap_path, dt, log):
        return {"layer": str(rap_path)}

    monkeypatch.setattr(coordinator.mrms_ingest, "download_detection_files_async", fake_detection)
    monkeypatch.setattr(coordinator.mrms_ingest, "download_detection_files", sync_detection)
    monkeypatch.setattr(
        coordinator.mrms_ingest, "download_integration_files_async", fake_mrms_integration
    )
    monkeypatch.setattr(coordinator, "download_rap_async", fake_rap)
    monkeypatch.setattr(coordinator, "_run_rap_uint16_conversion", fake_uint16)
    # The shadow consumer resolves EWMRS render layers from configuration;
    # point them at this test's staged directories.
    def fake_layer_list():
        return [
            {"name": "DetectionLayer", "filepath": str(tmp_path / "staged" / "Detection")},
            {
                "name": "IntegrationLayer",
                "filepath": str(tmp_path / "staged" / "Integration"),
            },
        ]

    try:
        import EWMRS.render.config as ewmrs_render_config

        monkeypatch.setattr(
            ewmrs_render_config, "get_mrms_file_list", fake_layer_list
        )
    except ImportError:
        pass


def _run_cycle(tmp_path, manager):
    goes_render_task_queue = multiprocessing.Queue()
    goes_render_log_queue = multiprocessing.Queue()
    try:
        return run_tandem_cycle_once(
            DT,
            goes_render_task_queue,
            goes_render_log_queue,
            manager,
            config=_config(tmp_path),
            goes_cycle_active_event=multiprocessing.Event(),
        )
    finally:
        goes_render_task_queue.close()
        goes_render_log_queue.close()


def test_cycle_publishes_mrms_and_rap_ready_records(stubbed_workers, monkeypatch, tmp_path):
    _patch_downloaders(monkeypatch, tmp_path)
    with multiprocessing.Manager() as manager:
        _run_cycle(tmp_path, manager)

    mrms_record = read_phase_record(
        phase_record_path(tmp_path, canonical_cycle_id(DT), "mrms-ready")
    )
    rap_record = read_phase_record(
        phase_record_path(tmp_path, canonical_cycle_id(DT), "rap-ready")
    )
    assert mrms_record is not None and mrms_record.success
    assert rap_record is not None and rap_record.success
    products = {staged.product for staged in mrms_record.inputs}
    assert {"Detection", "Integration"} <= products
    # The committed exact paths are the ones actually staged this cycle, and
    # every configured layer binds to an exact pinned file.
    assert shadow_validate_phase_record(mrms_record) == ()
    assert shadow_validate_phase_record(rap_record) == ()


def test_failed_mrms_phase_publishes_no_ready_record(stubbed_workers, monkeypatch, tmp_path):
    _patch_downloaders(monkeypatch, tmp_path)

    async def failing_detection(dt, max_entries=10, remove_old_files=True):
        raise RuntimeError("S3 unavailable")

    def failing_sync(*args, **kwargs):
        raise RuntimeError("S3 unavailable")

    monkeypatch.setattr(
        coordinator.mrms_ingest, "download_detection_files_async", failing_detection
    )
    monkeypatch.setattr(coordinator.mrms_ingest, "download_detection_files", failing_sync)

    with multiprocessing.Manager() as manager:
        outcome = _run_cycle(tmp_path, manager)

    assert outcome.completed is False
    # The MRMS phase failed, so no successful mrms-ready record may exist,
    # but the independently validated RAP phase still commits its record.
    assert all(record is None for _, record in iter_committed_records(tmp_path, "mrms-ready"))
    rap_records = iter_committed_records(tmp_path, "rap-ready")
    assert any(record is not None and record.success for _, record in rap_records)


def test_missing_rap_source_publishes_no_rap_ready_record(stubbed_workers, monkeypatch, tmp_path):
    _patch_downloaders(monkeypatch, tmp_path)

    async def missing_rap(dt):
        return tmp_path / "rap" / "does-not-exist.grib2"

    monkeypatch.setattr(coordinator, "download_rap_async", missing_rap)

    with multiprocessing.Manager() as manager:
        _run_cycle(tmp_path, manager)

    # MRMS rendering may proceed, but the raw-RAP phase was not validated, so
    # no successful rap-ready record may exist.
    assert all(record is None for _, record in iter_committed_records(tmp_path, "rap-ready"))


def test_republication_of_same_cycle_is_idempotent(stubbed_workers, monkeypatch, tmp_path):
    _patch_downloaders(monkeypatch, tmp_path)
    with multiprocessing.Manager() as manager:
        _run_cycle(tmp_path, manager)
        _run_cycle(tmp_path, manager)

    records = iter_committed_records(tmp_path, "mrms-ready")
    assert [cycle_id for cycle_id, _ in records] == [canonical_cycle_id(DT)]
    record_dir = tmp_path / "state" / "realtime" / "cycles" / canonical_cycle_id(DT)
    assert sorted(p.name for p in record_dir.iterdir()) == [
        "mrms-ready.json",
        "rap-ready.json",
    ]


def test_disabled_handoff_publishes_nothing(stubbed_workers, monkeypatch, tmp_path):
    _patch_downloaders(monkeypatch, tmp_path)
    goes_render_task_queue = multiprocessing.Queue()
    goes_render_log_queue = multiprocessing.Queue()
    try:
        with multiprocessing.Manager() as manager:
            run_tandem_cycle_once(
                DT,
                goes_render_task_queue,
                goes_render_log_queue,
                manager,
                config=_config_with_handoff_disabled(tmp_path),
                goes_cycle_active_event=multiprocessing.Event(),
            )
    finally:
        goes_render_task_queue.close()
        goes_render_log_queue.close()

    assert iter_committed_records(tmp_path, "mrms-ready") == []
    assert iter_committed_records(tmp_path, "rap-ready") == []
    assert not (tmp_path / "state" / "realtime").exists()


def _config_with_handoff_disabled(tmp_path):
    config = _config(tmp_path)
    return type(config)(
        **{**config.__dict__, "handoff_enabled": False}
    )
