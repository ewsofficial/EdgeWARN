import gzip
import json
import os
from datetime import datetime, timezone

import EWMRS.pipeline as ewmrs_pipeline
from common.ingest.manifest import CycleInputManifest
from common.ingest.manifest import staged_input_from_path


class _FakeFuture:
    def __init__(self, result):
        self._result = result

    def result(self):
        return self._result


class _FakeExecutor:
    def __init__(self, max_workers, initializer=None):
        self.max_workers = max_workers
        self.initializer = initializer
        self.futures = []

    def __enter__(self):
        if self.initializer is not None:
            self.initializer()
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def submit(self, func, layer):
        future = _FakeFuture(func(layer))
        self.futures.append(future)
        return future


class _FakeQueue:
    def __init__(self):
        self.messages = []

    def put(self, message):
        self.messages.append(message)


class _FakeEvent:
    def __init__(self):
        self.wait_calls = 0

    def wait(self):
        self.wait_calls += 1


def _manifest(dt):
    return CycleInputManifest(cycle_time=dt)


def test_run_render_pipeline_collects_layer_results(monkeypatch):
    dt = datetime(2026, 3, 17, 20, 0, tzinfo=timezone.utc)
    created = {}
    cleanup_calls = []
    layers = [
        {"name": "LayerOne"},
        {"name": "LayerTwo"},
    ]

    monkeypatch.setattr(ewmrs_pipeline, "get_file_list", lambda: layers)
    monkeypatch.setattr(ewmrs_pipeline, "cleanup_old_gui_files", lambda max_age_minutes: cleanup_calls.append(max_age_minutes))
    monkeypatch.setattr(ewmrs_pipeline, "_render_layer", lambda layer: (layer["name"], [f"{layer['name']}.png"]))
    monkeypatch.setattr(
        "concurrent.futures.ProcessPoolExecutor",
        lambda max_workers, initializer=None: created.setdefault("executor", _FakeExecutor(max_workers=max_workers, initializer=initializer)),
    )
    monkeypatch.setattr("concurrent.futures.as_completed", lambda futures: list(futures))

    results = ewmrs_pipeline.run_render_pipeline(dt)
    fake_executor = created["executor"]

    assert results == {
        "LayerOne": ["LayerOne.png"],
        "LayerTwo": ["LayerTwo.png"],
    }
    assert fake_executor.max_workers == 2
    assert fake_executor.initializer is ewmrs_pipeline._worker_initializer
    assert cleanup_calls == [120]


def test_run_render_pipeline_binds_manifest_path_before_worker_submit(
    monkeypatch,
    tmp_path,
):
    dt = datetime(2026, 3, 17, 20, 0, tzinfo=timezone.utc)
    source_dir = tmp_path / "source"
    source_dir.mkdir()
    pinned = source_dir / "MRMS_Test_20260317-200000.grib2"
    newer = source_dir / "MRMS_Test_20260317-200200.grib2"
    pinned.write_bytes(b"pinned")
    newer.write_bytes(b"newer")
    manifest = CycleInputManifest(
        cycle_time=dt,
        inputs=(
            staged_input_from_path(
                "Test",
                pinned,
                source="test",
                family="mrms",
            ),
        ),
    )
    submitted = []

    monkeypatch.setattr(
        ewmrs_pipeline,
        "_render_layer",
        lambda layer: submitted.append(layer) or (layer["name"], ["tile.png"]),
    )
    monkeypatch.setattr(
        "concurrent.futures.ProcessPoolExecutor",
        lambda max_workers, initializer=None: _FakeExecutor(
            max_workers=max_workers,
            initializer=initializer,
        ),
    )
    monkeypatch.setattr(
        "concurrent.futures.as_completed",
        lambda futures: list(futures),
    )
    monkeypatch.setattr(
        ewmrs_pipeline,
        "cleanup_old_gui_files",
        lambda **_kwargs: None,
    )

    ewmrs_pipeline.run_render_pipeline(
        dt,
        layers=[
            {
                "name": "Test",
                "filepath": source_dir,
                "outdir": tmp_path / "gui",
            }
        ],
        input_manifest=manifest,
    )

    assert submitted[0]["input_path"] == str(pinned)
    assert submitted[0]["input_manifest_bound"] is True


def test_run_goes_render_pipeline_is_explicit_no_op_without_layers(monkeypatch):
    monkeypatch.setattr(ewmrs_pipeline, "get_goes_file_list", lambda: [])

    results = ewmrs_pipeline.run_goes_render_pipeline(datetime(2026, 3, 17, 20, 0, tzinfo=timezone.utc))

    assert results == {}


def test_run_goes_render_pipeline_processes_configured_layers(monkeypatch):
    dt = datetime(2026, 3, 17, 20, 0, tzinfo=timezone.utc)
    captured = {}

    monkeypatch.setattr(
        ewmrs_pipeline,
        "get_goes_file_list",
        lambda: [{"name": "GOES_ABI_C02_Reflectance", "source_type": "goes_abi"}],
    )

    def fake_run_render_pipeline(
        dt_arg,
        max_entries=10,
        layers=None,
        phase_name="EWMRS",
        cleanup_after=True,
        input_manifest=None,
    ):
        captured["dt"] = dt_arg
        captured["max_entries"] = max_entries
        captured["layers"] = list(layers or [])
        captured["phase_name"] = phase_name
        captured["cleanup_after"] = cleanup_after
        return {"GOES_ABI_C02_Reflectance": ["tile_0_0.png"]}

    monkeypatch.setattr(ewmrs_pipeline, "run_render_pipeline", fake_run_render_pipeline)

    results = ewmrs_pipeline.run_goes_render_pipeline(dt, max_entries=4)

    assert results == {"GOES_ABI_C02_Reflectance": ["tile_0_0.png"]}
    assert captured["dt"] == dt
    assert captured["max_entries"] == 4
    assert captured["phase_name"] == "GOES"
    assert captured["cleanup_after"] is False
    assert captured["layers"] == [{"name": "GOES_ABI_C02_Reflectance", "source_type": "goes_abi"}]


def _chunk_format():
    return {"version": 2, "encoding": "float16", "file_suffix": ".f16.gz", "compression": "gzip", "channels": 1, "value_kind": "scalar", "no_data": "nan", "bytes_per_component": 2, "pixel_row_order": "top_to_bottom", "grid_origin": "bottom_left"}


def _write_chunk(path, tile_size=350):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(gzip.compress(bytes(tile_size * tile_size * 2), mtime=0))


def test_current_render_paths_returns_sparse_cached_chunks(tmp_path):
    out_dir = tmp_path / "gui"
    tile_dir = out_dir / "20260317-200000"
    (tile_dir / "chunks").mkdir(parents=True)

    for chunk_name in ("chunk_1_0.f16.gz", "chunk_0_0.f16.gz", "chunk_5_3.f16.gz"):
        _write_chunk(tile_dir / "chunks" / chunk_name)
    (tile_dir / "index.json").write_text(json.dumps({
        "schema_version": 2, "timestamp": "20260317-200000", "representation": "binary_chunks", "chunk_format": _chunk_format(),
        "chunks": [[1, 0], [0, 0], [5, 3]],
        "tile_grid": {"rows": 10, "cols": 20, "tile_size": 350},
    }))

    (out_dir / "index.json").write_text(json.dumps({
        "schema_version": 2, "timestamps": ["20260317-200000"], "representation": "binary_chunks", "chunk_format": {**_chunk_format(), "media_type": "application/octet-stream"},
        "tile_grid": {"rows": 10, "cols": 20, "tile_size": 350},
    }))

    paths = ewmrs_pipeline._current_render_paths(out_dir, "2026-03-17T20:00:00")

    assert paths == [
        tile_dir / "chunks" / "chunk_0_0.f16.gz",
        tile_dir / "chunks" / "chunk_1_0.f16.gz",
        tile_dir / "chunks" / "chunk_5_3.f16.gz",
    ]


def test_current_render_paths_accepts_valid_zero_tile_timestamp(tmp_path):
    out_dir = tmp_path / "gui"
    tile_dir = out_dir / "20260317-200000"
    (tile_dir / "chunks").mkdir(parents=True)
    (tile_dir / "index.json").write_text(json.dumps({
        "schema_version": 2, "timestamp": "20260317-200000", "representation": "binary_chunks", "chunk_format": _chunk_format(), "chunks": [],
        "tile_grid": {"rows": 10, "cols": 20, "tile_size": 350},
    }))
    (out_dir / "index.json").write_text(json.dumps({
        "schema_version": 2, "timestamps": ["20260317-200000"], "representation": "binary_chunks", "chunk_format": {**_chunk_format(), "media_type": "application/octet-stream"},
        "tile_grid": {"rows": 10, "cols": 20, "tile_size": 350},
    }))

    paths = ewmrs_pipeline._current_render_paths(out_dir, "2026-03-17T20:00:00")

    assert paths == []


def test_current_render_paths_rejects_invalid_chunk_index(tmp_path):
    out_dir = tmp_path / "gui"
    tile_dir = out_dir / "20260317-200000"
    (tile_dir / "chunks").mkdir(parents=True)
    _write_chunk(tile_dir / "chunks" / "chunk_0_0.f16.gz")
    (tile_dir / "index.json").write_text(json.dumps({
        "schema_version": 2, "timestamp": "20260317-200000", "representation": "binary_chunks", "chunk_format": _chunk_format(), "chunks": [[0, 0], [20, 0]],
        "tile_grid": {"rows": 10, "cols": 20, "tile_size": 350},
    }))
    (out_dir / "index.json").write_text(json.dumps({
        "schema_version": 2, "timestamps": ["20260317-200000"], "representation": "binary_chunks", "chunk_format": {**_chunk_format(), "media_type": "application/octet-stream"},
        "tile_grid": {"rows": 10, "cols": 20, "tile_size": 350},
    }))

    paths = ewmrs_pipeline._current_render_paths(out_dir, "2026-03-17T20:00:00")

    assert paths is None
    assert ewmrs_pipeline._summarize_results({"A": [], "B": None}) == "1/2 layers succeeded"


def test_cleanup_old_gui_files_uses_dynamic_render_configuration(monkeypatch, tmp_path):
    stale_dir = tmp_path / "stale"
    active_dir = tmp_path / "active"
    stale_dir.mkdir()
    active_dir.mkdir()

    old_timestamp = "20260317-180000"
    active_timestamp_dir = active_dir / old_timestamp
    active_timestamp_dir.mkdir()
    (active_timestamp_dir / "tile_0_0.png").write_bytes(b"tile")
    (active_dir / "index.json").write_text(json.dumps({"timestamps": [old_timestamp]}))

    monkeypatch.setattr(
        ewmrs_pipeline,
        "get_file_list",
        lambda: [{"outdir": active_dir}],
    )
    monkeypatch.setattr(
        ewmrs_pipeline,
        "file_list",
        [{"outdir": stale_dir}],
        raising=False,
    )

    ewmrs_pipeline.cleanup_old_gui_files(max_age_minutes=0)

    assert not active_timestamp_dir.exists()
    assert json.loads((active_dir / "index.json").read_text()) == []


def test_cleanup_old_gui_files_prunes_old_nexrad_site_timestamps(monkeypatch, tmp_path):
    active_dir = tmp_path / "active"
    nexrad_root = tmp_path / "gui" / "NEXRAD"
    stale_elevation_dir = nexrad_root / "KTLH" / "0.5"
    fresh_elevation_dir = nexrad_root / "KTLH" / "0.9"
    empty_site_elevation_dir = nexrad_root / "KJAX" / "0.5"

    active_dir.mkdir()
    stale_elevation_dir.mkdir(parents=True)
    fresh_elevation_dir.mkdir(parents=True)
    empty_site_elevation_dir.mkdir(parents=True)

    stale_file = stale_elevation_dir / "KTLH_DBZH_0.5_20260317-170000.bin.gz"
    fresh_file = fresh_elevation_dir / "KTLH_DBZH_0.9_20260317-193000.bin.gz"
    empty_site_file = empty_site_elevation_dir / "KJAX_DBZH_0.5_20260317-160000.bin.gz"
    stale_file.write_bytes(b"stale")
    fresh_file.write_bytes(b"fresh")
    empty_site_file.write_bytes(b"empty-site")

    now = 1_800_000_000
    stale_mtime = now - (3 * 60 * 60)
    fresh_mtime = now - (30 * 60)
    os.utime(stale_file, (stale_mtime, stale_mtime))
    os.utime(empty_site_file, (stale_mtime, stale_mtime))
    os.utime(fresh_file, (fresh_mtime, fresh_mtime))

    monkeypatch.setattr(ewmrs_pipeline, "get_file_list", lambda: [{"outdir": active_dir}])
    monkeypatch.setattr(ewmrs_pipeline.fs, "GUI_NEXRAD_DIR", nexrad_root)
    monkeypatch.setattr(ewmrs_pipeline.time, "time", lambda: now)

    ewmrs_pipeline.cleanup_old_gui_files(max_age_minutes=120)

    assert not stale_elevation_dir.exists()
    assert fresh_elevation_dir.exists()
    assert not (nexrad_root / "KJAX").exists()


def test_render_pending_nexrad_gui_files_skips_existing_same_timestamp(monkeypatch, tmp_path):
    nexrad_root = tmp_path / "data" / "NEXRAD_Level2" / "KTLH" / "0.5"
    gui_root = tmp_path / "gui" / "NEXRAD"
    artifact_path = nexrad_root / "KTLH_0.5_20260507-150001.nc"
    artifact_path.parent.mkdir(parents=True, exist_ok=True)
    artifact_path.write_bytes(b"artifact")
    artifact_path.with_suffix(".json").write_text(json.dumps({
        "site": "KTLH",
        "volume_id": "999",
        "scan_timestamp": "20260507-150000",
        "elevation": "0.5",
        "elevation_timestamp": "20260507-150001",
    }))
    existing_output = gui_root / "KTLH" / "0.5" / "KTLH_DBZH_0.5_20260507-150001.bin.gz"
    existing_output.parent.mkdir(parents=True, exist_ok=True)
    existing_output.write_bytes(b"existing")

    calls = []
    monkeypatch.setattr(ewmrs_pipeline.fs, "NEXRAD_LEVEL2_DIR", tmp_path / "data" / "NEXRAD_Level2")
    monkeypatch.setattr(ewmrs_pipeline.fs, "GUI_NEXRAD_DIR", gui_root)
    monkeypatch.setattr("EWMRS.render.nexrad.serialize_nexrad_elevation_artifacts", lambda *args, **kwargs: calls.append((args, kwargs)))

    rendered = ewmrs_pipeline.render_pending_nexrad_gui_files()

    assert rendered == 0
    assert calls == []


def test_render_pending_nexrad_gui_files_skips_stale_source_artifacts(monkeypatch, tmp_path):
    nexrad_root = tmp_path / "data" / "NEXRAD_Level2" / "KTLH" / "0.5"
    gui_root = tmp_path / "gui" / "NEXRAD"
    artifact_path = nexrad_root / "KTLH_0.5_20260507-150001.nc"
    artifact_path.parent.mkdir(parents=True, exist_ok=True)
    artifact_path.write_bytes(b"artifact")
    artifact_path.with_suffix(".json").write_text(json.dumps({
        "site": "KTLH",
        "volume_id": "999",
        "scan_timestamp": "20260507-150000",
        "elevation": "0.5",
        "elevation_timestamp": "20260507-150001",
    }))

    calls = []
    now = 1_800_000_000
    stale_mtime = now - (3 * 60 * 60)
    os.utime(artifact_path, (stale_mtime, stale_mtime))
    monkeypatch.setattr(ewmrs_pipeline.fs, "NEXRAD_LEVEL2_DIR", tmp_path / "data" / "NEXRAD_Level2")
    monkeypatch.setattr(ewmrs_pipeline.fs, "GUI_NEXRAD_DIR", gui_root)
    monkeypatch.setattr(ewmrs_pipeline.time, "time", lambda: now)
    monkeypatch.setattr("EWMRS.render.nexrad.serialize_nexrad_elevation_artifacts", lambda *args, **kwargs: calls.append((args, kwargs)))

    rendered = ewmrs_pipeline.render_pending_nexrad_gui_files()

    assert rendered == 0
    assert calls == []


def test_render_pending_nexrad_gui_files_normalizes_iso_sidecar_timestamps(monkeypatch, tmp_path):
    nexrad_root = tmp_path / "data" / "NEXRAD_Level2" / "KTLH" / "0.5"
    gui_root = tmp_path / "gui" / "NEXRAD"
    artifact_path = nexrad_root / "KTLH_0.5_20260519-132157.nc"
    artifact_path.parent.mkdir(parents=True, exist_ok=True)
    artifact_path.write_bytes(b"artifact")
    artifact_path.with_suffix(".json").write_text(json.dumps({
        "site": "KTLH",
        "volume_id": "999",
        "scan_timestamp": "2026-05-19T13:21:57Z",
        "elevation": "0.5",
        "elevation_timestamp": "2026-05-19T13:21:57Z",
    }))

    captured = []

    def _fake_serialize(site, volume_id, scan_timestamp, artifacts):
        captured.append((site, volume_id, scan_timestamp, artifacts[0].elevation_timestamp))
        out_dir = gui_root / site / artifacts[0].elevation
        out_dir.mkdir(parents=True, exist_ok=True)
        (out_dir / f"{site}_DBZH_{artifacts[0].elevation}_{artifacts[0].elevation_timestamp}.bin.gz").write_bytes(b"rendered")

    monkeypatch.setattr(ewmrs_pipeline.fs, "NEXRAD_LEVEL2_DIR", tmp_path / "data" / "NEXRAD_Level2")
    monkeypatch.setattr(ewmrs_pipeline.fs, "GUI_NEXRAD_DIR", gui_root)
    monkeypatch.setattr("EWMRS.render.nexrad.serialize_nexrad_elevation_artifacts", _fake_serialize)

    rendered = ewmrs_pipeline.render_pending_nexrad_gui_files()

    assert rendered == 1
    assert captured == [("KTLH", "999", "20260519-132157", "20260519-132157")]


def test_render_pending_nexrad_gui_files_uses_eight_workers(monkeypatch, tmp_path):
    gui_root = tmp_path / "gui" / "NEXRAD"
    nexrad_root = tmp_path / "data" / "NEXRAD_Level2"
    created = {}

    metadata_items = []
    for index in range(10):
        timestamp = f"20260507-1500{index:02d}"
        artifact_path = nexrad_root / f"K{index:03d}" / "0.5" / f"K{index:03d}_0.5_{timestamp}.nc"
        artifact_path.parent.mkdir(parents=True, exist_ok=True)
        artifact_path.write_bytes(b"artifact")
        metadata_items.append(
            {
                "site": f"K{index:03d}",
                "volume_id": str(index),
                "scan_timestamp": timestamp,
                "elevation": "0.5",
                "elevation_timestamp": timestamp,
                "artifact_path": str(artifact_path),
                "member_group_names": [],
                "member_sweeps": [],
            }
        )

    monkeypatch.setattr(ewmrs_pipeline.fs, "NEXRAD_LEVEL2_DIR", nexrad_root)
    monkeypatch.setattr(ewmrs_pipeline.fs, "GUI_NEXRAD_DIR", gui_root)
    monkeypatch.setattr(ewmrs_pipeline, "_iter_latest_nexrad_artifacts", lambda: metadata_items)
    monkeypatch.setattr(ewmrs_pipeline, "_nexrad_source_artifact_is_fresh", lambda *args, **kwargs: True)
    monkeypatch.setattr(ewmrs_pipeline, "_nexrad_gui_timestamp_exists", lambda *args, **kwargs: False)
    monkeypatch.setattr(ewmrs_pipeline, "_render_pending_nexrad_gui_artifact", lambda metadata: True)
    monkeypatch.setattr(
        "concurrent.futures.ThreadPoolExecutor",
        lambda max_workers: created.setdefault("executor", _FakeExecutor(max_workers=max_workers)),
    )
    monkeypatch.setattr("concurrent.futures.as_completed", lambda futures: list(futures))

    rendered = ewmrs_pipeline.render_pending_nexrad_gui_files()

    assert rendered == 10
    assert created["executor"].max_workers == 8


def test_render_pending_nexrad_gui_files_renders_all_fresh_source_timestamps(monkeypatch, tmp_path):
    nexrad_root = tmp_path / "data" / "NEXRAD_Level2" / "KTLH" / "0.5"
    gui_root = tmp_path / "gui" / "NEXRAD"
    rendered_timestamps = []

    for timestamp in ("20260507-150001", "20260507-150011", "20260507-150021"):
        artifact_path = nexrad_root / f"KTLH_0.5_{timestamp}.nc"
        artifact_path.parent.mkdir(parents=True, exist_ok=True)
        artifact_path.write_bytes(b"artifact")
        artifact_path.with_suffix(".json").write_text(json.dumps({
            "site": "KTLH",
            "volume_id": "999",
            "scan_timestamp": timestamp,
            "elevation": "0.5",
            "elevation_timestamp": timestamp,
        }))

    def _fake_serialize(site, volume_id, scan_timestamp, artifacts):
        artifact = artifacts[0]
        rendered_timestamps.append(artifact.elevation_timestamp)
        out_dir = gui_root / site / artifact.elevation
        out_dir.mkdir(parents=True, exist_ok=True)
        (out_dir / f"{site}_DBZH_{artifact.elevation}_{artifact.elevation_timestamp}.bin.gz").write_bytes(b"rendered")

    monkeypatch.setattr(ewmrs_pipeline.fs, "NEXRAD_LEVEL2_DIR", tmp_path / "data" / "NEXRAD_Level2")
    monkeypatch.setattr(ewmrs_pipeline.fs, "GUI_NEXRAD_DIR", gui_root)
    monkeypatch.setattr("EWMRS.render.nexrad.serialize_nexrad_elevation_artifacts", _fake_serialize)

    rendered = ewmrs_pipeline.render_pending_nexrad_gui_files()

    assert rendered == 3
    assert rendered_timestamps == ["20260507-150001", "20260507-150011", "20260507-150021"]


def test_ewmrs_tandem_worker_runs_mrms_and_skips_goes_when_only_mrms_ready(monkeypatch):
    queue = _FakeQueue()
    mrms_ready_event = _FakeEvent()
    dt = datetime(2026, 3, 17, 20, 0, tzinfo=timezone.utc)
    shared_state = {
        "ewmrs_mrms_inputs_ready": True,
        "ewmrs_goes_inputs_ready": False,
        "input_manifest": _manifest(dt).as_dict(),
    }
    mrms_calls = []
    goes_calls = []

    monkeypatch.setattr(
        ewmrs_pipeline,
        "run_mrms_render_pipeline",
        lambda *args, **kwargs: mrms_calls.append((args, kwargs)) or {"LayerOne": ["LayerOne.png"]},
    )
    monkeypatch.setattr(
        ewmrs_pipeline,
        "run_goes_render_pipeline",
        lambda *args, **kwargs: goes_calls.append((args, kwargs)) or {},
    )

    ewmrs_pipeline.ewmrs_tandem_worker(
        queue,
        shared_state,
        mrms_ready_event,
        dt,
    )

    assert mrms_ready_event.wait_calls == 1
    assert len(mrms_calls) == 1
    assert goes_calls == []
    assert shared_state["ewmrs_stage"] == {
        "status": "completed",
        "produced_artifacts": ["LayerOne.png"],
        "errors": [],
    }
    assert any("Starting EWMRS MRMS render phase" in message for message in queue.messages)
    assert any("GOES render is decoupled" in message for message in queue.messages)


def test_ewmrs_tandem_worker_skips_mrms_when_inputs_not_ready(monkeypatch):
    queue = _FakeQueue()
    mrms_ready_event = _FakeEvent()
    shared_state = {"ewmrs_mrms_inputs_ready": False, "ewmrs_goes_inputs_ready": True}
    mrms_calls = []

    monkeypatch.setattr(
        ewmrs_pipeline,
        "run_mrms_render_pipeline",
        lambda *args, **kwargs: mrms_calls.append((args, kwargs)) or {},
    )

    ewmrs_pipeline.ewmrs_tandem_worker(
        queue,
        shared_state,
        mrms_ready_event,
        datetime(2026, 3, 17, 20, 0, tzinfo=timezone.utc),
    )

    assert mrms_ready_event.wait_calls == 1
    assert mrms_calls == []
    assert shared_state["ewmrs_stage"]["status"] == "unavailable"
    assert any("skipping MRMS render" in message for message in queue.messages)


def test_ewmrs_tandem_worker_runs_only_mrms_phase_when_inputs_ready(monkeypatch):
    queue = _FakeQueue()
    mrms_ready_event = _FakeEvent()
    dt = datetime(2026, 3, 17, 20, 0, tzinfo=timezone.utc)
    shared_state = {
        "ewmrs_mrms_inputs_ready": True,
        "ewmrs_goes_inputs_ready": True,
        "input_manifest": _manifest(dt).as_dict(),
    }
    captured = {}

    def fake_run_mrms_render_pipeline(dt, max_entries=10, input_manifest=None):
        captured["mrms_dt"] = dt
        captured["mrms_max_entries"] = max_entries
        return {"LayerOne": ["LayerOne.png"], "LayerTwo": None}

    monkeypatch.setattr(ewmrs_pipeline, "run_mrms_render_pipeline", fake_run_mrms_render_pipeline)

    ewmrs_pipeline.ewmrs_tandem_worker(queue, shared_state, mrms_ready_event, dt, max_entries=3)

    assert mrms_ready_event.wait_calls == 1
    assert captured == {
        "mrms_dt": dt,
        "mrms_max_entries": 3,
    }
    assert shared_state["ewmrs_stage"]["status"] == "failed"
    assert "LayerTwo" in shared_state["ewmrs_stage"]["errors"][0]
    assert shared_state["ewmrs_stage"]["produced_artifacts"] == ["LayerOne.png"]
    assert any("Starting EWMRS MRMS render phase" in message for message in queue.messages)
    assert any("1/2 layers succeeded" in message for message in queue.messages)
    assert any("GOES render is decoupled" in message for message in queue.messages)


def test_ewmrs_goes_worker_runs_goes_phase(monkeypatch):
    queue = _FakeQueue()
    captured = {}

    def fake_run_goes_render_pipeline(dt, max_entries=10, input_manifest=None):
        captured["goes_dt"] = dt
        captured["goes_max_entries"] = max_entries
        return {}

    monkeypatch.setattr(ewmrs_pipeline, "run_goes_render_pipeline", fake_run_goes_render_pipeline)

    dt = datetime(2026, 3, 17, 20, 0, tzinfo=timezone.utc)
    ewmrs_pipeline.ewmrs_goes_worker(
        queue,
        dt,
        max_entries=3,
        input_manifest=_manifest(dt),
    )

    assert captured == {
        "goes_dt": dt,
        "goes_max_entries": 3,
    }
    assert any("Starting EWMRS GOES render phase" in message for message in queue.messages)
    assert any("0/0 layers succeeded" in message for message in queue.messages)
