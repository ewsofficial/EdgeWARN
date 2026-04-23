import json
from datetime import datetime, timezone

import EWMRS.pipeline as ewmrs_pipeline
from EWMRS.render.config import get_goes_rgb_file_list


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

    def fake_run_render_pipeline(dt_arg, max_entries=10, layers=None, phase_name="EWMRS", cleanup_after=True):
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


def test_run_goes_render_pipeline_writes_all_rgb_products(monkeypatch, tmp_path):
    dt = datetime(2026, 3, 17, 20, 0, tzinfo=timezone.utc)
    created = {}
    cleanup_calls = []
    timestamp_iso = "2026-03-17T20:00:00"
    (tmp_path / "ABI_RadC").mkdir()

    rgb_layers = []
    for layer in get_goes_rgb_file_list():
        rgb_layers.append({**layer, "filepath": tmp_path / "ABI_RadC", "outdir": tmp_path / layer["name"]})

    monkeypatch.setattr(ewmrs_pipeline, "get_goes_file_list", lambda: rgb_layers)
    monkeypatch.setattr(ewmrs_pipeline, "cleanup_old_gui_files", lambda max_age_minutes: cleanup_calls.append(max_age_minutes))
    monkeypatch.setattr(
        "EWMRS.render.goes_rgb.prepare_goes_rgb_batch",
        lambda layers, max_offset_minutes=20.0, requested_timestamp=None: {
            "timestamp_iso": timestamp_iso,
            "recipes": [
                {
                    "layer": layer,
                    "recipe_key": layer["recipe_key"],
                    "recipe": type("Recipe", (), {"display_name": layer["recipe_key"], "required_channels": ("C02",)})(),
                    "timestamp_iso": timestamp_iso,
                    "timestamp": dt,
                    "selected_files": {"C02": tmp_path / "c02.nc"},
                }
                for layer in layers
            ],
            "selected_files": {"C02": tmp_path / "c02.nc"},
        },
    )
    monkeypatch.setattr(
        "EWMRS.render.goes_rgb.iter_goes_rgb_batch",
        lambda prepared_batch, web_mercator_shape, web_mercator_transform, true_color_gamma=2.2: [
            (
                prepared["layer"]["name"],
                __import__("numpy").zeros((700, 700, 4), dtype=__import__("numpy").uint8),
                {"selected_files": {"C02": str(tmp_path / "c02.nc")}, "timestamp_iso": timestamp_iso},
            )
            for prepared in prepared_batch["recipes"]
        ],
    )

    results = ewmrs_pipeline.run_goes_render_pipeline(dt)

    assert len(results) == 6
    assert cleanup_calls == [120]
    for layer in rgb_layers:
        out_dir = layer["outdir"]
        index_data = json.loads((out_dir / "index.json").read_text())
        assert index_data["timestamps"] == ["20260317-200000"]
        assert index_data["tile_grid"] == {"rows": 2, "cols": 2, "tile_size": 350}
        assert len(list((out_dir / "20260317-200000").glob("tile_*.png"))) == 4


def test_current_render_paths_returns_cached_tiles_when_complete(tmp_path):
    out_dir = tmp_path / "gui"
    tile_dir = out_dir / "20260317-200000"
    tile_dir.mkdir(parents=True)

    for tile_y in range(10):
        for tile_x in range(20):
            (tile_dir / f"tile_{tile_x}_{tile_y}.png").write_bytes(b"tile")

    (out_dir / "index.json").write_text(json.dumps({"timestamps": ["20260317-200000"]}))

    paths = ewmrs_pipeline._current_render_paths(out_dir, "2026-03-17T20:00:00")

    assert paths is not None
    assert len(paths) == 200
    assert paths[0].name == "tile_0_0.png"


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


def test_ewmrs_tandem_worker_runs_mrms_and_skips_goes_when_only_mrms_ready(monkeypatch):
    queue = _FakeQueue()
    mrms_ready_event = _FakeEvent()
    goes_ready_event = _FakeEvent()
    shared_state = {
        "ewmrs_mrms_inputs_ready": True,
        "ewmrs_goes_inputs_ready": False,
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
        goes_ready_event,
        datetime(2026, 3, 17, 20, 0, tzinfo=timezone.utc),
    )

    assert mrms_ready_event.wait_calls == 1
    assert goes_ready_event.wait_calls == 0
    assert len(mrms_calls) == 1
    assert goes_calls == []
    assert any("Starting EWMRS MRMS render phase" in message for message in queue.messages)
    assert any("GOES render is decoupled" in message for message in queue.messages)


def test_ewmrs_tandem_worker_skips_mrms_when_inputs_not_ready(monkeypatch):
    queue = _FakeQueue()
    mrms_ready_event = _FakeEvent()
    goes_ready_event = _FakeEvent()
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
        goes_ready_event,
        datetime(2026, 3, 17, 20, 0, tzinfo=timezone.utc),
    )

    assert mrms_ready_event.wait_calls == 1
    assert goes_ready_event.wait_calls == 0
    assert mrms_calls == []
    assert any("skipping MRMS render" in message for message in queue.messages)


def test_ewmrs_tandem_worker_runs_only_mrms_phase_when_inputs_ready(monkeypatch):
    queue = _FakeQueue()
    mrms_ready_event = _FakeEvent()
    goes_ready_event = _FakeEvent()
    shared_state = {"ewmrs_mrms_inputs_ready": True, "ewmrs_goes_inputs_ready": True}
    captured = {}

    def fake_run_mrms_render_pipeline(dt, max_entries=10):
        captured["mrms_dt"] = dt
        captured["mrms_max_entries"] = max_entries
        return {"LayerOne": ["LayerOne.png"], "LayerTwo": None}

    monkeypatch.setattr(ewmrs_pipeline, "run_mrms_render_pipeline", fake_run_mrms_render_pipeline)

    dt = datetime(2026, 3, 17, 20, 0, tzinfo=timezone.utc)
    ewmrs_pipeline.ewmrs_tandem_worker(queue, shared_state, mrms_ready_event, goes_ready_event, dt, max_entries=3)

    assert mrms_ready_event.wait_calls == 1
    assert goes_ready_event.wait_calls == 0
    assert captured == {
        "mrms_dt": dt,
        "mrms_max_entries": 3,
    }
    assert any("Starting EWMRS MRMS render phase" in message for message in queue.messages)
    assert any("1/2 layers succeeded" in message for message in queue.messages)
    assert any("GOES render is decoupled" in message for message in queue.messages)


def test_ewmrs_goes_worker_runs_goes_phase(monkeypatch):
    queue = _FakeQueue()
    captured = {}

    def fake_run_goes_render_pipeline(dt, max_entries=10):
        captured["goes_dt"] = dt
        captured["goes_max_entries"] = max_entries
        return {}

    monkeypatch.setattr(ewmrs_pipeline, "run_goes_render_pipeline", fake_run_goes_render_pipeline)

    dt = datetime(2026, 3, 17, 20, 0, tzinfo=timezone.utc)
    ewmrs_pipeline.ewmrs_goes_worker(queue, dt, max_entries=3)

    assert captured == {
        "goes_dt": dt,
        "goes_max_entries": 3,
    }
    assert any("Starting EWMRS GOES render phase" in message for message in queue.messages)
    assert any("0/0 layers succeeded" in message for message in queue.messages)
