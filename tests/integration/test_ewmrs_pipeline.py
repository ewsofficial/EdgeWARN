from datetime import datetime, timezone

import EWMRS.pipeline as ewmrs_pipeline


class _FakeFuture:
    def __init__(self, result):
        self._result = result

    def result(self):
        return self._result


class _FakeExecutor:
    def __init__(self, max_workers):
        self.max_workers = max_workers
        self.futures = []

    def __enter__(self):
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
    fake_executor = _FakeExecutor(max_workers=4)
    cleanup_calls = []
    layers = [
        {"name": "LayerOne"},
        {"name": "LayerTwo"},
    ]

    monkeypatch.setattr(ewmrs_pipeline, "get_file_list", lambda: layers)
    monkeypatch.setattr(ewmrs_pipeline, "cleanup_old_gui_files", lambda max_age_minutes: cleanup_calls.append(max_age_minutes))
    monkeypatch.setattr(ewmrs_pipeline, "_render_layer", lambda layer: (layer["name"], [f"{layer['name']}.png"]))
    monkeypatch.setattr("concurrent.futures.ProcessPoolExecutor", lambda max_workers: fake_executor)
    monkeypatch.setattr("concurrent.futures.as_completed", lambda futures: list(futures))

    results = ewmrs_pipeline.run_render_pipeline(dt)

    assert results == {
        "LayerOne": ["LayerOne.png"],
        "LayerTwo": ["LayerTwo.png"],
    }
    assert fake_executor.max_workers == 4
    assert cleanup_calls == [120]


def test_ewmrs_tandem_worker_skips_render_when_inputs_not_ready(monkeypatch):
    queue = _FakeQueue()
    ready_event = _FakeEvent()
    shared_state = {"ewmrs_inputs_ready": False}
    pipeline_calls = []

    monkeypatch.setattr(ewmrs_pipeline, "run_ewmrs_pipeline", lambda *args, **kwargs: pipeline_calls.append((args, kwargs)))

    ewmrs_pipeline.ewmrs_tandem_worker(
        queue,
        shared_state,
        ready_event,
        datetime(2026, 3, 17, 20, 0, tzinfo=timezone.utc),
    )

    assert ready_event.wait_calls == 1
    assert pipeline_calls == []
    assert any("skipping render" in message for message in queue.messages)


def test_ewmrs_tandem_worker_runs_render_when_inputs_ready(monkeypatch):
    queue = _FakeQueue()
    ready_event = _FakeEvent()
    shared_state = {"ewmrs_inputs_ready": True}
    captured = {}

    def fake_run_ewmrs_pipeline(dt, max_entries=10):
        captured["dt"] = dt
        captured["max_entries"] = max_entries
        return {"LayerOne": ["LayerOne.png"], "LayerTwo": None}

    monkeypatch.setattr(ewmrs_pipeline, "run_ewmrs_pipeline", fake_run_ewmrs_pipeline)

    dt = datetime(2026, 3, 17, 20, 0, tzinfo=timezone.utc)
    ewmrs_pipeline.ewmrs_tandem_worker(queue, shared_state, ready_event, dt, max_entries=3)

    assert ready_event.wait_calls == 1
    assert captured == {"dt": dt, "max_entries": 3}
    assert any("Starting EWMRS render phase" in message for message in queue.messages)
    assert any("1/2 layers succeeded" in message for message in queue.messages)
