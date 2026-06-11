import queue
import sys

from util.runtime import background


class FakeLogQueue:
    def __init__(self):
        self.items = []

    def put(self, item):
        self.items.append(item)


class FakeHeartbeatQueue:
    def __init__(self, responses=None):
        self.responses = list(responses or [])
        self.items = []

    def get(self, timeout=None):
        if self.responses:
            response = self.responses.pop(0)
            if isinstance(response, Exception):
                raise response
            return response
        raise queue.Empty()

    def put_nowait(self, item):
        self.items.append(item)


class FakeProcess:
    def __init__(self, *, alive_sequence, exitcode=None):
        self.alive_sequence = list(alive_sequence)
        self.exitcode = exitcode
        self.terminate_calls = 0
        self.kill_calls = 0
        self.join_calls = []

    def is_alive(self):
        if self.alive_sequence:
            return self.alive_sequence.pop(0)
        return False

    def terminate(self):
        self.terminate_calls += 1
        self.exitcode = -15
        self.alive_sequence = [False]

    def kill(self):
        self.kill_calls += 1
        self.exitcode = -9
        self.alive_sequence = [False]

    def join(self, timeout=None):
        self.join_calls.append(timeout)


def test_supervise_nexrad_pipeline_process_terminates_stalled_child(monkeypatch):
    log_queue = FakeLogQueue()
    heartbeat_queue = FakeHeartbeatQueue([queue.Empty(), queue.Empty()])
    process = FakeProcess(alive_sequence=[True, True, False], exitcode=None)
    monotonic_values = iter([0.0, 31.0])

    monkeypatch.setattr(background.time, "monotonic", lambda: next(monotonic_values))

    stalled = background._supervise_nexrad_pipeline_process(
        log_queue,
        process,
        heartbeat_queue,
        stall_timeout_seconds=30.0,
        heartbeat_poll_seconds=1.0,
    )

    assert stalled is True
    assert process.terminate_calls == 1
    assert any("pipeline stalled" in item for item in log_queue.items)


def test_nexrad_ingest_pipeline_entry_emits_heartbeat(monkeypatch):
    log_queue = FakeLogQueue()
    heartbeat_queue = FakeHeartbeatQueue()
    captured = {}

    def _fake_run_realtime_ingestion_pipeline(**kwargs):
        captured.update(kwargs)
        kwargs["heartbeat_callback"]()

    monkeypatch.setattr(background, "QueueWriter", lambda _queue: sys.__stdout__)
    monkeypatch.setattr(background, "run_realtime_ingestion_pipeline", _fake_run_realtime_ingestion_pipeline)
    monkeypatch.setattr(background.time, "monotonic", lambda: 123.0)

    background._nexrad_ingest_pipeline_entry(log_queue, heartbeat_queue, "/tmp/nexrad-base")

    assert captured["base_dir"] == "/tmp/nexrad-base"
    assert callable(captured["heartbeat_callback"])
    assert heartbeat_queue.items == [123.0]
    assert any("Starting NEXRAD ingest pipeline" in item for item in log_queue.items)
