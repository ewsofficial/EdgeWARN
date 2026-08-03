from datetime import datetime, timezone

from common.ingest.manifest import CycleInputManifest
from util.runtime import (
    CycleOutcome,
    CycleRetryPolicy,
    CycleStageResult,
    CycleStateStore,
    CycleStatus,
    StartedProcessRegistry,
)
from util.runtime.cycle import _stage_result_from_shared


class FakeProcess:
    def __init__(self, *, alive=True):
        self.alive = alive
        self.start_calls = 0
        self.terminate_calls = 0
        self.kill_calls = 0
        self.join_calls = []

    def start(self):
        self.start_calls += 1

    def is_alive(self):
        return self.alive

    def terminate(self):
        self.terminate_calls += 1
        self.alive = False

    def kill(self):
        self.kill_calls += 1
        self.alive = False

    def join(self, timeout=None):
        self.join_calls.append(timeout)


class FakeQueue:
    def __init__(self):
        self.items = []

    def put(self, item):
        self.items.append(item)


class FakeManager:
    def __init__(self):
        self.shutdown_calls = 0

    def shutdown(self):
        self.shutdown_calls += 1


def test_started_process_registry_starts_and_shuts_down_processes_in_reverse_order():
    registry = StartedProcessRegistry()
    first = FakeProcess()
    second = FakeProcess()
    queue_obj = FakeQueue()
    manager = FakeManager()

    registry.start(first, "first")
    registry.start(second, "second")
    registry.shutdown(queue_sentinels=[(queue_obj, None)], manager=manager)

    assert first.start_calls == 1
    assert second.start_calls == 1
    assert second.terminate_calls == 1
    assert first.terminate_calls == 1
    assert queue_obj.items == [None]
    assert manager.shutdown_calls == 1
    assert registry.processes == []


def test_started_process_registry_ignores_none_processes():
    registry = StartedProcessRegistry()

    result = registry.start(None, "missing")

    assert result is None
    assert registry.processes == []


def test_cycle_state_store_keeps_attempt_separate_from_success(tmp_path):
    timestamp = datetime(2026, 7, 26, 18, 0, tzinfo=timezone.utc)
    store = CycleStateStore(tmp_path / "runtime" / "cycle_state.json")
    failed = CycleOutcome(
        timestamp=timestamp,
        stages={
            "edgewarn": CycleStageResult(
                CycleStatus.UNAVAILABLE,
                errors=("inputs unavailable",),
                worker_exit_status=0,
            )
        },
        retryable=True,
    )

    store.record_attempt(timestamp, 1)
    state = store.record_outcome(failed, 1)

    assert state.last_attempted == timestamp
    assert state.last_successful is None
    assert state.last_abandoned is None
    assert state.retry_timestamp == timestamp
    assert state.selection_cursor is None


def test_cycle_state_store_records_success_and_explicit_abandonment(tmp_path):
    first = datetime(2026, 7, 26, 18, 0, tzinfo=timezone.utc)
    second = datetime(2026, 7, 26, 18, 2, tzinfo=timezone.utc)
    store = CycleStateStore(tmp_path / "cycle_state.json")
    completed = CycleOutcome(
        timestamp=first,
        stages={
            "edgewarn": CycleStageResult(
                CycleStatus.COMPLETED,
                produced_artifacts=("stormcells.json",),
                worker_exit_status=0,
            )
        },
        retryable=False,
    )
    failed = CycleOutcome(
        timestamp=second,
        stages={
            "edgewarn": CycleStageResult(
                CycleStatus.FAILED,
                errors=("worker crashed",),
                worker_exit_status=9,
            )
        },
        retryable=True,
    )

    store.record_attempt(first, 1)
    store.record_outcome(completed, 1)
    store.record_attempt(second, 3)
    state = store.record_outcome(failed, 3, abandoned=True)

    assert state.last_successful == first
    assert state.last_abandoned == second
    assert state.selection_cursor == second
    assert state.retry_timestamp is None


def test_cycle_retry_policy_is_bounded():
    policy = CycleRetryPolicy(
        max_attempts=4,
        initial_backoff_seconds=2,
        max_backoff_seconds=5,
    )

    assert [policy.delay_after(attempt) for attempt in range(1, 5)] == [2, 4, 5, 5]


def test_cycle_outcome_includes_input_manifest_metadata():
    timestamp = datetime(2026, 7, 26, 18, 0, tzinfo=timezone.utc)
    outcome = CycleOutcome(
        timestamp=timestamp,
        stages={
            "ingest": CycleStageResult(CycleStatus.COMPLETED),
        },
        retryable=False,
        input_manifest=CycleInputManifest(cycle_time=timestamp),
    )

    assert outcome.as_dict()["input_manifest"]["cycle_time"] == timestamp.isoformat()


def test_nonzero_worker_exit_overrides_published_completion():
    stage = _stage_result_from_shared(
        {
            "status": "completed",
            "produced_artifacts": ["stormcells.json"],
            "errors": [],
        },
        worker_exit_status=17,
        fallback_error="missing terminal state",
    )

    assert stage.status is CycleStatus.FAILED
    assert stage.successful is False
    assert "17" in stage.errors[-1]


def test_seed_last_successful_does_not_promote_detection_only_watermark(tmp_path):
    timestamp = datetime(2026, 7, 26, 18, 0, tzinfo=timezone.utc)
    store = CycleStateStore(tmp_path / "cycle_state.json")
    failed = CycleOutcome(
        timestamp=timestamp,
        stages={
            "edgewarn": CycleStageResult(
                CycleStatus.UNAVAILABLE,
                errors=("inputs unavailable",),
                worker_exit_status=0,
            )
        },
        retryable=True,
    )

    store.record_attempt(timestamp, 1)
    store.record_outcome(failed, 1)

    state = store.seed_last_successful(timestamp)

    assert state.last_successful is None
    assert state.last_attempted == timestamp
    assert state.retry_timestamp == timestamp, "pending retry must survive the stormcell watermark seed"


def test_seed_last_successful_migrates_when_no_cycle_state_exists(tmp_path):
    timestamp = datetime(2026, 7, 26, 18, 0, tzinfo=timezone.utc)
    store = CycleStateStore(tmp_path / "cycle_state.json")

    state = store.seed_last_successful(timestamp)

    assert state.last_successful == timestamp
    assert state.last_attempted == timestamp
    assert state.retry_timestamp is None
