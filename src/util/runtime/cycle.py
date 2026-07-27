from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
import asyncio
import json
import multiprocessing
import os
from pathlib import Path
import queue
import time

from common.ingest.manifest import CycleInputManifest
from common.pipeline.coordinator import run_tandem_ingest_cycle
from EdgeWARN.pipeline import edgewarn_tandem_worker
from EWMRS.pipeline import ewmrs_tandem_worker

from .goes import (
    collect_local_goes_inputs,
    download_glm_for_scan,
    get_ewmrs_goes_render_specs,
    wait_for_local_goes_inputs,
)
from .logging import drain_log_queue, queue_log
from .processes import StartedProcessRegistry


class CycleStatus(str, Enum):
    """Authoritative terminal state for one required pipeline stage."""

    COMPLETED = "completed"
    DISABLED = "disabled"
    UNAVAILABLE = "unavailable"
    FAILED = "failed"


@dataclass(frozen=True)
class CycleStageResult:
    """Terminal state, outputs, and process status for one cycle stage."""

    status: CycleStatus
    produced_artifacts: tuple[str, ...] = ()
    errors: tuple[str, ...] = ()
    worker_exit_status: int | None = None

    def __post_init__(self):
        object.__setattr__(self, "status", CycleStatus(self.status))
        object.__setattr__(
            self,
            "produced_artifacts",
            tuple(str(path) for path in self.produced_artifacts),
        )
        object.__setattr__(self, "errors", tuple(str(error) for error in self.errors))

    @property
    def successful(self) -> bool:
        return (
            self.status in {CycleStatus.COMPLETED, CycleStatus.DISABLED}
            and self.worker_exit_status in {None, 0}
            and not self.errors
        )

    def as_dict(self) -> dict:
        return {
            "status": self.status.value,
            "produced_artifacts": list(self.produced_artifacts),
            "errors": list(self.errors),
            "worker_exit_status": self.worker_exit_status,
        }


@dataclass(frozen=True)
class CycleOutcome:
    """Validated terminal outcome for a full tandem cycle."""

    timestamp: datetime
    stages: dict[str, CycleStageResult]
    retryable: bool
    input_manifest: CycleInputManifest | None = None

    @property
    def completed(self) -> bool:
        return bool(self.stages) and all(stage.successful for stage in self.stages.values())

    @property
    def produced_artifacts(self) -> tuple[str, ...]:
        return tuple(
            artifact
            for stage in self.stages.values()
            for artifact in stage.produced_artifacts
        )

    @property
    def errors(self) -> tuple[str, ...]:
        return tuple(error for stage in self.stages.values() for error in stage.errors)

    @property
    def worker_exit_status(self) -> dict[str, int | None]:
        return {
            stage_name: stage.worker_exit_status
            for stage_name, stage in self.stages.items()
        }

    def as_dict(self) -> dict:
        return {
            "timestamp": self.timestamp.astimezone(timezone.utc).isoformat(),
            "completed": self.completed,
            "retryable": self.retryable,
            "produced_artifacts": list(self.produced_artifacts),
            "errors": list(self.errors),
            "worker_exit_status": self.worker_exit_status,
            "input_manifest": (
                self.input_manifest.as_dict()
                if self.input_manifest is not None
                else None
            ),
            "stages": {
                stage_name: stage.as_dict()
                for stage_name, stage in self.stages.items()
            },
        }


@dataclass(frozen=True)
class CycleRetryPolicy:
    """Bounded exponential retry policy for a single scan."""

    max_attempts: int = 3
    initial_backoff_seconds: float = 5.0
    max_backoff_seconds: float = 30.0

    def __post_init__(self):
        if self.max_attempts < 1:
            raise ValueError("max_attempts must be at least 1")
        if self.initial_backoff_seconds < 0 or self.max_backoff_seconds < 0:
            raise ValueError("retry backoff values must be non-negative")

    def delay_after(self, attempt: int) -> float:
        exponent = max(0, int(attempt) - 1)
        return min(
            self.max_backoff_seconds,
            self.initial_backoff_seconds * (2 ** exponent),
        )


@dataclass(frozen=True)
class PersistedCycleState:
    """Restart-visible distinction between attempted, successful, and abandoned scans."""

    last_attempted: datetime | None = None
    last_successful: datetime | None = None
    last_abandoned: datetime | None = None
    attempt_count: int = 0
    outcome: dict = field(default_factory=dict)

    @property
    def selection_cursor(self) -> datetime | None:
        values = [
            value
            for value in (self.last_successful, self.last_abandoned)
            if value is not None
        ]
        return max(values) if values else None

    @property
    def retry_timestamp(self) -> datetime | None:
        if self.last_attempted is None:
            return None
        if self.last_attempted == self.last_successful:
            return None
        if self.last_attempted == self.last_abandoned:
            return None
        if not bool(self.outcome.get("retryable")):
            return None
        return self.last_attempted


class CycleStateStore:
    """Persist cycle progress without conflating attempts with success."""

    def __init__(self, path: str | Path):
        self.path = Path(path)

    @staticmethod
    def _parse_timestamp(value) -> datetime | None:
        if not value:
            return None
        parsed = datetime.fromisoformat(str(value))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)

    def load(self) -> PersistedCycleState:
        if not self.path.is_file():
            return PersistedCycleState()
        try:
            payload = json.loads(self.path.read_text(encoding="utf-8"))
            return PersistedCycleState(
                last_attempted=self._parse_timestamp(payload.get("last_attempted")),
                last_successful=self._parse_timestamp(payload.get("last_successful")),
                last_abandoned=self._parse_timestamp(payload.get("last_abandoned")),
                attempt_count=max(0, int(payload.get("attempt_count", 0))),
                outcome=dict(payload.get("outcome") or {}),
            )
        except Exception:
            return PersistedCycleState()

    def _write(self, payload: dict) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        temporary = self.path.with_name(f".{self.path.name}.{os.getpid()}.tmp")
        try:
            with open(temporary, "w", encoding="utf-8") as handle:
                json.dump(payload, handle, indent=2, sort_keys=True)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, self.path)
        finally:
            temporary.unlink(missing_ok=True)

    def record_attempt(self, timestamp: datetime, attempt_count: int) -> PersistedCycleState:
        current = self.load()
        payload = {
            "last_attempted": timestamp.astimezone(timezone.utc).isoformat(),
            "last_successful": (
                current.last_successful.isoformat()
                if current.last_successful is not None
                else None
            ),
            "last_abandoned": (
                current.last_abandoned.isoformat()
                if current.last_abandoned is not None
                else None
            ),
            "attempt_count": int(attempt_count),
            "outcome": current.outcome,
        }
        self._write(payload)
        return self.load()

    def seed_last_successful(self, timestamp: datetime) -> PersistedCycleState:
        """Record an existing validated stormcell watermark during migration."""
        current = self.load()
        existing = current.last_successful
        successful = timestamp if existing is None else max(existing, timestamp)
        payload = {
            "last_attempted": (
                current.last_attempted.isoformat()
                if current.last_attempted is not None
                else successful.astimezone(timezone.utc).isoformat()
            ),
            "last_successful": successful.astimezone(timezone.utc).isoformat(),
            "last_abandoned": (
                current.last_abandoned.isoformat()
                if current.last_abandoned is not None
                else None
            ),
            "attempt_count": current.attempt_count,
            "outcome": current.outcome,
        }
        self._write(payload)
        return self.load()

    def record_outcome(
        self,
        outcome: CycleOutcome,
        attempt_count: int,
        *,
        abandoned: bool = False,
    ) -> PersistedCycleState:
        current = self.load()
        payload = {
            "last_attempted": outcome.timestamp.astimezone(timezone.utc).isoformat(),
            "last_successful": (
                outcome.timestamp.astimezone(timezone.utc).isoformat()
                if outcome.completed
                else (
                    current.last_successful.isoformat()
                    if current.last_successful is not None
                    else None
                )
            ),
            "last_abandoned": (
                outcome.timestamp.astimezone(timezone.utc).isoformat()
                if abandoned
                else (
                    current.last_abandoned.isoformat()
                    if current.last_abandoned is not None
                    else None
                )
            ),
            "attempt_count": int(attempt_count),
            "outcome": outcome.as_dict(),
        }
        self._write(payload)
        return self.load()


@dataclass(frozen=True)
class TandemCycleConfig:
    lat_limits: tuple[float, float]
    lon_limits: tuple[float, float]
    profile: bool
    disable_ctam: bool
    disable_tracking: bool
    disable_polygon_expansion: bool
    refl_threshold: float
    min_seed_percentage: float
    drop_offset: float
    ewmrs_enabled: bool
    goes_enabled: bool
    mrms_core_only: bool
    goes_render_wait_seconds: float
    goes_render_wait_interval_seconds: float


def run_tandem_cycle_once(
    dt,
    goes_render_task_queue,
    goes_render_log_queue,
    manager,
    *,
    config: TandemCycleConfig,
    goes_cycle_active_event,
):
    log_queue = multiprocessing.Queue()
    shared_state = manager.dict()

    detection_ready_event = multiprocessing.Event()
    ewmrs_mrms_ready_event = multiprocessing.Event()
    integration_ready_event = multiprocessing.Event()
    shared_state.update({
        "detection_inputs_ready": False,
        "ewmrs_mrms_inputs_ready": False,
        "ewmrs_goes_inputs_ready": False,
        "edgewarn_integration_inputs_ready": False,
        "edgewarn_generated_file": "",
        "input_manifest": {},
        "edgewarn_stage": {
            "status": "pending",
            "produced_artifacts": [],
            "errors": [],
        },
        "ewmrs_stage": {
            "status": "pending" if config.ewmrs_enabled else CycleStatus.DISABLED.value,
            "produced_artifacts": [],
            "errors": [],
        },
        "errors": {},
    })

    edgewarn_proc = multiprocessing.Process(
        target=edgewarn_tandem_worker,
        args=(
            log_queue, shared_state, detection_ready_event, integration_ready_event,
            dt, config.lat_limits, config.lon_limits, config.profile,
            config.disable_ctam, config.disable_tracking,
            config.disable_polygon_expansion, config.refl_threshold,
            config.min_seed_percentage, config.drop_offset,
            config.mrms_core_only,
        ),
    )
    ewmrs_proc = (
        multiprocessing.Process(
            target=ewmrs_tandem_worker,
            args=(log_queue, shared_state, ewmrs_mrms_ready_event, dt),
        ) if config.ewmrs_enabled else None
    )
    started_processes = StartedProcessRegistry()
    released_phases: set[str] = set()

    def emit_phase(phase: str, status: str):
        """Temporary direct phase telemetry; bypasses delayed queue draining."""
        print(
            "[PhaseTelemetry] "
            f"utc={datetime.now(timezone.utc).isoformat()} "
            f"monotonic={time.perf_counter():.6f} "
            f"cycle={dt.isoformat()} phase={phase} status={status}",
            flush=True,
        )

    def release(event, phase: str, status: str):
        if phase not in released_phases:
            emit_phase(phase, status)
            released_phases.add(phase)
        event.set()

    def publish(state, event, phase: str, *, integration=False):
        """Write the complete snapshot before waking a worker."""
        shared_state["detection_inputs_ready"] = state.detection_inputs_ready
        shared_state["ewmrs_mrms_inputs_ready"] = state.ewmrs_mrms_inputs_ready
        if state.input_manifest is not None:
            shared_state["input_manifest"] = state.input_manifest.as_dict()
        if integration:
            shared_state["edgewarn_integration_inputs_ready"] = state.edgewarn_integration_inputs_ready
        shared_state["errors"] = dict(state.errors)
        ready_key = {
            "detection_released": "detection_inputs_ready",
            "ewmrs_mrms_released": "ewmrs_mrms_inputs_ready",
        }[phase]
        if phase == "detection_released":
            emit_phase(
                "detection_mrms_validated",
                "validated" if shared_state[ready_key] else "unavailable",
            )
        release(event, phase, "ready" if shared_state[ready_key] else "unavailable")

    try:
        started_processes.start(edgewarn_proc, "EdgeWARN")
        emit_phase("edgewarn_worker_started", "started")
        started_processes.start(ewmrs_proc, "EWMRS")
        if ewmrs_proc is not None:
            emit_phase("ewmrs_worker_started", "started")

        async def ingest_and_glm():
            glm_task = None
            if config.goes_enabled:
                glm_task = asyncio.create_task(asyncio.to_thread(download_glm_for_scan, dt))
            base_ready = False
            glm_ready = not config.goes_enabled
            glm_records = ()

            def publish_integration_if_ready():
                if base_ready and glm_ready:
                    if glm_records:
                        base_manifest = CycleInputManifest.from_dict(
                            shared_state.get("input_manifest")
                        ) or CycleInputManifest(cycle_time=dt)
                        shared_state["input_manifest"] = base_manifest.with_inputs(
                            glm_records
                        ).as_dict()
                    shared_state["edgewarn_integration_inputs_ready"] = True
                    release(integration_ready_event, "integration_released", "ready")

            def base_integration_ready(state):
                nonlocal base_ready
                base_ready = state.edgewarn_integration_inputs_ready
                if state.input_manifest is not None:
                    shared_state["input_manifest"] = state.input_manifest.as_dict()
                shared_state["errors"] = dict(state.errors)
                publish_integration_if_ready()

            cycle_task = asyncio.create_task(run_tandem_ingest_cycle(
                dt, lambda msg: queue_log(log_queue, msg), include_goes=False,
                include_rap=not config.mrms_core_only,
                include_ewmrs=config.ewmrs_enabled,
                on_detection_ready=lambda state: publish(state, detection_ready_event, "detection_released"),
                on_ewmrs_mrms_ready=lambda state: publish(state, ewmrs_mrms_ready_event, "ewmrs_mrms_released"),
                on_base_integration_ready=base_integration_ready,
            ))
            if glm_task is not None:
                try:
                    glm_results = tuple(await glm_task)
                    glm_records = glm_results
                    glm_manifest = CycleInputManifest(
                        cycle_time=dt,
                        inputs=glm_results,
                    )
                    glm_errors = glm_manifest.validate_alignment()
                    glm_ready = bool(glm_results) and not glm_errors
                    glm_path = glm_results[-1].path if glm_ready else None
                    queue_log(log_queue, (
                        f"INFO: Scan-time GLM ingest satisfied by {len(glm_results)} file(s)"
                        if glm_results else f"INFO: Scan-time GLM ingest found no files for {dt.isoformat()}"
                    ))
                    if glm_ready:
                        queue_log(log_queue, f"INFO: Local GLM readiness satisfied by {glm_path}")
                    else:
                        detail = "; ".join(glm_errors) if glm_errors else "no staged file"
                        queue_log(log_queue, f"INFO: No valid pinned GLM input for {dt.isoformat()}: {detail}")
                except Exception as exc:
                    queue_log(log_queue, f"WARN: Scan-time GLM ingest failed for {dt.isoformat()}: {exc}")
                    glm_ready = False
                publish_integration_if_ready()
            else:
                queue_log(log_queue, "INFO: GOES/GLM components disabled; EdgeWARN integration will not wait for GLM inputs")
            return await cycle_task, glm_ready

        cycle_state, glm_ready = asyncio.run(ingest_and_glm())
    except Exception as exc:
        print(f"[Scheduler] Tandem ingest cycle failed for {dt}: {exc}")
        cycle_state = None
        glm_ready = False

    goes_specs = get_ewmrs_goes_render_specs() if config.ewmrs_enabled and config.goes_enabled else []
    edgewarn_integration_ready = bool(
        cycle_state
        and cycle_state.detection_inputs_ready
        and cycle_state.mrms_integration_inputs_ready
        and (cycle_state.rap_inputs_ready or config.mrms_core_only)
        and (glm_ready or not config.goes_enabled)
    )
    shared_state["edgewarn_integration_inputs_ready"] = edgewarn_integration_ready
    errors = dict(shared_state.get("errors", {}))
    if not edgewarn_integration_ready:
        errors.setdefault("edgewarn_integration_ingest", "EdgeWARN integration inputs unavailable")
    shared_state["errors"] = errors
    # Failure paths may have occurred before a coordinator callback.  Release
    # every waiter after the terminal false state has been written.
    release(detection_ready_event, "detection_released", "ready" if shared_state["detection_inputs_ready"] else "unavailable")
    release(ewmrs_mrms_ready_event, "ewmrs_mrms_released", "ready" if shared_state["ewmrs_mrms_inputs_ready"] else "unavailable")
    release(integration_ready_event, "integration_released", "ready" if edgewarn_integration_ready else "unavailable")

    goes_ready = False
    goes_path = None
    goes_manifest = None
    try:
        if config.ewmrs_enabled and config.goes_enabled:
            goes_inputs = collect_local_goes_inputs(dt, specs=goes_specs)
            goes_manifest = CycleInputManifest(
                cycle_time=dt,
                inputs=goes_inputs,
            )
            goes_ready = (
                len(goes_inputs) == len(goes_specs)
                and not goes_manifest.validate_alignment()
                and not goes_cycle_active_event.is_set()
            )
            goes_path = goes_inputs[0].path if goes_ready else None

            if not goes_ready:
                queue_log(
                    log_queue,
                    f"INFO: Waiting for background GOES ABI ingest cycle to fully stage render inputs for {dt.isoformat()}",
                )
                goes_inputs = wait_for_local_goes_inputs(
                    dt,
                    specs=goes_specs,
                    timeout_seconds=config.goes_render_wait_seconds,
                    interval_seconds=config.goes_render_wait_interval_seconds,
                    activity_event=goes_cycle_active_event,
                )
                goes_manifest = CycleInputManifest(
                    cycle_time=dt,
                    inputs=goes_inputs,
                )
                goes_ready = (
                    len(goes_inputs) == len(goes_specs)
                    and not goes_manifest.validate_alignment()
                )
                goes_path = goes_inputs[0].path if goes_ready else None

            if not goes_ready:
                queue_log(
                    log_queue,
                    f"INFO: Background GOES ABI ingest did not finish staging the full render input set for {dt.isoformat()}; GOES render phase will be skipped",
                )
            else:
                base_manifest = CycleInputManifest.from_dict(
                    shared_state.get("input_manifest")
                ) or CycleInputManifest(cycle_time=dt)
                shared_state["input_manifest"] = base_manifest.with_inputs(
                    goes_inputs
                ).as_dict()
                queue_log(
                    log_queue,
                    "INFO: Full pinned GOES ABI render input set is staged: "
                    + ", ".join(record.path for record in goes_inputs),
                )
                dropped_render_tasks = 0
                saw_shutdown = False
                while True:
                    try:
                        queued_task = goes_render_task_queue.get_nowait()
                    except queue.Empty:
                        break

                    if queued_task is None:
                        saw_shutdown = True
                        continue
                    dropped_render_tasks += 1

                goes_render_task_queue.put((
                    dt,
                    10,
                    datetime.now(timezone.utc).isoformat(),
                    goes_manifest.as_dict(),
                ))
                if saw_shutdown:
                    goes_render_task_queue.put(None)
                if dropped_render_tasks > 0:
                    queue_log(
                        log_queue,
                        f"INFO: Replaced {dropped_render_tasks} stale queued GOES render task(s) with latest ready cycle {dt.isoformat()}",
                    )
                queue_log(log_queue, f"INFO: Queued decoupled EWMRS GOES render for {dt.isoformat()}")
        else:
            goes_ready = False
    except Exception as exc:
        if config.ewmrs_enabled and config.goes_enabled:
            queue_log(log_queue, f"WARN: Local GOES readiness check failed for {dt.isoformat()}: {exc}")
    finally:
        shared_state["ewmrs_goes_inputs_ready"] = config.ewmrs_enabled and goes_ready
        errors = dict(shared_state.get("errors", {}))
        if not config.ewmrs_enabled:
            errors.pop("ewmrs_ingest", None)
            errors.pop("ewmrs_goes_ingest", None)
            errors.pop("ewmrs_rap_uint16", None)
        elif goes_ready:
            errors.pop("ewmrs_goes_ingest", None)
        elif config.goes_enabled:
            errors.setdefault("ewmrs_goes_ingest", "EWMRS GOES inputs unavailable")
        shared_state["errors"] = errors

    try:
        while edgewarn_proc.is_alive() or (ewmrs_proc is not None and ewmrs_proc.is_alive()) or not log_queue.empty():
            drain_log_queue(log_queue)
            drain_log_queue(goes_render_log_queue)
            time.sleep(1)
    except KeyboardInterrupt:
        print("CTRL+C detected, stopping tandem cycle workers...")
        raise
    finally:
        started_processes.shutdown()
        drain_log_queue(log_queue)
        drain_log_queue(goes_render_log_queue)

    edgewarn_stage = _stage_result_from_shared(
        shared_state.get("edgewarn_stage"),
        worker_exit_status=edgewarn_proc.exitcode,
        fallback_error="EdgeWARN worker exited without publishing a terminal stage result",
    )
    if ewmrs_proc is None:
        ewmrs_stage = CycleStageResult(status=CycleStatus.DISABLED)
    else:
        ewmrs_stage = _stage_result_from_shared(
            shared_state.get("ewmrs_stage"),
            worker_exit_status=ewmrs_proc.exitcode,
            fallback_error="EWMRS worker exited without publishing a terminal stage result",
        )

    ingest_errors = tuple(
        f"{name}: {message}"
        for name, message in dict(shared_state.get("errors", {})).items()
        if name not in {"ewmrs_goes_ingest", "ewmrs_rap_uint16"}
    )
    ingest_ready = bool(
        cycle_state
        and cycle_state.detection_inputs_ready
        and cycle_state.mrms_integration_inputs_ready
        and (cycle_state.rap_inputs_ready or config.mrms_core_only)
    )
    ingest_stage = CycleStageResult(
        status=CycleStatus.COMPLETED if ingest_ready else CycleStatus.UNAVAILABLE,
        errors=() if ingest_ready else (ingest_errors or ("Required ingest inputs unavailable",)),
    )

    stages = {
        "ingest": ingest_stage,
        "edgewarn": edgewarn_stage,
        "ewmrs": ewmrs_stage,
    }
    retryable = any(
        stage.status in {CycleStatus.UNAVAILABLE, CycleStatus.FAILED}
        for stage in stages.values()
    )
    return CycleOutcome(
        timestamp=dt,
        stages=stages,
        retryable=retryable,
        input_manifest=CycleInputManifest.from_dict(
            shared_state.get("input_manifest")
        ),
    )


def _stage_result_from_shared(
    payload,
    *,
    worker_exit_status: int | None,
    fallback_error: str,
) -> CycleStageResult:
    """Convert a worker-published mapping into an authoritative stage result."""
    stage_payload = dict(payload or {})
    status_value = stage_payload.get("status")
    try:
        status = CycleStatus(status_value)
    except (TypeError, ValueError):
        status = CycleStatus.FAILED

    errors = tuple(stage_payload.get("errors") or ())
    if worker_exit_status not in {None, 0}:
        status = CycleStatus.FAILED
        errors = (*errors, f"Worker exited with status {worker_exit_status}")
    elif status not in {
        CycleStatus.COMPLETED,
        CycleStatus.DISABLED,
        CycleStatus.UNAVAILABLE,
        CycleStatus.FAILED,
    }:
        status = CycleStatus.FAILED

    if status is CycleStatus.FAILED and not errors:
        errors = (fallback_error,)

    return CycleStageResult(
        status=status,
        produced_artifacts=tuple(stage_payload.get("produced_artifacts") or ()),
        errors=errors,
        worker_exit_status=worker_exit_status,
    )
