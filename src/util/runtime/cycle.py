from dataclasses import dataclass
from datetime import datetime, timezone
import asyncio
import multiprocessing
import queue
import time

from common.pipeline.coordinator import run_tandem_ingest_cycle
from EdgeWARN.pipeline import edgewarn_tandem_worker
from EWMRS.pipeline import ewmrs_tandem_worker

from .goes import (
    check_local_glm_ready,
    check_local_goes_ready,
    download_glm_for_scan,
    get_ewmrs_goes_render_specs,
    wait_for_local_goes_ready,
)
from .logging import drain_log_queue, queue_log
from .processes import StartedProcessRegistry


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

            def publish_integration_if_ready():
                if base_ready and glm_ready:
                    shared_state["edgewarn_integration_inputs_ready"] = True
                    release(integration_ready_event, "integration_released", "ready")

            def base_integration_ready(state):
                nonlocal base_ready
                base_ready = state.edgewarn_integration_inputs_ready
                shared_state["errors"] = dict(state.errors)
                publish_integration_if_ready()

            cycle_task = asyncio.create_task(run_tandem_ingest_cycle(
                dt, lambda msg: queue_log(log_queue, msg), include_goes=False,
                include_ewmrs=config.ewmrs_enabled,
                on_detection_ready=lambda state: publish(state, detection_ready_event, "detection_released"),
                on_ewmrs_mrms_ready=lambda state: publish(state, ewmrs_mrms_ready_event, "ewmrs_mrms_released"),
                on_base_integration_ready=base_integration_ready,
            ))
            if glm_task is not None:
                try:
                    glm_results = await glm_task
                    glm_ready, glm_path = check_local_glm_ready(dt)
                    queue_log(log_queue, (
                        f"INFO: Scan-time GLM ingest satisfied by {len(glm_results)} file(s)"
                        if glm_results else f"INFO: Scan-time GLM ingest found no files for {dt.isoformat()}"
                    ))
                    if glm_ready:
                        queue_log(log_queue, f"INFO: Local GLM readiness satisfied by {glm_path}")
                    else:
                        queue_log(log_queue, f"INFO: No local GLM files staged at or after {dt.isoformat()}")
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
        and cycle_state.rap_inputs_ready
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
    try:
        if config.ewmrs_enabled and config.goes_enabled:
            goes_ready, goes_path = check_local_goes_ready(dt, specs=goes_specs)
            if goes_ready and goes_cycle_active_event.is_set():
                goes_ready = False
                goes_path = None

            if not goes_ready:
                queue_log(
                    log_queue,
                    f"INFO: Waiting for background GOES ABI ingest cycle to fully stage render inputs for {dt.isoformat()}",
                )
                goes_ready, goes_path = wait_for_local_goes_ready(
                    dt,
                    specs=goes_specs,
                    timeout_seconds=config.goes_render_wait_seconds,
                    interval_seconds=config.goes_render_wait_interval_seconds,
                    activity_event=goes_cycle_active_event,
                )

            if not goes_ready:
                queue_log(
                    log_queue,
                    f"INFO: Background GOES ABI ingest did not finish staging the full render input set for {dt.isoformat()}; GOES render phase will be skipped",
                )
            else:
                queue_log(log_queue, f"INFO: Full GOES ABI render input set is staged; representative file {goes_path}")
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

                goes_render_task_queue.put((dt, 10, datetime.now(timezone.utc).isoformat()))
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

    ewmrs_proc_exitcode = 0 if ewmrs_proc is None else ewmrs_proc.exitcode
    return edgewarn_proc.exitcode == 0 and ewmrs_proc_exitcode == 0
