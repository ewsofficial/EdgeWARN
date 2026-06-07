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
    ewmrs_goes_ready_event = multiprocessing.Event()
    integration_ready_event = multiprocessing.Event()

    try:
        cycle_state = asyncio.run(
            run_tandem_ingest_cycle(
                dt,
                lambda msg: queue_log(log_queue, msg),
                include_goes=False,
                include_ewmrs=config.ewmrs_enabled,
            )
        )
    except Exception as exc:
        drain_log_queue(log_queue)
        print(f"[Scheduler] Tandem ingest cycle failed for {dt}: {exc}")
        return False

    glm_ready = False
    glm_path = None
    if config.goes_enabled:
        try:
            glm_results = download_glm_for_scan(dt)
            if glm_results:
                queue_log(log_queue, f"INFO: Scan-time GLM ingest satisfied by {len(glm_results)} file(s)")
            else:
                queue_log(log_queue, f"INFO: Scan-time GLM ingest found no files for {dt.isoformat()}")
        except Exception as exc:
            queue_log(log_queue, f"WARN: Scan-time GLM ingest failed for {dt.isoformat()}: {exc}")

        glm_ready, glm_path = check_local_glm_ready(dt)
    else:
        queue_log(log_queue, "INFO: GOES/GLM components disabled; EdgeWARN integration will not wait for GLM inputs")

    goes_specs = get_ewmrs_goes_render_specs() if config.ewmrs_enabled and config.goes_enabled else []
    rap_ready = "rap_ingest" not in cycle_state.errors
    mrms_integration_ready = cycle_state.detection_inputs_ready and "mrms_integration_ingest" not in cycle_state.errors
    edgewarn_integration_ready = mrms_integration_ready and rap_ready and (glm_ready or not config.goes_enabled)
    if config.goes_enabled and not glm_ready:
        queue_log(log_queue, f"INFO: No local GLM files staged at or after {dt.isoformat()}; EdgeWARN integration will wait for GOES")
    elif config.goes_enabled:
        queue_log(log_queue, f"INFO: Local GLM readiness satisfied by {glm_path}")

    shared_state["detection_inputs_ready"] = cycle_state.detection_inputs_ready
    shared_state["ewmrs_mrms_inputs_ready"] = cycle_state.ewmrs_mrms_inputs_ready
    shared_state["ewmrs_goes_inputs_ready"] = False
    shared_state["edgewarn_integration_inputs_ready"] = edgewarn_integration_ready
    shared_state["edgewarn_generated_file"] = ""
    errors = {
        key: value
        for key, value in dict(cycle_state.errors).items()
        if key not in {"goes_ingest", "ewmrs_goes_ingest", "edgewarn_integration_ingest"}
    }
    if config.goes_enabled and not glm_ready:
        errors.setdefault("goes_ingest", "GOES inputs unavailable")
    if not edgewarn_integration_ready:
        errors.setdefault("edgewarn_integration_ingest", "EdgeWARN integration inputs unavailable")
    shared_state["errors"] = errors

    edgewarn_proc = multiprocessing.Process(
        target=edgewarn_tandem_worker,
        args=(
            log_queue,
            shared_state,
            detection_ready_event,
            integration_ready_event,
            dt,
            config.lat_limits,
            config.lon_limits,
            config.profile,
            config.disable_ctam,
            config.disable_tracking,
            config.refl_threshold,
            config.min_seed_percentage,
            config.drop_offset,
        ),
    )
    ewmrs_proc = None
    if config.ewmrs_enabled:
        ewmrs_proc = multiprocessing.Process(
            target=ewmrs_tandem_worker,
            args=(log_queue, shared_state, ewmrs_mrms_ready_event, ewmrs_goes_ready_event, dt),
        )

    started_processes = StartedProcessRegistry()
    started_processes.start(edgewarn_proc, "EdgeWARN")
    started_processes.start(ewmrs_proc, "EWMRS")

    detection_ready_event.set()
    if config.ewmrs_enabled:
        ewmrs_mrms_ready_event.set()
    integration_ready_event.set()

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
        if config.ewmrs_enabled:
            ewmrs_goes_ready_event.set()

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
