"""Temporary all-in-one runner (decomposition Phase 1 adapter).

This module is being decomposed into three independently operable services
(plans/realtime-runner-decomposition-plan.md). The service logic now lives in:

- ``util.runtime.primary_service`` — MRMS selection, cycle state, retry, and
  the primary polling loop;
- ``util.runtime.ewmrs_service`` — METAR/NWS/WPC/GOES accessory supervision;
- ``util.runtime.nexrad_service`` — NEXRAD ingest/render supervision.

``main()`` below wires those pieces together exactly as before; deployment is
unchanged. It performs no import-time work: parsing, runtime initialization,
and stream wrapping all happen inside ``main()``.
"""

import sys
import multiprocessing

from common.config import loader as config_loader, overlay
from EdgeWARN import initialize_runtime
from EdgeWARN.schedule.scheduler import MRMSUpdateChecker
from util.io import TimestampedOutput, IOManager
from util.release import get_release_version
from util.runtime import (
    AccessorySupervisor,
    StartedProcessRegistry,
    drain_log_queue,
)
from util.runtime.config import resolve_file, section
from util.runtime.ewmrs_service import register_ewmrs_accessories
from util.runtime.nexrad_service import register_nexrad_supervision
from util.runtime.primary_service import (
    build_cycle_config,
    log_effective_flags,
    report_effective_config,
    run_primary_cycle_loop,
)


def main():
    """Scheduler: run a shared ingest cycle and launch EdgeWARN/EWMRS in tandem."""
    sys.stdout = TimestampedOutput(sys.stdout)
    sys.stderr = TimestampedOutput(sys.stderr)

    io_manager = IOManager("[Main]")
    args = io_manager.get_args()

    initialize_runtime(base_dir=args.base_dir, io_manager=io_manager)

    print("Scheduler started. Press CTRL+C to exit.")
    log_effective_flags(args)

    mrms_core_only = args.mrms_core_only
    ewmrs_enabled = not args.disable_ewmrs and not mrms_core_only
    nws_enabled = not args.disable_nws and not mrms_core_only
    metar_enabled = not args.disable_metar and not mrms_core_only
    goes_enabled = not args.disable_goes and not mrms_core_only
    nexrad_enabled = not args.disable_nexrad and not mrms_core_only

    cycle_config, goes_coordination = build_cycle_config(args)
    goes_pause_ingest_during_render = overlay.resolve(
        None,
        env_names=["EDGEWARN_PAUSE_GOES_INGEST_DURING_RENDER"],
        yaml_value=goes_coordination["pause_ingest_during_render"],
        key="goes_coordination.pause_ingest_during_render",
    )
    goes_cycle_active = multiprocessing.Event()
    goes_render_active = multiprocessing.Event()

    print("[Scheduler] Starting background accessory ingests...")
    # Hoisted out of the tandem cycle: a Manager spawns a child server
    # process and IPC machinery on construction; reusing one across cycles
    # avoids that startup cost every minute.
    manager = multiprocessing.Manager()
    goes_render_task_queue = multiprocessing.Queue()
    goes_render_log_queue = multiprocessing.Queue()
    nexrad_log_queue = multiprocessing.Queue()

    supervisor_settings = section("supervisor")
    nexrad_heartbeat_path = str(resolve_file(
        supervisor_settings["nexrad_heartbeat_file"], "supervisor.nexrad_heartbeat_file"
    ))
    supervisor = AccessorySupervisor(
        max_restarts=supervisor_settings["max_restarts"],
        restart_window_seconds=supervisor_settings["restart_window_seconds"],
        base_backoff_seconds=supervisor_settings["base_backoff_seconds"],
        max_backoff_seconds=supervisor_settings["max_backoff_seconds"],
        health_path=str(resolve_file(
            supervisor_settings["health_file"], "supervisor.health_file"
        )),
    )
    register_ewmrs_accessories(
        supervisor,
        mrms_core_only=mrms_core_only,
        metar_enabled=metar_enabled,
        nws_enabled=nws_enabled,
        # Scan-time GLM is a primary integration input: it runs whenever GOES
        # is enabled, independent of EWMRS. GOES ABI rendering requires EWMRS.
        goes_ingest_enabled=goes_enabled,
        goes_render_enabled=bool(ewmrs_enabled and goes_enabled),
        goes_cycle_active=goes_cycle_active,
        goes_render_active=goes_render_active,
        goes_pause_ingest_during_render=goes_pause_ingest_during_render,
        goes_poll_seconds=goes_coordination["poll_seconds"],
        goes_render_task_queue=goes_render_task_queue,
        goes_render_log_queue=goes_render_log_queue,
    )
    register_nexrad_supervision(
        supervisor,
        base_dir=args.base_dir,
        nexrad_log_queue=nexrad_log_queue,
        nexrad_heartbeat_path=nexrad_heartbeat_path,
        enabled=bool(ewmrs_enabled and nexrad_enabled),
    )
    supervisor.start_all()
    # Deliberate reordering versus the pre-split monolith: accessory children
    # start before cycle-state restore instead of after. Any failure raised
    # during restore is still cleaned up by the finally block below.

    started_processes = StartedProcessRegistry()
    started_processes.processes = [
        (info["process"], info["name"])
        for info in supervisor._process_info
        if info["process"] is not None
    ]

    try:
        run_primary_cycle_loop(
            checker=MRMSUpdateChecker(verbose=True),
            cycle_config=cycle_config,
            goes_render_task_queue=goes_render_task_queue,
            goes_render_log_queue=goes_render_log_queue,
            nexrad_log_queue=nexrad_log_queue,
            goes_cycle_active_event=goes_cycle_active,
            manager=manager,
            supervisor=supervisor,
        )
    finally:
        supervisor.request_stop()
        started_processes.shutdown(queue_sentinels=[(goes_render_task_queue, None)], manager=manager)
        supervisor.shutdown()
        drain_log_queue(nexrad_log_queue)


if __name__ == "__main__":
    try:
        print(f"Running EdgeWARN v{get_release_version()}")
        main()
    except KeyboardInterrupt:
        print("CTRL+C detected, exiting ...")
        sys.exit(0)
    except config_loader.ConfigError:
        # This catches configuration failures reached after argument parsing
        # (for example a malformed filesystem attribute in cycle.state_file).
        # Import-time ConfigError instances cannot be caught here; CI's
        # validate-config gate is responsible for rejecting those before startup.
        report_effective_config()
        raise
