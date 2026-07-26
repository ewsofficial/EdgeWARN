import sys
import os
from datetime import datetime, timezone
import time
import multiprocessing

import util.file as fs
from common.ingest.mrms.config import get_check_modifiers
from EdgeWARN import initialize_runtime
from EdgeWARN.schedule.scheduler import MRMSUpdateChecker
from util.io import TimestampedOutput, IOManager
from util.runtime import (
    CycleRetryPolicy,
    CycleStateStore,
    StartedProcessRegistry,
    TandemCycleConfig,
    drain_log_queue,
    goes_loop,
    goes_render_loop,
    load_last_processed_from_stormcells,
    metar_loop,
    nexrad_ingest_loop,
    nexrad_render_loop,
    nws_loop,
    run_tandem_cycle_once,
    wpc_loop,
)
from util.release import get_release_version

sys.stdout = TimestampedOutput(sys.stdout)
sys.stderr = TimestampedOutput(sys.stderr)

io_manager = IOManager("[Main]")
args = io_manager.get_args()

lat_limits = tuple(args.lat_limits)
lon_limits = tuple(args.lon_limits)

initialize_runtime(base_dir=args.base_dir, io_manager=io_manager)

GOES_POLL_SECONDS = 60
GOES_RENDER_WAIT_SECONDS = 30
GOES_RENDER_WAIT_INTERVAL_SECONDS = 1.0
GOES_CYCLE_ACTIVE = multiprocessing.Event()
GOES_RENDER_ACTIVE = multiprocessing.Event()
GOES_PAUSE_INGEST_DURING_RENDER = os.environ.get("EDGEWARN_PAUSE_GOES_INGEST_DURING_RENDER", "0").strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}
MRMS_CORE_ONLY = args.mrms_core_only
EWMRS_ENABLED = not args.disable_ewmrs and not MRMS_CORE_ONLY
NWS_ENABLED = not args.disable_nws and not MRMS_CORE_ONLY
METAR_ENABLED = not args.disable_metar and not MRMS_CORE_ONLY
GOES_ENABLED = not args.disable_goes and not MRMS_CORE_ONLY
NEXRAD_ENABLED = not args.disable_nexrad and not MRMS_CORE_ONLY

cycle_config = TandemCycleConfig(
    lat_limits=lat_limits,
    lon_limits=lon_limits,
    profile=args.profile,
    disable_ctam=args.disable_ctam,
    disable_tracking=args.disable_tracking,
    disable_polygon_expansion=args.disable_polygon_expansion,
    refl_threshold=args.refl_threshold,
    min_seed_percentage=args.min_seed_percentage,
    drop_offset=args.drop_offset,
    ewmrs_enabled=EWMRS_ENABLED,
    goes_enabled=GOES_ENABLED,
    mrms_core_only=MRMS_CORE_ONLY,
    goes_render_wait_seconds=GOES_RENDER_WAIT_SECONDS,
    goes_render_wait_interval_seconds=GOES_RENDER_WAIT_INTERVAL_SECONDS,
)



def main():
    """Scheduler: run a shared ingest cycle and launch EdgeWARN/EWMRS in tandem."""
    print("Scheduler started. Press CTRL+C to exit.")
    print(
        "[Scheduler] Configuration: "
        f"lat={lat_limits}, lon={lon_limits}, "
        f"refl_threshold={args.refl_threshold}, "
        f"min_seed_percentage={args.min_seed_percentage}, "
        f"drop_offset={args.drop_offset}, "
        f"goes_decoupled={'yes' if GOES_ENABLED else 'no'}"
    )
    if args.disable_ctam:
        print("[Scheduler] CTAM execution disabled via --disable-ctam")
    if args.disable_tracking:
        print("[Scheduler] Tracking disabled via --disable-tracking")
    if args.disable_polygon_expansion:
        print("[Scheduler] Polygon expansion disabled via --disable-polygon-expansion; using original ProbSevere polygons")
    if args.disable_ewmrs:
        print("[Scheduler] EWMRS pipeline disabled via --disable-ewmrs")
    if args.disable_nws:
        print("[Scheduler] NWS background ingest disabled via --disable-nws")
    if args.disable_metar:
        print("[Scheduler] METAR background ingest disabled via --disable-metar")
    if args.disable_goes:
        print("[Scheduler] GOES/GLM ingest and GOES rendering disabled via --disable-goes")
    if args.disable_nexrad:
        print("[Scheduler] NEXRAD ingest and rendering disabled via --disable-nexrad")
    if MRMS_CORE_ONLY:
        print("[Scheduler] MRMS-core-only mode: running MRMS detection, MRMS integration, and CTAM only")
    checker = MRMSUpdateChecker(verbose=True)
    stormcell_last_successful, init_message = load_last_processed_from_stormcells(fs.STORMCELL_DIR)
    print(init_message)
    cycle_state_store = CycleStateStore(fs.DATA_DIR / "runtime" / "cycle_state.json")
    persisted_cycle_state = cycle_state_store.load()
    if stormcell_last_successful is not None:
        persisted_cycle_state = cycle_state_store.seed_last_successful(
            stormcell_last_successful
        )

    last_successful = persisted_cycle_state.last_successful
    last_abandoned = persisted_cycle_state.last_abandoned
    selection_cursor = persisted_cycle_state.selection_cursor
    pending_timestamp = persisted_cycle_state.retry_timestamp
    pending_attempt_count = (
        persisted_cycle_state.attempt_count if pending_timestamp is not None else 0
    )
    retry_not_before = 0.0
    retry_policy = CycleRetryPolicy(
        max_attempts=max(1, int(os.environ.get("EDGEWARN_CYCLE_MAX_ATTEMPTS", "3"))),
        initial_backoff_seconds=max(
            0.0,
            float(os.environ.get("EDGEWARN_CYCLE_RETRY_BACKOFF_SECONDS", "5")),
        ),
        max_backoff_seconds=max(
            0.0,
            float(os.environ.get("EDGEWARN_CYCLE_MAX_BACKOFF_SECONDS", "30")),
        ),
    )
    print(
        "[Scheduler] Cycle progress: "
        f"last_successful={last_successful}, "
        f"last_attempted={persisted_cycle_state.last_attempted}, "
        f"last_abandoned={last_abandoned}, "
        f"pending_retry={pending_timestamp}"
    )

    print("[Scheduler] Starting background accessory ingests...")
    # Hoisted out of _run_tandem_cycle: a Manager spawns a child server
    # process and IPC machinery on construction; reusing one across cycles
    # avoids that startup cost every minute.
    manager = multiprocessing.Manager()
    metar_proc = multiprocessing.Process(target=metar_loop, daemon=True) if METAR_ENABLED else None
    nws_proc = multiprocessing.Process(target=nws_loop, daemon=True) if NWS_ENABLED else None
    wpc_proc = multiprocessing.Process(target=wpc_loop, daemon=True) if not MRMS_CORE_ONLY else None
    goes_render_task_queue = multiprocessing.Queue()
    goes_render_log_queue = multiprocessing.Queue()
    nexrad_log_queue = multiprocessing.Queue()
    goes_render_proc = multiprocessing.Process(
        target=goes_render_loop,
        args=(goes_render_task_queue, goes_render_log_queue, GOES_RENDER_ACTIVE),
        daemon=True,
    ) if EWMRS_ENABLED and GOES_ENABLED else None
    nexrad_render_proc = multiprocessing.Process(
        target=nexrad_render_loop,
        args=(args.base_dir,),
        name="NEXRAD-Render",
        daemon=True,
    ) if EWMRS_ENABLED and NEXRAD_ENABLED else None
    # NEXRAD ingest uses a ProcessPoolExecutor for parser workers, so this
    # process must not be daemonic or child worker creation will fail.
    nexrad_ingest_proc = multiprocessing.Process(
        target=nexrad_ingest_loop,
        args=(nexrad_log_queue, args.base_dir),
        name="NEXRAD-Ingest",
        daemon=False,
    ) if EWMRS_ENABLED and NEXRAD_ENABLED else None
    started_processes = StartedProcessRegistry()
    started_processes.start(metar_proc, "METAR")
    started_processes.start(nws_proc, "NWS")
    started_processes.start(wpc_proc, "WPC")
    goes_proc = multiprocessing.Process(
        target=goes_loop,
        args=(GOES_CYCLE_ACTIVE, GOES_RENDER_ACTIVE, GOES_PAUSE_INGEST_DURING_RENDER, GOES_POLL_SECONDS),
        daemon=True,
    ) if GOES_ENABLED else None
    started_processes.start(goes_proc, "GOES")
    started_processes.start(goes_render_proc, "GOES Render")
    started_processes.start(nexrad_render_proc, "NEXRAD Render")
    started_processes.start(nexrad_ingest_proc, "NEXRAD Ingest")

    try:
        while True:
            drain_log_queue(goes_render_log_queue)
            drain_log_queue(nexrad_log_queue)
            now = datetime.now(timezone.utc)
            check_modifiers = get_check_modifiers()
            latest_common = None
            if pending_timestamp is None:
                # The selection cursor may include an explicitly abandoned
                # scan, while last_successful always remains truthful.
                latest_common = checker.latest_common_minute_1h(
                    check_modifiers,
                    last_processed=selection_cursor,
                )

                is_new_s3 = bool(
                    latest_common
                    and (selection_cursor is None or latest_common > selection_cursor)
                )
                if not is_new_s3:
                    latest_https = checker.check_https_fallback(check_modifiers, now)
                    if latest_https and (
                        selection_cursor is None or latest_https > selection_cursor
                    ):
                        print(
                            f"[Scheduler] HTTPS Fallback found NEWER timestamp: {latest_https}"
                        )
                        latest_common = latest_https

                if latest_common and (
                    selection_cursor is None or latest_common > selection_cursor
                ):
                    pending_timestamp = latest_common
                    pending_attempt_count = 0

            should_run_pipeline = (
                pending_timestamp is not None
                and time.monotonic() >= retry_not_before
            )

            if should_run_pipeline:
                dt = pending_timestamp
                pending_attempt_count += 1
                cycle_state_store.record_attempt(dt, pending_attempt_count)
                print(
                    f"[Scheduler] Starting tandem cycle for {dt} "
                    f"(attempt {pending_attempt_count}/{retry_policy.max_attempts})"
                )

                outcome = run_tandem_cycle_once(
                    dt,
                    goes_render_task_queue,
                    goes_render_log_queue,
                    manager,
                    config=cycle_config,
                    goes_cycle_active_event=GOES_CYCLE_ACTIVE,
                )
                if outcome.completed:
                    cycle_state_store.record_outcome(outcome, pending_attempt_count)
                    last_successful = dt
                    selection_cursor = max(
                        value
                        for value in (last_successful, last_abandoned)
                        if value is not None
                    )
                    pending_timestamp = None
                    pending_attempt_count = 0
                    retry_not_before = 0.0
                    print(
                        f"Tandem cycle for {dt} finished with "
                        f"{len(outcome.produced_artifacts)} validated artifact(s)"
                    )
                else:
                    abandon = pending_attempt_count >= retry_policy.max_attempts
                    cycle_state_store.record_outcome(
                        outcome,
                        pending_attempt_count,
                        abandoned=abandon,
                    )
                    if abandon:
                        last_abandoned = dt
                        selection_cursor = max(
                            value
                            for value in (last_successful, last_abandoned)
                            if value is not None
                        )
                        pending_timestamp = None
                        pending_attempt_count = 0
                        retry_not_before = 0.0
                        print(
                            f"[Scheduler] Tandem cycle for {dt} was explicitly "
                            f"abandoned after {retry_policy.max_attempts} attempts; "
                            f"errors={list(outcome.errors)}"
                        )
                    else:
                        delay = retry_policy.delay_after(pending_attempt_count)
                        retry_not_before = time.monotonic() + delay
                        print(
                            f"[Scheduler] Tandem cycle for {dt} failed and remains "
                            f"pending; retrying in {delay:.1f}s; "
                            f"errors={list(outcome.errors)}"
                        )

            else:
                if pending_timestamp is not None:
                    remaining = max(0.0, retry_not_before - time.monotonic())
                    print(
                        f"[Scheduler] Retry for {pending_timestamp} is pending "
                        f"for another {remaining:.1f}s"
                    )
                elif not latest_common:
                     print("[Scheduler] No new data found (S3 or HTTPS). Waiting...")
                else:
                     print(
                         f"[Scheduler] Timestamp {latest_common} is not newer than "
                         f"selection cursor {selection_cursor}. Waiting..."
                     )

            # Wait/Check loop (15 seconds)
            for _ in range(30):
                time.sleep(0.5)

    except KeyboardInterrupt:
        print("CTRL+C detected, exiting ...")
    finally:
        started_processes.shutdown(queue_sentinels=[(goes_render_task_queue, None)], manager=manager)
        drain_log_queue(nexrad_log_queue)

if __name__ == "__main__":
    try:
        print(f"Running EdgeWARN v{get_release_version()}")
        main()
    except KeyboardInterrupt:
        print("CTRL+C detected, exiting ...")
        sys.exit(0)
