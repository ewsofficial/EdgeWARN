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
EWMRS_ENABLED = not args.disable_ewmrs
NWS_ENABLED = not args.disable_nws
METAR_ENABLED = not args.disable_metar
GOES_ENABLED = not args.disable_goes

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
    goes_render_wait_seconds=GOES_RENDER_WAIT_SECONDS,
    goes_render_wait_interval_seconds=GOES_RENDER_WAIT_INTERVAL_SECONDS,
)



def main():
    """Scheduler: run a shared ingest cycle and launch EdgeWARN/EWMRS in tandem."""
    print("Scheduler started. Press CTRL+C to exit.")
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
    print(
        "[Scheduler] Detection thresholds: "
        f"disable_polygon_expansion={args.disable_polygon_expansion}, "
        f"refl_threshold={args.refl_threshold}, "
        f"min_seed_percentage={args.min_seed_percentage}, "
        f"drop_offset={args.drop_offset}"
    )
    if GOES_ENABLED:
        print("[Scheduler] GOES ingest decoupled: running as independent background process")
    checker = MRMSUpdateChecker(verbose=True)
    last_processed, init_message = load_last_processed_from_stormcells(fs.STORMCELL_DIR)
    print(init_message)

    print("[Scheduler] Starting background accessory ingests...")
    # Hoisted out of _run_tandem_cycle: a Manager spawns a child server
    # process and IPC machinery on construction; reusing one across cycles
    # avoids that startup cost every minute.
    manager = multiprocessing.Manager()
    metar_proc = multiprocessing.Process(target=metar_loop, daemon=True) if METAR_ENABLED else None
    nws_proc = multiprocessing.Process(target=nws_loop, daemon=True) if NWS_ENABLED else None
    wpc_proc = multiprocessing.Process(target=wpc_loop, daemon=True)
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
    ) if EWMRS_ENABLED else None
    # NEXRAD ingest uses a ProcessPoolExecutor for parser workers, so this
    # process must not be daemonic or child worker creation will fail.
    nexrad_ingest_proc = multiprocessing.Process(
        target=nexrad_ingest_loop,
        args=(nexrad_log_queue, args.base_dir),
        name="NEXRAD-Ingest",
        daemon=False,
    ) if EWMRS_ENABLED else None
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
            # Pass last_processed to allow StartAfter optimization
            latest_common = checker.latest_common_minute_1h(check_modifiers, last_processed=last_processed)

            # Strict check: Only accept if new AND strictly newer than last_processed
            is_new_s3 = False
            if latest_common:
                if last_processed is None:
                    is_new_s3 = True
                elif latest_common > last_processed:
                    is_new_s3 = True
            
            # If S3 didn't give us a NEW timestamp, try HTTPS immediately
            if not is_new_s3:
                # print(f"[Scheduler] S3 yielded no new data (Latest: {latest_common}, Last: {last_processed}). Checking HTTPS...")
                latest_https = checker.check_https_fallback(check_modifiers, now)
                
                # Check if HTTPS result is better
                if latest_https:
                    is_new_https = False
                    if last_processed is None:
                         is_new_https = True
                    elif latest_https > last_processed:
                         is_new_https = True
                    
                    if is_new_https:
                        print(f"[Scheduler] HTTPS Fallback found NEWER timestamp: {latest_https}")
                        latest_common = latest_https # Take the HTTPS one
                    else:
                        pass # HTTPS found data but it's also old
                else:
                    pass # HTTPS failed or no files

            # Now proceed with latest_common if it is effectively new
            # Re-evaluate newness (in case we switched to HTTPS one)
            should_run_pipeline = False
            if latest_common:
                 if last_processed is None:
                     should_run_pipeline = True
                 elif latest_common > last_processed:
                     should_run_pipeline = True

            if should_run_pipeline:
                print(f"[Scheduler] DEBUG: New latest common timestamp: {latest_common}")
                dt = latest_common
                last_processed = latest_common

                cycle_ok = run_tandem_cycle_once(
                    dt,
                    goes_render_task_queue,
                    goes_render_log_queue,
                    manager,
                    config=cycle_config,
                    goes_cycle_active_event=GOES_CYCLE_ACTIVE,
                )
                if cycle_ok:
                    print(f"Tandem cycle for {dt} finished")
                else:
                    print(f"Tandem cycle for {dt} did not complete successfully")

            else:
                if not latest_common:
                     print("[Scheduler] No new data found (S3 or HTTPS). Waiting...")
                else:
                     print(f"[Scheduler] Timestamp {latest_common} already processed. Waiting...")

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
        print(f"Latitude limits: {lat_limits}, Longitude limits: {lon_limits}")
        main()
    except KeyboardInterrupt:
        print("CTRL+C detected, exiting ...")
        sys.exit(0)
