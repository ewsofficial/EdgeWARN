import sys
import re
from datetime import datetime, timezone, timedelta
import time
import multiprocessing
import asyncio

import util.file as fs
import common.ingest.nws.main as nws_ingest
import common.ingest.metar as metar_ingest
from common.ingest.wpc.main import run_wpc_ingest
from common.pipeline.coordinator import run_tandem_ingest_cycle
from EdgeWARN import initialize_runtime
from EdgeWARN.pipeline import edgewarn_tandem_worker
from EWMRS.pipeline import ewmrs_tandem_worker
from EdgeWARN.schedule.scheduler import MRMSUpdateChecker
from common.ingest.mrms.config import get_check_modifiers
from util.io import TimestampedOutput, IOManager

sys.stdout = TimestampedOutput(sys.stdout)
sys.stderr = TimestampedOutput(sys.stderr)

io_manager = IOManager("[Main]")
args = io_manager.get_args()

lat_limits = tuple(args.lat_limits)
lon_limits = tuple(args.lon_limits)

initialize_runtime(base_dir=args.base_dir, io_manager=io_manager)


def _queue_log(log_queue, message):
    timestamp = datetime.now(timezone.utc).isoformat()
    log_queue.put(f"[{timestamp}] {message}")


def _drain_log_queue(log_queue):
    while not log_queue.empty():
        print(log_queue.get())


def _sleep(total_seconds, interval=1.0):
    end_time = time.time() + total_seconds
    while time.time() < end_time:
        time.sleep(min(interval, max(0.0, end_time - time.time())))


def _sleep_until_boundary(minutes):
    now = datetime.now(timezone.utc)
    minutes_to_next = minutes - (now.minute % minutes)
    if minutes_to_next == 0 and now.second == 0 and now.microsecond == 0:
        minutes_to_next = minutes
    next_run = now.replace(second=0, microsecond=0) + timedelta(minutes=minutes_to_next)
    sleep_seconds = max(0.0, (next_run - now).total_seconds())
    if sleep_seconds > 0:
        _sleep(sleep_seconds, interval=1.0)


def metar_loop():
    try:
        while True:
            _sleep_until_boundary(5)

            try:
                asyncio.run(metar_ingest.ingest_metars_async())
            except Exception as e:
                print(f"[METAR Loop] Error: {e}")
    except KeyboardInterrupt:
        return


def nws_loop():
    try:
        while True:
            try:
                asyncio.run(nws_ingest.download_alerts_async(datetime.now(timezone.utc)))
            except Exception as e:
                print(f"[NWS Loop] Error: {e}")

            _sleep(120, interval=1.0)
    except KeyboardInterrupt:
        return


def wpc_loop():
    try:
        while True:
            _sleep_until_boundary(15)

            try:
                run_wpc_ingest()
            except Exception as e:
                print(f"[WPC Loop] Error: {e}")
    except KeyboardInterrupt:
        return

def _run_tandem_cycle(dt):
    log_queue = multiprocessing.Queue()
    manager = multiprocessing.Manager()
    shared_state = manager.dict()

    detection_ready_event = multiprocessing.Event()
    ewmrs_ready_event = multiprocessing.Event()
    integration_ready_event = multiprocessing.Event()

    try:
        cycle_state = asyncio.run(
            run_tandem_ingest_cycle(
                dt,
                lambda msg: _queue_log(log_queue, msg),
            )
        )
    finally:
        pass

    shared_state["detection_inputs_ready"] = cycle_state.detection_inputs_ready
    shared_state["ewmrs_inputs_ready"] = cycle_state.ewmrs_inputs_ready
    shared_state["edgewarn_integration_inputs_ready"] = cycle_state.edgewarn_integration_inputs_ready
    shared_state["edgewarn_generated_file"] = ""
    shared_state["errors"] = dict(cycle_state.errors)

    detection_ready_event.set()
    ewmrs_ready_event.set()
    integration_ready_event.set()

    edgewarn_proc = multiprocessing.Process(
        target=edgewarn_tandem_worker,
        args=(
            log_queue,
            shared_state,
            detection_ready_event,
            integration_ready_event,
            dt,
            lat_limits,
            lon_limits,
            args.profile,
            args.disable_ctam,
            args.disable_tracking,
            args.refl_threshold,
            args.min_seed_percentage,
            args.drop_offset,
        ),
    )
    ewmrs_proc = multiprocessing.Process(
        target=ewmrs_tandem_worker,
        args=(log_queue, shared_state, ewmrs_ready_event, dt),
    )

    edgewarn_proc.start()
    ewmrs_proc.start()

    while edgewarn_proc.is_alive() or ewmrs_proc.is_alive() or not log_queue.empty():
        _drain_log_queue(log_queue)
        time.sleep(1)

    edgewarn_proc.join()
    ewmrs_proc.join()
    _drain_log_queue(log_queue)
    manager.shutdown()



def main():
    """Scheduler: run a shared ingest cycle and launch EdgeWARN/EWMRS in tandem."""
    print("Scheduler started. Press CTRL+C to exit.")
    if args.disable_ctam:
        print("[Scheduler] CTAM execution disabled via --disable-ctam")
    if args.disable_tracking:
        print("[Scheduler] Tracking disabled via --disable-tracking")
    print(
        "[Scheduler] Detection thresholds: "
        f"refl_threshold={args.refl_threshold}, "
        f"min_seed_percentage={args.min_seed_percentage}, "
        f"drop_offset={args.drop_offset}"
    )
    checker = MRMSUpdateChecker(verbose=True)
    last_processed = None  # Track last processed timestamp

    # Check for existing JSON output to initialize last_processed
    try:
        if fs.STORMCELL_DIR.exists():
            files = sorted(fs.STORMCELL_DIR.glob("stormcells_*.json"))
            if files:
                latest_file = files[-1]
                match = re.search(r"stormcells_(\d{8}-\d{6})\.json", latest_file.name)
                if match:
                    ts_str = match.group(1) 
                    dt_exact = datetime.strptime(ts_str, "%Y%m%d-%H%M%S").replace(tzinfo=timezone.utc)
                    last_processed = dt_exact.replace(second=0, microsecond=0)
                    print(f"[Scheduler] Initialized last_processed from {latest_file}: {last_processed}")
                else:
                    print(f"[Scheduler] Could not parse timestamp from {latest_file}")
            else:
                 print(f"[Scheduler] No previous stormcell data found in {fs.STORMCELL_DIR}. Starting fresh.")
        else:
             print(f"[Scheduler] {fs.STORMCELL_DIR} does not exist. Starting fresh.")

    except Exception as e:
        print(f"[Scheduler] Failed to initialize last_processed: {e}")

    print("[Scheduler] Starting background accessory ingests (METAR, NWS, WPC)...")
    metar_proc = multiprocessing.Process(target=metar_loop, daemon=True)
    nws_proc = multiprocessing.Process(target=nws_loop, daemon=True)
    wpc_proc = multiprocessing.Process(target=wpc_loop, daemon=True)
    metar_proc.start()
    nws_proc.start()
    wpc_proc.start()

    try:
        while True:
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

                _run_tandem_cycle(dt)
                print(f"Tandem cycle for {dt} finished")

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
        sys.exit(0)

if __name__ == "__main__":
    try:
        print("Running EdgeWARN v2.0.0-rc1")
        print(f"Latitude limits: {lat_limits}, Longitude limits: {lon_limits}")
        main()
    except KeyboardInterrupt:
        print("CTRL+C detected, exiting ...")
        sys.exit(0)
