import sys
import re
from datetime import datetime, timezone
import time
import multiprocessing
import asyncio

import util.file as fs
import common.ingest.nws.main as nws_ingest
import common.ingest.metar as metar_ingest
from EdgeWARN import initialize_runtime, realtime_pipeline
from EdgeWARN.core.schedule.scheduler import MRMSUpdateChecker
from common.ingest.mrms.config import get_check_modifiers
import EdgeWARN.ui.monitor_app as monitor_app
from util.io import TimestampedOutput, IOManager, QueueWriter

sys.stdout = TimestampedOutput(sys.stdout)
sys.stderr = TimestampedOutput(sys.stderr)

io_manager = IOManager("[Main]")
args = io_manager.get_args()

lat_limits = tuple(args.lat_limits)
lon_limits = tuple(args.lon_limits)

initialize_runtime(base_dir=args.base_dir, io_manager=io_manager)

def metar_loop(ui_process=None):
    from datetime import timedelta
    while True:
        if ui_process and not ui_process.is_alive():
            sys.exit(0)
            
        now = datetime.now(timezone.utc)
        minutes_to_next = 5 - (now.minute % 5)
        next_run = now.replace(second=0, microsecond=0) + timedelta(minutes=minutes_to_next)
        sleep_seconds = (next_run - now).total_seconds()
        
        if sleep_seconds > 0:
            time.sleep(sleep_seconds)
            
        if ui_process and not ui_process.is_alive():
             sys.exit(0)
             
        try:
            asyncio.run(metar_ingest.ingest_metars_async())
        except Exception as e:
            print(f"[METAR Loop] Error: {e}")

def nws_loop(ui_process=None):
    while True:
        if ui_process and not ui_process.is_alive():
            sys.exit(0)
            
        try:
            asyncio.run(nws_ingest.download_alerts_async(datetime.now(timezone.utc)))
        except Exception as e:
            print(f"[NWS Loop] Error: {e}")
            
        for _ in range(120):
             if ui_process and not ui_process.is_alive():
                 sys.exit(0)
             time.sleep(1)



def main(ui_process=None):
    """Scheduler: spawn pipeline() every 15s if a new latest_common timestamp is available."""
    print("Scheduler started. Press CTRL+C to exit.")
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

    print("[Scheduler] Starting background loops (METAR, NWS)...")
    metar_proc = multiprocessing.Process(target=metar_loop, args=(ui_process,), daemon=True)
    nws_proc = multiprocessing.Process(target=nws_loop, args=(ui_process,), daemon=True)
    metar_proc.start()
    nws_proc.start()

    try:
        while True:
            if ui_process and not ui_process.is_alive():
                print("GUI closed. Exiting.")
                sys.exit(0)

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

                # Queue to capture logs
                log_queue = multiprocessing.Queue()

                # Spawn the pipeline process
                proc = multiprocessing.Process(
                    target=realtime_pipeline,
                    args=(log_queue, dt, lat_limits, lon_limits, args.profile),
                )
                proc.start()
                print(f"Spawned pipeline process PID={proc.pid} for {dt}")

                # Wait for process to complete, printing logs in real-time
                while proc.is_alive() or not log_queue.empty():
                    if ui_process and not ui_process.is_alive():
                        print("GUI closed. Terminating pipeline and exiting.")
                        proc.terminate()
                        sys.exit(0)

                    while not log_queue.empty():
                        print(log_queue.get())
                    time.sleep(1)

                proc.join()
                print(f"Pipeline process PID={proc.pid} finished")

            else:
                if not latest_common:
                     print("[Scheduler] No new data found (S3 or HTTPS). Waiting...")
                else:
                     print(f"[Scheduler] Timestamp {latest_common} already processed. Waiting...")

            # Wait/Check loop (15 seconds)
            for _ in range(30):
                if ui_process and not ui_process.is_alive():
                    print("GUI closed. Exiting.")
                    sys.exit(0)
                time.sleep(0.5)

    except KeyboardInterrupt:
        print("CTRL+C detected, exiting ...")
        sys.exit(0)

if __name__ == "__main__":
    if args.nogui:
        # No GUI mode: Print directly to console (already set up by default sys.stdout/stderr)
        try:
            print(f"Running EdgeWARN v2.0.0-rc1")
            print(f"Latitude limits: {lat_limits}, Longitude limits: {lon_limits}")
            main()
        except KeyboardInterrupt:
            print("CTRL+C detected, exiting ...")
            sys.exit(0)
    else:
        # GUI mode: Redirect output to UI queue and spawn UI process
        
        # Create a queue for the UI logs
        ui_queue = multiprocessing.Queue()
        
        # Redirect stdout/stderr to the UI queue
        sys.stdout = QueueWriter(ui_queue)
        sys.stderr = QueueWriter(ui_queue)
        
        # Spawn the UI process
        ui_process = multiprocessing.Process(target=monitor_app.run, args=(None, ui_queue))
        ui_process.start()
        
        try:
            print(f"Running EdgeWARN v2.0.0-rc1")
            print(f"Latitude limits: {lat_limits}, Longitude limits: {lon_limits}")
            main(ui_process)
        except KeyboardInterrupt:
            print("CTRL+C detected, exiting ...")
            sys.exit(0)
        finally:
            ui_process.terminate()
            ui_process.join()
