import os
import sys
import json
import re
from pathlib import Path
from datetime import datetime, timezone
import time
import multiprocessing
import asyncio
import xarray as xr
# Suppress cfgrib/xarray compatibility warnings
xr.set_options(use_new_combine_kwarg_defaults=True)

import util.file as fs
import EdgeWARN.core.ingest.mrms.main as ingest_main
from EdgeWARN.core.ingest.synoptic.main import download_rap, download_rap_async
import EdgeWARN.core.ingest.nws.main as nws_ingest
import EdgeWARN.core.ingest.metar as metar_ingest
import EdgeWARN.core.process.detect.main as detect
import EdgeWARN.core.process.integrate.main as integration
from EdgeWARN.core.schedule.scheduler import MRMSUpdateChecker
from EdgeWARN.core.ingest.mrms.config import get_check_modifiers
import EdgeWARN.ui.monitor_app as monitor_app
from util.io import TimestampedOutput, IOManager, QueueWriter
from EdgeWARN.core.api_integration.index_manager import APIIndexManager

# Remove aiodns - Some users report issues with DNS resolution with it
sys.modules.pop("aiodns", None)

sys.stdout = TimestampedOutput(sys.stdout)
sys.stderr = TimestampedOutput(sys.stderr)

io_manager = IOManager("[Main]")
args = io_manager.get_args()

lat_limits = tuple(args.lat_limits)
lon_limits = tuple(args.lon_limits)

# Initialize custom filesystem if provided
if args.base_dir:
    fs.initialize_filesystem(args.base_dir)

# Initialize API indexes at startup
try:
    index_manager = APIIndexManager(io_manager)
    index_manager.initialize_indexes()
except Exception as e:
    io_manager.write_error(f"Failed to initialize API indexes: {e}")

def pipeline(log_queue, dt):
    """Run the full ingestion → detection → integration pipeline once, logging to queue."""
    # Redirect stdout/stderr to the queue for this process
    sys.stdout = QueueWriter(log_queue)
    sys.stderr = QueueWriter(log_queue)

    def log(msg):
        log_queue.put(f"{msg}")

    async def run_async_ingest():
        log(f"INFO: Starting Async Data Ingestion for timestamp {dt}")
        
        async def safe_ingest(task_name, async_func, sync_fallback, *args):
            try:
                if asyncio.iscoroutinefunction(async_func):
                    await async_func(*args)
                else:
                    # In case it's a wrapper that isn't itself a coroutine but returns one
                    await async_func(*args)
                log(f"INFO: Async {task_name} ingestion successful")
                return True
            except Exception as e:
                log(f"WARN: Async {task_name} ingestion failed: {e}. Falling back to sync.")
                try:
                    sync_fallback(*args)
                    log(f"INFO: Sync fallback for {task_name} successful")
                    return True
                except Exception as ef:
                    log(f"ERROR: Both async and sync ingestion failed for {task_name}: {ef}")
                    return False

        # Run all ingestion tasks concurrently with individual fallbacks
        results = await asyncio.gather(
            safe_ingest("MRMS/GOES", ingest_main.download_all_files_async, ingest_main.download_all_files, dt),
            safe_ingest("RAP", download_rap_async, download_rap, dt),
            safe_ingest("NWS Alerts", nws_ingest.download_alerts_async, nws_ingest.download_alerts, dt),
            safe_ingest("METAR", metar_ingest.ingest_metars_async, metar_ingest.ingest_metars),
            return_exceptions=True
        )
        
        for i, res in enumerate(results):
            if isinstance(res, Exception):
                log(f"CRITICAL: Unexpected error in async wrapper {i}: {res}")

    # 1. Ingestion (Async with Granular Fallback)
    try:
        asyncio.run(run_async_ingest())
    except Exception as e:
        log(f"ERROR: Global async ingestion wrapper failed: {e}")

    # 2. Detection (Sync)
    try:
        log("INFO: Starting Storm Cell Detection")
        try:
            filepath_old, filepath_new = fs.latest_files(fs.MRMS_COMPOSITE_DIR, 2) 
            ps_old, ps_new = fs.latest_files(fs.MRMS_PROBSEVERE_DIR, 2)
            pt_old, pt_new = fs.latest_files(fs.MRMS_PRECIPTYP_DIR, 2)

        except RuntimeError:
            filepath_old, filepath_new = fs.latest_files(fs.MRMS_COMPOSITE_DIR, 1)[-1], None
            ps_old, ps_new = fs.latest_files(fs.MRMS_PROBSEVERE_DIR, 1)[-1], None
            pt_old, pt_new = fs.latest_files(fs.MRMS_PRECIPTYP_DIR, 1)[-1], None
        
        generated_file = detect.main(filepath_old, filepath_new, ps_old, ps_new, pt_old, pt_new, lat_limits, lon_limits, Path("stormcell_test.json"))
        
        # 3. Integration (Sync)
        integration.main(generated_file)
        log("Pipeline completed successfully")
        
    except Exception as e:
        log(f"Error in pipeline: {e}")

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

    try:
        while True:
            if ui_process and not ui_process.is_alive():
                print("GUI closed. Exiting.")
                sys.exit(0)

            now = datetime.now(timezone.utc)
            check_modifiers = get_check_modifiers()
            latest_common = checker.latest_common_minute_1h(check_modifiers)

            if latest_common and latest_common != last_processed:
                print(f"[Scheduler] DEBUG: New latest common timestamp: {latest_common}")
                dt = latest_common
                last_processed = latest_common

                # Queue to capture logs
                log_queue = multiprocessing.Queue()

                # Spawn the pipeline process
                proc = multiprocessing.Process(target=pipeline, args=(log_queue, dt))
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
                    print("[Scheduler] WARN: No common timestamp available yet. Waiting ...")
                else:
                    print(f"[Scheduler] DEBUG: Timestamp {latest_common} already processed. Waiting ...")

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
            print(f"Running EdgeWARN v1.2.0")
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
            print(f"Running EdgeWARN v1.2.0")
            print(f"Latitude limits: {lat_limits}, Longitude limits: {lon_limits}")
            main(ui_process)
        except KeyboardInterrupt:
            print("CTRL+C detected, exiting ...")
            sys.exit(0)
        finally:
            ui_process.terminate()
            ui_process.join()
