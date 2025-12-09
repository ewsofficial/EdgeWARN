import os
import sys
import json
from pathlib import Path
from datetime import datetime, timezone
import time
import multiprocessing
import util.file as fs
import EdgeWARN.core.ingest.main as ingest_main
import EdgeWARN.core.process.detect.main as detect
import EdgeWARN.core.process.integrate.main as integration
from EdgeWARN.core.schedule.scheduler import MRMSUpdateChecker
from EdgeWARN.core.ingest.config import check_modifiers
import EdgeWARN.ui.monitor_app as monitor_app
from util.io import TimestampedOutput, IOManager, QueueWriter

sys.stdout = TimestampedOutput(sys.stdout)
sys.stderr = TimestampedOutput(sys.stderr)

io_manager = IOManager("[Main]")
args = io_manager.get_args()

lat_limits = tuple(args.lat_limits)
lon_limits = tuple(args.lon_limits)

def pipeline(log_queue, dt):
    """Run the full ingestion → detection → integration pipeline once, logging to queue."""
    # Redirect stdout/stderr to the queue for this process
    sys.stdout = QueueWriter(log_queue)
    sys.stderr = QueueWriter(log_queue)

    def log(msg):
        log_queue.put(f"{msg}")

    try:
        log(f"INFO: Starting Data Ingestion for timestamp {dt}")
        ingest_main.download_all_files(dt)
        log("INFO: Starting Storm Cell Detection")
        try:
            filepath_old, filepath_new = fs.latest_files(fs.MRMS_COMPOSITE_DIR, 2) 
            ps_old, ps_new = fs.latest_files(fs.MRMS_PROBSEVERE_DIR, 2)
            pt_old, pt_new = fs.latest_files(fs.MRMS_PRECIPTYP_DIR, 2)

        except RuntimeError:
            filepath_old, filepath_new = fs.latest_files(fs.MRMS_COMPOSITE_DIR, 1)[-1], None
            ps_old, ps_new = fs.latest_files(fs.MRMS_PROBSEVERE_DIR, 1)[-1], None
            pt_old, pt_new = fs.latest_files(fs.MRMS_PRECIPTYP_DIR, 1)[-1], None
        
        detect.main(filepath_old, filepath_new, ps_old, ps_new, pt_old, pt_new, lat_limits, lon_limits, Path("stormcell_test.json"))
        integration.main()
        log("Pipeline completed successfully")
    except Exception as e:
        log(f"Error in pipeline: {e}")

def main(ui_process=None):
    """Scheduler: spawn pipeline() every 15 s if a new latest_common timestamp is available."""
    print("Scheduler started. Press CTRL+C to exit.")
    checker = MRMSUpdateChecker(verbose=True)
    last_processed = None  # Track last processed timestamp

    # Check for existing JSON output to initialize last_processed
    json_path = Path("stormcell_test.json")
    if json_path.exists():
        try:
            with open(json_path, 'r') as f:
                data = json.load(f)
                if isinstance(data, dict) and "latest_timestamp" in data:
                    ts_str = data["latest_timestamp"]
                    # Parse ISO format timestamp
                    last_processed = datetime.fromisoformat(ts_str)
                    print(f"[Scheduler] Initialized last_processed from {json_path}: {last_processed}")
        except Exception as e:
            print(f"[Scheduler] Failed to load initial timestamp from {json_path}: {e}")

    try:
        while True:
            if ui_process and not ui_process.is_alive():
                print("GUI closed. Exiting.")
                sys.exit(0)

            now = datetime.now(timezone.utc)
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
                print(f"Spawned pipeline process PID={proc.pid}")

                # Print logs in real-time
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

            # Wait/Check loop
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
            print(f"Running EdgeWARN v0.6.2-alpha (No-GUI Mode)")
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
            print(f"Running EdgeWARN v0.6.2-alpha")
            print(f"Latitude limits: {lat_limits}, Longitude limits: {lon_limits}")
            main(ui_process)
        except KeyboardInterrupt:
            print("CTRL+C detected, exiting ...")
            sys.exit(0)
        finally:
            ui_process.terminate()
            ui_process.join()
