import os
import sys
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
from util.io import TimestampedOutput, IOManager

sys.stdout = TimestampedOutput(sys.stdout)
sys.stderr = TimestampedOutput(sys.stderr)

io_manager = IOManager("[Main]")
args = io_manager.get_args()

lat_limits = tuple(args.lat_limits)
lon_limits = tuple(args.lon_limits)

if __name__ == '__main__':
    print(f"Running EdgeWARN v0.5.2-alpha")
    print(f"Latitude limits: {lat_limits}, Longitude limits: {lon_limits}")

def pipeline(log_queue, dt):
    """Run the full ingestion → detection → integration pipeline once, logging to queue."""
    def log(msg):
        log_queue.put(f"{msg}")

    try:
        log(f"Starting Data Ingestion for timestamp {dt}")
        ingest_main.download_all_files(dt)
        log("Starting Storm Cell Detection")
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

def main():
    """Scheduler: spawn pipeline() every 15 s if a new latest_common timestamp is available."""
    print("Scheduler started. Press CTRL+C to exit.")
    checker = MRMSUpdateChecker(verbose=True)
    last_processed = None  # Track last processed timestamp

    try:
        while True:
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

            time.sleep(15)  # Check every 15 seconds

    except KeyboardInterrupt:
        print("CTRL+C detected, exiting ...")
        sys.exit(0)

if __name__ == "__main__":
    main()
