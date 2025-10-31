import os
import sys
from pathlib import Path
from datetime import datetime, timezone
import time
import multiprocessing
import util.file as fs
import EdgeWARN.DataIngestion.main as ingest_main
import EdgeWARN.PreProcess.CellDetection.main as detect
import EdgeWARN.PreProcess.CellIntegration.main as integration
from EdgeWARN.schedule.scheduler import MRMSUpdateChecker
from EdgeWARN.DataIngestion.config import check_modifiers
from util.io import TimestampedOutput
import argparse

sys.stdout = TimestampedOutput(sys.stdout)
sys.stderr = TimestampedOutput(sys.stderr)

# ===== Process modifiers =====
parser = argparse.ArgumentParser(description="EdgeWARN modifier specification")
parser.add_argument(
    "--lat_limits",
    type=float,
    nargs=2,
    metavar=("LAT_MIN", "LAT_MAX"),
    default=[0, 0],
    help="Latitude limits for processing"
)
parser.add_argument(
    "--lon_limits",
    type=float,
    nargs=2,
    metavar=("LON_MIN", "LON_MAX"),
    default=[0, 0],
    help="Longitude limits for processing"
)
args = parser.parse_args()

# ===== Validation =====
if not args.lat_limits or not args.lon_limits or len(args.lat_limits) != 2 or len(args.lon_limits) != 2:
    print("ERROR: Latitude and longitude limits must both be provided as two numeric values each.")
    print("Example: --lat_limits 33.5 35.7 --lon_limits 280.7 284.6")
    sys.exit(1)

if args.lat_limits == [0, 0] or args.lon_limits == [0, 0]:
    print("ERROR: lat_limits or lon_limits not specified! They must be two numeric values each.")
    sys.exit(1)

# ===== Convert longitude from -180:180 to 0:360 if needed =====
lon_limits = [lon % 360 for lon in args.lon_limits]

print(f"Running EdgeWARN v0.4.3")
print(f"Latitude limits: {tuple(args.lat_limits)}, Longitude limits: {tuple(lon_limits)}")

lat_limits = tuple(args.lat_limits)
lon_limits = tuple(lon_limits)

def pipeline(log_queue, dt):
    """Run the full ingestion → detection → integration pipeline once, logging to queue."""
    def log(msg):
        log_queue.put(f"[{datetime.now(timezone.utc).isoformat()}] {msg}")

    try:
        log(f"Starting Data Ingestion for timestamp {dt}")
        ingest_main.download_all_files(dt)
        log("Starting Storm Cell Detection")
        try:
            filepath_old, filepath_new = fs.latest_files(fs.MRMS_COMPOSITE_DIR, 2) 
            ps_old, ps_new = fs.latest_files(fs.MRMS_PROBSEVERE_DIR, 2)

        except RuntimeError:
            filepath_old, filepath_new = fs.latest_files(fs.MRMS_COMPOSITE_DIR, 1)[-1], None
            ps_old, ps_new = fs.latest_files(fs.MRMS_PROBSEVERE_DIR, 1)[-1], None
        
        detect.main(filepath_old, filepath_new, ps_old, ps_new, lat_limits, lon_limits, Path("stormcell_test.json"))
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
