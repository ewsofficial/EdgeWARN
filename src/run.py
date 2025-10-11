import sys
from pathlib import Path
from datetime import datetime, timedelta, timezone
import time
import multiprocessing
import util.core.file as fs
import EdgeWARN.DataIngestion.main as ingest_main
import EdgeWARN.PreProcess.CellDetection.main as detect
import EdgeWARN.PreProcess.CellIntegration.main as integration
from EdgeWARN.schedule.scheduler import MRMSUpdateChecker
from EdgeWARN.DataIngestion.config import check_modifiers

# ===== Wrap stdout/stderr to add timestamps to all prints =====
class TimestampedOutput:
    def __init__(self, stream):
        self.stream = stream

    def write(self, message):
        if message.strip():  # skip empty lines
            timestamp = datetime.now(timezone.utc).isoformat()
            self.stream.write(f"[{timestamp}] {message}")
        else:
            self.stream.write(message)

    def flush(self):
        self.stream.flush()

sys.stdout = TimestampedOutput(sys.stdout)
sys.stderr = TimestampedOutput(sys.stderr)

# Constants
lat_limits, lon_limits = (33.5, 35.7), (280.7, 284.6)

def pipeline(log_queue, dt):
    """Run the full ingestion → detection → integration pipeline once, logging to queue."""
    def log(msg):
        log_queue.put(f"[{datetime.now(timezone.utc).isoformat()}] {msg}")

    try:
        log(f"Starting Data Ingestion for timestamp {dt}")
        ingest_main.download_all_files(dt)
        log("Starting Storm Cell Detection")
        filepath_old, filepath_new = fs.latest_files(fs.MRMS_COMPOSITE_DIR, 2)
        ps_old, ps_new = fs.latest_files(fs.MRMS_PROBSEVERE_DIR, 2)
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
                print(f"[Scheduler] ✅ New latest common timestamp: {latest_common}")
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
                    time.sleep(0.1)

                proc.join()
                print(f"Pipeline process PID={proc.pid} finished")
            else:
                if not latest_common:
                    print("[Scheduler] ⚠️ No common timestamp available yet. Waiting ...")
                else:
                    print(f"[Scheduler] ⏸ Timestamp {latest_common} already processed. Waiting ...")

            time.sleep(15)  # Check every 15 seconds

    except KeyboardInterrupt:
        print("CTRL+C detected, exiting ...")
        sys.exit(0)

if __name__ == "__main__":
    main()
