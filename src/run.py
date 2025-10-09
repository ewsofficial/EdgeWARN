import sys
from pathlib import Path
from datetime import datetime, timedelta
import time
import multiprocessing
import util.core.file as fs
import EdgeWARN.DataIngestion.main as ingest_main
import EdgeWARN.PreProcess.CellDetection.main as detect
import EdgeWARN.PreProcess.CellIntegration.main as integration

# Constants
lat_limits, lon_limits = (36.0, 37.5), (282.4, 284.8)

def pipeline():
    """Run the full ingestion → detection → integration pipeline once."""
    try:
        current_time = datetime.now()
        print(f"[{current_time}] Starting Data Ingestion")
        ingest_main.main()

        print(f"[{datetime.now()}] Starting Storm Cell Detection")
        filepath_old, filepath_new = fs.latest_files(fs.MRMS_3D_DIR, 2)
        ps_old, ps_new = fs.latest_files(fs.MRMS_PROBSEVERE_DIR, 2)
        detect.main(filepath_old, filepath_new, ps_old, ps_new, lat_limits, lon_limits, Path("stormcell_test.json"))
        integration.main()

    except Exception as e:
        print(f"Error in pipeline: {e}")


def main():
    """Scheduler: spawn pipeline() as a separate process every even minute at :30 seconds."""
    print("Scheduler started. Press CTRL+C to exit.")
    try:
        while True:
            now = datetime.now()
            # Find next even minute :30
            next_even_minute = (now.minute // 2) * 2
            next_run = now.replace(minute=next_even_minute, second=30, microsecond=0)
            if next_run <= now:
                next_run += timedelta(minutes=2)

            sleep_seconds = (next_run - now).total_seconds()
            print(f"Sleeping for {sleep_seconds:.1f} seconds until {next_run.time()}")
            time.sleep(sleep_seconds)

            # Spawn pipeline as a separate process
            p = multiprocessing.Process(target=pipeline)
            p.start()
            print(f"[{datetime.now()}] Spawned pipeline process PID={p.pid}")

    except KeyboardInterrupt:
        print("CTRL+C detected, exiting...")
        sys.exit(0)


if __name__ == "__main__":
    main()
