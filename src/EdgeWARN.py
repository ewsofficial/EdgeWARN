import sys
from pathlib import Path
# Append Path
path = Path(__file__).parent
sys.path.append(str(path))
import util.core.file as fs
import EdgeWARN.DataIngestion.main as ingest_main
import EdgeWARN.PreProcess.CellDetection.main as detect_cells
from datetime import datetime
import time

# Constants
refresh_time = 240 # In seconds
storm_json = Path("stormcell_test.json")
lat_limits, lon_limits = (31.5, 34.2), (260.0, 265.0)

while True:
    current_time = datetime.now()

    # Ingest data
    print(f"Starting Data Ingestion at time: {current_time}")
    ingest_main.main()

    # Detect storm cells
    print("Starting Storm Cell Detection")
    try:
        filepath_old, filepath_new = fs.latest_files(fs.MRMS_3D_DIR, 2)
    except Exception as e:
        print(f"Error in file retrieval: {e}.")
        time.sleep(180)
    detect_cells.main(filepath_old, filepath_new, storm_json, lat_limits, lon_limits)
    time.sleep(170)


