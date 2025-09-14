import sys
from pathlib import Path
import util.file as fs
import EdgeWARN.DataIngestion.main as ingest_main
import EdgeWARN.PreProcess.CellDetection.main as detect_cells
from datetime import datetime
import time

# Main Execution Script

# Append Path
path = Path(__file__).parent
sys.path.append(str(path))

# Constants
refresh_time = 240 # In seconds
storm_json = Path("stormcell_test.json")
lat_limits, lon_limits = (45.3, 47.3), (256.6, 260.2)

while True:
    current_time = datetime.now()

    # Ingest data
    print(f"Starting Data Ingestion at time: {current_time}")
    ingest_main.main()

    # Detect storm cells
    print("Starting Storm Cell Detection")
    filepath_old, filepath_new = fs.latest_mosaic(2)    
    detect_cells.main(filepath_old, filepath_new, storm_json, lat_limits, lon_limits)
    time.sleep(170)


