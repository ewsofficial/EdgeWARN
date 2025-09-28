import sys
from pathlib import Path
# Append Path
path = Path(__file__).parent
sys.path.append(str(path))
import util.core.file as fs
import EdgeWARN.DataIngestion.main as ingest_main
import EdgeWARN.PreProcess.CellIntegration.main as integration
from datetime import datetime
import time
"""
raw_lat_limits = input("Enter lat limits in the form: (lat_lower, lat_upper): ")
raw_lon_limits = input("Enter lon limits in 0-360 form: (lon_lower, lon_upper): ")

def _parse_limits(raw):
    if not raw:
        return None
    nums = [float(x) for x in re.findall(r"-?\d+\.?\d*", raw)]
    if len(nums) >= 2:
        return (nums[0], nums[1])
    return None

import re

lat_limits = _parse_limits(raw_lat_limits)
lon_limits = _parse_limits(raw_lon_limits)
"""
# Constants
storm_json = Path("stormcell_test.json")
lat_limits, lon_limits = (36.0, 37.5), (282.4, 284.8)

while True:
    try:
        current_time = datetime.now()

        # Ingest data
        print(f"Starting Data Ingestion at time: {current_time}")
        ingest_main.main()

        # Detect storm cells
        print("Starting Storm Cell Detection")
        try:
            filepath_old, filepath_new = fs.latest_files(fs.MRMS_3D_DIR, 2)
            integration.main()
            print("Press CTRL + C to exit")
            time.sleep(120)
        except Exception as e:
            print(f"Error in file retrieval: {e}.")
            print("Press CTRL + C to exit")
            time.sleep(180)
    
    except KeyboardInterrupt:
        print("CTRL + C Detected, exiting ...")
        sys.exit(0)


