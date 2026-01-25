import time
import sys
import gc
from pathlib import Path
from datetime import datetime

# Add src to path
SRC_DIR = Path(__file__).resolve().parents[1] / "src"
sys.path.insert(0, str(SRC_DIR))

from EdgeWARN.core.process.detect.detect import detect_cells
from util.io import IOManager
import util.file as fs

def run_verification():
    io_manager = IOManager("[VerifyDetect]")
    
    # 1. Find Data (just like main.py/detect.py does)
    try:
        radar_path = fs.latest_files(fs.MRMS_COMPOSITE_DIR, 1)[-1]
        ps_path = fs.latest_files(fs.MRMS_PROBSEVERE_DIR, 1)[-1]
        
        # PrecipType is optional for the core "detect" logic correctness test 
        # (detect_cells handles execution flow), but we need it to fully exercise create_entry/hailcore
        try:
            pt_path = fs.latest_files(fs.MRMS_PRECIPTYP_DIR, 1)[-1]
        except:
             io_manager.write_warning("No PrecipType found, using Radar as dummy (will fail logic but run pipeline)")
             pt_path = radar_path 

        io_manager.write_info(f"Radar: {Path(radar_path).name}")
        io_manager.write_info(f"ProbSevere: {Path(ps_path).name}")
        
    except Exception as e:
        io_manager.write_error(f"Failed to find necessary data files: {e}")
        return

    # Define Bounds (Standard CONUS subset used in dev)
    lat_min, lat_max = 30.0, 45.0
    lon_min, lon_max = -100.0, -80.0
    # Convert to 0-360 for internal logic if needed, but pipeline expects -180 to 180 usually or 0-360
    # The existing pipeline seems to handle 0-360 internally or expects input in ...? 
    # detect.py usage: lat_min, lat_max = 35.0, 38.0; lon_min, lon_max = 283.0, 285.0 (Positive East)
    # Let's use the values found in detect.py __main__ as a safe default for a "hot" area
    lat_min, lat_max = 33.0, 42.0
    lon_min, lon_max = 260.0, 290.0 # ~ -100 to -70 deg

    io_manager.write_info("Starting Detection Benchmark...")
    
    gc.collect()
    start_time = time.time()
    
    entries = detect_cells(
        radar_path, 
        ps_path, 
        pt_path, 
        io_manager, 
        lat_min, 
        lat_max, 
        lon_min, 
        lon_max
    )
    
    end_time = time.time()
    duration = end_time - start_time
    
    io_manager.write_info(f"Detection completed in {duration:.4f} seconds")
    io_manager.write_info(f"Number of cells detected: {len(entries)}")
    
    if len(entries) > 0:
        sample = entries[0]
        io_manager.write_info(f"Sample Cell ID: {sample.get('id')}")
        io_manager.write_info(f"Sample Centroid: {sample.get('centroid')}")
        io_manager.write_info(f"Sample Max Refl: {sample.get('max_refl')}")
    else:
        io_manager.write_warning("No cells detected! verification might be inconclusive regarding correctness.")

if __name__ == "__main__":
    run_verification()
