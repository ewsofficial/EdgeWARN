import argparse
import sys
import json
import time
from pathlib import Path
from datetime import datetime, timedelta, timezone
from util.io import IOManager, TimestampedOutput
import util.file as fs
import EdgeWARN.core.ingest.main as ingest_main
from EdgeWARN.core.ingest.config import mrms_modifiers, goes_modifiers
from EdgeWARN.core.process.detect.main import main as detect_main
from EdgeWARN.core.process.integrate.integrate import StormCellIntegrator
from EdgeWARN.core.process.integrate.utils import StatFileHandler
from EdgeWARN.core.process.detect.tools.save import CellDataSaver
from util.handler import extract_timestamp

# Redirect stdout/stderr to ensure timestamped logging
sys.stdout = TimestampedOutput(sys.stdout)
sys.stderr = TimestampedOutput(sys.stderr)

io_manager = IOManager("[HistoricalProcess]")

def find_file_for_ts(directory: Path, dt: datetime, max_delta_minutes=2):
    """
    Finds a file in the directory that matches the given timestamp (to the minute).
    Allows for a small tolerance window if exact match isn't found (e.g. slight delay),
    but preferably looks for the exact file.
    """
    if not directory.exists():
        return None

    dt_str_mrms = dt.strftime("%Y%m%d-%H%M") # Common in MRMS
    # Note: MRMS files are often named YYYYMMDD-HHMMSS. 
    # We will look for files where the extracted timestamp matches dt within tolerance.

    files = sorted([f for f in directory.glob("*") if f.is_file() and f.suffix.lower() != ".idx"])
    
    # First pass: Exact match on minute
    for f in files:
        ts = extract_timestamp(f.name)
        if ts:
            ts_dt = ts if isinstance(ts, datetime) else datetime.fromisoformat(ts)
            # Ensure timezone awareness
            if ts_dt.tzinfo is None:
                ts_dt = ts_dt.replace(tzinfo=timezone.utc)
            
            # Compare up to minute
            if ts_dt.replace(second=0, microsecond=0) == dt.replace(second=0, microsecond=0):
                return str(f)
    
    return None

def process_timestep(dt, lat_limits, lon_limits, json_output, previous_files):
    """
    Process a single timestep.
    """
    io_manager.write_info(f"=== Processing Timestamp: {dt.isoformat()} ===")

    # 1. Ingest
    io_manager.write_info("Step 1: Ingesting Data...")
    ingest_main.download_all_files(dt, max_entries=60)

    # 2. Identify Files for Detection
    io_manager.write_info("Step 2: Identifying Files...")
    
    # Helper to get file or None
    def get_file(dir_path):
        f = find_file_for_ts(dir_path, dt)
        if f: 
            return f
        else:
            return None

    # MRMS Composite Refresh (Radar)
    radar_new = get_file(fs.MRMS_COMPOSITE_DIR)
    
    # ProbSevere
    ps_new = get_file(fs.MRMS_PROBSEVERE_DIR)
    
    # PrecipType
    pt_new = get_file(fs.MRMS_PRECIPTYP_DIR)

    # Retrieve 'old' files from previous successful step state
    radar_old = previous_files.get('radar')
    ps_old = previous_files.get('ps')
    pt_old = previous_files.get('pt')

    if not radar_new:
        io_manager.write_warning(f"No Radar file found for {dt}. Skipping detection.")
        return previous_files

    # 3. Detection
    io_manager.write_info("Step 3: Detection...")
    try:
        if radar_old is None:
            # First frame or restart: Run single-frame detection on current file to initialize
            io_manager.write_info("Initializing tracking with single-frame detection")
            detect_main(
                radar_new, None,
                ps_new, None,
                pt_new, None,
                lat_limits, lon_limits,
                json_output
            )
        else:
            # Sequential tracking
            detect_main(
                radar_old, radar_new,
                ps_old, ps_new,
                pt_old, pt_new,
                lat_limits, lon_limits,
                json_output
            )
    except Exception as e:
        io_manager.write_error(f"Detection failed: {e}")
        # Even if detection fails, we might want to continue or abort. 
        # usually aborting the step is safer to avoid corrupt state.
        return previous_files

    # 4. Integration
    io_manager.write_info("Step 4: Integration...")
    try:
        # Load the just-saved JSON
        handler = StatFileHandler(io_manager)
        if not json_output.exists():
             io_manager.write_warning("No JSON output found after detection. Skipping integration.")
        else:
            cells, timestamp = handler.load_json(str(json_output))
            integrator = StormCellIntegrator(io_manager)
            result_cells = cells

            # Integration Datasets mapping from integrate/main.py
            datasets = [
                ("NLDN", fs.MRMS_NLDN_DIR, "CGFlashDensity"),
                ("EchoTop18", fs.MRMS_ECHOTOP18_DIR, "EchoTop18"),
                ("EchoTop30", fs.MRMS_ECHOTOP30_DIR, "EchoTop30"),
                ("PrecipRate", fs.MRMS_PRECIPRATE_DIR, "PrecipRate"),
                ("VIL Density", fs.MRMS_VIL_DIR, "VILDensity"),
                ("Reflectivity at Lowest Altitude", fs.MRMS_RALA_DIR, "RALA"),
                ("VII", fs.MRMS_VII_DIR, "VII")
            ]

            # Integrate each dataset using specific file for this timestamp
            for name, outdir, key in datasets:
                file_path = find_file_for_ts(outdir, dt)
                if file_path:
                    io_manager.write_debug(f"Integrating {name} with {file_path}")
                    try:
                        result_cells = integrator.integrate_ds_via_max(file_path, result_cells, key)
                    except Exception as e:
                         io_manager.write_error(f"Failed to integrate {name}: {e}")
                else:
                    io_manager.write_debug(f"No file found for {name} at {dt}")

            # Integrate ProbSevere (already loaded as ps_new if available, or load from file)
            if ps_new:
                 try:
                    with open(ps_new, 'r') as f:
                        ps_data = json.load(f)
                    result_cells = integrator.integrate_probsevere(ps_data, result_cells)
                    io_manager.write_debug("Integrated ProbSevere")
                 except Exception as e:
                     io_manager.write_error(f"Failed to integrate ProbSevere: {e}")
            
            # Save integrated results
            saver = CellDataSaver(None, None, None, None, None, None)
            final_data = saver.create_json_structure(timestamp, result_cells)
            handler.write_json(final_data, str(json_output))
            
    except Exception as e:
        io_manager.write_error(f"Integration failed: {e}")

    # Update previous files for next iteration
    return {
        'radar': radar_new,
        'ps': ps_new,
        'pt': pt_new
    }

def main():
    parser = argparse.ArgumentParser(description="Process EdgeWARN data historically.")
    parser.add_argument("--start", type=str, required=True, help="Start timestamp (ISO, e.g. 2023-01-01T12:00:00)")
    parser.add_argument("--end", type=str, required=True, help="End timestamp (ISO)")
    parser.add_argument("--lat", nargs=2, type=float, default=[20, 55], help="Latitude limits (min max)")
    parser.add_argument("--lon", nargs=2, type=float, default=[-130, -60], help="Longitude limits (min max)")
    parser.add_argument("--output", type=str, default="stormcell_test.json", help="Output JSON file")
    
    args = parser.parse_args()

    try:
        start_time = datetime.fromisoformat(args.start).replace(tzinfo=timezone.utc)
        end_time = datetime.fromisoformat(args.end).replace(tzinfo=timezone.utc)
    except ValueError as e:
        io_manager.write_error(f"Invalid timestamp format: {e}")
        return

    lat_limits = tuple(args.lat)
    lon_limits = tuple(args.lon)
    json_output = Path(args.output)
    
    # Initialize state
    previous_files = {
        'radar': None,
        'ps': None,
        'pt': None
    }

    # If resuming, we might want to load previous_files from somewhere, but for now assume fresh start or 
    # that the first iteration acts as the "new" and second iteration establishes tracking.
    
    current_time = start_time
    while current_time <= end_time:
        previous_files = process_timestep(current_time, lat_limits, lon_limits, json_output, previous_files)
        current_time += timedelta(minutes=1)
        
    io_manager.write_info("Historical processing complete.")

if __name__ == "__main__":
    main()
