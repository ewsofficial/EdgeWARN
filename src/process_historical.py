"""
Historical processing script for EdgeWARN.
Uses the same pipeline as run.py but iterates through a historical time range.
"""

import sys
from pathlib import Path
from datetime import datetime, timezone, timedelta
import time
import argparse
import util.file as fs
import EdgeWARN.core.ingest.mrms.main as ingest_main
from EdgeWARN.core.ingest.synoptic.main import download_rap
import EdgeWARN.core.process.detect.main as detect
import EdgeWARN.core.process.integrate.main as integration
from EdgeWARN.core.schedule.scheduler import MRMSUpdateChecker
from EdgeWARN.core.ingest.mrms.config import get_check_modifiers
from util.io import TimestampedOutput, IOManager
from util.performance import tracker as perf_tracker

sys.stdout = TimestampedOutput(sys.stdout)
sys.stderr = TimestampedOutput(sys.stderr)

io_manager = IOManager("[HistoricalProcess]")

def get_utc_time(time_str):
    """
    Parse a timestamp string and ensure it is UTC-aware.
    - If the input has timezone info, convert to UTC.
    - If the input is naive, assume it is UTC.
    """
    dt = datetime.fromisoformat(time_str)
    if dt.tzinfo is not None:
        return dt.astimezone(timezone.utc)
    return dt.replace(tzinfo=timezone.utc)

def pipeline(dt, lat_limits, lon_limits, json_output, profile=False):
    """Run the full ingestion → detection → integration pipeline once (same as run.py)."""
    
    try:
        perf_tracker.reset()
        perf_tracker.start("Total Pipeline")

        io_manager.write_info(f"Starting Data Ingestion for timestamp {dt}")
        perf_tracker.start("Ingestion")
        ingest_main.download_all_files(dt, remove_old_files=False)
        download_rap(dt)
        perf_tracker.stop("Ingestion")
        
        io_manager.write_info("Starting Storm Cell Detection")
        try:
            # Safely get latest files
            comp_files = fs.latest_files(fs.MRMS_COMPOSITE_DIR, 2)
            if not comp_files:
                raise RuntimeError(f"No composite reflectivity files found in {fs.MRMS_COMPOSITE_DIR}")
            filepath_old, filepath_new = comp_files
            
            # For other files, we can use None if missing (single scan mode logic handles None)
            # But latest_files returns None if directory is missing, so we must check.
            
            ps_files = fs.latest_files(fs.MRMS_PROBSEVERE_DIR, 2)
            if ps_files:
                ps_old, ps_new = ps_files
            else:
                ps_old, ps_new = None, None

            pt_files = fs.latest_files(fs.MRMS_PRECIPTYP_DIR, 2)
            if pt_files:
                pt_old, pt_new = pt_files
            else:
                pt_old, pt_new = None, None
            
            # If any are None where we need pairs, we might trigger single-scan fallback below implicitly 
            # or we should just raise RuntimeError to force the except block.
            # Actually, standard behavior is: if latest_files fails (not enough files), it raises RuntimeError.
            # But if directory doesn't exist, it returns None.
            # So we manually raise RuntimeError if None to trigger the fallback logic cleanly.
            if not ps_files or not pt_files:
                 raise RuntimeError("Missing pairs for tracking")

        except (RuntimeError, ValueError):
            # Not enough files - single scan mode
            io_manager.write_info("Not enough files for tracking, using single-scan mode")
            
            # Handle Composite
            comp_files = fs.latest_files(fs.MRMS_COMPOSITE_DIR, 1)
            filepath_old = comp_files[-1] if comp_files else None
            filepath_new = None
            if not filepath_old:
                 raise RuntimeError(f"Cannot run detection: No composite reflectivity files in {fs.MRMS_COMPOSITE_DIR}")

            # Handle ProbSevere
            ps_files = fs.latest_files(fs.MRMS_PROBSEVERE_DIR, 1)
            ps_old = ps_files[-1] if ps_files else None
            ps_new = None
            
            # Handle PrecipType
            pt_files = fs.latest_files(fs.MRMS_PRECIPTYP_DIR, 1)
            pt_old = pt_files[-1] if pt_files else None
            pt_new = None
        
        perf_tracker.start("Detection")
        generated_file = detect.main(filepath_old, filepath_new, ps_old, ps_new, pt_old, pt_new, lat_limits, lon_limits, json_output)
        perf_tracker.stop("Detection")
        
        io_manager.write_info("Starting Integration")
        perf_tracker.start("Integration")
        integration.main(generated_file, remove_old_cells=False)
        perf_tracker.stop("Integration")
        
        perf_tracker.stop("Total Pipeline")
        io_manager.write_info("Pipeline completed successfully")
        
        if profile:
            perf_tracker.print_summary()

        return generated_file
        
    except Exception as e:
        io_manager.write_error(f"Error in pipeline: {e}")
        raise

def main():
    """Historical scheduler: iterate through time range and process each available timestamp."""
    
    parser = argparse.ArgumentParser(description="Process EdgeWARN data historically.")
    parser.add_argument("--start", type=str, required=True, help="Start timestamp (ISO, e.g. 2023-01-01T12:00:00)")
    parser.add_argument("--end", type=str, required=True, help="End timestamp (ISO)")
    parser.add_argument("--lat", nargs=2, type=float, default=[20, 55], help="Latitude limits (min max)")
    parser.add_argument("--lon", nargs=2, type=float, default=[-130, -60], help="Longitude limits (min max)")
    parser.add_argument("--output", type=str, default="stormcell_test.json", help="Output JSON file")
    parser.add_argument("--base_dir", type=str, default=None, help="Custom base directory for input data")
    parser.add_argument("--profile", action="store_true", help="Enable performance profiling")

    args = parser.parse_args()

    # Initialize custom filesystem if provided
    if args.base_dir:
        fs.initialize_filesystem(args.base_dir)

    try:
        start_time = get_utc_time(args.start)
        end_time = get_utc_time(args.end)
    except ValueError as e:
        io_manager.write_error(f"Invalid timestamp format: {e}")
        return

    lat_limits = tuple(args.lat)
    lon_limits = tuple(args.lon)
    json_output = Path(args.output)
    
    # Initialize scheduler
    checker = MRMSUpdateChecker(verbose=True)
    
    current_time = start_time
    last_processed_timestamp = None  # Track the actual data timestamp that was processed
    
    io_manager.write_info(f"Starting historical processing from {start_time} to {end_time}")
    
    while current_time <= end_time:
        io_manager.write_info(f"\n{'='*60}")
        io_manager.write_info(f"Checking for data near: {current_time.isoformat()}")
        
        # Check modifiers dynamically
        check_modifiers = get_check_modifiers()
        
        # Find latest common timestamp on S3 within 1 hour of current_time
        latest_common = checker.latest_common_minute_1h(check_modifiers, reference_dt=current_time)
        
        if latest_common is None:
            io_manager.write_warning(f"No common timestamp found near {current_time}")
            current_time += timedelta(minutes=1)
            continue
        
        # Check if this is the same timestamp we already processed
        if latest_common == last_processed_timestamp:
            io_manager.write_info(f"Timestamp {latest_common} already processed, skipping")
            current_time += timedelta(minutes=1)
            continue
        
        io_manager.write_info(f"Processing timestamp: {latest_common.isoformat()}")
        
        # Run the pipeline
        try:
            pipeline(latest_common, lat_limits, lon_limits, json_output, profile=args.profile)
            last_processed_timestamp = latest_common
            
            # Verify output
            if json_output.exists():
                io_manager.write_info(f"✓ Output saved to {json_output}")
            else:
                io_manager.write_warning(f"✗ Warning: No output file at {json_output}")
                
        except Exception as e:
            io_manager.write_error(f"Pipeline failed for {latest_common}: {e}")
            # Continue processing other timestamps even if this one failed
            last_processed_timestamp = latest_common
        
        # Increment search time by 1 minute
        current_time += timedelta(minutes=1)
        
        # Small delay between iterations
        time.sleep(1)
    
    io_manager.write_info(f"\n{'='*60}")
    io_manager.write_info("Historical processing complete.")

if __name__ == "__main__":
    main()
