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

def pipeline(dt, lat_limits, lon_limits, json_output, profile=False, cached_objs=(None, None, None)):
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
            # Locate files for this timestamp
            def find_file(directory, target_dt):
                if not Path(directory).exists(): 
                    io_manager.write_debug(f"Dir not found: {directory}")
                    return None
                
                # Try hyphen separator (MRMS GRIB2)
                pattern1 = f"*{target_dt.strftime('%Y%m%d-%H%M')}*"
                files = sorted(Path(directory).glob(pattern1))
                
                # If not found, try underscore (ProbSevere)
                if not files:
                    pattern2 = f"*{target_dt.strftime('%Y%m%d_%H%M')}*"
                    files = sorted(Path(directory).glob(pattern2))
                
                if not files:
                    io_manager.write_debug(f"No match for {pattern1} or underscore version in {directory}")
                else:
                    io_manager.write_debug(f"Found {files[-1]}")
                return str(files[-1]) if files else None

            radar_new = find_file(fs.MRMS_COMPOSITE_DIR, dt)
            ps_new = find_file(fs.MRMS_PROBSEVERE_DIR, dt)
            pt_new = find_file(fs.MRMS_PRECIPTYP_DIR, dt)
            
            # Old files (dt - 2 mins)
            dt_old = dt - timedelta(minutes=2)
            radar_old = find_file(fs.MRMS_COMPOSITE_DIR, dt_old)
            ps_old = find_file(fs.MRMS_PROBSEVERE_DIR, dt_old)
            pt_old = find_file(fs.MRMS_PRECIPTYP_DIR, dt_old)
            
            perf_tracker.start("Detection")
            
            # Unpack cached objects for the "Old" scan
            rad_old_obj, ps_old_obj, pt_old_obj = cached_objs
            
            generated_file, new_objs = detect.main(
                radar_old, radar_new, ps_old, ps_new, pt_old, pt_new, 
                lat_limits, lon_limits, json_output,
                radar_old_obj=rad_old_obj,
                ps_old_obj=ps_old_obj,
                pt_old_obj=pt_old_obj
            )
            perf_tracker.stop("Detection")
            
            perf_tracker.stop("Detection")
            
            if generated_file:
                io_manager.write_info("Starting Integration")
                perf_tracker.start("Integration")
                integration.main(generated_file, remove_old_cells=False)
                perf_tracker.stop("Integration")
            else:
                io_manager.write_warning("Detection failed or produced no output, skipping integration")
            
            perf_tracker.stop("Total Pipeline")
            io_manager.write_info("Pipeline completed successfully")
            
            if profile:
                perf_tracker.print_summary()

            return generated_file, new_objs

        except Exception as e:
            io_manager.write_error(f"Error in pipeline step: {e}")
            import traceback
            traceback.print_exc()
            return None, (None, None, None)

    except Exception as e:
         io_manager.write_error(f"Pipeline failed: {e}")
         return None, (None, None, None)

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
    
    cached_objs = (None, None, None) # Initialize cache

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
            cached_objs = (None, None, None) # Reset cache on gap
            continue
        
        # Check if this is the same timestamp we already processed
        if latest_common == last_processed_timestamp:
            io_manager.write_info(f"Timestamp {latest_common} already processed, skipping")
            current_time += timedelta(minutes=1)
            continue
        
        io_manager.write_info(f"Processing timestamp: {latest_common.isoformat()}")
        
        # Run the pipeline
        try:
            # Reset cache if time gap is too large (> 5 mins) to ensure we don't use stale data
            if last_processed_timestamp and (latest_common - last_processed_timestamp).total_seconds() > 300:
                io_manager.write_info("Time gap detected, resetting detection cache.")
                cached_objs = (None, None, None)

            _, new_objs = pipeline(latest_common, lat_limits, lon_limits, json_output, profile=args.profile, cached_objs=cached_objs)
            
            # Update cache for next iteration if valid
            if new_objs and new_objs[0] is not None:
                cached_objs = new_objs
            
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
            cached_objs = (None, None, None) # Reset on error
        
        # Increment search time by 1 minute
        current_time += timedelta(minutes=1)
        
        # Small delay between iterations
        time.sleep(1)
    
    io_manager.write_info(f"\n{'='*60}")
    io_manager.write_info("Historical processing complete.")

if __name__ == "__main__":
    main()
