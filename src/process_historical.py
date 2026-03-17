import sys
from pathlib import Path
from datetime import timedelta
import time
import argparse

import common.ingest.mrms.config as mrms_config
from EdgeWARN import historical_pipeline, initialize_runtime, parse_utc_time
from EdgeWARN.core.schedule.scheduler import MRMSUpdateChecker
from util.io import TimestampedOutput, IOManager

sys.stdout = TimestampedOutput(sys.stdout)
sys.stderr = TimestampedOutput(sys.stderr)

io_manager = IOManager("[HistoricalProcess]")

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
    initialize_runtime(base_dir=args.base_dir, io_manager=io_manager, initialize_indexes=False)

    try:
        start_time = parse_utc_time(args.start)
        end_time = parse_utc_time(args.end)
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
        check_modifiers = mrms_config.get_check_modifiers()
        
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

            _, new_objs = historical_pipeline(
                latest_common,
                lat_limits,
                lon_limits,
                json_output,
                profile=args.profile,
                cached_objs=cached_objs,
                io_manager=io_manager,
            )
            
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
