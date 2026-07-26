import sys
from pathlib import Path
from datetime import timedelta
import time

import common.ingest.mrms.config as mrms_config
from EdgeWARN import historical_pipeline, initialize_runtime, parse_utc_time
from EdgeWARN.schedule.scheduler import MRMSUpdateChecker
from util.io import TimestampedOutput, IOManager

sys.stdout = TimestampedOutput(sys.stdout)
sys.stderr = TimestampedOutput(sys.stderr)

io_manager = IOManager("[HistoricalProcess]")

def main():
    """Historical scheduler: iterate through time range and process each available timestamp."""
    args = io_manager.get_historical_args()

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
    if args.disable_ctam:
        io_manager.write_info("CTAM execution disabled via --disable-ctam")
    if args.disable_tracking:
        io_manager.write_info("Tracking disabled via --disable-tracking")
    if args.disable_polygon_expansion:
        io_manager.write_info("Polygon expansion disabled via --disable-polygon-expansion; using original ProbSevere polygons")
    io_manager.write_info(
        "Detection thresholds: "
        f"disable_polygon_expansion={args.disable_polygon_expansion}, "
        f"refl_threshold={args.refl_threshold}, "
        f"min_seed_percentage={args.min_seed_percentage}, "
        f"drop_offset={args.drop_offset}"
    )

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
            continue
        
        # Check if this is the same timestamp we already processed
        if latest_common == last_processed_timestamp:
            io_manager.write_info(f"Timestamp {latest_common} already processed, skipping")
            current_time += timedelta(minutes=1)
            continue
        
        io_manager.write_info(f"Processing timestamp: {latest_common.isoformat()}")
        
        # Run the pipeline
        try:
            result = historical_pipeline(
                latest_common,
                lat_limits,
                lon_limits,
                json_output,
                profile=args.profile,
                io_manager=io_manager,
                disable_ctam=args.disable_ctam,
                disable_tracking=args.disable_tracking,
                disable_polygon_expansion=args.disable_polygon_expansion,
                refl_threshold=args.refl_threshold,
                min_seed_percentage=args.min_seed_percentage,
                drop_offset=args.drop_offset,
            )

            generated_file = result[0] if isinstance(result, tuple) else result
            generated_path = Path(generated_file) if generated_file else None
            if generated_path is None or not generated_path.is_file():
                raise RuntimeError(
                    "Historical pipeline did not produce a validated output artifact"
                )

            last_processed_timestamp = latest_common
            io_manager.write_info(f"✓ Output saved to {generated_path}")
                
        except Exception as e:
            io_manager.write_error(f"Pipeline failed for {latest_common}: {e}")
            io_manager.write_warning(
                f"Timestamp {latest_common} remains unprocessed and may be retried"
            )

        # Increment search time by 1 minute
        current_time += timedelta(minutes=1)
        
        # Small delay between iterations
        time.sleep(1)
    
    io_manager.write_info(f"\n{'='*60}")
    io_manager.write_info("Historical processing complete.")

if __name__ == "__main__":
    main()
