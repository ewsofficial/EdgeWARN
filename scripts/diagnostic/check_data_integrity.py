#!/usr/bin/env python3
"""
Data Integrity Check Script

Tests data integrity by fetching all files in check_modifiers from the MRMS S3 bucket
for the past hour and listing all common timestamps.

Usage:
    python scripts/check_data_integrity.py [--base-dir /path/to/data]
"""

import re
import sys
import concurrent.futures
from pathlib import Path
from datetime import datetime, timezone, timedelta

# Add src to path
SRC_DIR = Path(__file__).resolve().parents[2] / "src"
sys.path.insert(0, str(SRC_DIR))

import util.file as fs
from util.io import IOManager
from EdgeWARN.core.ingest.mrms.config import get_check_modifiers, bucket
from EdgeWARN.core.ingest.mrms.s3_sync import FileFinder
from EdgeWARN.core.ingest.mrms.parse import parse_mrms_bucket_path

io_manager = IOManager("[DataIntegrityCheck]")


def get_s3_timestamps(modifier_tuple, reference_dt: datetime) -> set:
    """
    Get all unique timestamps (rounded to minute) from S3 bucket for a modifier.
    Returns a set of datetime objects.
    """
    region, product, _ = modifier_tuple
    
    finder = FileFinder(reference_dt, bucket, max_entries=60, io_manager=io_manager)
    
    # Build bucket path for current and previous hour (to cover full 1h window)
    bucket_paths = []
    for hour_offset in range(2):  # Current hour and previous hour
        dt_offset = reference_dt - timedelta(hours=hour_offset)
        bucket_path = parse_mrms_bucket_path(dt_offset, region, product)
        bucket_paths.append(bucket_path)
    
    try:
        files_with_timestamps = finder.lookup_files(bucket_paths, verbose=False)
    except Exception as e:
        io_manager.write_error(f"Error fetching from S3: {e}")
        return set()
    
    if not files_with_timestamps:
        return set()
    
    # Filter to past hour and round to minute
    one_hour_ago = reference_dt - timedelta(hours=1)
    timestamps = set()
    
    for s3_path, ts in files_with_timestamps:
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        elif ts.tzinfo != timezone.utc:
            ts = ts.astimezone(timezone.utc)
        
        # Filter to past hour
        if ts >= one_hour_ago:
            ts_rounded = ts.replace(second=0, microsecond=0)
            timestamps.add(ts_rounded)
    
    return timestamps


def check_data_integrity(base_dir: str = None):
    """
    Main function to check data integrity across all check_modifiers by querying S3.
    
    Args:
        base_dir: Optional custom base directory (for filesystem initialization).
    """
    # Initialize filesystem if custom base_dir provided
    if base_dir:
        fs.initialize_filesystem(base_dir)
    
    io_manager.write_info("=" * 60)
    io_manager.write_info("EdgeWARN Data Integrity Check (S3 Bucket)")
    io_manager.write_info("=" * 60)
    
    reference_dt = datetime.now(timezone.utc)
    io_manager.write_info(f"Reference Time: {reference_dt.strftime('%Y-%m-%d %H:%M:%S')} UTC")
    io_manager.write_info(f"Bucket: {bucket}")
    io_manager.write_info(f"Checking S3 for files from past hour...")
    io_manager.write_info("")
    
    # Get check_modifiers
    check_modifiers = get_check_modifiers()
    io_manager.write_info(f"Checking {len(check_modifiers)} data sources:")
    io_manager.write_info("")
    
    # Collect timestamps per modifier (in parallel)
    modifier_timestamps = {}
    all_timestamps = set()
    
    def fetch_modifier_timestamps(mod_tuple):
        """Helper to fetch timestamps for a single modifier."""
        region, product, _ = mod_tuple
        product_name = product if product else region
        timestamps = get_s3_timestamps(mod_tuple, reference_dt)
        return (product_name, timestamps)
    
    # Use ThreadPoolExecutor for parallel S3 queries
    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as executor:
        futures = {executor.submit(fetch_modifier_timestamps, mod): mod for mod in check_modifiers}
        
        for future in concurrent.futures.as_completed(futures):
            try:
                product_name, timestamps = future.result()
                modifier_timestamps[product_name] = timestamps
                all_timestamps.update(timestamps)
                io_manager.write_info(f"  [{product_name}]: {len(timestamps)} files in past hour")
            except Exception as e:
                mod = futures[future]
                product_name = mod[1] if mod[1] else mod[0]
                io_manager.write_error(f"  [{product_name}]: Error - {e}")
                modifier_timestamps[product_name] = set()
    
    io_manager.write_info("")
    io_manager.write_info("-" * 60)
    
    # Find common timestamps
    if not modifier_timestamps:
        io_manager.write_warning("No modifiers found!")
        return
    
    # Get sets for intersection
    timestamp_sets = [ts for ts in modifier_timestamps.values() if ts]
    
    if not timestamp_sets:
        io_manager.write_warning("No files found in any data source for the past hour!")
        return
    
    common_timestamps = set.intersection(*timestamp_sets) if len(timestamp_sets) > 1 else timestamp_sets[0]
    
    io_manager.write_info("")
    io_manager.write_info(f"COMMON TIMESTAMPS ({len(common_timestamps)} found):")
    io_manager.write_info("-" * 60)
    
    if common_timestamps:
        for ts in sorted(common_timestamps, reverse=True):
            io_manager.write_info(f"  {ts.strftime('%Y-%m-%d %H:%M')} UTC")
    else:
        io_manager.write_warning("  No common timestamps found across all data sources!")
    
    io_manager.write_info("")
    io_manager.write_info("-" * 60)
    
    # Missing products analysis for even-minute timestamps
    io_manager.write_info("")
    io_manager.write_info("MISSING PRODUCTS BY TIMESTAMP (even minutes only):")
    io_manager.write_info("-" * 60)
    
    # Get all even-minute timestamps across all sources
    all_even_timestamps = sorted(
        {ts for ts in all_timestamps if ts.minute % 2 == 0},
        reverse=True
    )
    
    for ts in all_even_timestamps:
        missing_products = []
        for product_name, timestamps in sorted(modifier_timestamps.items()):
            if ts not in timestamps:
                missing_products.append(product_name)
        
        if missing_products:
            io_manager.write_info(f"\n  {ts.strftime('%Y-%m-%d %H:%M')} UTC - MISSING {len(missing_products)}:")
            for product in missing_products:
                io_manager.write_info(f"    ✗ {product}")
        else:
            io_manager.write_info(f"  {ts.strftime('%Y-%m-%d %H:%M')} UTC - ✓ All products available")
    
    io_manager.write_info("")
    io_manager.write_info("-" * 60)

    
    # Per-source breakdown - show ALL timestamps
    io_manager.write_info("")
    io_manager.write_info("PER-SOURCE TIMESTAMPS:")
    io_manager.write_info("-" * 60)
    
    for product_name, timestamps in sorted(modifier_timestamps.items()):
        io_manager.write_info(f"\n[{product_name}] ({len(timestamps)} files):")
        if timestamps:
            for ts in sorted(timestamps, reverse=True):
                in_common = "✓" if ts in common_timestamps else "✗"
                io_manager.write_info(f"  {in_common} {ts.strftime('%Y-%m-%d %H:%M')} UTC")
        else:
            io_manager.write_info("  (no files)")

    
    io_manager.write_info("")
    io_manager.write_info("=" * 60)
    
    # Summary
    total_sources = len(check_modifiers)
    sources_with_data = sum(1 for ts in modifier_timestamps.values() if ts)
    
    io_manager.write_info("SUMMARY:")
    io_manager.write_info(f"  Total data sources: {total_sources}")
    io_manager.write_info(f"  Sources with data: {sources_with_data}")
    io_manager.write_info(f"  Common timestamps: {len(common_timestamps)}")
    
    if len(common_timestamps) > 0:
        latest = max(common_timestamps)
        io_manager.write_info(f"  Latest common: {latest.strftime('%Y-%m-%d %H:%M')} UTC")
    
    io_manager.write_info("=" * 60)
    
    # Return data for programmatic use
    return {
        "reference_time": reference_dt,
        "common_timestamps": sorted(common_timestamps, reverse=True),
        "per_source": modifier_timestamps,
        "sources_with_data": sources_with_data,
        "total_sources": total_sources
    }


def main():
    """Entry point with CLI argument parsing."""
    # Simple CLI parsing for --base-dir
    base_dir = None
    args = sys.argv[1:]
    
    for i, arg in enumerate(args):
        if arg == "--base-dir" and i + 1 < len(args):
            base_dir = args[i + 1]
            break
        elif arg.startswith("--base-dir="):
            base_dir = arg.split("=", 1)[1]
            break
    
    check_data_integrity(base_dir)


if __name__ == "__main__":
    main()
