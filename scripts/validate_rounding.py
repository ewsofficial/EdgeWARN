#!/usr/bin/env python3
"""
Validate Round-to-Even-Minute Logic

This script fetches data for the last 6 hours and checks if the round_to_nearest_even_minute
implementation causes any collisions (two files from the same product assigned to the same timestamp).

Usage:
    python scripts/validate_rounding.py
"""

import sys
import concurrent.futures
from pathlib import Path
from datetime import datetime, timezone, timedelta
from collections import defaultdict

# Add src to path
SRC_DIR = Path(__file__).resolve().parents[1] / "src"
sys.path.insert(0, str(SRC_DIR))

import util.file as fs
from util.io import IOManager
from EdgeWARN.core.ingest.mrms.config import get_check_modifiers, bucket
from EdgeWARN.core.ingest.mrms.s3_sync import FileFinder
from EdgeWARN.core.ingest.mrms.parse import parse_mrms_bucket_path
from EdgeWARN.core.schedule.scheduler import round_to_nearest_even_minute

io_manager = IOManager("[RoundingValidator]")


def get_files_with_rounding(modifier_tuple, reference_dt: datetime, hours: int = 6):
    """
    Get all files for a modifier within the specified hours and apply rounding.
    Returns a dict mapping rounded timestamps to list of (original_ts, s3_path) tuples.
    """
    region, product, _ = modifier_tuple
    
    # Need to look at multiple hour prefixes for 6 hours of data
    finder = FileFinder(reference_dt, bucket, max_entries=200, io_manager=io_manager)
    
    bucket_paths = []
    for hour_offset in range(hours + 1):
        dt_offset = reference_dt - timedelta(hours=hour_offset)
        bucket_path = parse_mrms_bucket_path(dt_offset, region, product)
        bucket_paths.append(bucket_path)
    
    try:
        files_with_timestamps = finder.lookup_files(bucket_paths, verbose=False)
    except Exception as e:
        io_manager.write_error(f"Error fetching from S3: {e}")
        return {}
    
    if not files_with_timestamps:
        return {}
    
    # Filter to the time window
    cutoff = reference_dt - timedelta(hours=hours)
    
    # Group by rounded timestamp
    rounded_to_originals = defaultdict(list)
    
    for s3_path, ts in files_with_timestamps:
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        elif ts.tzinfo != timezone.utc:
            ts = ts.astimezone(timezone.utc)
        
        if ts < cutoff:
            continue
        
        rounded = round_to_nearest_even_minute(ts)
        rounded_to_originals[rounded].append((ts, s3_path))
    
    return rounded_to_originals


def validate_rounding():
    """
    Main function to validate the rounding logic doesn't cause collisions.
    """
    io_manager.write_info("=" * 70)
    io_manager.write_info("Rounding Validation: Checking for Collisions")
    io_manager.write_info("=" * 70)
    
    reference_dt = datetime.now(timezone.utc)
    hours = 6
    
    io_manager.write_info(f"Reference Time: {reference_dt.strftime('%Y-%m-%d %H:%M:%S')} UTC")
    io_manager.write_info(f"Checking past {hours} hours of data...")
    io_manager.write_info("")
    
    check_modifiers = get_check_modifiers()
    io_manager.write_info(f"Checking {len(check_modifiers)} products...")
    io_manager.write_info("")
    
    total_collisions = 0
    products_with_collisions = []
    
    def check_product(mod_tuple):
        """Check a single product for collisions."""
        region, product, _ = mod_tuple
        product_name = product if product else region
        rounded_to_originals = get_files_with_rounding(mod_tuple, reference_dt, hours)
        
        collisions = []
        for rounded_ts, originals in rounded_to_originals.items():
            if len(originals) > 1:
                collisions.append((rounded_ts, originals))
        
        return (product_name, len(rounded_to_originals), collisions)
    
    # Check all products in parallel
    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as executor:
        futures = {executor.submit(check_product, mod): mod for mod in check_modifiers}
        
        for future in concurrent.futures.as_completed(futures):
            try:
                product_name, total_timestamps, collisions = future.result()
                
                if collisions:
                    io_manager.write_warning(f"[{product_name}]: {len(collisions)} COLLISIONS in {total_timestamps} timestamps")
                    products_with_collisions.append(product_name)
                    total_collisions += len(collisions)
                    
                    # Show collision details
                    for rounded_ts, originals in collisions[:5]:  # Show up to 5
                        io_manager.write_info(f"  Rounded to: {rounded_ts.strftime('%Y-%m-%d %H:%M')} UTC")
                        for orig_ts, s3_path in originals:
                            filename = s3_path.split('/')[-1] if '/' in s3_path else s3_path
                            io_manager.write_info(f"    - {orig_ts.strftime('%H:%M:%S')} → {filename[:60]}...")
                    if len(collisions) > 5:
                        io_manager.write_info(f"  ... and {len(collisions) - 5} more collisions")
                else:
                    io_manager.write_info(f"[{product_name}]: ✓ No collisions ({total_timestamps} timestamps)")
            except Exception as e:
                mod = futures[future]
                product_name = mod[1] if mod[1] else mod[0]
                io_manager.write_error(f"[{product_name}]: Error - {e}")
    
    io_manager.write_info("")
    io_manager.write_info("=" * 70)
    io_manager.write_info("SUMMARY:")
    io_manager.write_info("-" * 70)
    
    if total_collisions == 0:
        io_manager.write_info("✓ NO COLLISIONS DETECTED - Rounding logic is safe!")
    else:
        io_manager.write_warning(f"✗ {total_collisions} COLLISIONS DETECTED across {len(products_with_collisions)} products:")
        for product in products_with_collisions:
            io_manager.write_warning(f"  - {product}")
    
    io_manager.write_info("=" * 70)
    
    return total_collisions == 0


if __name__ == "__main__":
    success = validate_rounding()
    sys.exit(0 if success else 1)
