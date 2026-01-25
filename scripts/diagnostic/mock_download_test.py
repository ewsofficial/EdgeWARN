#!/usr/bin/env python3
"""
Mock Download Verification Script

Simulates the download process for the last 6 hours of common timestamps
to verify that the rounding logic correctly matches files without collisions
or missed downloads.

This script does NOT actually download files - it only simulates the matching logic.

Usage:
    python scripts/mock_download_test.py
"""

import sys
import concurrent.futures
from pathlib import Path
from datetime import datetime, timezone, timedelta
from collections import defaultdict

# Add src to path
SRC_DIR = Path(__file__).resolve().parents[1] / "src"
sys.path.insert(0, str(SRC_DIR))

from util.io import IOManager
from EdgeWARN.core.ingest.mrms.config import get_check_modifiers, bucket
from EdgeWARN.core.ingest.mrms.s3_sync import FileFinder
from EdgeWARN.core.ingest.mrms.parse import parse_mrms_bucket_path
from EdgeWARN.core.ingest.mrms.timestamp_utils import round_to_nearest_even_minute
from EdgeWARN.core.schedule.scheduler import MRMSUpdateChecker

io_manager = IOManager("[MockDownloadTest]")


def get_all_files_with_timestamps(modifier_tuple, reference_dt: datetime, hours: int = 6):
    """
    Get all files for a product within the specified hours.
    Returns list of (s3_path, original_ts, rounded_ts) tuples.
    """
    region, product, _ = modifier_tuple
    
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
        return []
    
    if not files_with_timestamps:
        return []
    
    # Filter to time window and add rounded timestamps
    cutoff = reference_dt - timedelta(hours=hours)
    
    results = []
    for s3_path, ts in files_with_timestamps:
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        elif ts.tzinfo != timezone.utc:
            ts = ts.astimezone(timezone.utc)
        
        if ts < cutoff:
            continue
        
        rounded = round_to_nearest_even_minute(ts)
        results.append((s3_path, ts, rounded))
    
    return results


def simulate_file_selection(target_dt, files_with_rounded):
    """
    Simulate the _select_target_file logic.
    Returns (selected_file, was_rounded_match, was_exact_match)
    """
    target_rounded = round_to_nearest_even_minute(target_dt)
    target_key = (target_rounded.year, target_rounded.month, target_rounded.day,
                  target_rounded.hour, target_rounded.minute)
    
    for s3_path, orig_ts, rounded_ts in files_with_rounded:
        rounded_key = (rounded_ts.year, rounded_ts.month, rounded_ts.day,
                       rounded_ts.hour, rounded_ts.minute)
        
        if rounded_key == target_key:
            was_exact = (orig_ts.minute == target_rounded.minute and 
                         orig_ts.hour == target_rounded.hour)
            return (s3_path, True, was_exact)
    
    # No match found - would fall back to latest
    if files_with_rounded:
        return (files_with_rounded[0][0], False, False)
    return (None, False, False)


def run_mock_download_test():
    """
    Main function to run mock download verification.
    """
    io_manager.write_info("=" * 70)
    io_manager.write_info("Mock Download Verification Test")
    io_manager.write_info("=" * 70)
    
    reference_dt = datetime.now(timezone.utc)
    hours = 6
    
    io_manager.write_info(f"Reference Time: {reference_dt.strftime('%Y-%m-%d %H:%M:%S')} UTC")
    io_manager.write_info(f"Testing past {hours} hours of data...")
    io_manager.write_info("")
    
    # Step 1: Get common timestamps (what scheduler would produce)
    io_manager.write_info("Step 1: Computing common timestamps (like scheduler)...")
    checker = MRMSUpdateChecker(verbose=False)
    check_modifiers = get_check_modifiers()
    
    # Get timestamps for each product
    all_product_timestamps = {}
    
    def fetch_product_data(mod_tuple):
        region, product, _ = mod_tuple
        product_name = product if product else region
        files = get_all_files_with_timestamps(mod_tuple, reference_dt, hours)
        rounded_timestamps = {f[2] for f in files}
        return (product_name, files, rounded_timestamps)
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as executor:
        futures = {executor.submit(fetch_product_data, mod): mod for mod in check_modifiers}
        
        for future in concurrent.futures.as_completed(futures):
            try:
                product_name, files, rounded_ts = future.result()
                all_product_timestamps[product_name] = (files, rounded_ts)
                io_manager.write_info(f"  [{product_name}]: {len(files)} files, {len(rounded_ts)} unique rounded timestamps")
            except Exception as e:
                mod = futures[future]
                product_name = mod[1] if mod[1] else mod[0]
                io_manager.write_error(f"  [{product_name}]: Error - {e}")
                all_product_timestamps[product_name] = ([], set())
    
    # Find common timestamps
    all_rounded_sets = [ts_set for _, ts_set in all_product_timestamps.values() if ts_set]
    if not all_rounded_sets:
        io_manager.write_error("No data found!")
        return False
    
    common_timestamps = set.intersection(*all_rounded_sets)
    io_manager.write_info(f"\nFound {len(common_timestamps)} common timestamps")
    
    # Step 2: Simulate downloads for each common timestamp
    io_manager.write_info("")
    io_manager.write_info("Step 2: Simulating file selection for each common timestamp...")
    io_manager.write_info("-" * 70)
    
    total_tests = 0
    exact_matches = 0
    rounded_matches = 0
    fallbacks = 0
    failures = 0
    
    issues = []
    
    for target_ts in sorted(common_timestamps, reverse=True)[:30]:  # Test most recent 30
        for product_name, (files, _) in all_product_timestamps.items():
            total_tests += 1
            selected, matched, was_exact = simulate_file_selection(target_ts, files)
            
            if selected is None:
                failures += 1
                issues.append((target_ts, product_name, "NO_FILE_FOUND"))
            elif not matched:
                fallbacks += 1
                issues.append((target_ts, product_name, "FALLBACK_TO_LATEST"))
            elif was_exact:
                exact_matches += 1
            else:
                rounded_matches += 1
    
    # Summary
    io_manager.write_info("")
    io_manager.write_info("=" * 70)
    io_manager.write_info("RESULTS:")
    io_manager.write_info("-" * 70)
    io_manager.write_info(f"Total simulated downloads: {total_tests}")
    io_manager.write_info(f"  ✓ Exact matches: {exact_matches}")
    io_manager.write_info(f"  ✓ Rounded matches: {rounded_matches}")
    io_manager.write_info(f"  ⚠ Fallbacks to latest: {fallbacks}")
    io_manager.write_info(f"  ✗ Failures (no file): {failures}")
    
    if issues:
        io_manager.write_info("")
        io_manager.write_info("ISSUES DETECTED:")
        for ts, product, issue_type in issues[:10]:
            io_manager.write_warning(f"  {ts.strftime('%H:%M')} [{product}]: {issue_type}")
        if len(issues) > 10:
            io_manager.write_warning(f"  ... and {len(issues) - 10} more issues")
    
    io_manager.write_info("")
    io_manager.write_info("=" * 70)
    
    success_rate = (exact_matches + rounded_matches) / total_tests * 100 if total_tests > 0 else 0
    io_manager.write_info(f"SUCCESS RATE: {success_rate:.1f}% ({exact_matches + rounded_matches}/{total_tests})")
    
    if fallbacks == 0 and failures == 0:
        io_manager.write_info("✓ ALL DOWNLOADS WOULD SUCCEED - Rounding logic is working correctly!")
        return True
    else:
        io_manager.write_warning(f"⚠ {fallbacks + failures} potential issues detected")
        return False


if __name__ == "__main__":
    success = run_mock_download_test()
    sys.exit(0 if success else 1)
