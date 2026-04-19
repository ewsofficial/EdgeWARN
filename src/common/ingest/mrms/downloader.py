import re

from common.ingest.mrms.config import (
    bucket,
    get_goes_modifiers,
    get_mrms_modifiers,
    goes_bucket,
    normalize_goes_modifier,
)
from common.ingest.mrms.s3_sync import FileFinder, FileDownloader
from common.ingest.mrms.s3_async import AsyncFileFinder, AsyncFileDownloader
from common.ingest.mrms.https_client import HttpsFileFinder, HttpsFileDownloader
from common.ingest.mrms.parse import parse_mrms_bucket_path, parse_goes_bucket_path
from common.ingest.mrms.utils import merge_glm_files, extract_timestamp
from util.io import IOManager
import util.file as fs
from concurrent.futures import ThreadPoolExecutor, as_completed
import asyncio
import aioboto3
from botocore import UNSIGNED
from botocore.client import Config

from util.io import IOManager, PerformanceTimer
from util.performance import tracker as perf_tracker
import uuid

io_manager = IOManager("[Ingest]")

GOES_MAX_ENTRIES = 96


def _get_goes_spec_label(goes_spec):
    return goes_spec.label if goes_spec.channel_id else goes_spec.product


def _get_goes_search_max_entries(goes_spec, max_entries):
    _ = (goes_spec, max_entries)
    return GOES_MAX_ENTRIES


def _get_goes_bucket_paths(dt, product, hour_lookback):
    return [
        parse_goes_bucket_path(dt, product, hour_offset=hour_offset)
        for hour_offset in range(hour_lookback)
    ]


def _filter_goes_files_for_spec(file_list, goes_spec, trace_id=None):
    if not goes_spec.filename_matcher:
        return file_list

    matcher = re.compile(goes_spec.filename_matcher)
    filtered_files = [
        (s3_path, timestamp)
        for s3_path, timestamp in file_list
        if matcher.search(s3_path)
    ]

    if not filtered_files:
        prefix = f"[{trace_id}] " if trace_id else ""
        io_manager.write_warning(
            f"{prefix}No files matched {goes_spec.channel_id} for GOES product {goes_spec.product}"
        )

    return filtered_files


def _cleanup_goes_outdir_sync(goes_spec, max_age_minutes=60):
    """Run pre-download cleanup for a GOES product output directory (sync path)."""
    try:
        outdir = goes_spec.outdir
        label = _get_goes_spec_label(goes_spec)
        io_manager.write_debug(
            f"[GOES:{label}] Running pre-download cleanup in {outdir} "
            f"(max_age_minutes={max_age_minutes}, max_files={goes_spec.max_files})"
        )
        fs.clean_old_files(outdir, max_age_minutes=max_age_minutes, max_files=goes_spec.max_files)
        io_manager.write_debug(f"[GOES:{label}] Pre-download cleanup completed for {outdir}")
    except Exception as e:
        io_manager.write_warning(f"[GOES:{label}] Pre-download cleanup failed for {outdir}: {e}")


async def _cleanup_goes_outdir_async(goes_spec, trace_id, max_age_minutes=60):
    """Run pre-download cleanup for a GOES product output directory (async path)."""
    try:
        outdir = goes_spec.outdir
        label = _get_goes_spec_label(goes_spec)
        io_manager.write_debug(
            f"[{trace_id}] [GOES:{label}] Running async pre-download cleanup in {outdir} "
            f"(max_age_minutes={max_age_minutes}, max_files={goes_spec.max_files})"
        )
        await fs.async_clean_old_files(outdir, max_age_minutes=max_age_minutes, max_files=goes_spec.max_files)
        io_manager.write_debug(
            f"[{trace_id}] [GOES:{label}] Async pre-download cleanup completed for {outdir}"
        )
    except Exception as e:
        io_manager.write_warning(
            f"[{trace_id}] [GOES:{label}] Async pre-download cleanup failed for {outdir}: {e}"
        )

async def download_all_files_async_internal(dt, max_entries, target_modifiers=None):
    """Internal async function that handles the actual download operations"""
    trace_id = f"INGEST-{uuid.uuid4().hex[:8]}"
    
    # Create shared async S3 client for all operations
    async with aioboto3.Session().client("s3", config=Config(signature_version=UNSIGNED)) as s3:
        with PerformanceTimer(io_manager, "MRMS_Ingest_Total", trace_id):
            io_manager.write_debug(f"[{trace_id}] Starting async downloads...")
            
            # Create async tasks for all modifiers
            tasks = []
            for region, modifier, outdir in get_mrms_modifiers():
                if target_modifiers is not None and modifier not in target_modifiers:
                    continue
                task = download_modifier_async(
                    region, modifier, outdir, dt, max_entries, s3, trace_id
                )
                tasks.append(task)
            
            # Execute all downloads concurrently using asyncio.gather
            # This is the key performance improvement - all S3 operations run in parallel
            io_manager.write_debug(f"[{trace_id}] Downloading from {len(tasks)} sources concurrently...")
            await asyncio.gather(*tasks, return_exceptions=True)
            
            io_manager.write_info(f"[{trace_id}] All async downloads completed")

async def download_modifier_async(region, modifier, outdir, dt, max_entries, s3_client, parent_trace_id=None):
    """Internal async version of download_modifier using aioboto3 for non-blocking S3 operations"""
    # Enforce minute-precision dt
    dt = dt.replace(second=0, microsecond=0)
    
    # Use parent trace ID or generate new one
    trace_id = parent_trace_id or f"MOD-{uuid.uuid4().hex[:8]}"
    modifier_name = modifier if modifier else "ProbSevere"

    finder = AsyncFileFinder(dt, bucket, max_entries, io_manager, s3_client=s3_client)
    downloader = AsyncFileDownloader(dt, bucket, io_manager, s3_client=s3_client)

    perf_tracker.start(f"Ingest - MRMS - {modifier_name}")
    try:
        bucket_path = parse_mrms_bucket_path(dt, region, modifier)
        
        # Optimization: Append filename prefix to search only this hour
        # Also limit search with S3 StartAfter to skip previous hours
        start_after = None
        from datetime import timedelta
        
        if modifier is not None:
            # Standard MRMS: MRMS_{modifier}_{YYYYMMDD}-{HHMMSS}
            filename_prefix = f"MRMS_{modifier}_{dt.strftime('%Y%m%d-%H')}"
            bucket_path = f"{bucket_path}{filename_prefix}"
            
            # StartAfter: Previous hour to rely on safe margin (though filename_prefix in bucket_path already filters stricter?)
            # Wait, if we append filename_prefix to bucket_path passed to lookup_files,
            # lookup_files uses that as Prefix.
            # If Prefix is .../MRMS_Modifier_20260208-14, then we ONLY get files from hour 14.
            # S3 Prefix filtering is very efficient.
            # So StartAfter is NOT NEEDED if we include hour in Prefix!
            # The current code ALREADY ADDS filename_prefix (including hour) to bucket_path!
            # Check lines 59-61:
            # filename_prefix = f"MRMS_{modifier}_{dt.strftime('%Y%m%d-%H')}"
            # bucket_path = f"{bucket_path}{filename_prefix}"
            
            # So for non-ProbSevere, we effectively filter by hour already!!!
            pass

        else:
            # ProbSevere (modifier is None)
            # Prefix is ProbSevere/YYYYMMDD/
            # We assume we can't easily append prefix because filename format "MRMS_PROBSEVERE_..." 
            # might not match "ProbSevere" folder name exactly (Case sensitivity).
            # Folder: ProbSevere/
            # File: MRMS_PROBSEVERE_...
            # If we try to add prefix "MRMS_PROBSEVERE_..." to "ProbSevere/YYYYMMDD/"
            # "ProbSevere/YYYYMMDD/MRMS_PROBSEVERE_..." matches!
            # But the existing code `filename_prefix` logic was inside `if modifier is not None`.
            # So ProbSevere was NOT getting the hour prefix optimization.
            
            # Let's add StartAfter optimization for ProbSevere!
            # Filename: MRMS_PROBSEVERE_YYYYMMDD_HHMMSS
            start_after_dt = dt - timedelta(hours=1)
            start_after = f"{bucket_path}MRMS_PROBSEVERE_{start_after_dt.strftime('%Y%m%d_%H')}"
        
        # Async file lookup (S3)
        with PerformanceTimer(io_manager, f"Lookup_{modifier_name}", trace_id, threshold_ms=100):
            perf_tracker.start(f"Ingest - MRMS - {modifier_name} - Lookup")
            file_list = await finder.async_lookup_files(bucket_path, start_after=start_after)
            perf_tracker.stop(f"Ingest - MRMS - {modifier_name} - Lookup")

        if not file_list:
            io_manager.write_warning(f"[{trace_id}] No files found in S3 for {bucket_path} at {dt}. Attempting HTTPS fallback...")
            
            # --- HTTPS FALLBACK ---
            try:
                https_finder = HttpsFileFinder(dt, io_manager)
                https_file_list = await https_finder.find_files(region, modifier)
                
                if not https_file_list:
                    io_manager.write_error(f"[{trace_id}] HTTPS Fallback failed: No files found for {modifier} at {dt}")
                    perf_tracker.stop(f"Ingest - MRMS - {modifier_name}")
                    return

                https_downloader = HttpsFileDownloader(dt, io_manager)
                downloaded = await https_downloader.download_matching(https_file_list, outdir)
                
                if downloaded:
                     if downloaded.suffix == ".gz":
                        with PerformanceTimer(io_manager, f"Decompress_HTTPS_{modifier_name}", trace_id, threshold_ms=50):
                            await downloader.async_decompress_file(downloaded) 
                else:
                    io_manager.write_error(f"[{trace_id}] HTTPS Fallback failed: Could not download matching file for {modifier}")

            except Exception as e:
                io_manager.write_error(f"[{trace_id}] HTTPS Fallback Exception: {e}")
            
            perf_tracker.stop(f"Ingest - MRMS - {modifier_name}")
            return
        
        # Download most recent file asynchronously (S3)
        downloaded = None
        with PerformanceTimer(io_manager, f"Download_{modifier_name}", trace_id, threshold_ms=100):
            perf_tracker.start(f"Ingest - MRMS - {modifier_name} - Download")
            downloaded = await downloader.async_download_matching(file_list, outdir)
            perf_tracker.stop(f"Ingest - MRMS - {modifier_name} - Download")
            
        if downloaded:
            if downloaded.suffix == ".gz":
                with PerformanceTimer(io_manager, f"Decompress_{modifier_name}", trace_id, threshold_ms=50):
                    await downloader.async_decompress_file(downloaded)
        else:
            io_manager.write_error(f"[{trace_id}] Failed to download {bucket_path} file")
        
        perf_tracker.stop(f"Ingest - MRMS - {modifier_name}")
    
    except Exception as e:
        io_manager.write_error(f"[{trace_id}] Failed to process {bucket_path} - {e}")
        perf_tracker.stop(f"Ingest - MRMS - {modifier_name}")

def download_all_files_sync_fallback(dt, max_entries):
    """Sync fallback for downloading all MRMS files"""
    # Multithread MRMS downloads
    mrms_modifiers_list = get_mrms_modifiers()
    with ThreadPoolExecutor(max_workers=len(mrms_modifiers_list) + 2) as executor:
        futures = [
            executor.submit(download_modifier_sync, region, modifier, outdir, dt, max_entries)
            for region, modifier, outdir in mrms_modifiers_list
        ]

        for future in as_completed(futures):
            future.result()

def download_modifier_sync(region, modifier, outdir, dt, max_entries):
    """Internal sync version of download_modifier for fallback"""
    # Enforce minute-precision dt
    dt = dt.replace(second=0, microsecond=0)

    finder = FileFinder(dt, bucket, max_entries, io_manager)
    downloader = FileDownloader(dt, bucket, io_manager)

    try:
        bucket_path = parse_mrms_bucket_path(dt, region, modifier)
        
        # Optimization: Append filename prefix to search only this hour
        start_after = None
        from datetime import timedelta

        if modifier is not None:
            filename_prefix = f"MRMS_{modifier}_{dt.strftime('%Y%m%d-%H')}"
            bucket_path = f"{bucket_path}{filename_prefix}"
        else:
            # ProbSevere optimization
            start_after_dt = dt - timedelta(hours=1)
            start_after = f"{bucket_path}MRMS_PROBSEVERE_{start_after_dt.strftime('%Y%m%d_%H')}"

        file_list = finder.lookup_files(bucket_path, start_after=start_after)

        if not file_list:
            io_manager.write_warning(f"No files found for {bucket_path} at {dt}")
            return
        
        # Download most recent file that matches the target minute
        downloaded = downloader.download_matching(file_list, outdir)
        if downloaded:
            downloader.decompress_file(downloaded)
        else:
            io_manager.write_error(f"Failed to download {bucket_path} file")
    
    except Exception as e:
        io_manager.write_error(f"Failed to process {bucket_path} - {e}")

# ==================== GOES-19 Download Functions ====================

def download_goes_product(goes_spec, dt, max_entries=10, hour_lookback=3, preloaded_files=None):
    """
    Download a specific GOES-19 product.
    
    Args:
        goes_spec: GOES ingest specification or legacy ``(product, outdir)`` tuple
        dt (datetime): Target datetime (UTC, timezone-aware)
        max_entries (int): Maximum number of file entries to retrieve (default: 10)
        hour_lookback (int): Number of hours to look back (default: 3).
    
    Returns:
        Path: Path to downloaded file, or None if failed
    """
    # Enforce minute-precision dt
    # dt = dt.replace(second=0, microsecond=0) # Allow seconds for sliding window
    
    # Increase max_entries to ensure we find files in the past (GLM has ~180 files/hour)
    goes_spec = normalize_goes_modifier(goes_spec)
    product = goes_spec.product
    outdir = goes_spec.outdir
    label = _get_goes_spec_label(goes_spec)

    _cleanup_goes_outdir_sync(goes_spec, max_age_minutes=60)

    search_max_entries = _get_goes_search_max_entries(goes_spec, max_entries)
    finder = FileFinder(dt, goes_bucket, search_max_entries, io_manager)
    downloader = FileDownloader(dt, goes_bucket, io_manager)
    
    try:
        all_files = preloaded_files
        if all_files is None:
            bucket_paths = _get_goes_bucket_paths(dt, product, hour_lookback)
            # Lookup files across all paths (FileFinder handles the loop and max_entries check)
            all_files = finder.lookup_files(bucket_paths)
        
        if not all_files:
            io_manager.write_warning(f"No files found for GOES product {label} at {dt}")
            return None

        all_files = _filter_goes_files_for_spec(all_files, goes_spec)
        if not all_files:
            return []
        
        
        # Download all matching files
        if goes_spec.is_glm:
            downloaded_files = downloader.download_all_matching(all_files, outdir)
        else:
            downloaded = downloader.download_matching(all_files, outdir)
            downloaded_files = [downloaded] if downloaded else []
        

        
        if downloaded_files:
            processed_files = []
            for downloaded in downloaded_files:
                # Decompress if .gz
                if downloaded.suffix == ".gz":
                    decompressed = downloader.decompress_file(downloaded)
                    if decompressed:
                        processed_files.append(decompressed)
                    else:
                        processed_files.append(downloaded)
                else:
                    processed_files.append(downloaded)
            
            # Check if we need to merge GLM files
            if goes_spec.is_glm and len(processed_files) > 1:
                io_manager.write_info(f"Merging {len(processed_files)} GLM files...")
                merged_ds = merge_glm_files(processed_files, io_manager)
                
                if merged_ds:
                    # Find the newest timestamp among the files
                    try:
                        timestamps = [extract_timestamp(str(f)) for f in processed_files]
                        newest_ts = max(timestamps)
                        ts_str = newest_ts.strftime('%Y%m%d-%H%M%S')
                    except Exception as e:
                        io_manager.write_warning(f"Could not extract timestamps for naming, using target dt: {e}")
                        ts_str = dt.strftime('%Y%m%d-%H%M%S')

                    # Create a merged filename
                    # Format: OR_{product}_merged_YYYYMMDD-HHMMSS.nc
                    merged_filename = f"OR_{product}_merged_{ts_str}.nc"
                    merged_path = outdir / merged_filename
                    
                    try:
                        merged_ds.to_netcdf(merged_path)
                        io_manager.write_info(f"Saved merged GLM file to: {merged_path}")
                        merged_ds.close()
                        
                        # Delete individual files after successful merge
                        for f in processed_files:
                            try:
                                f.unlink()
                            except Exception as del_e:
                                io_manager.write_warning(f"Failed to delete {f}: {del_e}")
                        io_manager.write_debug(f"Deleted {len(processed_files)} individual GLM files")
                        
                        # Return only the merged file path
                        return [merged_path]
                    except Exception as e:
                        io_manager.write_error(f"Failed to save merged GLM file: {e}")
                        merged_ds.close()
                        # Fallback to returning individual files? Or fail?
                        # Let's return individual files as fallback
                        return processed_files
                else:
                    io_manager.write_error("GLM merge failed, returning individual files")
                    return processed_files

            return processed_files
        else:
            io_manager.write_error(f"Failed to download GOES {label} file")
            return []
    
    except Exception as e:
        io_manager.write_error(f"Failed to process GOES {label} - {e}")
        return []


async def _download_goes_product_async(
    goes_spec,
    dt,
    max_entries,
    hour_lookback,
    s3_client,
    parent_trace_id=None,
    preloaded_files=None,
):
    """
    Async version of download_goes_product.
    
    Internal async function for downloading a single GOES product using aioboto3.
    """
    goes_spec = normalize_goes_modifier(goes_spec)
    product = goes_spec.product
    outdir = goes_spec.outdir
    label = _get_goes_spec_label(goes_spec)
    trace_id = parent_trace_id or f"GOES-{uuid.uuid4().hex[:8]}"

    await _cleanup_goes_outdir_async(goes_spec, trace_id, max_age_minutes=60)
    
    # Increase max_entries to ensure we find files in the past
    search_max_entries = _get_goes_search_max_entries(goes_spec, max_entries)
    finder = AsyncFileFinder(dt, goes_bucket, search_max_entries, io_manager, s3_client=s3_client)
    downloader = AsyncFileDownloader(dt, goes_bucket, io_manager, s3_client=s3_client)
    
    perf_tracker.start(f"Ingest - GOES - {label}")
    try:
        all_files = preloaded_files
        if all_files is None:
            bucket_paths = _get_goes_bucket_paths(dt, product, hour_lookback)
            # Lookup files across all paths (AsyncFileFinder handles the loop and max_entries check)
            with PerformanceTimer(io_manager, f"Lookup_GOES_{product}", trace_id, threshold_ms=100):
                perf_tracker.start(f"Ingest - GOES - {label} - Lookup")
                all_files = await finder.async_lookup_files(bucket_paths)
                perf_tracker.stop(f"Ingest - GOES - {label} - Lookup")
        
        if not all_files:
            io_manager.write_warning(f"[{trace_id}] No files found for GOES product {label} at {dt}")
            perf_tracker.stop(f"Ingest - GOES - {label}")
            return None

        all_files = _filter_goes_files_for_spec(all_files, goes_spec, trace_id=trace_id)
        if not all_files:
            perf_tracker.stop(f"Ingest - GOES - {label}")
            return []
        
        
        # Download all matching files
        with PerformanceTimer(io_manager, f"Download_GOES_{product}", trace_id, threshold_ms=100):
            perf_tracker.start(f"Ingest - GOES - {label} - Download")
            if goes_spec.is_glm:
                downloaded_files = await downloader.async_download_all_matching(all_files, outdir)
            else:
                downloaded = await downloader.async_download_matching(all_files, outdir)
                downloaded_files = [downloaded] if downloaded else []
            perf_tracker.stop(f"Ingest - GOES - {label} - Download")
        
        if downloaded_files:
            processed_files = []
            decompress_tasks = []
            
            # Helper to handle decompression result mapping
            async def _decompress_wrapper(f):
                if f.suffix == ".gz":
                        with PerformanceTimer(io_manager, f"Decompress_GOES_{label}", trace_id, threshold_ms=50):
                            res = await downloader.async_decompress_file(f)
                            return res if res else f
                return f

            # Gather all decompression tasks
            results = await asyncio.gather(*[_decompress_wrapper(f) for f in downloaded_files])
            processed_files = list(results)
            
            # Check if we need to merge GLM files
            if goes_spec.is_glm and len(processed_files) > 1:
                io_manager.write_info(f"[{trace_id}] Merging {len(processed_files)} GLM files (Async)...")
                
                # merge_glm_files is synchronous, so we offload it to a thread pool
                loop = asyncio.get_running_loop()
                # merge_glm_files is synchronous, so we offload it to a thread pool
                loop = asyncio.get_running_loop()
                with PerformanceTimer(io_manager, f"Merge_GLM", trace_id):
                    perf_tracker.start(f"Ingest - GOES - {label} - Merge")
                    merged_ds = await loop.run_in_executor(None, merge_glm_files, processed_files, io_manager)
                    perf_tracker.stop(f"Ingest - GOES - {label} - Merge")
                
                if merged_ds:
                    # Find the newest timestamp among the files
                    try:
                        timestamps = [extract_timestamp(str(f)) for f in processed_files]
                        newest_ts = max(timestamps)
                        ts_str = newest_ts.strftime('%Y%m%d-%H%M%S')
                    except Exception as e:
                        io_manager.write_warning(f"[{trace_id}] Could not extract timestamps for naming, using target dt: {e}")
                        ts_str = dt.strftime('%Y%m%d-%H%M%S')

                    merged_filename = f"OR_{product}_merged_{ts_str}.nc"
                    merged_path = outdir / merged_filename
                    
                    try:
                        # to_netcdf is also synchronous
                        merged_ds.to_netcdf(merged_path)
                        io_manager.write_info(f"[{trace_id}] Saved merged GLM file to: {merged_path}")
                        merged_ds.close()
                        
                        # Delete individual files after successful merge
                        for f in processed_files:
                            try:
                                f.unlink()
                            except Exception as del_e:
                                io_manager.write_warning(f"[{trace_id}] Failed to delete {f}: {del_e}")
                        io_manager.write_debug(f"[{trace_id}] Deleted {len(processed_files)} individual GLM files")
                        
                        perf_tracker.stop(f"Ingest - GOES - {label}")
                        return [merged_path]
                    except Exception as e:
                        io_manager.write_error(f"[{trace_id}] Failed to save merged GLM file: {e}")
                        merged_ds.close()
                        perf_tracker.stop(f"Ingest - GOES - {label}")
                        return processed_files
                else:
                    io_manager.write_error(f"[{trace_id}] GLM merge failed, returning individual files")
                    perf_tracker.stop(f"Ingest - GOES - {label}")
                    return processed_files

            perf_tracker.stop(f"Ingest - GOES - {label}")
            return processed_files
        else:
            io_manager.write_error(f"[{trace_id}] Failed to download GOES {label} file")
            perf_tracker.stop(f"Ingest - GOES - {label}")
            return []
    
    except Exception as e:
        io_manager.write_error(f"[{trace_id}] Failed to process GOES {label} - {e}")
        perf_tracker.stop(f"Ingest - GOES - {label}")
        return []

    perf_tracker.stop(f"Ingest - GOES - {label}")


def download_all_goes_files(dt, max_entries=10, hour_lookback=3):
    """
    Download all configured GOES-19 products.
    
    Args:
        dt (datetime): Target datetime (UTC, timezone-aware)
        max_entries (int): Maximum number of file entries per product (default: 10)
        hour_lookback (int): Number of hours to look back (default: 3)
    """
    io_manager.write_info("Starting GOES-19 downloads...")
    
    # Use ThreadPoolExecutor for concurrent downloads
    goes_modifiers_list = [normalize_goes_modifier(spec) for spec in get_goes_modifiers()]
    shared_channel_files_by_product = {}

    for goes_spec in goes_modifiers_list:
        if not goes_spec.channel_id or goes_spec.product in shared_channel_files_by_product:
            continue

        search_max_entries = _get_goes_search_max_entries(goes_spec, max_entries)
        finder = FileFinder(dt, goes_bucket, search_max_entries, io_manager)
        bucket_paths = _get_goes_bucket_paths(dt, goes_spec.product, hour_lookback)
        shared_channel_files_by_product[goes_spec.product] = finder.lookup_files(bucket_paths)

    with ThreadPoolExecutor(max_workers=len(goes_modifiers_list)) as executor:
        futures = [
            executor.submit(
                download_goes_product,
                goes_spec,
                dt,
                max_entries,
                hour_lookback,
                shared_channel_files_by_product.get(goes_spec.product)
                if goes_spec.channel_id
                else None,
            )
            for goes_spec in goes_modifiers_list
        ]
        
        for future in as_completed(futures):
            try:
                result = future.result()
                if result:
                    io_manager.write_debug(f"Successfully downloaded {len(result)} files")
            except Exception as e:
                io_manager.write_error(f"GOES download error: {e}")
    
    io_manager.write_info("GOES-19 downloads completed")


async def download_all_goes_files_async(dt, max_entries=10, hour_lookback=3):
    """
    Async version: Download all configured GOES-19 products concurrently.
    
    Args:
        dt (datetime): Target datetime (UTC, timezone-aware)
        max_entries (int): Maximum number of file entries per product (default: 10)
        hour_lookback (int): Number of hours to look back (default: 3)
    """
    trace_id = f"GOES_ALL-{uuid.uuid4().hex[:8]}"
    async with aioboto3.Session().client("s3", config=Config(signature_version=UNSIGNED)) as s3:
        io_manager.write_info(f"[{trace_id}] Starting async GOES-19 downloads...")
        goes_modifiers_list = [normalize_goes_modifier(spec) for spec in get_goes_modifiers()]
        shared_channel_files_by_product = {}

        for goes_spec in goes_modifiers_list:
            if not goes_spec.channel_id or goes_spec.product in shared_channel_files_by_product:
                continue

            search_max_entries = _get_goes_search_max_entries(goes_spec, max_entries)
            finder = AsyncFileFinder(dt, goes_bucket, search_max_entries, io_manager, s3_client=s3)
            bucket_paths = _get_goes_bucket_paths(dt, goes_spec.product, hour_lookback)
            with PerformanceTimer(
                io_manager,
                f"Lookup_GOES_{goes_spec.product}_shared",
                trace_id,
                threshold_ms=100,
            ):
                shared_channel_files_by_product[goes_spec.product] = await finder.async_lookup_files(bucket_paths)
        
        tasks = [
            _download_goes_product_async(
                goes_spec,
                dt,
                max_entries,
                hour_lookback,
                s3,
                trace_id,
                preloaded_files=(
                    shared_channel_files_by_product.get(goes_spec.product)
                    if goes_spec.channel_id
                    else None
                ),
            )
            for goes_spec in goes_modifiers_list
        ]
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        for result in results:
            if isinstance(result, Exception):
                io_manager.write_error(f"[{trace_id}] GOES async download error: {result}")
            elif result:
                io_manager.write_debug(f"[{trace_id}] Successfully downloaded {len(result)} files")
        
        io_manager.write_info(f"[{trace_id}] Async GOES-19 downloads completed")
