from EWMRS.ingest.mrms.config import get_mrms_modifiers, bucket, get_goes_modifiers, goes_bucket
from EWMRS.ingest.mrms.s3_sync import FileFinder, FileDownloader
from EWMRS.ingest.mrms.s3_async import AsyncFileFinder, AsyncFileDownloader
from EWMRS.ingest.mrms.https_client import HttpsFileFinder, HttpsFileDownloader
from EWMRS.ingest.mrms.parse import parse_mrms_bucket_path, parse_goes_bucket_path
from EWMRS.ingest.mrms.utils import merge_glm_files, extract_timestamp
from EWMRS.util.io import IOManager
from concurrent.futures import ThreadPoolExecutor, as_completed
import asyncio
import aioboto3
from botocore import UNSIGNED
from botocore.client import Config

io_manager = IOManager("[Ingest]")

async def download_all_files_async_internal(dt, max_entries):
    """Internal async function that handles the actual download operations"""
    # Create shared async S3 client for all operations
    async with aioboto3.Session().client("s3", config=Config(signature_version=UNSIGNED)) as s3:
        io_manager.write_debug("Starting async downloads...")
        
        # Create async tasks for all modifiers
        tasks = []
        for region, modifier, outdir in get_mrms_modifiers():
            task = download_modifier_async(
                region, modifier, outdir, dt, max_entries, s3
            )
            tasks.append(task)
        
        # Execute all downloads concurrently using asyncio.gather
        # This is the key performance improvement - all S3 operations run in parallel
        io_manager.write_debug(f"Downloading from {len(tasks)} sources concurrently...")
        await asyncio.gather(*tasks, return_exceptions=True)
        
        io_manager.write_info("All async downloads completed")

async def download_modifier_async(region, modifier, outdir, dt, max_entries, s3_client):
    """Internal async version of download_modifier using aioboto3 for non-blocking S3 operations"""
    # Enforce minute-precision dt
    dt = dt.replace(second=0, microsecond=0)

    finder = AsyncFileFinder(dt, bucket, max_entries, io_manager, s3_client=s3_client)
    downloader = AsyncFileDownloader(dt, bucket, io_manager, s3_client=s3_client)

    try:
        bucket_path = parse_mrms_bucket_path(dt, region, modifier)
        
        # Optimization: Append filename prefix to search only this hour
        # File format: MRMS_{modifier}_{YYYYMMDD}-{HH}MMSS
        # This significantly reduces the search space for historical downloads
        # Skip for ProbSevere (modifier=None) which has different naming
        if modifier is not None:
            filename_prefix = f"MRMS_{modifier}_{dt.strftime('%Y%m%d-%H')}"
            bucket_path = f"{bucket_path}{filename_prefix}"
        
        # Async file lookup (S3)
        file_list = await finder.async_lookup_files(bucket_path)

        if not file_list:
            io_manager.write_warning(f"No files found in S3 for {bucket_path} at {dt}. Attempting HTTPS fallback...")
            
            # --- HTTPS FALLBACK ---
            try:
                https_finder = HttpsFileFinder(dt, io_manager)
                https_file_list = await https_finder.find_files(region, modifier)
                
                if not https_file_list:
                    io_manager.write_error(f"HTTPS Fallback failed: No files found for {modifier} at {dt}")
                    return

                https_downloader = HttpsFileDownloader(dt, io_manager)
                downloaded = await https_downloader.download_matching(https_file_list, outdir)
                
                if downloaded:
                     if downloaded.suffix == ".gz":
                        await downloader.async_decompress_file(downloaded) # Reuse existing decompressor logic from s3_async downloader if possible or just use the local file path
                        # Note: downloader.async_decompress_file expects a path, which matches what we return.
                        # However, strictly speaking, AsyncFileDownloader method might belong to that instance. 
                        # It is stateless regarding S3 for decompression, so it should work.
                else:
                    io_manager.write_error(f"HTTPS Fallback failed: Could not download matching file for {modifier}")

            except Exception as e:
                io_manager.write_error(f"HTTPS Fallback Exception: {e}")
            
            return
        
        # Download most recent file asynchronously (S3)
        downloaded = await downloader.async_download_matching(file_list, outdir)
        if downloaded:
            if downloaded.suffix == ".gz":
                await downloader.async_decompress_file(downloaded)
        else:
            io_manager.write_error(f"Failed to download {bucket_path} file")
    
    except Exception as e:
        io_manager.write_error(f"Failed to process {bucket_path} - {e}")

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
        # File format: MRMS_{modifier}_{YYYYMMDD}-{HH}MMSS
        # This significantly reduces the search space for historical downloads
        filename_prefix = f"MRMS_{modifier}_{dt.strftime('%Y%m%d-%H')}"
        bucket_path = f"{bucket_path}{filename_prefix}"
        file_list = finder.lookup_files(bucket_path)

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

def download_goes_product(product, outdir, dt, max_entries=10, hour_lookback=3):
    """
    Download a specific GOES-19 product.
    
    Args:
        product (str): GOES product name (e.g., "GLM-L2-LCFA", "ABI-L2-ACHAC")
        outdir (Path): Output directory for downloaded files
        dt (datetime): Target datetime (UTC, timezone-aware)
        max_entries (int): Maximum number of file entries to retrieve (default: 10)
        hour_lookback (int): Number of hours to look back (default: 3).
    
    Returns:
        Path: Path to downloaded file, or None if failed
    """
    # Enforce minute-precision dt
    # dt = dt.replace(second=0, microsecond=0) # Allow seconds for sliding window
    
    # Increase max_entries to ensure we find files in the past (GLM has ~180 files/hour)
    search_max_entries = max(max_entries, 300)
    finder = FileFinder(dt, goes_bucket, search_max_entries, io_manager)
    downloader = FileDownloader(dt, goes_bucket, io_manager)
    
    try:
        # Generate list of paths to check
        bucket_paths = []
        for hour_offset in range(hour_lookback):
            bucket_path = parse_goes_bucket_path(dt, product, hour_offset=hour_offset)
            bucket_paths.append(bucket_path)
            
        
        # Lookup files across all paths (FileFinder handles the loop and max_entries check)
        all_files = finder.lookup_files(bucket_paths)
        
        if not all_files:
            io_manager.write_warning(f"No files found for GOES product {product} at {dt}")
            return None
        
        
        # Download all matching files
        downloaded_files = downloader.download_all_matching(all_files, outdir)
        
        

        
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
            if "GLM" in product and len(processed_files) > 1:
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
            io_manager.write_error(f"Failed to download GOES {product} file")
            return []
    
    except Exception as e:
        io_manager.write_error(f"Failed to process GOES {product} - {e}")
        return []


async def _download_goes_product_async(product, outdir, dt, max_entries, hour_lookback, s3_client):
    """
    Async version of download_goes_product.
    
    Internal async function for downloading a single GOES product using aioboto3.
    """
    # Enforce minute-precision dt
    # dt = dt.replace(second=0, microsecond=0) # Allow seconds for sliding window
    
    # Increase max_entries to ensure we find files in the past
    search_max_entries = max(max_entries, 300)
    finder = AsyncFileFinder(dt, goes_bucket, search_max_entries, io_manager, s3_client=s3_client)
    downloader = AsyncFileDownloader(dt, goes_bucket, io_manager, s3_client=s3_client)
    
    try:
        # Generate list of paths to check
        bucket_paths = []
        for hour_offset in range(hour_lookback):
            bucket_path = parse_goes_bucket_path(dt, product, hour_offset=hour_offset)
            bucket_paths.append(bucket_path)
            
        
        # Lookup files across all paths (AsyncFileFinder handles the loop and max_entries check)
        all_files = await finder.async_lookup_files(bucket_paths)
        
        if not all_files:
            io_manager.write_warning(f"No files found for GOES product {product} at {dt}")
            return None
        
        
        # Download all matching files
        downloaded_files = await downloader.async_download_all_matching(all_files, outdir)
        
        if downloaded_files:
            processed_files = []
            for downloaded in downloaded_files:
                # Decompress if .gz
                if downloaded.suffix == ".gz":
                    decompressed = await downloader.async_decompress_file(downloaded)
                    if decompressed:
                        processed_files.append(decompressed)
                    else:
                        processed_files.append(downloaded)
                else:
                    processed_files.append(downloaded)
            
            # Check if we need to merge GLM files
            if "GLM" in product and len(processed_files) > 1:
                io_manager.write_info(f"Merging {len(processed_files)} GLM files (Async)...")
                
                # merge_glm_files is synchronous, so we offload it to a thread pool
                loop = asyncio.get_running_loop()
                merged_ds = await loop.run_in_executor(None, merge_glm_files, processed_files, io_manager)
                
                if merged_ds:
                    # Find the newest timestamp among the files
                    try:
                        timestamps = [extract_timestamp(str(f)) for f in processed_files]
                        newest_ts = max(timestamps)
                        ts_str = newest_ts.strftime('%Y%m%d-%H%M%S')
                    except Exception as e:
                        io_manager.write_warning(f"Could not extract timestamps for naming, using target dt: {e}")
                        ts_str = dt.strftime('%Y%m%d-%H%M%S')

                    merged_filename = f"OR_{product}_merged_{ts_str}.nc"
                    merged_path = outdir / merged_filename
                    
                    try:
                        # to_netcdf is also synchronous
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
                        
                        return [merged_path]
                    except Exception as e:
                        io_manager.write_error(f"Failed to save merged GLM file: {e}")
                        merged_ds.close()
                        return processed_files
                else:
                    io_manager.write_error("GLM merge failed, returning individual files")
                    return processed_files

            return processed_files
        else:
            io_manager.write_error(f"Failed to download GOES {product} file")
            return []
    
    except Exception as e:
        io_manager.write_error(f"Failed to process GOES {product} - {e}")
        return []


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
    goes_modifiers_list = get_goes_modifiers()
    with ThreadPoolExecutor(max_workers=len(goes_modifiers_list)) as executor:
        futures = [
            executor.submit(download_goes_product, product, outdir, dt, max_entries, hour_lookback)
            for product, outdir in goes_modifiers_list
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
    async with aioboto3.Session().client("s3", config=Config(signature_version=UNSIGNED)) as s3:
        io_manager.write_info("Starting async GOES-19 downloads...")
        
        tasks = [
            _download_goes_product_async(product, outdir, dt, max_entries, hour_lookback, s3)
            for product, outdir in get_goes_modifiers()
        ]
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        for result in results:
            if isinstance(result, Exception):
                io_manager.write_error(f"GOES async download error: {result}")
            elif result:
                io_manager.write_debug(f"Successfully downloaded {len(result)} files")
        
        io_manager.write_info("Async GOES-19 downloads completed")
