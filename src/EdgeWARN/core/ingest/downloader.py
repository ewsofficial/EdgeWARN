from EdgeWARN.core.ingest.config import mrms_modifiers, bucket, goes_modifiers, goes_bucket
from EdgeWARN.core.ingest.s3_sync import FileFinder, FileDownloader
from EdgeWARN.core.ingest.s3_async import AsyncFileFinder, AsyncFileDownloader
from EdgeWARN.core.ingest.parse import parse_mrms_bucket_path, parse_goes_bucket_path
from util.io import IOManager
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
        for region, modifier, outdir in mrms_modifiers:
            task = download_modifier_async(
                region, modifier, outdir, dt, max_entries, s3
            )
            tasks.append(task)
        
        # Execute all downloads concurrently using asyncio.gather
        # This is the key performance improvement - all S3 operations run in parallel
        io_manager.write_debug(f"Downloading from {len(tasks)} sources concurrently...")
        await asyncio.gather(*tasks, return_exceptions=True)
        
        io_manager.write_debug("All async downloads completed")

async def download_modifier_async(region, modifier, outdir, dt, max_entries, s3_client):
    """Internal async version of download_modifier using aioboto3 for non-blocking S3 operations"""
    # Enforce minute-precision dt
    dt = dt.replace(second=0, microsecond=0)

    finder = AsyncFileFinder(dt, bucket, max_entries, io_manager, s3_client=s3_client)
    downloader = AsyncFileDownloader(dt, bucket, io_manager, s3_client=s3_client)

    try:
        bucket_path = parse_mrms_bucket_path(dt, region, modifier)
        
        # Async file lookup
        file_list = await finder.async_lookup_files(bucket_path)

        if not file_list:
            io_manager.write_warning(f"No files found for {bucket_path} at {dt}")
            return
        
        # Download most recent file asynchronously
        downloaded = await downloader.async_download_latest(file_list, outdir)
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
    with ThreadPoolExecutor(max_workers=len(mrms_modifiers) + 2) as executor:
        futures = [
            executor.submit(download_modifier_sync, region, modifier, outdir, dt, max_entries)
            for region, modifier, outdir in mrms_modifiers
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
        file_list = finder.lookup_files(bucket_path)

        if not file_list:
            io_manager.write_warning(f"No files found for {bucket_path} at {dt}")
            return
        
        # Download most recent file that matches the target minute
        downloaded = downloader.download_latest(file_list, outdir)
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
    dt = dt.replace(second=0, microsecond=0)
    
    finder = FileFinder(dt, goes_bucket, max_entries, io_manager)
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
        
        
        # Download most recent file
        downloaded = downloader.download_latest(all_files, outdir)
        if downloaded:
            # Decompress if .gz
            if downloaded.suffix == ".gz":
                decompressed = downloader.decompress_file(downloaded)
                return decompressed if decompressed else downloaded
            return downloaded
        else:
            io_manager.write_error(f"Failed to download GOES {product} file")
            return None
    
    except Exception as e:
        io_manager.write_error(f"Failed to process GOES {product} - {e}")
        return None


async def _download_goes_product_async(product, outdir, dt, max_entries, hour_lookback, s3_client):
    """
    Async version of download_goes_product.
    
    Internal async function for downloading a single GOES product using aioboto3.
    """
    # Enforce minute-precision dt
    dt = dt.replace(second=0, microsecond=0)
    
    finder = AsyncFileFinder(dt, goes_bucket, max_entries, io_manager, s3_client=s3_client)
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
        
        
        # Download most recent file
        downloaded = await downloader.async_download_latest(all_files, outdir)
        if downloaded:
            # Decompress if .gz
            if downloaded.suffix == ".gz":
                decompressed = await downloader.async_decompress_file(downloaded)
                return decompressed if decompressed else downloaded
            return downloaded
        else:
            io_manager.write_error(f"Failed to download GOES {product} file")
            return None
    
    except Exception as e:
        io_manager.write_error(f"Failed to process GOES {product} - {e}")
        return None


def download_all_goes_files(dt, max_entries=10, hour_lookback=3):
    """
    Download all configured GOES-19 products.
    
    Args:
        dt (datetime): Target datetime (UTC, timezone-aware)
        max_entries (int): Maximum number of file entries per product (default: 10)
        hour_lookback (int): Number of hours to look back (default: 3)
    """
    io_manager.write_debug("Starting GOES-19 downloads...")
    
    # Use ThreadPoolExecutor for concurrent downloads
    with ThreadPoolExecutor(max_workers=len(goes_modifiers)) as executor:
        futures = [
            executor.submit(download_goes_product, product, outdir, dt, max_entries, hour_lookback)
            for product, outdir in goes_modifiers
        ]
        
        for future in as_completed(futures):
            try:
                result = future.result()
                if result:
                    io_manager.write_debug(f"Successfully downloaded: {result}")
            except Exception as e:
                io_manager.write_error(f"GOES download error: {e}")
    
    io_manager.write_debug("GOES-19 downloads completed")


async def download_all_goes_files_async(dt, max_entries=10, hour_lookback=3):
    """
    Async version: Download all configured GOES-19 products concurrently.
    
    Args:
        dt (datetime): Target datetime (UTC, timezone-aware)
        max_entries (int): Maximum number of file entries per product (default: 10)
        hour_lookback (int): Number of hours to look back (default: 3)
    """
    async with aioboto3.Session().client("s3", config=Config(signature_version=UNSIGNED)) as s3:
        io_manager.write_debug("Starting async GOES-19 downloads...")
        
        tasks = [
            _download_goes_product_async(product, outdir, dt, max_entries, hour_lookback, s3)
            for product, outdir in goes_modifiers
        ]
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        for result in results:
            if isinstance(result, Exception):
                io_manager.write_error(f"GOES async download error: {result}")
            elif result:
                io_manager.write_debug(f"Successfully downloaded: {result}")
        
        io_manager.write_debug("Async GOES-19 downloads completed")
