from EdgeWARN.core.ingest.config import mrms_modifiers, bucket, goes_modifiers, goes_bucket
from EdgeWARN.core.ingest.utils import FileFinder, FileDownloader, AsyncFileFinder, AsyncFileDownloader
from EdgeWARN.core.ingest.parse import GOESBucketParser
from EdgeWARN.core.ingest.downloader import download_all_files_async_internal, download_all_files_sync_fallback
from util.io import IOManager
import util.file as fs
from datetime import timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import asyncio
import aioboto3
from botocore import UNSIGNED
from botocore.client import Config

io_manager = IOManager("[Ingest]")

def download_all_files(dt):
    """
    Main function for downloading all MRMS files.
    
    This function is called by src/run.py without any modifications needed.
    It uses async operations internally for better performance while maintaining
    the same synchronous interface.
    """
    # Clear files first
    folders = [outdir for _, _, outdir in mrms_modifiers]
    for f in folders:
        fs.clean_old_files(f, max_age_minutes=60)
    fs.wipe_temp()

    max_time = timedelta(hours=6)   # Look back 6 hours
    max_entries = 10                         # How many files to check per source

    # Use async operations internally for better performance
    # This maintains the same API but with improved performance
    try:
        asyncio.run(download_all_files_async_internal(dt, max_time, max_entries))
    except Exception as e:
        io_manager.write_error(f"Async downloads failed: {e}")
        io_manager.write_debug("Falling back to synchronous downloads...")
        download_all_files_sync_fallback(dt, max_time, max_entries)


# ==================== GOES-19 Download Functions ====================

def download_goes_product(product, outdir, dt, max_time=None, max_entries=10, hour_lookback=3):
    """
    Download a specific GOES-19 product.
    
    Args:
        product (str): GOES product name (e.g., "GLM-L2-LCFA", "ABI-L2-ACHAC")
        outdir (Path): Output directory for downloaded files
        dt (datetime): Target datetime (UTC, timezone-aware)
        max_time (timedelta): Maximum time to look back for files (default: None)
        max_entries (int): Maximum number of file entries to retrieve (default: 10)
        hour_lookback (int): Number of hours to look back (default: 3)
    
    Returns:
        Path: Path to downloaded file, or None if failed
    """
    # Enforce minute-precision dt
    dt = dt.replace(second=0, microsecond=0)
    
    finder = FileFinder(dt, goes_bucket, max_time, max_entries, io_manager)
    downloader = FileDownloader(dt, goes_bucket, io_manager)
    parser = GOESBucketParser(dt)
    
    try:
        # Try multiple hours (current + lookback)
        all_files = []
        for hour_offset in range(hour_lookback):
            bucket_path = parser.parse_bucket_path(product, hour_offset=hour_offset)
            io_manager.write_debug(f"Checking GOES path: {bucket_path}")
            
            file_list = finder.lookup_files(bucket_path)
            if file_list:
                all_files.extend(file_list)
        
        if not all_files:
            io_manager.write_warning(f"No files found for GOES product {product} at {dt}")
            return None
        
        # Sort by timestamp (latest first)
        all_files.sort(key=lambda x: x[1], reverse=True)
        
        io_manager.write_debug(f"Found {len(all_files)} GOES {product} file(s). Downloading latest.")
        
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


async def _download_goes_product_async(product, outdir, dt, max_time, max_entries, hour_lookback, s3_client):
    """
    Async version of download_goes_product.
    
    Internal async function for downloading a single GOES product using aioboto3.
    """
    # Enforce minute-precision dt
    dt = dt.replace(second=0, microsecond=0)
    
    finder = AsyncFileFinder(dt, goes_bucket, max_time, max_entries, io_manager, s3_client=s3_client)
    downloader = AsyncFileDownloader(dt, goes_bucket, io_manager, s3_client=s3_client)
    parser = GOESBucketParser(dt)
    
    try:
        # Try multiple hours (current + lookback)
        all_files = []
        for hour_offset in range(hour_lookback):
            bucket_path = parser.parse_bucket_path(product, hour_offset=hour_offset)
            io_manager.write_debug(f"Checking GOES path: {bucket_path}")
            
            file_list = await finder.async_lookup_files(bucket_path)
            if file_list:
                all_files.extend(file_list)
        
        if not all_files:
            io_manager.write_warning(f"No files found for GOES product {product} at {dt}")
            return None
        
        # Sort by timestamp (latest first)
        all_files.sort(key=lambda x: x[1], reverse=True)
        
        io_manager.write_debug(f"Found {len(all_files)} GOES {product} file(s). Downloading latest.")
        
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


def download_all_goes_files(dt, max_time=None, max_entries=10, hour_lookback=3):
    """
    Download all configured GOES-19 products.
    
    Args:
        dt (datetime): Target datetime (UTC, timezone-aware)
        max_time (timedelta): Maximum time to look back for files (default: None)
        max_entries (int): Maximum number of file entries per product (default: 10)
        hour_lookback (int): Number of hours to look back (default: 3)
    """
    io_manager.write_debug("Starting GOES-19 downloads...")
    
    # Use ThreadPoolExecutor for concurrent downloads
    with ThreadPoolExecutor(max_workers=len(goes_modifiers)) as executor:
        futures = [
            executor.submit(download_goes_product, product, outdir, dt, max_time, max_entries, hour_lookback)
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


async def download_all_goes_files_async(dt, max_time=None, max_entries=10, hour_lookback=3):
    """
    Async version: Download all configured GOES-19 products concurrently.
    
    Args:
        dt (datetime): Target datetime (UTC, timezone-aware)
        max_time (timedelta): Maximum time to look back for files (default: None)
        max_entries (int): Maximum number of file entries per product (default: 10)
        hour_lookback (int): Number of hours to look back (default: 3)
    """
    async with aioboto3.Session().client("s3", config=Config(signature_version=UNSIGNED)) as s3:
        io_manager.write_debug("Starting async GOES-19 downloads...")
        
        tasks = [
            _download_goes_product_async(product, outdir, dt, max_time, max_entries, hour_lookback, s3)
            for product, outdir in goes_modifiers
        ]
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        for result in results:
            if isinstance(result, Exception):
                io_manager.write_error(f"GOES async download error: {result}")
            elif result:
                io_manager.write_debug(f"Successfully downloaded: {result}")
        
        io_manager.write_debug("Async GOES-19 downloads completed")