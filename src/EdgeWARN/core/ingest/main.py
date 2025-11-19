from EdgeWARN.core.ingest.config import mrms_modifiers, bucket
from EdgeWARN.core.ingest.download import FileFinder, FileDownloader, AsyncFileFinder, AsyncFileDownloader
from EdgeWARN.core.ingest.parse import MRMSBucketParser
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
        asyncio.run(_download_all_files_async_internal(dt, max_time, max_entries))
    except Exception as e:
        io_manager.write_error(f"Async downloads failed: {e}")
        io_manager.write_debug("Falling back to synchronous downloads...")
        _download_all_files_sync_fallback(dt, max_time, max_entries)

async def _download_all_files_async_internal(dt, max_time, max_entries):
    """Internal async function that handles the actual download operations"""
    # Create shared async S3 client for all operations
    async with aioboto3.Session().client("s3", config=Config(signature_version=UNSIGNED)) as s3:
        io_manager.write_debug("Starting async downloads...")
        
        # Create async tasks for all modifiers
        tasks = []
        for region, modifier, outdir in mrms_modifiers:
            task = _download_modifier_async(
                region, modifier, outdir, dt, max_time, max_entries, s3
            )
            tasks.append(task)
        
        # Execute all downloads concurrently using asyncio.gather
        # This is the key performance improvement - all S3 operations run in parallel
        io_manager.write_debug(f"Downloading from {len(tasks)} sources concurrently...")
        await asyncio.gather(*tasks, return_exceptions=True)
        
        io_manager.write_debug("All async downloads completed")

async def _download_modifier_async(region, modifier, outdir, dt, max_time, max_entries, s3_client):
    """Internal async version of download_modifier using aioboto3 for non-blocking S3 operations"""
    # Enforce minute-precision dt
    dt = dt.replace(second=0, microsecond=0)

    finder = AsyncFileFinder(dt, bucket, max_time, max_entries, io_manager, s3_client=s3_client)
    downloader = AsyncFileDownloader(dt, bucket, io_manager, s3_client=s3_client)
    parser = MRMSBucketParser(dt)

    try:
        bucket_path = parser.parse_bucket_path(region, modifier)
        
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

def download_all_files(dt):
    # Clear Files
    folders = [outdir for _, _, outdir in mrms_modifiers]
    for f in folders:
        fs.clean_old_files(f, max_age_minutes=60)
    fs.wipe_temp()

    max_time = timedelta(hours=2)   # Look back 2 hours
    max_entries = 5                        # How many files to check per source

    # Multithread MRMS downloads
    with ThreadPoolExecutor(max_workers=len(mrms_modifiers) + 2) as executor:
        futures = [
            executor.submit(_download_modifier_sync, region, modifier, outdir, dt, max_time, max_entries)
            for region, modifier, outdir in mrms_modifiers
        ]

        for future in as_completed(futures):
            future.result()

def _download_modifier_sync(region, modifier, outdir, dt, max_time, max_entries):
    """Internal sync version of download_modifier for fallback"""
    # Enforce minute-precision dt
    dt = dt.replace(second=0, microsecond=0)

    finder = FileFinder(dt, bucket, max_time, max_entries, io_manager)
    downloader = FileDownloader(dt, bucket, io_manager)
    parser = MRMSBucketParser(dt)

    try:
        bucket_path = parser.parse_bucket_path(region, modifier)
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
