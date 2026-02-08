from EdgeWARN.core.ingest.mrms.config import get_mrms_modifiers, get_goes_modifiers, bucket
from EdgeWARN.core.ingest.mrms.s3_sync import FileFinder, FileDownloader
from EdgeWARN.core.ingest.mrms.s3_async import AsyncFileFinder, AsyncFileDownloader
from EdgeWARN.core.ingest.mrms.parse import parse_goes_bucket_path
from EdgeWARN.core.ingest.mrms.downloader import (
    download_all_files_async_internal,
    download_all_files_sync_fallback,
    download_all_goes_files,
    download_all_goes_files_async
)
from util.io import IOManager
import util.file as fs
from concurrent.futures import ThreadPoolExecutor, as_completed
import asyncio
import aioboto3
from botocore import UNSIGNED
from botocore.client import Config
import traceback

io_manager = IOManager("[Ingest]")


async def download_all_files_async(dt, max_entries=10, remove_old_files=True):
    """
    Async version of download_all_files.
    """
    # Clear files first
    mrms_modifiers = get_mrms_modifiers()
    goes_modifiers_list = get_goes_modifiers()
    
    folders = [outdir for _, _, outdir in mrms_modifiers]
    # Add GOES folders
    folders.extend([outdir for _, outdir in goes_modifiers_list])
    cleanup_tasks = []
    if remove_old_files:
        io_manager.write_debug(f"Starting async cleanup for {len(folders)} directories...")
        for f in folders:
            cleanup_tasks.append(fs.async_clean_old_files(f, max_age_minutes=60))
            
    # Run cleanup and downloads concurrently
    # Note: We technically could await cleanup_tasks separately if we wanted, 
    # but gathering them with downloads is fine as long as clean_old_files is robust.
    # However, usually we want to clear space *before* downloading if disk is full. 
    # But here it's time-based expiry. Let's run them in parallel with downloads for max speed.
    
    all_tasks = [
        download_all_files_async_internal(dt, max_entries),
        download_all_goes_files_async(dt, max_entries)
    ]
    all_tasks.extend(cleanup_tasks)
    
    await asyncio.gather(*all_tasks)

def download_all_files(dt, max_entries=10, remove_old_files=True):
    """
    Main function for downloading all MRMS files.
    
    This operates synchronously as a wrapper/fallback or for legacy calls.
    It catches exceptions and falls back to synchronous downloads if async fails.
    """
    try:
        asyncio.run(download_all_files_async(dt, max_entries, remove_old_files))
    except Exception as e:
        io_manager.write_error(f"Async downloads failed: {e}")
        io_manager.write_info("Falling back to synchronous downloads...")
        download_all_files_sync_fallback(dt, max_entries)
        # Fallback for GOES as well (synchronous)
        download_all_goes_files(dt, max_entries)