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
    if remove_old_files:
        for f in folders:
            fs.clean_old_files(f, max_age_minutes=60)
            
    await asyncio.gather(
        download_all_files_async_internal(dt, max_entries),
        download_all_goes_files_async(dt, max_entries)
    )

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