import asyncio
import aioboto3
from datetime import datetime, timedelta
from botocore import UNSIGNED
from botocore.client import Config
from util.io import IOManager
import util.file as fs
from EdgeWARN.core.ingest.synoptic.config import RAP_BUCKET, RAP_FILE_PATTERN, RAP_DIR_PATTERN
from EdgeWARN.core.ingest.synoptic.s3_sync import SynopticFileDownloader
from EdgeWARN.core.ingest.synoptic.s3_async import AsyncSynopticFileDownloader

io_manager = IOManager("[DataIngestion]")

async def download_synoptic_async(dt, bucket, file_pattern, dir_pattern, out_dir):
    """
    Attempt to download a synoptic file asynchronously.
    """
    date_str = dt.strftime("%Y%m%d")
    hour = dt.hour
    
    dir_name = dir_pattern.format(date=date_str)
    file_name = file_pattern.format(hour=hour)
    s3_key = f"{dir_name}/{file_name}"
    local_path = out_dir / file_name
    
    async with aioboto3.Session().client("s3", config=Config(signature_version=UNSIGNED)) as s3:
        downloader = AsyncSynopticFileDownloader(bucket, io_manager, s3_client=s3)
        return await downloader.async_download_file(s3_key, local_path)

def download_synoptic_sync(dt, bucket, file_pattern, dir_pattern, out_dir):
    """
    Attempt to download a synoptic file synchronously.
    """
    date_str = dt.strftime("%Y%m%d")
    hour = dt.hour
    
    dir_name = dir_pattern.format(date=date_str)
    file_name = file_pattern.format(hour=hour)
    s3_key = f"{dir_name}/{file_name}"
    local_path = out_dir / file_name
    
    downloader = SynopticFileDownloader(bucket, io_manager)
    return downloader.download_file(s3_key, local_path)

async def download_synoptic(dt, bucket, file_pattern, dir_pattern, out_dir, dataset_name="Synoptic"):
    """
    Main synoptic downloader function: async first, sync fallback.
    Retries with previous hour if original timestamp fails.
    """
    for current_dt in [dt, dt - timedelta(hours=1)]:
        if current_dt != dt:
            io_manager.write_info(f"Attempting {dataset_name} fallback to previous hour: {current_dt}")
        else:
            io_manager.write_info(f"Starting {dataset_name} download for {dt}")
        
        result = None
        try:
            # Try async first
            result = await download_synoptic_async(current_dt, bucket, file_pattern, dir_pattern, out_dir)
            if result:
                return result
        except Exception as e:
            # Do not log error on first attempt if we are going to fallback
            if current_dt == dt:
                io_manager.write_warning(f"Async {dataset_name} download for {current_dt} failed: {e}")
            else:
                io_manager.write_error(f"Async {dataset_name} download failed: {e}")
        
        # Fallback to sync
        try:
            result = download_synoptic_sync(current_dt, bucket, file_pattern, dir_pattern, out_dir)
            if result:
                return result
        except Exception as e:
            if current_dt == dt:
                io_manager.write_warning(f"Sync {dataset_name} download for {current_dt} failed: {e}")
            else:
                io_manager.write_error(f"Sync {dataset_name} download also failed: {e}")
    
    return None

async def download_rap(dt):
    """
    Wrapper for RAP dataset download.
    """
    return await download_synoptic(
        dt, 
        RAP_BUCKET, 
        RAP_FILE_PATTERN, 
        RAP_DIR_PATTERN, 
        fs.RAP_DIR, 
        dataset_name="RAP"
    )
