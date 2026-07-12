import asyncio
import aioboto3
from datetime import datetime, timedelta
from botocore import UNSIGNED
from botocore.client import Config
from util.io import IOManager
import util.file as fs
from common.ingest.aws_async_compat import ensure_aiobotocore_endpoint_compat
from common.ingest.synoptic.config import RAP_BUCKET, RAP_FILE_PATTERN, RAP_DIR_PATTERN
from common.ingest.synoptic.s3_sync import SynopticFileDownloader
from common.ingest.synoptic.s3_async import AsyncSynopticFileDownloader

io_manager = IOManager("[DataIngestion]")


def _log_synoptic_not_found(bucket, s3_key):
    io_manager.write_warning(f"Synoptic file not found on S3 (404): s3://{bucket}/{s3_key}")


def _build_synoptic_s3_params(dt, file_pattern, dir_pattern, out_dir):
    """
    Build the S3 key and local file path for a synoptic download.

    Args:
        dt: Target datetime.
        file_pattern (str): ``str.format``-compatible pattern for the filename
            (receives ``hour=<int>``).
        dir_pattern (str): ``str.format``-compatible pattern for the S3 directory
            (receives ``date=<str>``).
        out_dir (Path): Local output directory.

    Returns:
        tuple[str, Path]: ``(s3_key, local_path)``
    """
    date_str = dt.strftime("%Y%m%d")
    hour = dt.hour

    dir_name = dir_pattern.format(date=date_str)
    file_name = file_pattern.format(hour=hour)
    s3_key = f"{dir_name}/{file_name}"

    local_filename = f"RAP.{date_str}-{hour:02d}z.awp130pgrbf00.grib2"
    local_path = out_dir / local_filename

    return s3_key, local_path


async def download_synoptic_async(dt, bucket, file_pattern, dir_pattern, out_dir):
    """
    Attempt to download a synoptic file asynchronously.
    """
    s3_key, local_path = _build_synoptic_s3_params(dt, file_pattern, dir_pattern, out_dir)

    ensure_aiobotocore_endpoint_compat()
    async with aioboto3.Session().client("s3", config=Config(signature_version=UNSIGNED)) as s3:
        downloader = AsyncSynopticFileDownloader(bucket, io_manager, s3_client=s3)
        return await downloader.async_download_file(s3_key, local_path)

def download_synoptic_sync(dt, bucket, file_pattern, dir_pattern, out_dir):
    """
    Attempt to download a synoptic file synchronously.
    """
    s3_key, local_path = _build_synoptic_s3_params(dt, file_pattern, dir_pattern, out_dir)

    downloader = SynopticFileDownloader(bucket, io_manager)
    return downloader.download_file(s3_key, local_path)

async def download_synoptic(dt, bucket, file_pattern, dir_pattern, out_dir, dataset_name="Synoptic"):
    """
    Main synoptic downloader function: async first, sync fallback.
    Retries with previous hour if original timestamp fails.
    """
    for current_dt in [dt, dt - timedelta(hours=1)]:
        s3_key, _ = _build_synoptic_s3_params(current_dt, file_pattern, dir_pattern, out_dir)
        if current_dt != dt:
            io_manager.write_info(
                f"Attempting {dataset_name} fallback to previous hour: {current_dt} "
                f"(s3://{bucket}/{s3_key})"
            )
        else:
            io_manager.write_info(f"Attempting {dataset_name} download: s3://{bucket}/{s3_key}")
        
        result = None
        try:
            # Try async first
            result = await download_synoptic_async(current_dt, bucket, file_pattern, dir_pattern, out_dir)
            if result:
                return result
        except FileNotFoundError:
            _log_synoptic_not_found(bucket, s3_key)
            continue
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
        except FileNotFoundError:
            _log_synoptic_not_found(bucket, s3_key)
            continue
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
