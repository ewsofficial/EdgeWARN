from EWMRS.ingest.mrms.config import get_mrms_modifiers, get_goes_modifiers, bucket
from EWMRS.ingest.mrms.downloader import (
    download_all_files_async_internal,
    download_all_files_sync_fallback,
    download_all_goes_files,
    download_all_goes_files_async
)
from common.ingest.mrms.pipeline import get_output_dirs, run_ingestion_pipeline, run_with_async_fallback
from util.io import IOManager
import util.ewmrs_file as fs

io_manager = IOManager("[Ingest]")


async def download_all_files_async(dt, max_entries=10, remove_old_files=True):
    mrms_modifiers = get_mrms_modifiers()
    goes_modifiers = get_goes_modifiers()
    cleanup_dirs = get_output_dirs(mrms_modifiers, goes_modifiers=goes_modifiers)

    async def cleanup(folder, **kwargs):
        fs.clean_old_files(folder, **kwargs)

    await run_ingestion_pipeline(
        io_manager=io_manager,
        async_downloads=[
            download_all_files_async_internal(dt, max_entries),
            download_all_goes_files_async(dt, max_entries),
        ],
        cleanup_dirs=cleanup_dirs if remove_old_files else (),
        cleanup_async=cleanup,
        cleanup_message=f"Starting cleanup for {len(cleanup_dirs)} directories...",
        cleanup_kwargs={"max_age_minutes": 60},
    )

def download_all_files(dt, max_entries=10, remove_old_files=True):
    run_with_async_fallback(
        io_manager=io_manager,
        async_runner=lambda: download_all_files_async(dt, max_entries, remove_old_files),
        sync_fallback=lambda: (
            download_all_files_sync_fallback(dt, max_entries),
            download_all_goes_files(dt, max_entries),
        ),
    )
