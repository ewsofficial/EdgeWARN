import asyncio
from datetime import datetime
from common.ingest.synoptic.downloader import download_rap as _download_rap
import util.file as fs


RAP_MAX_FILES = 3
RAP_PRE_DOWNLOAD_MAX_FILES = RAP_MAX_FILES - 1
RAP_MAX_AGE_MINUTES = 90


async def download_rap_async(dt: datetime):
    """
    Async version of download_rap.
    Cleans up RAP files before and after downloading so the RAP directory stays bounded.
    """
    await fs.async_clean_old_files(
        fs.RAP_DIR,
        max_age_minutes=RAP_MAX_AGE_MINUTES,
        max_files=RAP_PRE_DOWNLOAD_MAX_FILES,
    )
    result = await _download_rap(dt)
    if result:
        await fs.async_clean_old_files(
            fs.RAP_DIR,
            max_age_minutes=RAP_MAX_AGE_MINUTES,
            max_files=RAP_MAX_FILES,
        )
    return result

def download_rap(dt: datetime):
    """
    Public API to download a RAP file for a given datetime.
    Handles the async loop if necessary.
    Enforces a 3-file limit using clean_old_files.
    """
    try:
        # Check if there is a running event loop
        loop = asyncio.get_running_loop()
    except RuntimeError:
        fs.clean_old_files(
            fs.RAP_DIR,
            max_age_minutes=RAP_MAX_AGE_MINUTES,
            max_files=RAP_PRE_DOWNLOAD_MAX_FILES,
        ) # 90 min to ensure that there is another RAP file
        # If no loop, run with asyncio.run
        result = asyncio.run(_download_rap(dt))
        if result:
            fs.clean_old_files(
                fs.RAP_DIR,
                max_age_minutes=RAP_MAX_AGE_MINUTES,
                max_files=RAP_MAX_FILES,
            )
        return result
    else:
        # If loop exists, we can't use asyncio.run
        return loop.create_task(download_rap_async(dt))

if __name__ == "__main__":
    # Test with current time or specific timestamp
    import sys
    from util.io import IOManager
    
    io_manager = IOManager("[RAPTest]")
    test_dt = datetime.now()
    io_manager.write_info(f"Running RAP download test (Synoptic Refactor) for {test_dt}")
    
    result = download_rap(test_dt)
    if result:
        io_manager.write_info(f"Test successful: {result}")
    else:
        io_manager.write_error("Test failed")
