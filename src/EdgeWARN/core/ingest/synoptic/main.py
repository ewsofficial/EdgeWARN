import asyncio
from datetime import datetime
from EdgeWARN.core.ingest.synoptic.downloader import download_rap as _download_rap

def download_rap(dt: datetime):
    """
    Public API to download a RAP file for a given datetime.
    Handles the async loop if necessary.
    """
    try:
        # Check if there is a running event loop
        loop = asyncio.get_running_loop()
    except RuntimeError:
        # If no loop, run with asyncio.run
        return asyncio.run(_download_rap(dt))
    else:
        # If loop exists, we can't use asyncio.run
        return loop.create_task(_download_rap(dt))

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
