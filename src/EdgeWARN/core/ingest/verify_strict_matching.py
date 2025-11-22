import asyncio
import os
from datetime import datetime, timezone, timedelta
from pathlib import Path
import shutil

# Import the functions to test
from EdgeWARN.core.ingest.main import download_goes_product, download_all_goes_files_async
from EdgeWARN.core.ingest.config import goes_bucket
from EdgeWARN.core.ingest.s3_sync import FileFinder, FileDownloader
from EdgeWARN.core.ingest.s3_async import AsyncFileFinder, AsyncFileDownloader
from util.io import IOManager

# Mock IOManager to avoid cluttering logs
io_manager = IOManager("[Test]")

async def test_strict_matching():
    print("Starting strict matching verification...")
    
    # Setup
    outdir = Path("test_output")
    if outdir.exists():
        shutil.rmtree(outdir)
    outdir.mkdir()
    
    # 1. Find a real file in the bucket to use as a target
    # We'll look for something recent
    now = datetime.now(timezone.utc)
    finder = FileFinder(now, goes_bucket, 10, io_manager)
    
    # Look for GLM files
    product = "GLM-L2-LCFA"
    # We need to find a valid path first
    # Let's just try to list some files from the last few hours
    found_file = None
    found_ts = None
    
    print("Searching for a valid file to test with...")
    from EdgeWARN.core.ingest.parse import parse_goes_bucket_path
    
    for i in range(5):
        bucket_path = parse_goes_bucket_path(now, product, hour_offset=i)
        files = finder.lookup_files(bucket_path)
        if files:
            found_file, found_ts = files[0]
            print(f"Found valid file: {found_file} with timestamp {found_ts}")
            break
            
    if not found_file:
        print("Could not find any recent files to test with. Aborting test.")
        return

    # 2. Test Sync Download with EXACT timestamp
    print(f"\nTesting Sync Download with EXACT timestamp: {found_ts}")
    result = download_goes_product(product, outdir, found_ts, max_entries=10, hour_lookback=5)
    
    if result and result.exists():
        print(f"✓ Sync download successful: {result.name}")
    else:
        print("✗ Sync download failed for exact timestamp")

    # 3. Test Sync Download with DIFFERENT timestamp (should FALLBACK now)
    # We'll use a timestamp 1 minute after the found file
    fake_ts = found_ts + timedelta(minutes=1)
    print(f"\nTesting Sync Download with DIFFERENT timestamp: {fake_ts}")
    print("(This should now FALLBACK to the latest file instead of returning None)")
    
    result_fallback = download_goes_product(product, outdir, fake_ts, max_entries=10, hour_lookback=5)
    
    if result_fallback and result_fallback.exists():
        print(f"✓ Sync download correctly fell back to a file: {result_fallback.name}")
        # Verify it's the same file as the exact match (since it's the latest)
        if result_fallback.name == result.name:
             print("  (Confirmed it fell back to the expected latest file)")
        else:
             print(f"  (Warning: It fell back to {result_fallback.name}, expected {result.name})")
    else:
        print("✗ Sync download failed to fallback (returned None)")

    # 4. Test Async Download with EXACT timestamp
    print(f"\nTesting Async Download with EXACT timestamp: {found_ts}")
    # We need to call the internal async function or wrap the main one
    # The main one returns a list of results, let's use that
    
    # We need to mock the modifiers list to only download what we want, 
    # or just check the logs/output directory
    # But download_all_goes_files_async iterates over ALL modifiers.
    # Let's just use the low-level classes directly for precise testing
    
    import aioboto3
    from botocore import UNSIGNED
    from botocore.client import Config
    
    async with aioboto3.Session().client("s3", config=Config(signature_version=UNSIGNED)) as s3:
        downloader = AsyncFileDownloader(found_ts, goes_bucket, io_manager, s3_client=s3)
        
        # We need to pass the list of files. In the real flow, the finder gets them.
        # Let's simulate what the finder would return (which includes the file we found)
        file_list = [(found_file, found_ts)]
        
        downloaded = await downloader.async_download_matching(file_list, outdir)
        
        if downloaded and downloaded.exists():
            print(f"✓ Async download successful: {downloaded.name}")
        else:
            print("✗ Async download failed for exact timestamp")
            
        # 5. Test Async Download with DIFFERENT timestamp (should FALLBACK)
        print(f"\nTesting Async Download with DIFFERENT timestamp: {fake_ts}")
        downloader_fallback = AsyncFileDownloader(fake_ts, goes_bucket, io_manager, s3_client=s3)
        
        # Even if the file list contains the file, the downloader should use it as fallback
        downloaded_fallback = await downloader_fallback.async_download_matching(file_list, outdir)
        
        if downloaded_fallback and downloaded_fallback.exists():
            print(f"✓ Async download correctly fell back to a file: {downloaded_fallback.name}")
        else:
            print("✗ Async download failed to fallback (returned None)")

    # Cleanup
    # shutil.rmtree(outdir)

if __name__ == "__main__":
    asyncio.run(test_strict_matching())
