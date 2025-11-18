from EdgeWARN.core.ingest.config import mrms_modifiers, bucket
from EdgeWARN.core.ingest.download import FileFinder, FileDownloader
from EdgeWARN.core.ingest.parse import MRMSBucketParser
from util.io import IOManager
import util.file as fs
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

io_manager = IOManager("[Ingest]")

def download_modifier(region, modifier, outdir, dt, max_time, max_entries):

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

def download_all_files(dt):
    # Clear Files
    folders = [outdir for _, _, outdir in mrms_modifiers]
    for f in folders:
        fs.clean_old_files(f, max_age_minutes=60)
    fs.wipe_temp()

    max_time = timedelta(hours=6)   # Look back 6 hours
    max_entries = 10                         # How many files to check per source

    # Multithread MRMS downloads
    with ThreadPoolExecutor(max_workers=len(mrms_modifiers) + 2) as executor:
        futures = [
            executor.submit(download_modifier, region, modifier, outdir, dt, max_time, max_entries)
            for region, modifier, outdir in mrms_modifiers
        ]

        for future in as_completed(futures):
            future.result()

