import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

from EdgeWARN.core.ingest.config import base_dir, mrms_modifiers, check_modifiers
from EdgeWARN.core.ingest.download import FileFinder, FileDownloader
from EdgeWARN.core.schedule.scheduler import MRMSUpdateChecker
from util.io import IOManager
import util.file as fs

io_manager = IOManager("[DataIngestion]")

def process_modifier(modifier, outdir, dt, max_time, max_entries):
    
    # Ensure dt has minute precision (ignore seconds)
    dt_minute_precision = dt.replace(second=0, microsecond=0)
    
    finder = FileFinder(dt_minute_precision, base_dir, max_time, max_entries, io_manager)
    downloader = FileDownloader(dt_minute_precision, io_manager)

    try:
        files_with_timestamps = finder.lookup_files(modifier)
        if not files_with_timestamps:
            io_manager.write_warning(f"No files found for {modifier} at exact minute {dt_minute_precision}")
            return

        # Download the most recent file that matches our target minute
        downloaded = downloader.download_latest(files_with_timestamps, outdir)
        if downloaded:
            downloader.decompress_file(downloaded)
        else:
            io_manager.write_error(f"Failed to download {modifier} file")

    except Exception as e:
        io_manager.write_error(f"Failed to process {modifier} - {e}")
    
def download_all_files(dt):
    # Clear Files
    folders = [modifier[1] for modifier in mrms_modifiers]
    for f in folders:
        fs.clean_old_files(f, max_age_minutes=20)
    fs.wipe_temp()

    max_time = datetime.timedelta(hours=6)   # Look back 6 hours
    max_entries = 10                         # How many files to check per source

    # Multithread MRMS downloads
    with ThreadPoolExecutor(max_workers=len(mrms_modifiers) + 2) as executor:
        futures = [
            executor.submit(process_modifier, modifier, outdir, dt, max_time, max_entries)
            for modifier, outdir in mrms_modifiers
        ]

        for future in as_completed(futures):
            future.result()

if __name__ == "__main__":
    import time
    checker = MRMSUpdateChecker(verbose=True)
    last_processed = None

    now = datetime.datetime.now(datetime.timezone.utc)
    print(f"\n[Scheduler] Current time: {now}")

    # Determine latest common timestamp in the last hour
    latest_common = checker.latest_common_minute_1h(check_modifiers)

    if latest_common:
        # Convert to minute precision (ignore seconds)
        latest_common_minute = latest_common.replace(second=0, microsecond=0)
        
        print(f"[Scheduler] DEBUG: Latest common minute found: {latest_common_minute}")
        
        if latest_common_minute != last_processed:
            print(f"[Scheduler] DEBUG: New latest common timestamp found: {latest_common_minute}")
            
            # Verify that ALL modifiers have files at this exact minute
            all_have_files = True
            for modifier, outdir in check_modifiers:
                dt_minute_precision = latest_common_minute.replace(second=0, microsecond=0)
                finder = FileFinder(dt_minute_precision, base_dir, datetime.timedelta(hours=6), 10, io_manager)
                files = finder.lookup_files(modifier, verbose=False)
                if not files:
                    print(f"[Scheduler] WARNING: {modifier} has no files at {latest_common_minute}")
                    all_have_files = False
            
            if all_have_files:
                dt = latest_common_minute
                download_all_files(dt)
                last_processed = latest_common_minute
            else:
                print(f"[Scheduler] Not all products have files at {latest_common_minute}. Skipping...")
        else:
            print(f"[Scheduler] Latest common timestamp {latest_common_minute} already processed. Waiting ...")
    else:
        print("[Scheduler] No common timestamp in last hour. Waiting ...")

    time.sleep(10)
