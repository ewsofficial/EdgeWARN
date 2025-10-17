import datetime
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

from EdgeWARN.DataIngestion.config import base_dir, mrms_modifiers, check_modifiers
from EdgeWARN.DataIngestion.download import FileFinder, FileDownloader
from EdgeWARN.DataIngestion.custom import MRMSDownloader, SynopticDownloader
from EdgeWARN.PreProcess.CellDetection.tools.utils import DetectionDataHandler
from EdgeWARN.schedule.scheduler import MRMSUpdateChecker
import util.core.file as fs

#################################
### EWS Data Ingestion Module ###
### Build Version: v1.1.0     ###
### Contributors: Yuchen Wei  ###
#################################

def process_modifier(modifier, outdir, dt, max_time, max_entries):
    print(f"[DataIngestion] DEBUG: Checking MRMS source: {modifier}")
    
    # Ensure dt has minute precision (ignore seconds)
    dt_minute_precision = dt.replace(second=0, microsecond=0)
    
    finder = FileFinder(dt_minute_precision, base_dir, max_time, max_entries)
    downloader = FileDownloader(dt_minute_precision)

    try:
        files_with_timestamps = finder.lookup_files(modifier)
        if not files_with_timestamps:
            print(f"[DataIngestion] WARNING: No files found for {modifier} at exact minute {dt_minute_precision}")
            return

        print(f"[DataIngestion] DEBUG: Found {len(files_with_timestamps)} candidate files for {modifier} at minute {dt_minute_precision}")

        # Download the most recent file that matches our target minute
        downloaded = downloader.download_latest(files_with_timestamps, outdir)
        if downloaded:
            print(f"[DataIngestion] DEBUG: Downloaded {modifier} file to {downloaded}")
            print(f"[DataIngestion] DEBUG: Attempting to decompress {downloaded}")
            downloader.decompress_file(downloaded)
        else:
            print(f"[DataIngestion] ERROR: Failed to download {modifier} file")

    except Exception as e:
        print(f"[DataIngestion] ERROR: Failed to process {modifier}: {e}")
    
def download_all_files(dt):
    # Clear Files
    folders = [
        fs.MRMS_ECHOTOP18_DIR, fs.MRMS_FLASH_DIR, fs.MRMS_NLDN_DIR, fs.MRMS_COMPOSITE_DIR, 
        fs.MRMS_PRECIPRATE_DIR, fs.MRMS_QPE_DIR, fs.MRMS_ROTATIONT_DIR, fs.MRMS_VIL_DIR,
        fs.MRMS_PROBSEVERE_DIR, fs.MRMS_ECHOTOP30_DIR, fs.MRMS_RALA_DIR, fs.MRMS_VII_DIR
    ]
    for f in folders:
        fs.clean_old_files(f, max_age_minutes=20)
    fs.wipe_temp()

    max_time = datetime.timedelta(hours=6)   # Look back 6 hours
    max_entries = 10                         # How many files to check per source

    # Multithread MRMS downloads
    with ThreadPoolExecutor(max_workers=len(mrms_modifiers)) as executor:
        futures = [executor.submit(process_modifier, modifier, outdir, dt, max_time, max_entries)
                   for modifier, outdir in mrms_modifiers]
        for future in as_completed(futures):
            future.result()  # Will raise exceptions if any occurred

    # Download Synoptic feeds (can also be threaded if desired)
    SynopticDownloader.download_latest_rtma(dt, fs.THREDDS_RTMA_DIR)
    SynopticDownloader.download_rap_awp(dt, fs.NOAA_RAP_DIR)

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
                finder = FileFinder(dt_minute_precision, base_dir, datetime.timedelta(hours=6), 10)
                files = finder.lookup_files(modifier, verbose=False)
                if not files:
                    print(f"[Scheduler] WARNING: {modifier} has no files at {latest_common_minute}")
                    all_have_files = False
            
            if all_have_files:
                dt = latest_common_minute
                download_all_files(dt)
                last_processed = latest_common_minute
            else:
                print(f"[Scheduler] ⚠️ Not all products have files at {latest_common_minute}. Skipping...")
        else:
            print(f"[Scheduler] ⏸ Latest common timestamp {latest_common_minute} already processed. Waiting ...")
    else:
        print("[Scheduler] ⚠️ No common timestamp in last hour. Waiting ...")

    time.sleep(10)
