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

    finder = FileFinder(dt, base_dir, max_time, max_entries)
    downloader = FileDownloader(dt)

    try:
        files_with_timestamps = finder.lookup_files(modifier)
        if not files_with_timestamps:
            print(f"[DataIngestion] ERROR: No files found for {modifier}")
            return

        print(f"[DataIngestion] DEBUG: Found {len(files_with_timestamps)} candidate files for {modifier}")

        downloaded = downloader.download_latest(files_with_timestamps, outdir)
        if downloaded:
            print(outdir)
            print(f"[DataIngestion] DEBUG: Downloaded latest {modifier} file to {downloaded}")
            print(f"[DataIngestion] DEBUG: Attempting to decompress {downloaded}")
            downloader.decompress_file(downloaded)
        else:
            print(f"[DataIngestion] ERROR: Failed to download latest {modifier} file")

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
    last_processed = None  # Keep track of the last downloaded timestamp

    while True:
        now = datetime.datetime.now(datetime.timezone.utc)
        print(f"\n[Scheduler] Current time: {now}")

        # Determine latest common timestamp in the last hour
        latest_common = checker.latest_common_minute_1h(check_modifiers)

        if latest_common:
            if latest_common != last_processed:
                print(f"[Scheduler] ✅ New latest common timestamp found: {latest_common}")
                dt = latest_common
                download_all_files(dt)
                last_processed = latest_common  # Update the last processed timestamp
            else:
                print(f"[Scheduler] ⏸ Latest common timestamp {latest_common} already processed. Waiting ...")
        else:
            print("[Scheduler] ⚠️ No common timestamp in last hour. Waiting ...")

        time.sleep(10)
