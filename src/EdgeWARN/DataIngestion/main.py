import datetime
from pathlib import Path

from EdgeWARN.DataIngestion.config import base_dir, mrms_modifiers
from EdgeWARN.DataIngestion.download import FileFinder, FileDownloader
from EdgeWARN.DataIngestion.custom import MRMSDownloader, SynopticDownloader
from EdgeWARN.PreProcess.core.utils import extract_timestamp_from_filename
import util.core.file as fs

#################################
### EWS Data Ingestion Module ###
### Build Version: v1.0.0     ###
### Contributors: Yuchen Wei  ###
#################################

def main():
    # Clear Files
    folders = [fs.MRMS_3D_DIR, fs.MRMS_ECHOTOP18_DIR, fs.MRMS_FLASH_DIR, fs.MRMS_NLDN_DIR, fs.MRMS_PRECIPRATE_DIR, fs.MRMS_QPE15_DIR, fs.MRMS_ROTATIONT_DIR, fs.MRMS_VIL_DIR, fs.MRMS_PROBSEVERE_DIR]
    for f in folders:
        fs.clean_old_files(f, max_age_minutes=20)
    fs.wipe_temp()

    # Download MRMS Files
    MRMSDownloader.download_mrms_composite_reflectivity(outdir=fs.MRMS_3D_DIR, tempdir=fs.TEMP_DIR)
    MRMSDownloader.find_and_concat_refl()

    # Find the most recent MRMS reflectivity file and extract its timestamp
    refl_files = sorted(
            fs.MRMS_RADAR_DIR.glob("MRMS_MergedReflectivityQC_max_*.nc"),
            key=lambda f: f.stat().st_mtime,
            reverse=True
        )
    if not refl_files:
        print("No MRMS reflectivity files found! Using current UTC time.")
        dt = datetime.datetime.now(datetime.timezone.utc)
    else:
        refl_file = refl_files[0]
        ts_str = extract_timestamp_from_filename(str(refl_file))
        try:
            dt = datetime.datetime.fromisoformat(ts_str)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=datetime.timezone.utc)
        except Exception:
            print(f"Could not parse timestamp '{ts_str}', using current UTC time.")
            dt = datetime.datetime.now(datetime.timezone.utc)

    max_time = datetime.timedelta(hours=6)   # Look back 6 hours
    max_entries = 10                         # How many files to check per source

    for modifier, outdir in mrms_modifiers:
        print("=" * 80)
        print(f"🔍 Checking MRMS source: {modifier}")

        # Create finder and downloader for this modifier
        finder = FileFinder(dt, base_dir, max_time, max_entries)
        downloader = FileDownloader(dt)

        # Search for files
        try:
            files_with_timestamps = finder.lookup_files(modifier)
            if not files_with_timestamps:
                print(f"⚠️ No files found for {modifier}")
                continue

            print(f"Found {len(files_with_timestamps)} candidate files for {modifier}")

            # Download the latest file for this source
            downloaded = downloader.download_latest(files_with_timestamps, outdir)
            if downloaded:
                print(outdir)
                print(f"✅ Downloaded latest {modifier} file to {downloaded}")
                print(f"Attempting to decompress {downloaded}")
                downloader.decompress_file(downloaded)
            else:
                print(f"❌ Failed to download latest {modifier} file")

        except Exception as e:
            print(f"❌ Error processing {modifier}: {e}")

    SynopticDownloader.download_latest_rtma(dt, fs.THREDDS_RTMA_DIR)
    SynopticDownloader.download_rap_awp(dt, fs.NOAA_RAP_DIR)

if __name__ == "__main__":
    main()
