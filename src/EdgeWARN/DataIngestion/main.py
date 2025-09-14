import datetime
import time
from pathlib import Path

from . import s3download as s3d
from . import refl
from . import mrms
from . import synoptic
from util import file as fs
from ..PreProcess.core.data_utils import extract_timestamp_from_filename
import os

# ---------- CREDITS ----------
def attribution():
    print("EdgeWARN Data Ingestion")
    print("Build: 2025-09-09")
    print("Credits: Yuchen Wei")
    print("Made by the EWS")
    time.sleep(1)

# ---------- CLEANUP ----------
def clean_old_files(directory: Path, max_age_minutes=20):
    now = datetime.datetime.now().timestamp()
    cutoff = now - (max_age_minutes * 60)
    for f in directory.glob("*"):
        if f.is_file() and f.stat().st_mtime < cutoff:
            try:
                f.unlink()
                print(f"Deleted old file: {f.name}")
            except Exception as e:
                print(f"Could not delete {f.name}: {e}")

def wipe_temp():
    for f in fs.TEMP_DIR.glob("*"):
        try:
            f.unlink()
            print(f"Deleted temporary file: {f.name}")
        except Exception as e:
            print(f"Could not delete temporary file {f.name}: {e}")

# MAIN
def main():
    attribution()
    print("Current UTC time:", datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")), time.sleep(1)
    # Clean old files
    folders = [fs.NEXRAD_L2_DIR, fs.GOES_GLM_DIR, fs.MRMS_NLDN_DIR, fs.MRMS_ECHOTOP18_DIR, fs.MRMS_QPE15_DIR, fs.MRMS_PRECIPRATE_DIR, fs.MRMS_PROBSEVERE_DIR, fs.THREDDS_RTMA_DIR, fs.MRMS_RADAR_DIR]
    for f in folders:
        clean_old_files(f)
    wipe_temp()

    # Download latest NEXRAD Level II
    s3d.download_nexrad_l2(site="KOKX")

    # Download latest MRMS Reflectivity Mosaic
    refl.download_mrms_composite_reflectivity(outdir=fs.MRMS_RADAR_DIR, tempdir=fs.TEMP_DIR)
    refl.find_and_concat_refl()

    # Find the most recent MRMS reflectivity file and extract its timestamp
    refl_files = sorted(
            fs.MRMS_RADAR_DIR.glob("MRMS_MergedReflectivityQC_max_*.nc"),
            key=lambda f: f.stat().st_mtime,
            reverse=True
        )
    if not refl_files:
        print("No MRMS reflectivity files found! Using current UTC time.")
        target_time = datetime.datetime.utcnow()
    else:
        refl_file = refl_files[0]
        ts_str = extract_timestamp_from_filename(str(refl_file))
        try:
            target_time = datetime.datetime.fromisoformat(ts_str)
        except Exception:
            print(f"Could not parse timestamp '{ts_str}', using current UTC time.")
            target_time = datetime.datetime.utcnow()

    # Download latest GOES GLM
    s3d.download_latest_goes_glm()

    # Download MRMS files with fallback logic, using the reflectivity timestamp
    mrms.download_latest_mrms_nldn(target_time, fs.MRMS_NLDN_DIR)
    mrms.download_mrms_echotop18(target_time, fs.MRMS_ECHOTOP18_DIR)
    mrms.download_latest_mrms_qpe15(target_time, fs.MRMS_QPE15_DIR)
    mrms.download_mrms_preciprate(target_time, fs.MRMS_PRECIPRATE_DIR)
    mrms.download_latest_mrms_probsevere(target_time, fs.MRMS_PROBSEVERE_DIR)
    mrms.download_mrms_vil_density(target_time, fs.MRMS_VIL_DIR)
    mrms.download_mrms_ffg(target_time, fs.MRMS_FLASH_DIR)

    # Download Synoptic
    synoptic.download_latest_rtma(target_time, fs.THREDDS_RTMA_DIR)
    synoptic.download_rap_awp(target_time, fs.NOAA_RAP_DIR)


# Only run this for testing purposes
# """
if __name__ == "__main__":
    import time
    for i in range(6):
        print("Ingesting data!")
        main()
        print("Sleeping for 180 sec")
        time.sleep(180)
# """