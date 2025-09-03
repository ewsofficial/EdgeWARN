import datetime
import time
from pathlib import Path

from . import nexrad as nr
from . import mosaic as ms
from . import g19
from . import nldn
from . import echotop as et
from . import qpe
from . import preciprate
from . import probsevere
from . import rtma
from util import file as fs
from EdgeWARN.detection.tools import timestamp
import os

# ---------- CREDITS ----------
def attribution():
    print("EdgeWARN Data Ingestion")
    print("Build: 2025-08-31")
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
    clean_old_files(fs.NEXRAD_L2_DIR)
    clean_old_files(fs.GOES_GLM_DIR)
    clean_old_files(fs.MRMS_LTNG_DIR)
    clean_old_files(fs.MRMS_NLDN_DIR)
    clean_old_files(fs.MRMS_ECHOTOP18_DIR)
    clean_old_files(fs.MRMS_QPE15_DIR)
    clean_old_files(fs.MRMS_PRECIPRATE_DIR)
    clean_old_files(fs.MRMS_MESH_DIR)
    clean_old_files(fs.MRMS_PROBSEVERE_DIR)
    clean_old_files(fs.MRMS_COMBINED_DIR)
    clean_old_files(fs.THREDDS_RTMA_DIR)
    clean_old_files(fs.MRMS_RADAR_DIR)
    wipe_temp()

    # Download latest NEXRAD Level II
    nr.download_nexrad_l2(site="KOKX")

    # Download latest MRMS Reflectivity Mosaic
    ms.download_mrms_composite_reflectivity(outdir=fs.MRMS_RADAR_DIR, tempdir=fs.TEMP_DIR)
    ms.find_and_concat_refl()

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
        ts_str = timestamp.extract_timestamp_from_filename(str(refl_file))
        try:
            target_time = datetime.datetime.fromisoformat(ts_str)
        except Exception:
            print(f"Could not parse timestamp '{ts_str}', using current UTC time.")
            target_time = datetime.datetime.utcnow()

    # Download latest GOES GLM
    g19.download_latest_goes_glm()

    # Download MRMS files with fallback logic, using the reflectivity timestamp
    nldn.download_latest_mrms_nldn(target_time, fs.MRMS_NLDN_DIR)
    et.download_mrms_echotop18(target_time, fs.MRMS_ECHOTOP18_DIR)
    qpe.download_latest_mrms_qpe15(target_time, fs.MRMS_QPE15_DIR)
    preciprate.download_latest_mrms_preciprate_10min(target_time, fs.MRMS_PRECIPRATE_DIR)
    probsevere.download_latest_mrms_probsevere_flexible(target_time, fs.MRMS_PROBSEVERE_DIR)

    # Download THREDDS RTMA
    rtma.download_latest_rtma(target_time, fs.THREDDS_RTMA_DIR)

# Only run this for testing purposes
# """
if __name__ == "__main__":
    main()
# """