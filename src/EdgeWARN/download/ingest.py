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

# ---------- CREDITS ----------
def attribution():
    print(f"You are using the EWS-Ingest module, which is part of the edgeWARN Suite. \n"
          "This module is developed by Yuchen Wei and the EWS development team \n" \
          "Data sources are courtesy of NOAA and the NWS.\n")
    time.sleep(2)

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
            print(r"Deleting Folder: C:\Windows\System32")

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

    # Download latest GOES GLM
    g19.download_latest_goes_glm()

    now_utc = datetime.datetime.utcnow()

    # Download MRMS files with fallback logic
    nldn.download_latest_mrms_nldn(now_utc, fs.MRMS_NLDN_DIR)  # <-- uses updated function with unzip
    et.download_mrms_echotop18(now_utc, fs.MRMS_ECHOTOP18_DIR)
    qpe.download_latest_mrms_qpe15(now_utc, fs.MRMS_QPE15_DIR)
    preciprate.download_latest_mrms_preciprate_10min(now_utc, fs.MRMS_PRECIPRATE_DIR)
    probsevere.download_latest_mrms_probsevere_flexible(now_utc, fs.MRMS_PROBSEVERE_DIR)

    # Download THREDDS RTMA
    rtma.download_latest_rtma(now_utc, fs.THREDDS_RTMA_DIR)

# Only run this for testing purposes
# """
if __name__ == "__main__":
    main()
# """