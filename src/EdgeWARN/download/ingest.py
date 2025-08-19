import os
import platform
import datetime
import boto3
import botocore
import requests
import s3fs
import re
import gzip
import shutil
import pyproj
import sys
from bs4 import BeautifulSoup
from scipy.spatial import cKDTree
from pathlib import Path
from datetime import timezone, timedelta
from netCDF4 import Dataset
import numpy as np
import xarray as xr
import time
import warnings
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

GOES_GLM_DIR = Path(r"C:\input_data\goes_glm")
"""
To Do: Offload Dataset Merges
 - MRMS Reflectivity merging (~3 min)
 - NLDN + LTNG merging (~15 sec)
Add safety checks
 - Fallback ingestion
 - Already exists check
"""

# ---------- SYBAU WARNINGS ----------
warnings.filterwarnings("ignore") # <- Translation: Shut the fuck up and don't give me "ECCODES ERROR" ahh messages for 185 lines

# ---------- RADAR SITE CONFIG ----------

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

# RTMA Downloading
def download_latest_rtma(dt: datetime.datetime, outdir: Path):
    global LATEST_RTMA_FILE
    outdir.mkdir(parents=True, exist_ok=True)

    base_url = "https://thredds.ucar.edu/thredds/fileServer/grib/NCEP/RTMA/CONUS_2p5km"
    filename_template = "RTMA_CONUS_2p5km_{date}_{hour}00.grib2"

    for hour_offset in range(2):  # current hour, fallback 1 hour earlier
        attempt_dt = dt - datetime.timedelta(hours=hour_offset)
        date_str = attempt_dt.strftime("%Y%m%d")
        hour_str = attempt_dt.strftime("%H")
        filename = filename_template.format(date=date_str, hour=hour_str)
        outpath = outdir / filename

        if outpath.exists():
            print(f"[RTMA] Already downloaded: {filename}")
            LATEST_RTMA_FILE = str(outpath)
            return outpath

        print(f"[RTMA] Attempting download: {filename}")
        try:
            r = requests.get(f"{base_url}/{filename}", stream=True, timeout=30)
            r.raise_for_status()
            with open(outpath, "wb") as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
            print(f"[RTMA] Downloaded: {filename}")
            LATEST_RTMA_FILE = str(outpath)
            return outpath
        except Exception as e:
            print(f"[RTMA] Failed to download {filename}: {e}")

    print("[RTMA] Could not find any valid file within fallback window.")
    return None

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