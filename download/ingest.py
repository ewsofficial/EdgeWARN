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
sys.path.append(str(Path(__file__).resolve().parent.parent))
import util.file as fs

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
RADAR_SITE = "KOKX"  # Default radar site (Upton, NY - Services NYC, LoHud, Long Island, CT)
CURRENT_TIME = datetime.datetime
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

# ---------- NEXRAD LEVEL II FROM S3 ----------
def download_nexrad_l2(site=RADAR_SITE):
    global LATEST_NEXRAD_L2
    client = boto3.client("s3", config=botocore.client.Config(signature_version=botocore.UNSIGNED))
    bucket = "noaa-nexrad-level2"
    now = datetime.datetime.now(timezone.utc)
    prefix = f"{now:%Y/%m/%d}/{site}/"
    outdir = fs.NEXRAD_L2_DIR
    outdir.mkdir(parents=True, exist_ok=True)

    try:
        response = client.list_objects_v2(Bucket=bucket, Prefix=prefix)
        files = response.get('Contents', [])
        if not files:
            print(f"No files found in s3://{bucket}/{prefix}")
            return

        files_sorted = sorted(files, key=lambda x: x['LastModified'], reverse=True)
        for obj in files_sorted:
            key = obj['Key']
            filename = key.split("/")[-1]

            # Skip files ending with 'MDM' (metadata files)
            if filename.endswith('MDM'):
                print(f"Skipping MDM file: {filename}")
                continue

            outpath = outdir / filename
            if not outpath.exists():
                client.download_file(bucket, key, str(outpath))
                print(f"Downloaded Level II: {filename} (LastModified: {obj['LastModified']})")
                LATEST_NEXRAD_L2 = str(outpath)
                return
            else:
                print(f"Latest Level II already present: {filename}")
                LATEST_NEXRAD_L2 = str(outpath)
                return
        print("No new NEXRAD Level II files to download.")
    except Exception as e:
        print(f"Failed to download NEXRAD Level II: {e}")

# ---------- MRMS MERGED REFLECTIVITY QC ----------
def download_mrms_composite_reflectivity(outdir: Path, tempdir: Path,
                                          sweep_heights=None,
                                          base_dir_url=None) -> Path | None:
    """
    Downloads the latest MRMS Merged Reflectivity QC 3D data, decompresses.
    Falls back to the most recent timestamp where ALL sweeps are available.
    """
    if sweep_heights is None:
        sweep_heights = [
            "00.50", "00.75", "01.00", "01.25", "01.50", "02.00",
            "02.50", "03.00", "03.50", "04.00", "04.50", "05.00", "05.50",
            "06.00", "06.50", "07.00", "07.50", "08.00", "08.50", "09.00", "10.00",
            "11.00", "12.00", "13.00", "14.00", "15.00"
        ]

    if base_dir_url is None:
        base_dir_url = "https://mrms.ncep.noaa.gov/3DRefl/MergedReflectivityQC_00.50/"

    # Clean tempdir
    if tempdir.exists():
        shutil.rmtree(tempdir)
    tempdir.mkdir(parents=True, exist_ok=True)
    outdir.mkdir(parents=True, exist_ok=True)

    print("🔍 Fetching available timestamps...")
    try:
        r = requests.get(base_dir_url, timeout=30)
        r.raise_for_status()
    except Exception as e:
        print(f"❌ Failed to get directory listing: {e}")
        return None

    soup = BeautifulSoup(r.text, "html.parser")
    hrefs = [a.get("href") for a in soup.find_all("a", href=True)]
    pattern = re.compile(r"MRMS_MergedReflectivityQC_00\.50_(\d{8}-\d{6})\.grib2\.gz")
    timestamps = sorted(
        {m.group(1) for href in hrefs if (m := pattern.match(href))},
        reverse=True
    )

    if not timestamps:
        print("❌ No timestamped GRIB2 files found.")
        return None

    def build_url(height: str, timestamp: str) -> str:
        return (
            f"https://mrms.ncep.noaa.gov/3DRefl/MergedReflectivityQC_{height}/"
            f"MRMS_MergedReflectivityQC_{height}_{timestamp}.grib2.gz"
        )

    # Find latest timestamp where all sweeps exist
    valid_timestamp = None
    for ts in timestamps:
        all_exist = True
        for height in sweep_heights:
            url = build_url(height, ts)
            try:
                head_r = requests.head(url, timeout=10)
                if head_r.status_code != 200:
                    all_exist = False
                    break
            except Exception:
                all_exist = False
                break
        if all_exist:
            valid_timestamp = ts
            break

    if not valid_timestamp:
        print("❌ No timestamp found with all sweep levels.")
        return None

    print(f"🕒 Using timestamp {valid_timestamp}")

    downloaded = {}
    for height in sweep_heights:
        url = build_url(height, valid_timestamp)
        gz_path = tempdir / f"MRMS_MergedReflectivityQC_{height}_{valid_timestamp}.grib2.gz"

        if gz_path.exists():
            print("[MRMS Refl] Terminating download. Reason: File already exists")
            return None
        try:
            print(f"⬇️  Downloading {url}")
            r = requests.get(url, timeout=30)
            if r.status_code != 200:
                print(f"⚠️  Failed to download {height}: HTTP {r.status_code}")
                continue
            with open(gz_path, "wb") as f:
                f.write(r.content)
            downloaded[height] = gz_path
        except Exception as e:
            print(f"❌ Download error {height}: {e}")

    if len(downloaded) != len(sweep_heights):
        print("❌ Missing files even for validated timestamp.")
        return None

    # Decompress .gz files
    grib_paths = {}
    for height, gz_path in downloaded.items():
        grib_path = tempdir / gz_path.name.replace(".gz", "")
        try:
            with gzip.open(gz_path, "rb") as f_in, open(grib_path, "wb") as f_out:
                shutil.copyfileobj(f_in, f_out)
            os.remove(gz_path)
            grib_paths[height] = grib_path
        except Exception as e:
            print(f"❌ Error decompressing {height}: {e}")

    if not grib_paths:
        print("❌ No GRIB files decompressed.")
        return None
    
def find_all_refl_files():
    """
    Find all files in fs.TEMP_DIR that contain any of the MRMS sweep elevations
    in their filename. Ignores .idx files. Returns a list of POSIX-style strings.
    """
    sweep_heights = [
        "00.50", "00.75", "01.00", "01.25", "01.50", "02.00",
        "02.50", "03.00", "03.50", "04.00", "04.50", "05.00", "05.50",
        "06.00", "06.50", "07.00", "07.50", "08.00", "08.50", "09.00", "10.00",
        "11.00", "12.00", "13.00", "14.00", "15.00"
    ]

    matching_files = []

    for sweep in sweep_heights:
        for f in fs.TEMP_DIR.glob(f"*_{sweep}_*"):
            if f.is_file() and not f.name.endswith(".idx"):
                # Convert to POSIX string
                matching_files.append(f.as_posix())

    if not matching_files:
        print("❌ No sweep files found.")
        return None

    # Optional: sort alphabetically
    matching_files = sorted(matching_files)

    print(f"Found {len(matching_files)} sweep files:")
    for f in matching_files:
        print(f)

    return matching_files

def concat_refl(files):
    """
    Merge multiple NetCDF sweep files into a single NetCDF file containing
    the maximum reflectivity at each grid point across all sweeps.
    Uses lazy evaluation with dask to handle large datasets.
    """

    if not files:
        raise ValueError("No files provided.")

    # Extract timestamp from the first file
    first_file = os.path.basename(files[0])
    match = re.search(r"\d{8}-\d{6}", first_file)
    if not match:
        raise ValueError(f"Could not find YYYYMMDD-HHMMSS in filename: {first_file}")
    timestamp = match.group(0)
    output_path = r"C:\input_data\nexrad_merged\MRMS_MergedReflectivityQC_max_{timestamp}.nc"

    # Open all files lazily with xarray
    datasets = [xr.open_dataset(f, chunks={'x': 500, 'y': 500}) for f in files]

    # Select the reflectivity variable from each
    refl_vars = [ds['unknown'] for ds in datasets]

    # Compute the element-wise maximum across all sweeps lazily
    max_reflectivity = xr.concat(refl_vars, dim='sweep').max(dim='sweep')

    # Copy coordinate variables and attributes from the first file
    max_reflectivity.attrs.update(datasets[0]['unknown'].attrs)

    # Save to NetCDF (computed lazily in chunks)
    max_reflectivity.to_netcdf(output_path, format='NETCDF4')

    # Close all datasets
    for ds in datasets:
        ds.close()

    print(f"Maximum reflectivity (lazy) file saved to {output_path}")


def find_and_concat_refl():
    files = find_all_refl_files()
    if not files:
        print("No files found!")
        return
    concat_refl(files)

# ---------- GOES GLM FROM S3 ----------
def download_latest_goes_glm(bucket_name="noaa-goes19"):
    global LATEST_GOES_GLM
    fs = s3fs.S3FileSystem(anon=True)
    now = datetime.datetime.now(timezone.utc)
    year = now.strftime("%Y")
    doy = now.strftime("%j")
    hour = now.strftime("%H")
    prefix = f"GLM-L2-LCFA/{year}/{doy}/{hour}/"
    outdir = GOES_GLM_DIR
    outdir.mkdir(parents=True, exist_ok=True)

    try:
        files = fs.ls(f"{bucket_name}/{prefix}")
        if not files:
            print(f"No GOES GLM files found in s3://{bucket_name}/{prefix}")
            return

        def extract_time(filename):
            match = re.search(r"s(\d{13})", filename)
            if match:
                ts_str = match.group(1)
                dt = datetime.datetime.strptime(ts_str[:7], "%Y%j")
                time_str = ts_str[7:]
                dt = dt.replace(hour=int(time_str[:2]), minute=int(time_str[2:4]), second=int(time_str[4:6]))
                return dt
            return datetime.datetime.min

        files_sorted = sorted(files, key=lambda f: extract_time(f.split("/")[-1]), reverse=True)
        latest_file = files_sorted[0].split("/")[-1]
        local_path = outdir / latest_file

        if local_path.exists():
            print(f"GOES GLM already exists: {latest_file}")
        else:
            fs.get(files_sorted[0], str(local_path))
            print(f"Downloaded GOES GLM: {latest_file}")

        LATEST_GOES_GLM = str(local_path)
    except Exception as e:
        print(f"Error downloading GOES GLM: {e}")

# ---------- MRMS NLDN ----------
def download_latest_mrms_nldn(dt: datetime.datetime, outdir: Path, max_lookback_minutes=20):
    """
    Download latest MRMS NLDN CG 5-min AvgDensity file by searching back minute-by-minute.
    Files named like:
      MRMS_NLDN_CG_005min_AvgDensity_00.00_YYYYMMDD-HHMMSS.grib2.gz
    Unzips into 'outdir' and deletes .gz and .idx files.
    """
    global LATEST_MRMS_NLDN
    base_url = "https://mrms.ncep.noaa.gov/2D/NLDN_CG_001min_AvgDensity"
    outdir.mkdir(parents=True, exist_ok=True)
    attempt_dt = dt.replace(second=0, microsecond=0)

    for _ in range(max_lookback_minutes + 1):
        date_str = attempt_dt.strftime("%Y%m%d")
        hhmm_str = attempt_dt.strftime("%H%M")
        prefix = f"MRMS_NLDN_CG_001min_AvgDensity_00.00_{date_str}-{hhmm_str}"

        print(f"Searching for NLDN files with prefix: {prefix}*")

        try:
            r = requests.get(base_url + "/", timeout=20)
            r.raise_for_status()
            html_text = r.text

            pattern = re.compile(rf"{re.escape(prefix)}\d{{2}}\.grib2\.gz")
            matches = pattern.findall(html_text)
            matches = sorted(matches, reverse=True)

            if matches:
                filename = matches[0]
                file_url = f"{base_url}/{filename}"
                gz_outpath = outdir / filename

                # Path for the uncompressed .grib2 file inside outdir
                extracted_path = outdir / filename[:-3]  # strip ".gz"

                if extracted_path.exists():
                    print(f"NLDN uncompressed file already exists: {extracted_path.name}")
                    LATEST_MRMS_NLDN = str(extracted_path)
                    return extracted_path

                print(f"Downloading NLDN file: {filename}")
                file_r = requests.get(file_url, stream=True, timeout=30)
                file_r.raise_for_status()
                with open(gz_outpath, "wb") as f:
                    for chunk in file_r.iter_content(chunk_size=8192):
                        f.write(chunk)

                print(f"Downloaded NLDN file: {filename}")

                # Decompress the .gz file
                with gzip.open(gz_outpath, 'rb') as f_in:
                    with open(extracted_path, 'wb') as f_out:
                        f_out.write(f_in.read())

                print(f"Extracted to: {extracted_path}")

                # Delete the original .gz file
                gz_outpath.unlink()

                # Delete the associated .idx file if it exists
                idx_file = outdir / filename[:-3].replace(".grib2", ".idx")
                if idx_file.exists():
                    try:
                        idx_file.unlink()
                        print(f"Deleted .idx file: {idx_file.name}")
                    except Exception as e:
                        print(f"Could not delete .idx file: {e}")

                LATEST_MRMS_NLDN = str(extracted_path)
                return extracted_path
            else:
                print(f"No matching NLDN file for {prefix}*, trying previous minute...")
                attempt_dt -= timedelta(minutes=1)
        except Exception as e:
            print(f"Error fetching/downloading NLDN files: {e}")
            break

    print(f"No NLDN file found in last {max_lookback_minutes} minutes.")
    return None

# ---------- MRMS ECHOTOP18 ----------
def download_mrms_echotop18(dt: datetime.datetime, outdir: Path, max_lookback_minutes=60):
    """
    Downloads the latest MRMS EchoTop 18 file by searching back minute-by-minute.
    """
    global LATEST_MRMS_ECHOTOP18
    base_url = "https://mrms.ncep.noaa.gov/2D/EchoTop_18"
    outdir.mkdir(parents=True, exist_ok=True)
    attempt_dt = dt

    for _ in range(max_lookback_minutes + 1):
        date_str = attempt_dt.strftime("%Y%m%d")
        hour_min = attempt_dt.strftime("%H%M")
        prefix = f"MRMS_EchoTop_18_00.50_{date_str}-{hour_min}"

        print(f"Looking for EchoTop18 files with prefix: {prefix}")

        try:
            r = requests.get(f"{base_url}/", timeout=20)
            r.raise_for_status()
            matches = sorted(re.findall(rf"{prefix}\d{{2}}\.grib2\.gz", r.text), reverse=True)

            if matches:
                filename = matches[0]
                gz_outpath = outdir / filename
                extracted_path = outdir / filename[:-3]  # Remove '.gz'

                if extracted_path.exists():
                    print(f"EchoTop18 uncompressed file already exists: {extracted_path.name}")
                    LATEST_MRMS_ECHOTOP18 = str(extracted_path)
                    return extracted_path

                print(f"Downloading EchoTop18 file: {filename}")
                file_req = requests.get(f"{base_url}/{filename}", stream=True, timeout=30)
                file_req.raise_for_status()
                with open(gz_outpath, "wb") as f:
                    for chunk in file_req.iter_content(chunk_size=8192):
                        f.write(chunk)
                print(f"Downloaded EchoTop18 file: {filename}")

                # Decompress into outdir
                with gzip.open(gz_outpath, 'rb') as f_in:
                    with open(extracted_path, 'wb') as f_out:
                        f_out.write(f_in.read())

                print(f"Extracted EchoTop18 to: {extracted_path.name}")

                # Delete .gz file
                gz_outpath.unlink()

                LATEST_MRMS_ECHOTOP18 = str(extracted_path)
                return extracted_path
            else:
                print(f"No EchoTop18 file for {hour_min}, trying previous minute...")
                attempt_dt -= timedelta(minutes=1)
        except Exception as e:
            print(f"Error fetching EchoTop18: {e}")
            break

    print(f"No EchoTop18 file found in last {max_lookback_minutes} minutes.")
    return None

# ---------- MRMS QPE 15 MIN ----------
def download_latest_mrms_qpe15(dt: datetime.datetime, outdir: Path):
    global LATEST_MRMS_QPE15
    outdir.mkdir(parents=True, exist_ok=True)
    base_url = "https://mrms.ncep.noaa.gov/2D/RadarOnly_QPE_15M"

    rounded_minute = (dt.minute // 15) * 15
    snapped_dt = dt.replace(minute=rounded_minute, second=0, microsecond=0)
    file_ts = snapped_dt.strftime("%Y%m%d-%H%M")
    filename = f"MRMS_RadarOnly_QPE_15M_00.00_{file_ts}00.grib2.gz"
    gz_outpath = outdir / filename
    extracted_path = outdir / filename[:-3]  # remove ".gz"

    if not extracted_path.exists():
        if not gz_outpath.exists():
            print(f"[MRMS QPE15] Attempting download: {filename}")
            try:
                r = requests.get(f"{base_url}/{filename}", stream=True, timeout=30)
                r.raise_for_status()
                with open(gz_outpath, "wb") as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        f.write(chunk)
                print(f"[MRMS QPE15] Downloaded: {filename}")
            except Exception as e:
                print(f"[MRMS QPE15] Failed to download: {e}")
                return None
        else:
            print(f"[MRMS QPE15] Found existing gz file: {filename}")

        # Decompress the .gz file into outdir
        try:
            with gzip.open(gz_outpath, 'rb') as f_in:
                with open(extracted_path, 'wb') as f_out:
                    f_out.write(f_in.read())
            print(f"[MRMS QPE15] Extracted to: {extracted_path.name}")

            # Delete the original .gz file
            gz_outpath.unlink()
        except Exception as e:
            print(f"[MRMS QPE15] Extraction failed: {e}")
            return None
    else:
        print(f"[MRMS QPE15] Uncompressed file already exists: {extracted_path.name}")

    LATEST_MRMS_QPE15 = str(extracted_path)
    return extracted_path

# ---------- MRMS PrecipRate 10 MIN ----------
def download_latest_mrms_preciprate_10min(dt: datetime.datetime, outdir: Path):
    global LATEST_MRMS_PRECIPRATE
    outdir.mkdir(parents=True, exist_ok=True)
    base_url = "https://mrms.ncep.noaa.gov/2D/PrecipRate"

    rounded_minute = (dt.minute // 10) * 10
    snapped_dt = dt.replace(minute=rounded_minute, second=0, microsecond=0)
    file_ts = snapped_dt.strftime("%Y%m%d-%H%M")
    filename = f"MRMS_PrecipRate_00.00_{file_ts}00.grib2.gz"
    gz_outpath = outdir / filename

    # Define where unzipped file goes — parent directory of outdir
    extracted_path = outdir / filename[:-3]  # remove ".gz"

    if not extracted_path.exists():
        if not gz_outpath.exists():
            print(f"[MRMS PrecipRate] Attempting download: {filename}")
            try:
                r = requests.get(f"{base_url}/{filename}", stream=True, timeout=30)
                r.raise_for_status()
                with open(gz_outpath, "wb") as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        f.write(chunk)
                print(f"[MRMS PrecipRate] Downloaded: {filename}")
            except Exception as e:
                print(f"[MRMS PrecipRate] Failed to download: {e}")
                return None
        else:
            print(f"[MRMS PrecipRate] Found existing gz file: {filename}")

        # Decompress the gz file into parent directory
        try:
            with gzip.open(gz_outpath, 'rb') as f_in:
                with open(extracted_path, 'wb') as f_out:
                    f_out.write(f_in.read())
            print(f"[MRMS PrecipRate] Extracted to: {extracted_path.name}")

            # Delete the original .gz file
            gz_outpath.unlink()
        except Exception as e:
            print(f"[MRMS PrecipRate] Extraction failed: {e}")
            return None
    else:
        print(f"[MRMS PrecipRate] Uncompressed file already exists: {extracted_path.name}")

    LATEST_MRMS_PRECIPRATE = str(extracted_path)
    return extracted_path

# ---------- MRMS ProbSevere ----------
def download_latest_mrms_probsevere_flexible(dt: datetime.datetime, outdir: Path, max_lookback_minutes=60):
    outdir.mkdir(parents=True, exist_ok=True)
    base_url = "https://mrms.ncep.noaa.gov/ProbSevere/PROBSEVERE/"
    attempt_dt = dt.replace(second=0, microsecond=0)

    for _ in range((max_lookback_minutes // 10) + 1):
        rounded_minute = (attempt_dt.minute // 10) * 10
        attempt_dt_rounded = attempt_dt.replace(minute=rounded_minute)
        date_str = attempt_dt_rounded.strftime("%Y%m%d")
        hhmm_str = attempt_dt_rounded.strftime("%H%M")

        print(f"[ProbSevere] Searching for datetime: {date_str} {hhmm_str}xx")
        try:
            r = requests.get(base_url, timeout=20)
            r.raise_for_status()
            matches = sorted(re.findall(rf"MRMS_PROBSEVERE_{date_str}_{hhmm_str}\d{{2}}\.json", r.text))
            if matches:
                filename = matches[-1]
                outpath = outdir / filename
                if not outpath.exists():
                    print(f"[ProbSevere] Downloading: {filename}")
                    file_r = requests.get(f"{base_url}{filename}", timeout=30)
                    file_r.raise_for_status()
                    with open(outpath, "wb") as f:
                        f.write(file_r.content)
                    print(f"[ProbSevere] Download complete: {filename}")
                else:
                    print(f"[ProbSevere] Already exists: {filename}")

                return outpath
            else:
                attempt_dt -= timedelta(minutes=10)
        except Exception as e:
            print(f"[ProbSevere] Error: {e}")
            break

    print("[ProbSevere] No file found in time window.")
    return None

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
    download_nexrad_l2(site="KOKX")

    # Download latest MRMS Reflectivity Mosaic
    download_mrms_composite_reflectivity(outdir=fs.MRMS_RADAR_DIR, tempdir=fs.TEMP_DIR)
    find_and_concat_refl()

    # Download latest GOES GLM
    download_latest_goes_glm()

    now_utc = datetime.datetime.utcnow()

    # Download MRMS files with fallback logic
    download_latest_mrms_nldn(now_utc, fs.MRMS_NLDN_DIR)  # <-- uses updated function with unzip
    download_mrms_echotop18(now_utc, fs.MRMS_ECHOTOP18_DIR)
    download_latest_mrms_qpe15(now_utc, fs.MRMS_QPE15_DIR)
    download_latest_mrms_preciprate_10min(now_utc, fs.MRMS_PRECIPRATE_DIR)
    download_latest_mrms_probsevere_flexible(now_utc, fs.MRMS_PROBSEVERE_DIR)

    # Download THREDDS RTMA
    download_latest_rtma(now_utc, fs.THREDDS_RTMA_DIR)

# Only run this for testing purposes
# """
if __name__ == "__main__":
    main()
# """