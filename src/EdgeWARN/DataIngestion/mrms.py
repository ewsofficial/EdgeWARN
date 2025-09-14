import datetime
import requests
import re
import gzip
from pathlib import Path
from datetime import timedelta

# ---------- MRMS ECHOTOP18 ----------
def download_mrms_echotop18(dt, outdir: Path, max_lookback_minutes=60):
    """
    Downloads the latest MRMS EchoTop 18 file by searching back minute-by-minute.
    """
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

# ---------- MRMS NLDN ----------
def download_latest_mrms_nldn(dt, outdir: Path, max_lookback_minutes=20):
    """
    Download latest MRMS NLDN CG 5-min AvgDensity file by searching back minute-by-minute.
    Files named like:
      MRMS_NLDN_CG_005min_AvgDensity_00.00_YYYYMMDD-HHMMSS.grib2.gz
    Unzips into 'outdir' and deletes .gz and .idx files.
    """
    base_url = "https://mrms.ncep.noaa.gov/2D/NLDN_CG_001min_AvgDensity"
    outdir.mkdir(parents=True, exist_ok=True)
    attempt_dt = dt.replace(second=0, microsecond=0)

    # Debug to check that timestamp works
    print(f"NLDN is using timestamp: {dt}")

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

# ---------- MRMS PrecipRate 10 MIN ----------
def download_mrms_preciprate(dt, outdir: Path, max_lookback_minutes=60):
    """
    Downloads the latest MRMS PrecipRate file by searching back minute-by-minute.
    Unzips the downloaded file and extracts the .grib2 file before deleting the .gz file.
    """
    base_url = "https://mrms.ncep.noaa.gov/2D/PrecipRate"
    outdir.mkdir(parents=True, exist_ok=True)
    attempt_dt = dt

    for _ in range(max_lookback_minutes + 1):
        date_str = attempt_dt.strftime("%Y%m%d")
        hour_min = attempt_dt.strftime("%H%M")
        prefix = f"MRMS_PrecipRate_00.00_{date_str}-{hour_min}"

        print(f"Looking for PrecipRate files with prefix: {prefix}")

        try:
            r = requests.get(f"{base_url}/", timeout=20)
            r.raise_for_status()
            matches = sorted(re.findall(rf"{prefix}\d{{2}}\.grib2\.gz", r.text), reverse=True)

            if matches:
                filename = matches[0]
                gz_outpath = outdir / filename
                extracted_path = outdir / filename[:-3]  # Remove '.gz'

                if extracted_path.exists():
                    print(f"PrecipRate uncompressed file already exists: {extracted_path.name}")
                    LATEST_MRMS_PRECIPRATE = str(extracted_path)
                    return extracted_path

                print(f"Downloading PrecipRate file: {filename}")
                file_req = requests.get(f"{base_url}/{filename}", stream=True, timeout=30)
                file_req.raise_for_status()
                with open(gz_outpath, "wb") as f:
                    for chunk in file_req.iter_content(chunk_size=8192):
                        f.write(chunk)
                print(f"Downloaded PrecipRate file: {filename}")

                # Decompress into outdir
                with gzip.open(gz_outpath, 'rb') as f_in:
                    with open(extracted_path, 'wb') as f_out:
                        f_out.write(f_in.read())

                print(f"Extracted PrecipRate to: {extracted_path.name}")

                # Delete .gz file
                gz_outpath.unlink()

                LATEST_MRMS_PRECIPRATE = str(extracted_path)
                return extracted_path
            else:
                print(f"No PrecipRate file for {hour_min}, trying previous minute...")
                attempt_dt -= timedelta(minutes=1)
        except Exception as e:
            print(f"Error fetching PrecipRate: {e}")
            break

    print(f"No PrecipRate file found in last {max_lookback_minutes} minutes.")
    return None

import datetime
from pathlib import Path
import requests, re
from datetime import timedelta

# ---------- MRMS ProbSevere ----------
def download_latest_mrms_probsevere(dt, outdir: Path, max_lookback_minutes=60):
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

import datetime
from pathlib import Path
import gzip
import requests

# ---------- MRMS QPE 15 MIN ----------
def download_latest_mrms_qpe15(dt, outdir: Path):
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

# ---------- MRMS VIL Density ----------
def download_mrms_vil_density(dt, outdir: Path, max_lookback_minutes=60):
    """
    Downloads the latest MRMS VIL Density file by searching back minute-by-minute.
    Unzips the downloaded file and extracts the .grib2 file before deleting the .gz file.
    """
    base_url = "https://mrms.ncep.noaa.gov/2D/VIL_Density"
    outdir.mkdir(parents=True, exist_ok=True)
    attempt_dt = dt

    for _ in range(max_lookback_minutes + 1):
        date_str = attempt_dt.strftime("%Y%m%d")
        hour_min = attempt_dt.strftime("%H%M")
        prefix = f"MRMS_VIL_Density_00.50_{date_str}-{hour_min}"

        print(f"Looking for VIL Density files with prefix: {prefix}")

        try:
            r = requests.get(f"{base_url}/", timeout=20)
            r.raise_for_status()
            matches = sorted(re.findall(rf"{prefix}\d{{2}}\.grib2\.gz", r.text), reverse=True)

            if matches:
                filename = matches[0]
                gz_outpath = outdir / filename
                extracted_path = outdir / filename[:-3]  # Remove '.gz'

                if extracted_path.exists():
                    print(f"VIL Density uncompressed file already exists: {extracted_path.name}")
                    LATEST_MRMS_VIL_DENSITY = str(extracted_path)
                    return extracted_path

                print(f"Downloading VIL Density file: {filename}")
                file_req = requests.get(f"{base_url}/{filename}", stream=True, timeout=30)
                file_req.raise_for_status()
                with open(gz_outpath, "wb") as f:
                    for chunk in file_req.iter_content(chunk_size=8192):
                        f.write(chunk)
                print(f"Downloaded VIL Density file: {filename}")

                # Decompress into outdir
                with gzip.open(gz_outpath, 'rb') as f_in:
                    with open(extracted_path, 'wb') as f_out:
                        f_out.write(f_in.read())

                print(f"Extracted VIL Density to: {extracted_path.name}")

                # Delete .gz file
                gz_outpath.unlink()

                LATEST_MRMS_VIL_DENSITY = str(extracted_path)
                return extracted_path
            else:
                print(f"No VIL Density file for {hour_min}, trying previous minute...")
                attempt_dt -= timedelta(minutes=1)
        except Exception as e:
            print(f"Error fetching VIL Density: {e}")
            break

    print(f"No VIL Density file found in last {max_lookback_minutes} minutes.")
    return None

# ---------- MRMS FLASH FLOOD GUIDANCE (FFG) ----------
def download_mrms_ffg(dt, outdir: Path, max_lookback_minutes=60):
    """
    Downloads the latest MRMS Flash Flood Guidance (FFG) file by searching back minute-by-minute.
    Unzips the downloaded file and extracts the .grib2 file before deleting the .gz file.
    """
    base_url = "https://mrms.ncep.noaa.gov/2D/FLASH/QPE_FFG01H"
    outdir.mkdir(parents=True, exist_ok=True)
    attempt_dt = dt

    for _ in range(max_lookback_minutes + 1):
        date_str = attempt_dt.strftime("%Y%m%d")
        hour_min = attempt_dt.strftime("%H%M")
        prefix = f"MRMS_FLASH_QPE_FFG01H_00.00_{date_str}-{hour_min}"

        print(f"Looking for FFG files with prefix: {prefix}")

        try:
            r = requests.get(f"{base_url}/", timeout=20)
            r.raise_for_status()
            matches = sorted(re.findall(rf"{prefix}\d{{2}}\.grib2\.gz", r.text), reverse=True)

            if matches:
                filename = matches[0]
                gz_outpath = outdir / filename
                extracted_path = outdir / filename[:-3]  # Remove '.gz'

                if extracted_path.exists():
                    print(f"FFG uncompressed file already exists: {extracted_path.name}")
                    LATEST_MRMS_FFG = str(extracted_path)
                    return extracted_path

                print(f"Downloading FFG file: {filename}")
                file_req = requests.get(f"{base_url}/{filename}", stream=True, timeout=30)
                file_req.raise_for_status()
                with open(gz_outpath, "wb") as f:
                    for chunk in file_req.iter_content(chunk_size=8192):
                        f.write(chunk)
                print(f"Downloaded FFG file: {filename}")

                # Decompress into outdir
                with gzip.open(gz_outpath, 'rb') as f_in:
                    with open(extracted_path, 'wb') as f_out:
                        f_out.write(f_in.read())

                print(f"Extracted FFG to: {extracted_path.name}")

                # Delete .gz file
                gz_outpath.unlink()

                LATEST_MRMS_FFG = str(extracted_path)
                return extracted_path
            else:
                print(f"No FFG file for {hour_min}, trying previous minute...")
                attempt_dt -= timedelta(minutes=1)
        except Exception as e:
            print(f"Error fetching FFG: {e}")
            break

    print(f"No FFG file found in last {max_lookback_minutes} minutes.")
    return None