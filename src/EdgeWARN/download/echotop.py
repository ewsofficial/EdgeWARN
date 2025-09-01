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