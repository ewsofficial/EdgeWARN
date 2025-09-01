import datetime
import requests
import re
import gzip
from pathlib import Path
from datetime import timedelta

# ---------- MRMS NLDN ----------
def download_latest_mrms_nldn(dt, outdir: Path, max_lookback_minutes=20):
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