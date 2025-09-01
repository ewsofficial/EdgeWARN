import datetime
from pathlib import Path
import gzip
import requests

# ---------- MRMS QPE 15 MIN ----------
def download_latest_mrms_qpe15(dt, outdir: Path):
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