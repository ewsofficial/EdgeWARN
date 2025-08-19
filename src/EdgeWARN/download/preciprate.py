import datetime
from pathlib import Path
import requests
import gzip


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