import datetime
from pathlib import Path
import requests

# RTMA Downloading
def download_latest_rtma(dt, outdir: Path):
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

def download_rap_awp(dt, outdir: Path):
    """
    Download RAP AWP product files (00hr forecast only)
    
    Args:
        dt: datetime object for the run time
        outdir: output directory path
    """
    outdir.mkdir(parents=True, exist_ok=True)
    
    # Construct filename and URL directly
    date_str = dt.strftime("%Y%m%d")
    hour_str = dt.strftime("%H")
    filename = f"rap.t{hour_str}z.awp130pgrbf00.grib2"
    outpath = outdir / filename
    
    # Direct URL format
    url = f"https://nomads.ncep.noaa.gov/cgi-bin/filter_rap.pl?dir=/rap/{date_str}&file={filename}"
    
    # Check if file already exists
    if outpath.exists():
        print(f"[RAP] Already downloaded: {filename}")
        return outpath
    
    print(f"[RAP] Attempting download: {filename}")
    try:
        r = requests.get(url, stream=True, timeout=30)
        r.raise_for_status()
        
        with open(outpath, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
        
        print(f"[RAP] Downloaded: {filename}")
        return outpath
        
    except Exception as e:
        print(f"[RAP] Failed to download {filename}: {e}")
        # Clean up partial download if it exists
        if outpath.exists():
            outpath.unlink()
        return None