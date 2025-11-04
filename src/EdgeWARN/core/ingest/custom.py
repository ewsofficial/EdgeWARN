from pathlib import Path
import requests
import datetime

class SynopticDownloader:
    @staticmethod
    def download_latest_rtma(dt, outdir: Path):
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
                print(f"[DataIngestion] DEBUG: Already downloaded RTMA file: {filename}")
                LATEST_RTMA_FILE = str(outpath)
                return outpath

            print(f"[DataIngestion] DEBUG: Attempting RTMA download: {filename}")
            try:
                r = requests.get(f"{base_url}/{filename}", stream=True, timeout=30)
                r.raise_for_status()
                with open(outpath, "wb") as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        f.write(chunk)
                print(f"[DataIngestion] DEBUG: Downloaded RTMA: {filename}")
                LATEST_RTMA_FILE = str(outpath)
                return outpath
            except Exception as e:
                print(f"[DataIngestion] ERROR: Failed to download RTMA {filename}: {e}")

        print("[DataIngestion] ERROR: Could not find any valid RTMA file within fallback window.")
        return None
    
    @staticmethod
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

        # URL is: https://nomads.ncep.noaa.gov/pub/data/nccf/com/rap/prod/rap.{YYYYMMDD}/rap.t{HH}z.awp130pgrbf00.grib2
        
        # Direct URL format
        url = f"https://nomads.ncep.noaa.gov/pub/data/nccf/com/rap/prod/rap.{date_str}/{filename}"
        
        # Check if file already exists
        if outpath.exists():
            print(f"[DataIngestion] DEBUG: Already downloaded RAP: {filename}")
            return outpath
        
        print(f"[DataIngestion] DEBUG: Attempting RAP download: {filename}")
        try:
            r = requests.get(url, stream=True, timeout=30)
            r.raise_for_status()
            
            with open(outpath, "wb") as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
            
            print(f"[DataIngestion] DEBUG: Downloaded RAP: {filename}")
            return outpath
            
        except Exception as e:
            print(f"[DataIngestion] ERROR: Failed to download RAP {filename}: {e}")
            # Clean up partial download if it exists
            if outpath.exists():
                outpath.unlink()
            return None