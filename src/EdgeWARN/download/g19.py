import s3fs
import datetime
import re
from pathlib import Path
from datetime import timezone

GOES_GLM_DIR = Path(r"C:\input_data\goes_glm")

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