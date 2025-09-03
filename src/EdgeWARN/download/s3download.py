import boto3
import botocore
import datetime
from datetime import timezone
from util import file as fs
from pathlib import Path
import s3fs
import re

# ---------- NEXRAD LEVEL II FROM S3 ----------
RADAR_SITE = "KOKX" # Default. Find the radar site closest to your area of interest.
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