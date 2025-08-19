import boto3
import botocore
import datetime
from datetime import timezone
from util import file as fs

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