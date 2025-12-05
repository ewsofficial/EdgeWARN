from datetime import datetime, timedelta, timezone
from pathlib import Path
import shutil
from EdgeWARN.core.ingest.downloader import download_goes_product

def test_glm_download():
    # Use a time from a few hours ago to ensure data exists
    # GLM data is usually available for the last few hours
    # Let's try 2 hours ago
    dt = datetime.now(timezone.utc) - timedelta(hours=2)
    dt = dt.replace(second=0, microsecond=0)
    
    outdir = Path("test_glm_output")
    if outdir.exists():
        shutil.rmtree(outdir)
    
    print(f"Testing GLM download for {dt}...")
    # GLM-L2-LCFA is the product name for GLM Lightning Detection
    files = download_goes_product("GLM-L2-LCFA", outdir, dt)
    
    if files:
        print(f"Downloaded {len(files)} files:")
        for f in files:
            print(f" - {f}")
        
        if len(files) >= 1:
            print("SUCCESS: Downloaded at least one file.")
            if len(files) >= 2:
                 print(f"PERFECT: Downloaded {len(files)} files (expected multiple).")
            else:
                 print("NOTE: Only 1 file downloaded. This might be correct if only 1 file exists for this minute, but usually there are 3.")
        else:
            print("WARNING: Downloaded files list is empty but not None?")
    else:
        print("FAILURE: No files downloaded.")

    # Cleanup
    if outdir.exists():
        shutil.rmtree(outdir)

if __name__ == "__main__":
    test_glm_download()
