from datetime import datetime, timedelta, timezone
from pathlib import Path
import shutil
import sys
import xarray as xr
sys.path.append("src")
from EdgeWARN.core.ingest.downloader import download_goes_product
from util.io import IOManager

def test_glm_integration():
    # Use a time from 2 hours ago, set to 30 seconds to get multiple files
    dt = datetime.now(timezone.utc) - timedelta(hours=2)
    dt = dt.replace(second=30, microsecond=0)
    
    outdir = Path("test_glm_integration_output")
    if outdir.exists():
        shutil.rmtree(outdir)
    
    print(f"Testing GLM integration for target time: {dt}")
    
    # This should now return a single merged file
    files = download_goes_product("GLM-L2-LCFA", outdir, dt)
    
    if not files:
        print("FAILURE: No files returned.")
        if outdir.exists():
            shutil.rmtree(outdir)
        return

    print(f"Returned {len(files)} files:")
    for f in files:
        print(f" - {f.name}")
        
    if len(files) == 1 and "_merged_" in files[0].name:
        print("SUCCESS: Returned a single merged file.")
        
        # Verify the merged file
        try:
            ds = xr.open_dataset(files[0], engine="netcdf4")
            print("Merged Dataset Summary:")
            print(ds)
            ds.close()
        except Exception as e:
            print(f"FAILURE: Could not open merged file: {e}")
            
    else:
        print(f"FAILURE: Expected 1 merged file, got {len(files)}")

    # Cleanup
    if outdir.exists():
        shutil.rmtree(outdir)

if __name__ == "__main__":
    test_glm_integration()
