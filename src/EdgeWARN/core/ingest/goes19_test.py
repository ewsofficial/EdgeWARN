"""
GOES-19 Download Test Script

This script demonstrates how to use the modular GOES download functions
from EdgeWARN.core.ingest.main to download GOES-19 products.

You can download a single product or all configured GOES products.
"""

from datetime import datetime, timezone, timedelta
from pathlib import Path
from EdgeWARN.core.ingest.main import download_goes_product, download_all_goes_files
import util.file as fs

def test_single_product():
    """Download a single GOES-19 product (GLM-L2-LCFA)"""
    print("Testing single GOES product download...")
    
    dt = datetime.now(timezone.utc)
    
    result = download_goes_product(
        product="GLM-L2-LCFA",
        outdir=fs.GOES_GLM_DIR,
        dt=dt,
        max_time=timedelta(hours=1),
        max_entries=10,
        hour_lookback=3
    )
    
    if result:
        print(f"✓ Downloaded: {result}")
    else:
        print("✗ Download failed")
    
    return result

def test_all_products():
    """Download all configured GOES-19 products"""
    print("Testing all GOES products download...")
    
    dt = datetime.now(timezone.utc)
    
    download_all_goes_files(
        dt=dt,
        max_time=timedelta(hours=1),
        max_entries=10,
        hour_lookback=3
    )
    
    print("✓ All GOES downloads completed")

if __name__ == "__main__":
    # Test single product download
    test_single_product()
    
    # Uncomment to test all products:
    # test_all_products()