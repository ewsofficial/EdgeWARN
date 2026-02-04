import time
import cfgrib
import numpy as np
import sys
from pathlib import Path

# The 37 levels found
LEVELS = [
    1000, 975, 950, 925, 900, 875, 850, 825, 800, 775, 750, 725, 700, 
    675, 650, 625, 600, 575, 550, 525, 500, 475, 450, 425, 400, 375, 
    350, 325, 300, 275, 250, 225, 200, 175, 150, 125, 100
]

def benchmark_integration(filepath):
    print(f"Benchmarking integration for 37 pressure levels on {filepath}")
    
    start_time = time.time()
    
    try:
        # 1. Load Datasets (simulating integrate_rap)
        t0 = time.time()
        # We need backend_kwargs used typically? defaults seem fine for now
        all_datasets = cfgrib.open_datasets(str(filepath))
        load_time = time.time() - t0
        print(f"File Load Time: {load_time:.4f}s")
        print(f"Datasets loaded: {len(all_datasets)}")

        # 2. Find the correct dataset for isobaric layers
        # Usually one of them contains 'isobaricInhPa'
        wind_ds = None
        for ds in all_datasets:
            if 'isobaricInhPa' in ds.coords and 'u' in ds.data_vars:
                wind_ds = ds
                break
        
        if wind_ds is None:
            print("Error: Could not find dataset with isobaric winds.")
            return

        # 3. Simulate extraction for all levels
        t1 = time.time()
        extraction_count = 0
        
        # Simulate selecting U and V for each level
        # In reality, xarray is lazy, so we must access .values to force read
        for level in LEVELS:
            try:
                # U component
                u_vals = wind_ds['u'].sel(isobaricInhPa=level).values
                extraction_count += 1
                
                # V component
                v_vals = wind_ds['v'].sel(isobaricInhPa=level).values
                extraction_count += 1
                
                # Verify shape to ensure data is actually read
                if u_vals.shape != v_vals.shape:
                    print("Shape mismatch!")
                    
            except KeyError:
                # print(f"Level {level} not found in dataset")
                pass
            except Exception as e:
                print(f"Error accessing level {level}: {e}")

        process_time = time.time() - t1
        print(f"Processing Time (Extract 2x{extraction_count//2} grids): {process_time:.4f}s")
        
        total_time = time.time() - start_time
        print(f"Total Time: {total_time:.4f}s")
        print(f"Average time per level (U+V): {process_time/len(LEVELS):.4f}s")

    except Exception as e:
        print(f"Benchmark failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    default_file = "snapshot_20260204-0614/data/RAP/RAP.20260204-06z.awp130pgrbf00.grib2"
    
    if len(sys.argv) > 1:
        target_file = sys.argv[1]
    else:
        target_file = default_file
    
    if not Path(target_file).exists():
        print(f"Target file not found: {target_file}")
        sys.exit(1)
        
    benchmark_integration(target_file)
