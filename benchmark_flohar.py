import os
import sys
import tracemalloc
import time
from pathlib import Path

# Add src to python path
src_path = str(Path(__file__).parent / "src")
if src_path not in sys.path:
    sys.path.insert(0, src_path)

import util.file as fs
fs.initialize_filesystem()

from EdgeWARN.core.ctam.modules.FLOHAR.flohar_module import FLOHARModule

def run_benchmark():
    print("Starting FLOHAR standalone memory benchmark with real data...\n")
    
    tracemalloc.start()
    t0 = time.time()
    
    module = FLOHARModule()
    
    try:
        # 1. Load Grids
        print(">>> Step 1: _load_grids()")
        tracemalloc.clear_traces()
        grids = module._load_grids()
        c, p = tracemalloc.get_traced_memory()
        print(f"    _load_grids Peak: {p / 1024 / 1024:.2f} MB | Current: {c / 1024 / 1024:.2f} MB")
        
        if not grids:
            print("Failed to load grids")
            sys.exit(1)
            
        lat_coords = grids.pop("latitude")
        lon_coords = grids.pop("longitude")
            
        # 2. Compute Threat Grid
        print(">>> Step 2: compute_threat_grid()")
        tracemalloc.clear_traces()
        from EdgeWARN.core.ctam.modules.FLOHAR.engine import compute_threat_grid
        threat_grid, rainfall_grid, hydro_grid, ffg_grid = compute_threat_grid(grids)
        c, p = tracemalloc.get_traced_memory()
        print(f"    compute_threat_grid Peak: {p / 1024 / 1024:.2f} MB | Current: {c / 1024 / 1024:.2f} MB")
        
        # 3. Cleanup grids
        print(">>> Step 3: Deleting input grids")
        tracemalloc.clear_traces()
        del grids
        c, p = tracemalloc.get_traced_memory()
        print(f"    Delete grids Peak: {p / 1024 / 1024:.2f} MB | Current: {c / 1024 / 1024:.2f} MB")

        # 4. Extract Regions
        print(">>> Step 4: extract_regions()")
        tracemalloc.clear_traces()
        from EdgeWARN.core.ctam.modules.FLOHAR.regions import extract_regions
        from EdgeWARN.core.ctam.modules.FLOHAR import config as cfg
        pillar_grids = {
            "rainfall": rainfall_grid,
            "hydro": hydro_grid,
            "ffg": ffg_grid,
        }

        regions = extract_regions(
            threat_grid,
            lat_coords,
            lon_coords,
            pillar_grids,
            threshold=cfg.THREAT_THRESHOLD,
            min_area_km2=cfg.MIN_REGION_AREA_KM2,
            max_regions=cfg.MAX_REGIONS,
            simplify_tolerance=cfg.POLYGON_SIMPLIFY_TOLERANCE,
        )
        c, p = tracemalloc.get_traced_memory()
        print(f"    extract_regions Peak: {p / 1024 / 1024:.2f} MB | Current: {c / 1024 / 1024:.2f} MB")
        
    except Exception as e:
        print(f"FLOHAR failed: {e}")
        tracemalloc.stop()
        sys.exit(1)
        
    elapsed = time.time() - t0
    
    print("\n" + "=" * 60)
    print("FLOHAR Benchmark Complete")
    print(f"Total Time: {elapsed:.2f} s")
    print("=" * 60)
    
    tracemalloc.stop()

if __name__ == "__main__":
    run_benchmark()
