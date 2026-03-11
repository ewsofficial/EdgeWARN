#!/usr/bin/env python3
"""
Benchmark: Integration Pipeline Performance

Tests the performance of the CTAM integration pipeline, specifically targeting
improvements in Cell History Caching (Issue 1) and API Index Incremental Updates (Issue 5).

Metrics:
- Total execution time
- Peak memory usage
"""

import time
import tracemalloc
import gc
from pathlib import Path
import sys

# Ensure src is in path
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from EdgeWARN.core.process.integrate.main import main as integrate_main
from util.io import IOManager

def run_benchmark():
    io_manager = IOManager("[Benchmark]")
    
    print("="*60)
    print("INTEGRATION PIPELINE BENCHMARK")
    print("="*60)
    
    # Find latest stormcells input (using data currently in EdgeWARN_input/stormcells)
    import util.file as fs
    latest_inputs = fs.latest_files(fs.STORMCELL_DIR, 1)
    if not latest_inputs:
        print("No stormcell inputs found. Cannot run integration benchmark.")
        return
        
    input_file = latest_inputs[0]
    print(f"Using input file: {input_file}")
    
    # Ensure garbage collector is clean before run
    gc.collect()
    
    # Start memory tracking
    tracemalloc.start()
    start_time = time.time()
    
    try:
        # Run pipeline
        # we set remove_old_cells=False to quickly benchmark the main routine
        integrate_main(json_path=input_file, remove_old_cells=False)
    except Exception as e:
        print(f"Pipeline error: {e}")
        import traceback
        traceback.print_exc()
        
    elapsed = time.time() - start_time
    current, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    
    peak_mb = peak / (1024 * 1024)
    
    print(f"\nResults:")
    print(f"  Total Time: {elapsed:.2f}s")
    print(f"  Peak Memory: {peak_mb:.1f} MB")
    
if __name__ == "__main__":
    run_benchmark()
