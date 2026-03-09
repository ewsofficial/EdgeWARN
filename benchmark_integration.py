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

from EdgeWARN.core.process.integrate.main import main as integrate_main
from util.performance import tracker as perf_tracker

# Monkey-patch perf_tracker to track memory per step
original_start = perf_tracker.start
original_stop = perf_tracker.stop

memory_stats = {}

def patched_start(name):
    original_start(name)
    current, peak = tracemalloc.get_traced_memory()
    memory_stats[name] = {'start_mb': current / 1024 / 1024, 'peak_at_start': peak / 1024 / 1024}

def patched_stop(name):
    current, peak = tracemalloc.get_traced_memory()
    if name in memory_stats:
        memory_stats[name]['end_mb'] = current / 1024 / 1024
        memory_stats[name]['peak_at_end'] = peak / 1024 / 1024
    original_stop(name)

# Apply patches
perf_tracker.start = patched_start
perf_tracker.stop = patched_stop

def find_latest_stormcell_path():
    stormcell_dir = fs.STORMCELL_DIR
    files = sorted(stormcell_dir.glob("stormcells_*.json"))
    return files[-1] if files else None

def run_benchmark():
    file_path = find_latest_stormcell_path()
    if not file_path:
        print("No stormcell files found.")
        sys.exit(1)
        
    print(f"Found latest stormcell file: {file_path}")
    print("Starting integration with monotonic memory tracking...\n")
    
    tracemalloc.start()
    t0 = time.time()
    
    try:
        integrate_main(str(file_path), remove_old_cells=False)
    except Exception as e:
        print(f"Integration failed: {e}")
        tracemalloc.stop()
        sys.exit(1)
        
    elapsed = time.time() - t0
    current, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    
    print("\n" + "=" * 120)
    print(f"{'Integration Step':<40} | {'Time (s)':<10} | {'End (MB)':<10} | {'All-Time Peak (MB)':<20}")
    print("-" * 120)
    
    timings = perf_tracker.get_timings()
    for name, duration in timings.items():
        if name in memory_stats:
            stats = memory_stats[name]
            end_mb = stats.get('end_mb', 0)
            peak_mb = stats.get('peak_at_end', 0)
            print(f"{name:<40} | {duration:<10.2f} | {end_mb:<10.2f} | {peak_mb:<20.2f}")
    
    print("=" * 120)
    print(f"Total Time:     {elapsed:.2f} seconds")
    print(f"Final Peak:     {peak / 1024 / 1024:.2f} MB")

if __name__ == "__main__":
    run_benchmark()
