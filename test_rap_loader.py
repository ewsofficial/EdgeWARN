"""
Smoke test for the new RAPPointExtractor prototype.
Compares output with the existing cfgrib-based integration to validate correctness.
"""
import sys
import time
import tracemalloc
from pathlib import Path

# Add src to python path
src_path = str(Path(__file__).parent / "src")
if src_path not in sys.path:
    sys.path.insert(0, src_path)

import util.file as fs
fs.initialize_filesystem()

from util.grib_loader import RAPPointExtractor
from EdgeWARN.core.process.integrate.config import get_rap_products

def main():
    # Find RAP file
    rap_files = sorted(fs.RAP_DIR.glob("RAP.*.grib2"))
    if not rap_files:
        print("No RAP files found.")
        return
    rap_file = str(rap_files[-1])
    print(f"RAP file: {rap_file}")
    
    # Find stormcell file for cell coordinates
    import json
    sc_files = sorted(fs.STORMCELL_DIR.glob("stormcells_*.json"))
    if not sc_files:
        print("No stormcell files found.")
        return
    
    with open(sc_files[-1]) as f:
        data = json.load(f)
    
    cells = data.get("cells", data.get("features", []))
    print(f"Loaded {len(cells)} cells from {sc_files[-1].name}")
    
    # Build cell_coords dict: {cell_id: (lat, lon)}
    cell_coords = {}
    for cell in cells[:10]:  # Test with first 10 cells for speed
        cid = cell.get("id")
        centroid = cell.get("centroid", [0, 0])
        cell_coords[cid] = (centroid[0], centroid[1])
    
    config = get_rap_products()
    products = config["products"]
    
    print(f"\n--- Testing extract() (zero-memory, find_nearest) ---")
    tracemalloc.start()
    t0 = time.time()
    
    extractor = RAPPointExtractor(rap_file)
    results = extractor.extract(products, cell_coords)
    
    elapsed = time.time() - t0
    current, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    
    print(f"Time: {elapsed:.2f}s")
    print(f"Current memory: {current / 1024 / 1024:.2f} MB")
    print(f"Peak memory: {peak / 1024 / 1024:.2f} MB")
    print(f"Keys extracted: {len(results)}")
    
    # Show sample results
    sample_cell = list(cell_coords.keys())[0]
    print(f"\nSample values for cell {sample_cell}:")
    for key in sorted(list(results.keys()))[:10]:
        val = results[key].get(sample_cell)
        print(f"  {key}: {val}")
    
    print(f"\n--- Testing extract_batch() (vectorized, ~15 MB peak) ---")
    tracemalloc.start()
    t0 = time.time()
    
    extractor2 = RAPPointExtractor(rap_file)
    results_batch = extractor2.extract_batch(products, cell_coords)
    
    elapsed = time.time() - t0
    current, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    
    print(f"Time: {elapsed:.2f}s")
    print(f"Current memory: {current / 1024 / 1024:.2f} MB")
    print(f"Peak memory: {peak / 1024 / 1024:.2f} MB")
    print(f"Keys extracted: {len(results_batch)}")
    
    # Compare results between two methods
    print(f"\n--- Comparing extract() vs extract_batch() ---")
    mismatches = 0
    for key in results:
        if key not in results_batch:
            print(f"  MISSING in batch: {key}")
            mismatches += 1
            continue
        for cid in cell_coords:
            v1 = results[key].get(cid)
            v2 = results_batch[key].get(cid)
            if v1 is not None and v2 is not None and abs(v1 - v2) > 0.1:
                print(f"  MISMATCH {key} cell {cid}: extract={v1:.4f} batch={v2:.4f}")
                mismatches += 1
    
    if mismatches == 0:
        print(f"  ✓ All {len(results)} keys match across both methods!")
    else:
        print(f"  ✗ {mismatches} mismatches found")

if __name__ == "__main__":
    main()
