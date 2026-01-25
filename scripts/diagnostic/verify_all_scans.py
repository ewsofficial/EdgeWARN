
import json
import os
import re
from shapely.geometry import Point, shape
from pathlib import Path

def verify_containment_batch():
    ew_dir = Path("/home/yuchenwei/EdgeWARN_input/data/stormcells")
    ps_dir = Path("/home/yuchenwei/EdgeWARN_input/data/ProbSevere")
    
    # Match pattern for stormcells_YYYYMMDD-HHMMSS.json
    files = sorted(list(ew_dir.glob("stormcells_*.json")))
    
    print(f"{'Timestamp':<20} | {'Total':<6} | {'Inside':<8} | {'%':<8} | {'Max Offset (km)'}")
    print("-" * 65)
    
    global_total = 0
    global_inside = 0
    all_offsets = []

    for ew_path in files:
        # Extract timestamp: stormcells_20260125-003838.json -> 20260125_003838
        basename = os.path.basename(ew_path)
        ts_match = re.search(r"stormcells_(\d{8})-(\d{6})\.json", basename)
        if not ts_match:
            continue
            
        ts = f"{ts_match.group(1)}_{ts_match.group(2)}"
        ps_path = ps_dir / f"MRMS_PROBSEVERE_{ts}.json"
        
        if not ps_path.exists():
            continue
            
        # Perform containment check
        with open(ew_path, 'r') as f: ew_data = json.load(f)
        with open(ps_path, 'r') as f: ps_data = json.load(f)
        
        ps_shapes = {}
        for feat in ps_data['features']:
            try:
                poly_id = int(feat['properties']['ID'])
                ps_shapes[poly_id] = shape(feat['geometry'])
            except: continue
            
        scan_total = 0
        scan_inside = 0
        max_dist = 0
        
        for feat in ew_data['features']:
            cid = feat['id']
            if cid not in ps_shapes: continue
            
            scan_total += 1
            global_total += 1
            
            c_lat, c_lon = feat['centroid']
            norm_lon = c_lon - 360 if c_lon > 180 else c_lon
            p = Point(norm_lon, c_lat)
            
            if ps_shapes[cid].contains(p):
                scan_inside += 1
                global_inside += 1
            else:
                dist = ps_shapes[cid].distance(p) * 111 # rough km scale
                max_dist = max(max_dist, dist)
                all_offsets.append(dist)
                
        percentage = (scan_inside / scan_total * 100) if scan_total > 0 else 0
        print(f"{ts:<20} | {scan_total:<6} | {scan_inside:<8} | {percentage:>6.1f}% | {max_dist:>12.3f}")

    if global_total > 0:
        overall_pct = (global_inside / global_total * 100)
        print("-" * 65)
        print(f"{'OVERALL':<20} | {global_total:<6} | {global_inside:<8} | {overall_pct:>6.1f}% | Max: {max(all_offsets) if all_offsets else 0:.3f}")

if __name__ == "__main__":
    verify_containment_batch()
