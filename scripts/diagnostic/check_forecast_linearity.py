
import json
import math
import numpy as np

def calculate_bearing(lat1, lon1, lat2, lon2):
    """Calculate bearing from point 1 to point 2 in degrees."""
    # Convert to radians
    lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])
    
    dlon = lon2 - lon1
    
    y = math.sin(dlon) * math.cos(lat2)
    x = math.cos(lat1) * math.sin(lat2) - \
        math.sin(lat1) * math.cos(lat2) * math.cos(dlon)
    
    bearing = math.atan2(y, x)
    return (math.degrees(bearing) + 360) % 360

def check_bent_paths(filepath):
    with open(filepath, 'r') as f:
        data = json.load(f)
    
    results = []
    
    for cell in data['features']:
        cell_id = cell['id']
        centroid = [cell['centroid'][0], cell['centroid'][1] % 360]
        
        if 'modules' not in cell or 'StormCast' not in cell['modules']:
            continue
            
        sc = cell['modules']['StormCast']
        if sc.get('status') != 'success':
            continue
            
        cones = sc.get('forecast_cones', [])
        if len(cones) < 4:
            continue
            
        # Cones are usually 900, 1800, 2700, 3600
        # Wrap all longitudes to 0-360 for consistency
        pts = [[centroid[0], centroid[1] % 360]]
        for c in cones:
            pts.append([c['center'][0], c['center'][1] % 360])
        
        # Segment bearings
        bearings = []
        for i in range(len(pts) - 1):
            bearings.append(calculate_bearing(pts[i][0], pts[i][1], pts[i+1][0], pts[i+1][1]))
            
        # Check diffs
        diffs = []
        for i in range(len(bearings) - 1):
            d = abs(bearings[i] - bearings[i+1])
            if d > 180: d = 360 - d
            diffs.append(d)
        
        results.append({
            "id": cell_id,
            "bearings": bearings,
            "diffs": diffs,
            "max_diff": max(diffs) if diffs else 0
        })
        
    return results

if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("Usage: python check_forecast_linearity.py <json_path>")
        sys.exit(1)
        
    path = sys.argv[1]
    analysis = check_bent_paths(path)
    
    print(f"{'Cell ID':<8} | {'B0':<8} | {'B1':<8} | {'B2':<8} | {'B3':<8} | {'Max D':<6}")
    print("-" * 60)
    for res in analysis:
        b = res['bearings']
        print(f"{res['id']:<8} | {b[0]:>8.2f} | {b[1]:>8.2f} | {b[2]:>8.2f} | {b[3]:>8.2f} | {res['max_diff']:>6.2f}")
    
    if analysis:
        total_max_diff = max(r['max_diff'] for r in analysis)
        print(f"\nGlobal Max angular deviation: {total_max_diff:.2f} degrees")
    else:
        print("\nNo StormCast data found to analyze.")
