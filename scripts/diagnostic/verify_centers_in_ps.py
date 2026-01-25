
import json
from shapely.geometry import Point, shape
from pathlib import Path

def verify_containment(ew_json_path, ps_json_path):
    with open(ew_json_path, 'r') as f:
        ew_data = json.load(f)
    with open(ps_json_path, 'r') as f:
        ps_data = json.load(f)
        
    ps_shapes = {}
    for feat in ps_data['features']:
        poly_id = int(feat['properties']['ID'])
        # normalize ps coordinates to 0-360 if needed
        # but shapely Point check needs consistency. 
        # let's normalize everything to -180 to 180.
        geom = shape(feat['geometry'])
        ps_shapes[poly_id] = geom
        
    results = []
    
    for feat in ew_data['features']:
        cid = feat['id']
        if cid not in ps_shapes:
            continue
            
        c_lat, c_lon = feat['centroid']
        # Convert EW lon (0-360) to -180 to 180
        norm_lon = c_lon - 360 if c_lon > 180 else c_lon
        
        point = Point(norm_lon, c_lat) # Point(x, y) = Point(lon, lat)
        is_inside = ps_shapes[cid].contains(point)
        
        # Buffer check: maybe it's on the edge?
        dist = ps_shapes[cid].distance(point)
        
        results.append({
            "id": cid,
            "is_inside": is_inside,
            "distance_deg": dist
        })
        
    return results

if __name__ == "__main__":
    import sys
    ew_p = "/home/yuchenwei/EdgeWARN_input/data/stormcells/stormcells_20260125-005839.json"
    ps_p = "/home/yuchenwei/EdgeWARN_input/data/ProbSevere/MRMS_PROBSEVERE_20260125_005839.json"
    
    analysis = verify_containment(ew_p, ps_p)
    
    total = len(analysis)
    inside = sum(1 for r in analysis if r['is_inside'])
    
    print(f"Scan: 20260125-005839")
    print(f"Total Matches: {total}")
    print(f"Centroids inside PS Polygon: {inside} ({(inside/total)*100:.1f}%)")
    
    if inside < total:
        print("\nExceptions (Centroid outside PS polygon):")
        print(f"{'Cell ID':<8} | {'Dist from Edge (deg)':<20}")
        print("-" * 35)
        for r in analysis:
            if not r['is_inside']:
                print(f"{r['id']:<8} | {r['distance_deg']:.6f}")
