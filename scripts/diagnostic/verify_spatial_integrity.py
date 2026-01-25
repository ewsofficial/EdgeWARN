
import json

def verify_spatial_integrity(filepath):
    with open(filepath, 'r') as f:
        data = json.load(f)
    
    print(f"{'Cell ID':<8} | {'Lat Centroid':<12} | {'Lat Range':<15} | {'Lon Centroid':<12} | {'Lon Range':<15} | {'Status'}")
    print("-" * 85)
    
    for cell in data['features']:
        cid = cell['id']
        centroid = cell['centroid']
        bbox = cell['bbox']
        
        if not centroid or not bbox:
            continue
            
        lats = [p[0] for p in bbox]
        lons = [p[1] for p in bbox]
        
        min_lat, max_lat = min(lats), max(lats)
        min_lon, max_lon = min(lons), max(lons)
        
        c_lat, c_lon = centroid
        
        lat_check = min_lat <= c_lat <= max_lat
        lon_check = min_lon <= c_lon <= max_lon
        
        status = "OK" if (lat_check and lon_check) else "OUT_OF_BOUNDS"
        
        print(f"{cid:<8} | {c_lat:<12.4f} | {min_lat:>6.3f}-{max_lat:<6.3f} | {c_lon:<12.4f} | {min_lon:>6.3f}-{max_lon:<6.3f} | {status}")

if __name__ == "__main__":
    import sys
    verify_spatial_integrity(sys.argv[1])
