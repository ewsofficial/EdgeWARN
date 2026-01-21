
import requests
import json
import concurrent.futures
from datetime import datetime, timezone
from pathlib import Path
import util.file as fs
from util.io import IOManager

# Configure logger
io_manager = IOManager("[Surface]")

# National Forecast Chart URL (Day 1, 2, 3 Forecasts including Fronts)
NFC_URL = "https://mapservices.weather.noaa.gov/vector/rest/services/outlooks/natl_fcst_wx_chart/MapServer"

def fetch_layer_metadata():
    """Fetch metadata for all layers in the service."""
    try:
        url = f"{NFC_URL}/layers?f=json"
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
        return resp.json().get("layers", [])
    except Exception as e:
        io_manager.write_error(f"Failed to fetch layer metadata: {e}")
        return []

def fetch_features_for_layer(layer):
    """Fetch features for a specific layer ID as GeoJSON."""
    layer_id = layer["id"]
    layer_name = layer["name"]
    
    # Skip Group Layers
    if layer.get("type") == "Group Layer":
        return []

    try:
        # Request GeoJSON
        query_url = f"{NFC_URL}/{layer_id}/query"
        params = {
            "where": "1=1",
            "outFields": "*",
            "f": "geojson",
            "resultRecordCount": 2000 # Max record count usually 2000
        }
        
        resp = requests.get(query_url, params=params, timeout=15)
        resp.raise_for_status()
        
        data = resp.json()
        features = data.get("features", [])
        
        # Enrich features with layer info
        for feature in features:
            if "properties" not in feature or feature["properties"] is None:
                feature["properties"] = {}
            feature["properties"]["layer_name"] = layer_name
            feature["properties"]["layer_id"] = layer_id
            
        return features
        
    except Exception as e:
        io_manager.write_warning(f"Failed to fetch features for layer {layer_id} ({layer_name}): {e}")
        return []

def ingest_surface_features():
    """Main ingestion function."""
    io_manager.write_info("Starting Surface Features ingestion...")
    
    # Ensure directory exists
    if not fs.SURFACE_DIR.exists():
        fs.SURFACE_DIR.mkdir(parents=True, exist_ok=True)
        
    start_time = datetime.now(timezone.utc)
    
    # 1. Fetch Metadata
    layers = fetch_layer_metadata()
    if not layers:
        io_manager.write_error("No layers found. Aborting.")
        return

    # 2. Filter for Feature Layers (Day 1, 2, 3)
    feature_layers = [l for l in layers if l.get("type") == "Feature Layer"]
    io_manager.write_info(f"Found {len(feature_layers)} feature layers to fetch.")
    
    all_features = []
    
    # 3. Parallel Fetch
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
        future_map = {executor.submit(fetch_features_for_layer, l): l for l in feature_layers}
        for future in concurrent.futures.as_completed(future_map):
            feats = future.result()
            all_features.extend(feats)
            
    if not all_features:
        io_manager.write_warning("No surface features found across all layers.")
        return

    io_manager.write_info(f"Retrieved {len(all_features)} total surface features.")

    # 4. Construct Output GeoJSON
    # Timestamp: Use current time as filename reference, but try to find valid time in props
    # MapServer fields: 'idp_filedate'
    
    # Attempt to extract a representative timestamp from features
    latest_ts = start_time
    
    # Try to find max 'idp_filedate'
    max_filedate = 0
    for f in all_features:
        props = f.get("properties", {})
        fd = props.get("idp_filedate")
        if fd:
            try:
                # Esri dates are ms since epoch
                if isinstance(fd, int) and fd > max_filedate:
                    max_filedate = fd
            except:
                pass
                
    if max_filedate > 0:
        latest_ts = datetime.fromtimestamp(max_filedate / 1000.0, tz=timezone.utc)
    
    # Round to minute
    ts_str = latest_ts.strftime("%Y%m%d-%H%M00")
    filename = f"surface_features_{ts_str}.json"
    filepath = fs.SURFACE_DIR / filename
    
    output_geojson = {
        "type": "FeatureCollection",
        "metadata": {
            "generated_at": start_time.isoformat(),
            "source": NFC_URL,
            "feature_count": len(all_features)
        },
        "features": all_features
    }
    
    # 5. Save
    try:
        with open(filepath, "w") as f:
            json.dump(output_geojson, f)
        io_manager.write_info(f"Saved surface features to {filepath}")
    except Exception as e:
        io_manager.write_error(f"Failed to save surface features: {e}")

if __name__ == "__main__":
    # Test run
    fs.initialize_filesystem()
    ingest_surface_features()
