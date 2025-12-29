import requests
import json
import os
from pathlib import Path
from datetime import datetime, timezone
from util.file import NWS_ALERTS_DIR
from util.io import IOManager

io_manager = IOManager("[NWS Ingest]")

VALID_EVENTS = {
    "Severe Thunderstorm Warning",
    "Severe Thunderstorm Watch",
    "Tornado Warning",
    "Tornado Watch",
    "Flash Flood Warning",
    "Special Weather Statement",
    "Severe Weather Statement"
}

def download_nws_alerts(dt: datetime):
    """
    Download active NWS alerts and save them to a JSON file.
    
    Args:
        dt (datetime): The timestamp to use for the output filename.
                       The actual query fetches *current* active alerts.
    """
    url = "https://api.weather.gov/alerts/active"
    headers = {
        "User-Agent": "(EdgeWARN, contact@edgewarn.com)", # Generic user agent
        "Accept": "application/geo+json"
    }

    try:
        io_manager.write_info(f"Downloading NWS active alerts...")
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        
        data = response.json()
        features = data.get("features", [])
        
        filtered_features = []
        for feature in features:
            props = feature.get("properties", {})
            event = props.get("event")
            if event in VALID_EVENTS:
                filtered_features.append(feature)
        
        if not filtered_features:
            io_manager.write_info("No matching NWS alerts found.")
            # Even if empty, we might want to verify if we save an empty list or nothing.
            # User said "save it", presumably the filtered result.
            # I will save an object with the filtered features to keep structure valid.
            output_data = {"type": "FeatureCollection", "features": []}
        else:
            io_manager.write_info(f"Found {len(filtered_features)} matching alerts.")
            output_data = {"type": "FeatureCollection", "features": filtered_features}

        # Ensure directory exists
        NWS_ALERTS_DIR.mkdir(parents=True, exist_ok=True)
        
        # Format filename: warnings_YYYYMMDD-HHMM00.json
        # Ensure dt is minute precision or just format it directly
        ts_str = dt.strftime("%Y%m%d-%H%M00")
        filename = f"warnings_{ts_str}.json"
        output_path = NWS_ALERTS_DIR / filename
        
        with open(output_path, "w") as f:
            json.dump(output_data, f)
            
        io_manager.write_info(f"Saved NWS alerts to {output_path}")
        
    except Exception as e:
        io_manager.write_error(f"Failed to download NWS alerts: {e}")
