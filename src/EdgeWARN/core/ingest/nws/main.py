import json
import ijson
import urllib.request
from datetime import datetime
from pathlib import Path
from decimal import Decimal
import util.file as fs
from util.io import IOManager
import aiohttp
import asyncio
import tempfile
import os
from typing import Dict, Any, List, Optional

# Import GeoMapper logic
from .geomapper import process_warning

# Custom JSON encoder to handle Decimal types from ijson
class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return super().default(obj)

io_manager = IOManager("[NWS Ingest]")

# Define the set of dropped events (blocklist)
DROPPED_EVENTS = {
    # Always drop
    "Administrative Message",
    "Freezing Spray Advisory",
    "Low Water Advisory",
    "High Surf Advisory",
    "Small Craft Advisory",
    "Brisk Wind Advisory",
    "Freezing Spray Advisory",
    "Low Water Advisory",
    "High Surf Advisory",
    "Small Craft Advisory",
    "Brisk Wind Advisory",
    "Practice/Demo Warning",
    "Required Weekly Test",
    "Required Monthly Test",
    "Hurricane Local Statement",
    "Flood Statement",
    "Flash Flood Statement",
    "Rip Current Statement",
    "Lakeshore Flood Statement",
    "Hydrologic Outlook",
    # Optional drops
    "Air Quality Alert",
    "Air Stagnation Advisory",
    "Beach Hazards Statement",
}


def download_alerts(dt: datetime):
    """
    Download active NWS alerts, filter them by event type, Apply GeoMapper,
    and save to a JSON file.
    """
    url = "https://api.weather.gov/alerts/active"

    # Ensure output directory exists (Using PROCESSED NWS DIR now)
    if not fs.MRMS_NWS_DIR.exists():
        fs.MRMS_NWS_DIR.mkdir(parents=True, exist_ok=True)

    # Clean old files
    fs.clean_files_by_age(fs.MRMS_NWS_DIR, max_age_minutes=120)

    # Output filename: alerts_active_YYYYMMDD-HHMM00.json
    filename = f"alerts_active_{dt.strftime('%Y%m%d-%H%M00')}.json"
    output_path = fs.MRMS_NWS_DIR / filename

    io_manager.write_info(f"Downloading active alerts to {output_path}...")

    headers = {
        "User-Agent": "(EdgeWARN/1.0, contact@edgewarn.com)",
        "Accept": "application/geo+json"
    }

    req = urllib.request.Request(url, headers=headers)

    try:
        with urllib.request.urlopen(req) as response:
            # For streaming + processing, implementing fully streaming parser + logic is complex
            # We'll buffer to temp file logic similar to async to reuse the robust processing
            with tempfile.NamedTemporaryFile(delete=False) as tmp:
                tmp.write(response.read())
                temp_path = tmp.name
        
        # Process
        count = _process_nws_file(temp_path, output_path)
        os.remove(temp_path)

        io_manager.write_info(f"Successfully processed {count} alerts to {filename}")

    except Exception as e:
        io_manager.write_error(f"Failed to download/process NWS alerts: {e}")
        if output_path.exists():
            try:
                output_path.unlink()
            except OSError:
                pass

async def download_alerts_async(dt: datetime):
    """
    Async version of download_alerts.
    Downloads active NWS alerts using aiohttp, saves to a temporary file,
    and then parses/processes with ijson and GeoMapper logic.
    """
    url = "https://api.weather.gov/alerts/active"

    # Ensure output directory exists (Using PROCESSED NWS DIR)
    if not fs.MRMS_NWS_DIR.exists():
        fs.MRMS_NWS_DIR.mkdir(parents=True, exist_ok=True)

    # Clean old files
    # Clean old files (async)
    await fs.async_clean_files_by_age(fs.MRMS_NWS_DIR, max_age_minutes=120)

    # Output filename: alerts_active_YYYYMMDD-HHMM00.json
    filename = f"alerts_active_{dt.strftime('%Y%m%d-%H%M00')}.json"
    output_path = fs.MRMS_NWS_DIR / filename

    io_manager.write_info(f"Downloading active alerts (async + mapping) to {output_path}...")

    headers = {
        "User-Agent": "(EdgeWARN/1.0, contact@edgewarn.com)",
        "Accept": "application/geo+json"
    }

    try:
        # 1. Download to temporary file
        fd, temp_path = tempfile.mkstemp()
        os.close(fd)

        async with aiohttp.ClientSession(headers=headers) as session:
            async with session.get(url) as response:
                response.raise_for_status()
                with open(temp_path, 'wb') as f:
                    async for chunk in response.content.iter_chunked(8192):
                        f.write(chunk)
        
        # 2. Process the temp file (apply filters + GeoMapper)
        # Run in executor to avoid blocking main loop
        loop = asyncio.get_running_loop()
        count = await loop.run_in_executor(None, _process_nws_file, temp_path, output_path)

        # 3. Cleanup temp file
        os.remove(temp_path)
        
        io_manager.write_info(f"Successfully processed {count} alerts to {filename} (async)")

    except Exception as e:
        io_manager.write_error(f"Failed to download/process NWS alerts (async): {e}")
        if output_path.exists():
            try:
                output_path.unlink()
            except OSError:
                pass
        raise e

def _process_nws_file(input_path, output_path):
    """
    Helper to process the raw NWS JSON file.
    Filters events AND applies GeoMapper logic (geocode->polygon, clean props).
    Runs in thread executor.
    """
    count = 0
    try:
        with open(input_path, 'r', encoding='utf-8') as infile, open(output_path, 'w', encoding='utf-8') as outfile:
             # Write the start of the GeoJSON object including context
            outfile.write('{"@context": ["https://geojson.org/geojson-ld/geojson-context.jsonld", {"@version": "1.1", "wx": "https://api.weather.gov/ontology#", "@vocab": "https://api.weather.gov/ontology#"}], "type": "FeatureCollection", "features": [')

            first = True
            
            # Stream parsing
            features = ijson.items(infile, 'features.item')

            for feature in features:
                props = feature.get('properties', {})
                event = props.get('event')

                if event in DROPPED_EVENTS:
                    continue

                # Apply GeoMapper Logic
                processed_feature = process_warning(feature)
                
                if not first:
                    outfile.write(',')
                else:
                    first = False

                # Serialize the single feature back to JSON and write it
                json.dump(processed_feature, outfile, cls=DecimalEncoder)
                count += 1
            
            # Write the end of the GeoJSON object
            outfile.write(']}')
            
        return count
    except Exception as e:
        raise e

if __name__ == "__main__":
    # Test block
    fs.initialize_filesystem()
    import asyncio
    asyncio.run(download_alerts_async(datetime.now()))
