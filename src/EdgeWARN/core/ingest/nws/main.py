import json
import ijson
import urllib.request
from datetime import datetime
from pathlib import Path
from decimal import Decimal
import util.file as fs
from util.io import IOManager
import util.file as fs
from util.io import IOManager
import aiohttp
import asyncio
import tempfile
import os

# Custom JSON encoder to handle Decimal types from ijson
class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return super().default(obj)

io_manager = IOManager("[NWS Ingest]")

# Define the set of allowed events
ALLOWED_EVENTS = {
    "Severe Thunderstorm Watch",
    "Tornado Watch",
    "Severe Thunderstorm Warning",
    "Tornado Warning",
    "Flash Flood Warning",
    "Severe Weather Statement",
    "Special Weather Statement",
    "Winter Weather Advisory",
    "Winter Storm Watch",
    "Winter Storm Warning",
}

def download_alerts(dt: datetime):
    """
    Download active NWS alerts, filter them by event type in a streaming fashion,
    and save to a JSON file.

    Args:
        dt: The timestamp to use for the output filename.
    """
    url = "https://api.weather.gov/alerts/active"

    # Ensure output directory exists
    if not fs.MRMS_NWS_RAW_DIR.exists():
        fs.MRMS_NWS_RAW_DIR.mkdir(parents=True, exist_ok=True)

    # Clean old files (120-minute retention, no file count limit)
    fs.clean_files_by_age(fs.MRMS_NWS_RAW_DIR, max_age_minutes=120)

    # Output filename: alerts_active_YYYYMMDD-HHMM00.json
    filename = f"alerts_active_{dt.strftime('%Y%m%d-%H%M00')}.json"
    output_path = fs.MRMS_NWS_RAW_DIR / filename

    io_manager.write_info(f"Downloading active alerts to {output_path}...")

    headers = {
        "User-Agent": "(EdgeWARN/1.0, contact@edgewarn.com)",
        "Accept": "application/geo+json"
    }

    req = urllib.request.Request(url, headers=headers)

    try:
        with urllib.request.urlopen(req) as response, open(output_path, 'w', encoding='utf-8') as outfile:
            # Write the start of the GeoJSON object including context
            outfile.write('{"@context": ["https://geojson.org/geojson-ld/geojson-context.jsonld", {"@version": "1.1", "wx": "https://api.weather.gov/ontology#", "@vocab": "https://api.weather.gov/ontology#"}], "type": "FeatureCollection", "features": [')

            first = True
            count = 0

            # Stream parsing: iterate over features.item
            # This avoids loading the entire JSON into memory
            features = ijson.items(response, 'features.item')

            for feature in features:
                props = feature.get('properties', {})
                event = props.get('event')

                if event in ALLOWED_EVENTS:
                    if not first:
                        outfile.write(',')
                    else:
                        first = False

                    # Serialize the single feature back to JSON and write it
                    json.dump(feature, outfile, cls=DecimalEncoder)
                    count += 1

            # Write the end of the GeoJSON object
            outfile.write(']}')

        io_manager.write_info(f"Successfully saved {count} alerts to {filename}")

    except Exception as e:
        io_manager.write_error(f"Failed to download/process NWS alerts: {e}")
        # Clean up partial file if it exists and we failed
        if output_path.exists():
            try:
                output_path.unlink()
            except OSError:
                pass  # Ignore errors during cleanup

async def download_alerts_async(dt: datetime):
    """
    Async version of download_alerts.
    Downloads active NWS alerts using aiohttp, saves to a temporary file,
    and then parses with ijson to filter events.
    """
    url = "https://api.weather.gov/alerts/active"

    # Ensure output directory exists
    if not fs.MRMS_NWS_RAW_DIR.exists():
        fs.MRMS_NWS_RAW_DIR.mkdir(parents=True, exist_ok=True)

    # Clean old files (120-minute retention, no file count limit)
    fs.clean_files_by_age(fs.MRMS_NWS_RAW_DIR, max_age_minutes=120)

    # Output filename: alerts_active_YYYYMMDD-HHMM00.json
    filename = f"alerts_active_{dt.strftime('%Y%m%d-%H%M00')}.json"
    output_path = fs.MRMS_NWS_RAW_DIR / filename

    io_manager.write_info(f"Downloading active alerts (async) to {output_path}...")

    headers = {
        "User-Agent": "(EdgeWARN/1.0, contact@edgewarn.com)",
        "Accept": "application/geo+json"
    }

    try:
        # 1. Download to temporary file
        # We need a file for ijson (it works best with file-like objects or strict buffers)
        # Streaming directly from aiohttp response to ijson is possible but tricky if chunks are small
        # For safety and memory efficiency with ijson, we'll stream to a temp file first.
        fd, temp_path = tempfile.mkstemp()
        os.close(fd) # processing will open it

        async with aiohttp.ClientSession(headers=headers) as session:
            async with session.get(url) as response:
                response.raise_for_status()
                with open(temp_path, 'wb') as f:
                    async for chunk in response.content.iter_chunked(8192):
                        f.write(chunk)
        
        # 2. Process the temp file with ijson (synchronous part, but fast if local file)
        # We can run this in an executor if it blocks the loop too much
        loop = asyncio.get_running_loop()
        count = await loop.run_in_executor(None, _process_nws_file, temp_path, output_path)

        # 3. Cleanup temp file
        os.remove(temp_path)
        
        io_manager.write_info(f"Successfully saved {count} alerts to {filename} (async)")

    except Exception as e:
        io_manager.write_error(f"Failed to download/process NWS alerts (async): {e}")
        # Clean up partial file if it exists
        if output_path.exists():
            try:
                output_path.unlink()
            except OSError:
                pass
        # Raise to trigger fallback
        raise e

def _process_nws_file(input_path, output_path):
    """
    Helper to process the raw NWS JSON file and filter events.
    Runs in thread executor to avoid blocking main loop during parsing.
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

                if event in ALLOWED_EVENTS:
                    if not first:
                        outfile.write(',')
                    else:
                        first = False

                    # Serialize the single feature back to JSON and write it
                    json.dump(feature, outfile, cls=DecimalEncoder)
                    count += 1
            
            # Write the end of the GeoJSON object
            outfile.write(']}')
            
        return count
    except Exception as e:
        raise e

if __name__ == "__main__":
    # Test block
    fs.initialize_filesystem()
    download_alerts(datetime.now())
