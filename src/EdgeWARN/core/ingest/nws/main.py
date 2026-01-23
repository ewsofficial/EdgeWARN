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
from shapely.geometry import Polygon, MultiPolygon
from shapely.ops import unary_union

# Custom JSON encoder to handle Decimal types from ijson
class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return super().default(obj)

io_manager = IOManager("[NWS Ingest]")

# NEW: GeoMapper Assets Path
# Path: src/EdgeWARN/core/ingest/nws/main.py
# parents[5] = project root (EdgeWARN-Core)
_ASSETS_DIR = Path(__file__).resolve().parents[5] / "assets" / "nws_zones"

# Keys to remove from properties (from GeoMapper)
JUNK_KEYS = [
    "references",
    "sender", 
    "parameters",
    "instruction",
    "response",
    "scope",
    "code",
    "language",
    "web",
    "eventCode",
]

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

# --- GeoMapper Logic ---

class ZoneLookup:
    """Lazy-loading lookup for NWS zone polygons."""
    
    _cache: Dict[str, Dict[str, List]] = {}  # {state_code: {zone_code: polygon_coords}}
    
    @classmethod
    def get_polygon(cls, zone_code: str) -> Optional[List]:
        """Get polygon coordinates for a zone code."""
        if len(zone_code) < 2:
            return None
            
        state_code = zone_code[:2]
        
        if state_code not in cls._cache:
            cls._load_state(state_code)
        
        return cls._cache.get(state_code, {}).get(zone_code)
    
    @classmethod
    def _load_state(cls, state_code: str) -> None:
        """Load zone data for a state into cache."""
        state_file = _ASSETS_DIR / state_code / "zones.json"
        
        if not state_file.exists():
            cls._cache[state_code] = {}
            return
        
        try:
            with open(state_file, "r", encoding="utf-8") as f:
                zones = json.load(f)
            
            cls._cache[state_code] = {
                zone["code"]: zone["Polygon"]
                for zone in zones
                if "code" in zone and "Polygon" in zone
            }
        except Exception:
            cls._cache[state_code] = {}

def extract_exterior_polygon(polygons: List[List]) -> List:
    """Compute the union of multiple polygons and return only exterior coordinates."""
    if not polygons:
        return []
    
    shapely_polys = []
    
    for poly_coords in polygons:
        if not poly_coords:
            continue
        try:
            if len(poly_coords) >= 3:
                if isinstance(poly_coords[0], (list, tuple)) and len(poly_coords[0]) == 2:
                    shapely_polys.append(Polygon(poly_coords))
        except Exception:
            continue
    
    if not shapely_polys:
        return []
    
    try:
        unified = unary_union(shapely_polys)
        unified = unified.buffer(0)
        
        if unified.geom_type == 'Polygon':
            return [list(unified.exterior.coords)]
        elif unified.geom_type == 'MultiPolygon':
            return [list(p.exterior.coords) for p in unified.geoms]
        else:
            return []
    except Exception:
        return []

def process_warning(feature: Dict[str, Any]) -> Dict[str, Any]:
    """Process a single NWS warning feature (Map Geocodes + Clean Props)."""
    props = feature.get("properties", {})
    
    # Extract geocodes
    geocodes = []
    geocode_data = props.get("geocode", {})
    
    if isinstance(geocode_data, dict):
        ugc_codes = geocode_data.get("UGC", [])
        if ugc_codes:
            geocodes = ugc_codes
    elif isinstance(geocode_data, list):
        geocodes = geocode_data
    
    # Collect all polygon coordinates from matching zones
    all_polygon_coords = []
    
    for code in geocodes:
        poly = ZoneLookup.get_polygon(code)
        if poly:
            all_polygon_coords.extend(poly)
    
    # Compute union and extract exterior
    if all_polygon_coords:
        exterior = extract_exterior_polygon(all_polygon_coords)
        if exterior:
            feature["Polygon"] = exterior
    
    # Remove "geocode" if valid geometry exists
    has_geometry = False
    if feature.get("geometry") and feature.get("geometry", {}).get("coordinates"):
        has_geometry = True
    if feature.get("Polygon"):
        has_geometry = True
        
    if has_geometry:
        props.pop("geocode", None)
    
    # Remove junk keys from properties
    for key in JUNK_KEYS:
        props.pop(key, None)
    
    return feature

# --- End GeoMapper Logic ---

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
    fs.clean_files_by_age(fs.MRMS_NWS_DIR, max_age_minutes=120)

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

                if event in ALLOWED_EVENTS:
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
