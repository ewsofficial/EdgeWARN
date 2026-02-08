import urllib.request
import aiohttp
import asyncio
import io as io_lib
import re
import json
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path
import util.file as fs
from util.io import IOManager

# Initialize IO Manager
io = IOManager("[METAR Ingest]")

# Station database cache
_station_cache = None
STATION_DB_URL = "https://aviationweather.gov/data/cache/stations.cache.json"

async def ensure_station_database():
    """
    Ensure station database is loaded/cached asynchronously.
    """
    global _station_cache
    if _station_cache is not None:
        return

    cache_file = fs.DATA_DIR / "stations_cache.json" if hasattr(fs, 'DATA_DIR') else Path("stations_cache.json")
    if cache_file.exists():
        # Will be loaded by sync function when needed
        return

    # Download async
    io.write_info("Downloading station database from Aviation Weather (Async)...")
    try:
         async with aiohttp.ClientSession() as session:
            async with session.get(STATION_DB_URL, timeout=60) as response:
                response.raise_for_status()
                stations = await response.json()
                
                # Process
                parsed_cache = {}
                for station in stations:
                    icao = station.get('icaoId') or station.get('stationId')
                    lat = station.get('lat')
                    lon = station.get('lon')
                    
                    if icao and lat is not None and lon is not None:
                        parsed_cache[icao] = [round(float(lat), 4), round(float(lon), 4)]
                
                # Save to cache file
                cache_file.parent.mkdir(parents=True, exist_ok=True)
                loop = asyncio.get_running_loop()
                await loop.run_in_executor(None, _write_json_sync, cache_file, parsed_cache)
                
                _station_cache = parsed_cache
                io.write_info(f"Async loaded {len(_station_cache)} stations")

    except Exception as e:
        io.write_error(f"Async station download failed: {e}")

def _load_station_database():
    """
    Load station database from cache file or download from Aviation Weather API.
    Returns a dict mapping ICAO codes to [lat, lon] lists.
    """
    global _station_cache
    if _station_cache is not None:
        return _station_cache
    
    cache_file = fs.DATA_DIR / "stations_cache.json" if hasattr(fs, 'DATA_DIR') else Path("stations_cache.json")
    
    # Try to load from cache file first
    if cache_file.exists():
        try:
            with open(cache_file, 'r') as f:
                _station_cache = json.load(f)
                io.write_debug(f"Loaded {len(_station_cache)} stations from cache")
                return _station_cache
        except Exception as e:
            io.write_warning(f"Failed to load station cache: {e}")
    
    # Download and parse station database from JSON API
    _station_cache = {}
    try:
        io.write_info("Downloading station database from Aviation Weather...")
        req = urllib.request.Request(
            STATION_DB_URL,
            headers={"User-Agent": "EdgeWARN/1.0"}
        )
        with urllib.request.urlopen(req, timeout=60) as response:
            stations = json.loads(response.read().decode('utf-8'))
        
        # Parse JSON format - each station has icaoId, lat, lon
        for station in stations:
            icao = station.get('icaoId') or station.get('stationId')
            lat = station.get('lat')
            lon = station.get('lon')
            
            if icao and lat is not None and lon is not None:
                _station_cache[icao] = [round(float(lat), 4), round(float(lon), 4)]
        
        io.write_info(f"Parsed {len(_station_cache)} stations")
        
        # Save to cache file
        try:
            cache_file.parent.mkdir(parents=True, exist_ok=True)
            with open(cache_file, 'w') as f:
                json.dump(_station_cache, f)
            io.write_debug(f"Saved station cache to {cache_file}")
        except Exception as e:
            io.write_warning(f"Failed to save station cache: {e}")
            
    except Exception as e:
        io.write_error(f"Failed to download station database: {e}")
    
    return _station_cache

def get_station_coordinates(icao):
    """
    Get [lat, lon] for a station ICAO code.
    Returns None if not found.
    """
    db = _load_station_database()
    return db.get(icao.upper())

def parse_metar(metar_str, observation_time):
    """
    Parses a single METAR string into a dict.
    """
    data = {
        "observation_time": observation_time
    }

    parts = metar_str.split()
    if not parts:
        return None

    # Location (usually first)
    # Sometimes METAR starts with METAR or SPECI
    idx = 0
    if parts[idx] in ["METAR", "SPECI"]:
        data["type"] = parts[idx]
        idx += 1

    if idx < len(parts):
        data["station"] = parts[idx]
        # Look up station coordinates
        coords = get_station_coordinates(parts[idx])
        if coords:
            data["coordinates"] = coords
        idx += 1

    # Extract other fields using regex from the raw string for simplicity
    # Wind
    wind_match = re.search(r'\b(\d{3}|VRB)(\d{2,3})(G(\d{2,3}))?KT\b', metar_str)
    if wind_match:
        data["wind"] = {
            "direction": wind_match.group(1),
            "speed": wind_match.group(2),
            "gust": wind_match.group(4)
        }

    # Visibility
    vis_match = re.search(r'\b(\d+(?:/\d+)?|\d+ \d+/\d+)SM\b', metar_str)
    if vis_match:
        data["visibility"] = vis_match.group(1)

    # Temp/Dewpoint
    temp_match = re.search(r'\b(M?\d{2})/(M?\d{2})\b', metar_str)
    if temp_match:
        data["temperature"] = temp_match.group(1)
        data["dewpoint"] = temp_match.group(2)

    # Pressure (altimeter setting) - convert from AXXXX format to decimal inHg
    alt_match = re.search(r'\bA(\d{4})\b', metar_str)
    if alt_match:
        # Convert e.g. "3039" -> 30.39 inHg
        alt_value = int(alt_match.group(1))
        data["pressure"] = round(alt_value / 100, 2)

    # Clouds / Sky Condition
    clouds = []
    # Match CLR, SKC, CAVOK
    if re.search(r'\b(CLR|SKC|CAVOK)\b', metar_str):
        clouds.append({"code": "CLR"})
    
    # Match layers like FEW020, SCT030CB, VV002
    cloud_matches = re.finditer(r'\b(FEW|SCT|BKN|OVC|VV)(\d{3}|///)?(CB|TCU)?\b', metar_str)
    for m in cloud_matches:
        layer = {"code": m.group(1)}
        if m.group(2) and m.group(2) != "///":
            try:
                layer["altitude"] = int(m.group(2)) * 100
            except ValueError:
                pass
        if m.group(3):
            layer["type"] = m.group(3)
        clouds.append(layer)
    
    if clouds:
        data["clouds"] = clouds

    # Weather Phenomena
    # Matches optional intensity/proximity, optional descriptor, precipitation/obscuration
    # We split by whitespace and check each token to handle symbols like '+' correctly
    # which are not word characters for \b boundaries.
    wx_regex = r'^(-|\+|VC)?(TS|SH|FZ|BL|DR|MI|BC|PR)?(RA|SN|SG|IC|PL|GR|GS|UP|DZ|FG|BR|SA|DU|HZ|FU|VA|PO|SQ|FC|SS|DS)+$'
    
    main_body = metar_str.split("RMK")[0]
    parts = main_body.split()
    
    weather = []
    for part in parts:
        if re.match(wx_regex, part):
            weather.append(part)
        
    if weather:
        data["weather"] = list(set(weather))

    # Remarks
    rmk_match = re.search(r'\bRMK\s+(.*)', metar_str)
    if rmk_match:
        data["remarks"] = rmk_match.group(1)

    return data

def fetch_metar_cycle(dt):
    """
    Fetches the METAR cycle file for the given datetime.
    """
    hour_str = dt.strftime("%H")
    # File is HHZ.TXT on server
    url = f"https://tgftp.nws.noaa.gov/data/observations/metar/cycles/{hour_str}Z.TXT"
    io.write_info(f"Fetching METAR data from {url}")

    try:
        with urllib.request.urlopen(url, timeout=30) as response:
            content = response.read().decode('utf-8', errors='ignore')
            return content
    except urllib.error.HTTPError as e:
        if e.code == 404:
            io.write_warning(f"METAR file not found for {hour_str}Z (404)")
        else:
            io.write_error(f"HTTP Error fetching {url}: {e}")
    except Exception as e:
        io.write_error(f"Failed to fetch {url}: {e}")

    return None

    return None

async def fetch_metar_cycle_async(dt, session=None):
    """
    Async version of fetch_metar_cycle.
    """
    hour_str = dt.strftime("%H")
    url = f"https://tgftp.nws.noaa.gov/data/observations/metar/cycles/{hour_str}Z.TXT"
    io.write_info(f"Fetching METAR data (async) from {url}")

    try:
        if session:
            async with session.get(url, timeout=30) as response:
                if response.status == 404:
                    io.write_warning(f"METAR file not found for {hour_str}Z (404)")
                    return None
                response.raise_for_status()
                content = await response.text(encoding='utf-8', errors='ignore')
                return content
        else:
             async with aiohttp.ClientSession() as new_session:
                async with new_session.get(url, timeout=30) as response:
                    # ... duplication or call recursive? NO, simple logic
                    if response.status == 404:
                        io.write_warning(f"METAR file not found for {hour_str}Z (404)")
                        return None
                    response.raise_for_status()
                    content = await response.text(encoding='utf-8', errors='ignore')
                    return content

    except Exception as e:
        io.write_error(f"Failed to fetch {url} (async): {e}")
    
    return None

# CONUS Boundaries
CONUS_BOUNDS = {
    "lat_min": 24.0,
    "lat_max": 50.0,
    "lon_min": -125.0,
    "lon_max": -66.0
}

def process_content(content):
    """
    Parses the content of a cycle file.
    """
    lines = content.splitlines()
    parsed_data = []

    current_time = None

    for line in lines:
        line = line.strip()
        if not line:
            continue

        # Check if line is a timestamp YYYY/MM/DD HH:MM
        if re.match(r'^\d{4}/\d{2}/\d{2} \d{2}:\d{2}$', line):
            current_time = line
            continue

        # Assume it is a METAR
        if current_time:
            metar_data = parse_metar(line, current_time)
            if metar_data:
                # Filter by CONUS bounds
                coords = metar_data.get("coordinates")
                if coords:
                    lat, lon = coords
                    if (CONUS_BOUNDS["lat_min"] <= lat <= CONUS_BOUNDS["lat_max"] and 
                        CONUS_BOUNDS["lon_min"] <= lon <= CONUS_BOUNDS["lon_max"]):
                        parsed_data.append(metar_data)
                # Skip if no coords or outside bounds

    return parsed_data

def save_metar_data(data, dt):
    """
    Saves the parsed METAR data to a JSON file.
    Enforces a 10-file limit using clean_old_files.
    """
    if not data:
        io.write_warning("No METAR data to save.")
        return

    if not fs.METAR_DIR.exists():
        io.write_info(f"Creating METAR directory: {fs.METAR_DIR}")
        fs.METAR_DIR.mkdir(parents=True, exist_ok=True)

    filename = f"METAR_{dt.strftime('%Y%m%d-%H')}z.json"
    filepath = fs.METAR_DIR / filename

    io.write_info(f"Saving {len(data)} METAR records to {filepath}")

    try:
        with open(filepath, 'w') as f:
            json.dump(data, f, indent=2)
    except Exception as e:
        io.write_error(f"Failed to save METAR data to {filepath}: {e}")
    
    # Enforce 10-file limit
    fs.clean_old_files(fs.METAR_DIR, max_age_minutes=60)

def ingest_metars():
    """
    Main entry point for METAR ingestion.
    Fetches and processes METAR data for the last 3 hours.
    """
    # Ensure paths are defined
    fs.initialize_filesystem()

    now = datetime.now(timezone.utc)

    # Process current hour and previous 2 hours
    for i in range(3):
        target_time = now - timedelta(hours=i)
        io.write_info(f"Processing METARs for {target_time.strftime('%Y-%m-%d %H:00')} UTC")

        content = fetch_metar_cycle(target_time)
        if content:
            parsed_data = process_content(content)
            save_metar_data(parsed_data, target_time)
        else:
            io.write_warning(f"Skipping {target_time.strftime('%H')}Z due to fetch failure.")

async def save_metar_data_async(data, dt):
    """
    Async version of save_metar_data.
    Uses thread executor for file I/O and async wrapper for cleanup.
    """
    if not data:
        io.write_warning("No METAR data to save.")
        return

    if not fs.METAR_DIR.exists():
        io.write_info(f"Creating METAR directory: {fs.METAR_DIR}")
        fs.METAR_DIR.mkdir(parents=True, exist_ok=True)

    filename = f"METAR_{dt.strftime('%Y%m%d-%H')}z.json"
    filepath = fs.METAR_DIR / filename

    io.write_info(f"Saving {len(data)} METAR records to {filepath}")

    try:
        # Offload JSON dump to thread
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, _write_json_sync, filepath, data)
    except Exception as e:
        io.write_error(f"Failed to save METAR data to {filepath}: {e}")
    
    # Enforce 10-file limit (async)
    await fs.async_clean_old_files(fs.METAR_DIR, max_age_minutes=60)

def _write_json_sync(filepath, data):
    with open(filepath, 'w') as f:
        json.dump(data, f, indent=2)

async def ingest_metars_async():
    """
    Async entry point for METAR ingestion.
    Fetches and processes METAR data for the last 3 hours concurrently.
    """
    # Ensure paths are defined
    fs.initialize_filesystem()

    # Ensure station DB is loaded async
    await ensure_station_database()

    now = datetime.now(timezone.utc)
    
    # Create tasks for current hour and previous 2 hours
    async with aiohttp.ClientSession() as session:
        async def _process_single_hour(i):
            target_time = now - timedelta(hours=i)
            io.write_info(f"Processing METARs (async) for {target_time.strftime('%Y-%m-%d %H:00')} UTC")
            
            content = await fetch_metar_cycle_async(target_time, session=session)
            if content:
                # CPU-bound parsing
                loop = asyncio.get_running_loop()
                parsed_data = await loop.run_in_executor(None, process_content, content)
                await save_metar_data_async(parsed_data, target_time)
            else:
                 io.write_warning(f"Skipping {target_time.strftime('%H')}Z due to fetch failure.")
    
        for i in range(3):
            tasks.append(_process_single_hour(i))
        
        await asyncio.gather(*tasks)

if __name__ == "__main__":
    ingest_metars()
