import urllib.request
import re
import json
import logging
from datetime import datetime, timedelta, timezone
import util.file as file
from util.io import IOManager

# Initialize IO Manager
io = IOManager("[METAR Ingest]")

def parse_metar(metar_str, observation_time):
    """
    Parses a single METAR string into a dict.
    """
    data = {
        "raw": metar_str,
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

    # Altimeter
    alt_match = re.search(r'\bA(\d{4})\b', metar_str)
    if alt_match:
        data["altimeter"] = alt_match.group(1)

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
                parsed_data.append(metar_data)

    return parsed_data

def save_metar_data(data, dt):
    """
    Saves the parsed METAR data to a JSON file.
    """
    if not data:
        io.write_warning("No METAR data to save.")
        return

    if not file.METAR_DIR.exists():
        io.write_info(f"Creating METAR directory: {file.METAR_DIR}")
        file.METAR_DIR.mkdir(parents=True, exist_ok=True)

    filename = f"METAR_{dt.strftime('%Y%m%d-%H')}z.json"
    filepath = file.METAR_DIR / filename

    io.write_info(f"Saving {len(data)} METAR records to {filepath}")

    try:
        with open(filepath, 'w') as f:
            json.dump(data, f, indent=2)
    except Exception as e:
        io.write_error(f"Failed to save METAR data to {filepath}: {e}")

def ingest_metars():
    """
    Main entry point for METAR ingestion.
    Fetches and processes METAR data for the last 3 hours.
    """
    # Ensure paths are defined
    file.initialize_filesystem()

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

if __name__ == "__main__":
    ingest_metars()
