"""
NWS Alert Ingest Module

Downloads active NWS alerts from the National Weather Service API,
filters them by event type, applies GeoMapper for zone-to-polygon mapping,
and stores them in a deduplicated registry.

Architecture:
    - Downloads from https://api.weather.gov/alerts/active every 2 minutes
    - Filters out non-severe event types (DROPPED_EVENTS blocklist)
    - Applies GeoMapper to map UGC zone codes to actual polygons
    - Stores unique alerts in alerts_registry.json with deduplication
    - TTL-based cleanup removes alerts not seen within 2 hours
"""

import json
import ijson
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from decimal import Decimal
import util.file as fs
from util.io import IOManager
import aiohttp
import asyncio
import tempfile
import os
from typing import Dict, Any, List, Optional, Tuple

# Import GeoMapper logic
from .geomapper import process_warning

# Import AlertRegistry for deduplication
from .registry import AlertRegistry, get_registry, reset_registry

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
    "Test Message",
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


def _get_registry() -> AlertRegistry:
    """Get or initialize the AlertRegistry singleton."""
    return get_registry(fs.NWS_REGISTRY_PATH, ttl_hours=2.0)


def download_alerts(dt: datetime):
    """
    Download active NWS alerts, filter them by event type, Apply GeoMapper,
    and update the alerts registry with deduplication.
    
    Args:
        dt: Current datetime (used for timestamp tracking)
    """
    url = "https://api.weather.gov/alerts/active"

    # Ensure output directory exists
    if not fs.MRMS_NWS_DIR.exists():
        fs.MRMS_NWS_DIR.mkdir(parents=True, exist_ok=True)

    # Get the registry
    registry = _get_registry()

    io_manager.write_info(f"Downloading active alerts for registry update...")

    headers = {
        "User-Agent": "(EdgeWARN/1.0, contact@edgewarn.com)",
        "Accept": "application/geo+json"
    }

    req = urllib.request.Request(url, headers=headers)

    try:
        with urllib.request.urlopen(req) as response:
            # Buffer to temp file for processing
            with tempfile.NamedTemporaryFile(delete=False) as tmp:
                tmp.write(response.read())
                temp_path = tmp.name
        
        # Process with registry
        current_time = dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
        new_count, updated_count = _process_nws_file_with_registry(temp_path, registry, current_time)
        
        # Cleanup expired alerts
        removed_count = registry.cleanup_expired(current_time)
        
        # Save registry
        registry.save()
        
        # Cleanup temp file
        os.remove(temp_path)

        total_active = registry.alert_count
        io_manager.write_info(
            f"Registry updated: {new_count} new, {updated_count} updated, "
            f"{removed_count} removed, {total_active} total active"
        )

    except Exception as e:
        io_manager.write_error(f"Failed to download/process NWS alerts: {e}")
        raise e


async def download_alerts_async(dt: datetime):
    """
    Async version of download_alerts.
    Downloads active NWS alerts using aiohttp, processes with deduplication,
    and updates the alerts registry.
    
    Args:
        dt: Current datetime (used for timestamp tracking)
    """
    url = "https://api.weather.gov/alerts/active"

    # Ensure output directory exists
    if not fs.MRMS_NWS_DIR.exists():
        fs.MRMS_NWS_DIR.mkdir(parents=True, exist_ok=True)

    # Get the registry
    registry = _get_registry()

    io_manager.write_info(f"Downloading active alerts (async) for registry update...")

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
        
        # 2. Process the temp file with registry
        current_time = dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
        
        # Run in executor to avoid blocking main loop
        loop = asyncio.get_running_loop()
        new_count, updated_count = await loop.run_in_executor(
            None, 
            _process_nws_file_with_registry, 
            temp_path, 
            registry, 
            current_time
        )
        
        # 3. Cleanup expired alerts
        removed_count = registry.cleanup_expired(current_time)
        
        # 4. Save registry
        registry.save()
        
        # 5. Cleanup temp file
        os.remove(temp_path)
        
        total_active = registry.alert_count
        io_manager.write_info(
            f"Registry updated (async): {new_count} new, {updated_count} updated, "
            f"{removed_count} removed, {total_active} total active"
        )

    except Exception as e:
        io_manager.write_error(f"Failed to download/process NWS alerts (async): {e}")
        raise e


def _process_nws_file_with_registry(
    input_path: str, 
    registry: AlertRegistry, 
    current_time: datetime
) -> Tuple[int, int]:
    """
    Process the raw NWS JSON file and update the registry.
    
    Filters events, applies GeoMapper logic, and adds/updates alerts in registry.
    
    Args:
        input_path: Path to the downloaded NWS JSON file
        registry: AlertRegistry instance to update
        current_time: Current timestamp for tracking
        
    Returns:
        Tuple of (new_count, updated_count)
    """
    new_count = 0
    updated_count = 0
    
    try:
        with open(input_path, 'r', encoding='utf-8') as infile:
            # Stream parsing
            features = ijson.items(infile, 'features.item')

            for feature in features:
                props = feature.get('properties', {})
                event = props.get('event')

                if event in DROPPED_EVENTS:
                    continue

                # Apply GeoMapper Logic
                processed_feature = process_warning(feature)
                
                # Process through registry (handles deduplication)
                is_new, alert_id = registry.process_alert(processed_feature, current_time)
                
                if alert_id:
                    if is_new:
                        new_count += 1
                    else:
                        updated_count += 1
        
        return new_count, updated_count
        
    except Exception as e:
        raise e


# =============================================================================
# Legacy Functions (for backward compatibility)
# =============================================================================

def download_alerts_legacy(dt: datetime):
    """
    Legacy version that saves to timestamped files.
    Kept for backward compatibility and migration purposes.
    
    Args:
        dt: Current datetime
    """
    url = "https://api.weather.gov/alerts/active"

    # Ensure output directory exists
    if not fs.MRMS_NWS_DIR.exists():
        fs.MRMS_NWS_DIR.mkdir(parents=True, exist_ok=True)

    # Clean old files
    fs.clean_files_by_age(fs.MRMS_NWS_DIR, max_age_minutes=120)

    # Output filename: alerts_active_YYYYMMDD-HHMM00.json
    filename = f"alerts_active_{dt.strftime('%Y%m%d-%H%M00')}.json"
    output_path = fs.MRMS_NWS_DIR / filename

    io_manager.write_info(f"Downloading active alerts to {output_path} (legacy mode)...")

    headers = {
        "User-Agent": "(EdgeWARN/1.0, contact@edgewarn.com)",
        "Accept": "application/geo+json"
    }

    req = urllib.request.Request(url, headers=headers)

    try:
        with urllib.request.urlopen(req) as response:
            with tempfile.NamedTemporaryFile(delete=False) as tmp:
                tmp.write(response.read())
                temp_path = tmp.name
        
        count = _process_nws_file_legacy(temp_path, output_path)
        os.remove(temp_path)

        io_manager.write_info(f"Successfully processed {count} alerts to {filename}")

    except Exception as e:
        io_manager.write_error(f"Failed to download/process NWS alerts: {e}")
        if output_path.exists():
            try:
                output_path.unlink()
            except OSError:
                pass


def _process_nws_file_legacy(input_path, output_path):
    """
    Legacy helper to process the raw NWS JSON file to a timestamped file.
    """
    count = 0
    try:
        with open(input_path, 'r', encoding='utf-8') as infile, open(output_path, 'w', encoding='utf-8') as outfile:
            outfile.write('{"@context": ["https://geojson.org/geojson-ld/geojson-context.jsonld", {"@version": "1.1", "wx": "https://api.weather.gov/ontology#", "@vocab": "https://api.weather.gov/ontology#"}], "type": "FeatureCollection", "features": [')

            first = True
            features = ijson.items(infile, 'features.item')

            for feature in features:
                props = feature.get('properties', {})
                event = props.get('event')

                if event in DROPPED_EVENTS:
                    continue

                processed_feature = process_warning(feature)
                
                if not first:
                    outfile.write(',')
                else:
                    first = False

                json.dump(processed_feature, outfile, cls=DecimalEncoder)
                count += 1
            
            outfile.write(']}')
            
        return count
    except Exception as e:
        raise e


if __name__ == "__main__":
    # Test block
    fs.initialize_filesystem()
    import asyncio
    asyncio.run(download_alerts_async(datetime.now(timezone.utc)))
