"""Main entry point for WPC Surface Analysis ingestion."""

import json
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional, Dict

from EWMRS.ingest.wpc.config import WPC_SFC_DIR
from EWMRS.ingest.wpc.parser import parse_coded_surface
from EWMRS.ingest.wpc.converter import parsed_to_geojson, save_geojson
from EWMRS.ingest.wpc.downloader import (
    download_coded_surface,
    get_output_filepath,
    get_latest_output_filepath
)
from EWMRS.util.io import IOManager

io_manager = IOManager("[WPC]")


def fetch_surface_analysis(dt: Optional[datetime] = None, save_timestamped: bool = False) -> Optional[Dict]:
    """Fetch, parse, and convert WPC surface analysis to GeoJSON.
    
    This is the main function to get WPC surface analysis data. It:
    1. Downloads the latest coded surface file
    2. Parses the coded format
    3. Converts to GeoJSON
    4. Saves to disk (latest.geojson, and optionally timestamped file)
    
    Args:
        dt: Reference datetime (defaults to now UTC)
        save_timestamped: If True, also save a timestamped copy
        
    Returns:
        GeoJSON dictionary, or None if failed
    """
    if dt is None:
        dt = datetime.now(timezone.utc)
    
    # Download
    io_manager.write_info("Starting WPC surface analysis fetch...")
    content = download_coded_surface(dt)
    
    if not content:
        io_manager.write_error("Failed to download WPC surface analysis")
        return None
    
    # Parse
    io_manager.write_info("Parsing coded surface data...")
    try:
        parsed = parse_coded_surface(content)
    except Exception as e:
        io_manager.write_error(f"Failed to parse surface data: {e}")
        return None
    
    # Convert to GeoJSON
    io_manager.write_info("Converting to GeoJSON...")
    geojson = parsed_to_geojson(parsed, dt)
    
    # Count features
    num_fronts = sum(1 for f in geojson["features"] if f["geometry"]["type"] == "LineString")
    num_centers = sum(1 for f in geojson["features"] if f["geometry"]["type"] == "Point")
    io_manager.write_info(f"Converted: {num_fronts} fronts/troughs, {num_centers} pressure centers")
    
    # Save latest
    latest_path = get_latest_output_filepath()
    save_geojson(geojson, str(latest_path))
    io_manager.write_info(f"Saved latest to: {latest_path}")
    
    # Optionally save timestamped copy
    if save_timestamped:
        ts_path = get_output_filepath(dt)
        save_geojson(geojson, str(ts_path))
        io_manager.write_info(f"Saved timestamped copy to: {ts_path}")
    
    return geojson


def run_wpc_ingest(log_queue=None):
    """Run the WPC ingest process.
    
    This function is designed to be called as a separate process.
    
    Args:
        log_queue: Optional multiprocessing Queue for logging
    """
    import sys
    from EWMRS.util.io import QueueWriter
    
    # Redirect output to queue if provided
    if log_queue is not None:
        sys.stdout = QueueWriter(log_queue)
        sys.stderr = QueueWriter(log_queue)
    
    try:
        # Fetch current (latest)
        dt_now = datetime.now(timezone.utc)
        result = fetch_surface_analysis(dt_now, save_timestamped=True)
        
        # Fetch previous (3 hours ago) to ensure coverage
        dt_prev = dt_now - timedelta(hours=3)
        fetch_surface_analysis(dt_prev, save_timestamped=True)
        
        if result:
            io_manager.write_info("WPC surface analysis ingest completed successfully")
        else:
            io_manager.write_error("WPC surface analysis ingest failed")
    except Exception as e:
        io_manager.write_error(f"WPC ingest error: {e}")


def clean_old_files(max_age_minutes: int = 360):
    """Remove old surface analysis files.
    
    Args:
        max_age_minutes: Maximum age in minutes (default 6 hours)
    """
    import time
    
    now = time.time()
    max_age_seconds = max_age_minutes * 60
    removed = 0
    
    for f in WPC_SFC_DIR.glob("surface_analysis_*.geojson"):
        try:
            age = now - f.stat().st_mtime
            if age > max_age_seconds:
                f.unlink()
                removed += 1
        except Exception:
            pass
    
    if removed > 0:
        io_manager.write_info(f"Cleaned up {removed} old WPC files")


if __name__ == "__main__":
    # Allow running directly for testing
    result = fetch_surface_analysis(save_timestamped=True)
    if result:
        print(json.dumps(result, indent=2))
