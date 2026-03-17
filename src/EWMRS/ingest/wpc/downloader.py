"""Downloader for WPC Coded Surface Analysis data."""

import urllib.request
import urllib.error
import ssl
from datetime import datetime, timezone
from typing import Optional, Tuple
from pathlib import Path

from EWMRS.ingest.wpc.config import WPC_CODED_SFC_BASE_URL, VALID_HOURS
from EWMRS.util.file import WPC_SFC_DIR
from EWMRS.util.io import IOManager

io_manager = IOManager("[WPC]")


def get_latest_valid_hour(dt: Optional[datetime] = None) -> Tuple[datetime, int]:
    """Get the most recent valid analysis hour.
    
    WPC surface analysis is available at 00, 03, 06, 09, 12, 15, 18, 21 UTC.
    
    Args:
        dt: Reference datetime (defaults to now UTC)
        
    Returns:
        Tuple of (date, hour) for the latest valid analysis
    """
    if dt is None:
        dt = datetime.now(timezone.utc)
    
    current_hour = dt.hour
    
    # Find the most recent valid hour
    valid_hour = max([h for h in VALID_HOURS if h <= current_hour], default=21)
    
    # If we're before the first valid hour of the day, use previous day's last hour
    if current_hour < VALID_HOURS[0]:
        valid_hour = VALID_HOURS[-1]
        # Move to previous day
        from datetime import timedelta
        dt = dt - timedelta(days=1)
    
    return dt, valid_hour


def build_url(dt: datetime, hour: int) -> str:
    """Build the URL for a specific coded surface file.
    
    Args:
        dt: Date for the analysis
        hour: Hour (0, 3, 6, 9, 12, 15, 18, 21)
        
    Returns:
        Full URL to the coded surface file
    """
    date_str = dt.strftime("%Y%m%d")
    hour_str = f"{hour:02d}"
    filename = f"codsus{hour_str}_hr"
    
    return f"{WPC_CODED_SFC_BASE_URL}/{date_str}/{filename}"


def download_coded_surface(dt: Optional[datetime] = None) -> Optional[str]:
    """Download the latest coded surface analysis file.
    
    Args:
        dt: Reference datetime (defaults to now UTC)
        
    Returns:
        Content of the coded surface file, or None if download failed
    """
    ref_dt, valid_hour = get_latest_valid_hour(dt)
    url = build_url(ref_dt, valid_hour)
    
    io_manager.write_info(f"Downloading WPC surface analysis from: {url}")
    
    try:
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        
        with urllib.request.urlopen(url, timeout=30, context=ctx) as response:
            content = response.read().decode('utf-8', errors='replace')
            io_manager.write_info(f"Downloaded {len(content)} bytes")
            return content
    except urllib.error.HTTPError as e:
        io_manager.write_warning(f"HTTP error {e.code}: {e.reason}")
        # Try the previous valid hour as fallback
        return _try_fallback_download(ref_dt, valid_hour)
    except urllib.error.URLError as e:
        io_manager.write_error(f"URL error: {e.reason}")
        return None
    except Exception as e:
        io_manager.write_error(f"Download failed: {e}")
        return None


def _try_fallback_download(dt: datetime, failed_hour: int) -> Optional[str]:
    """Try downloading from the previous valid hour.
    
    Args:
        dt: Date for the analysis
        failed_hour: Hour that failed to download
        
    Returns:
        Content of the coded surface file, or None if fallback failed
    """
    from datetime import timedelta
    
    # Find the previous valid hour
    idx = VALID_HOURS.index(failed_hour) if failed_hour in VALID_HOURS else 0
    
    if idx > 0:
        fallback_hour = VALID_HOURS[idx - 1]
        fallback_dt = dt
    else:
        # Go to previous day
        fallback_hour = VALID_HOURS[-1]
        fallback_dt = dt - timedelta(days=1)
    
    url = build_url(fallback_dt, fallback_hour)
    io_manager.write_info(f"Trying fallback URL: {url}")
    
    try:
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        
        with urllib.request.urlopen(url, timeout=30, context=ctx) as response:
            content = response.read().decode('utf-8', errors='replace')
            io_manager.write_info(f"Fallback downloaded {len(content)} bytes")
            return content
    except Exception as e:
        io_manager.write_error(f"Fallback download also failed: {e}")
        return None


def get_output_filepath(dt: Optional[datetime] = None) -> Path:
    """Get the output filepath for the GeoJSON file.
    
    Format: wpc_sfc_YYYYMMDD-HHz.geojson (e.g., wpc_sfc_20260122-12z.geojson)
    
    Args:
        dt: Reference datetime
        
    Returns:
        Path to the output GeoJSON file
    """
    if dt is None:
        dt = datetime.now(timezone.utc)
        
    # Snap to the nearest valid analysis hour
    dt_valid, valid_hour = get_latest_valid_hour(dt)
        
    # Ensure WPC_SFC_DIR exists (it is initialized in file.py but we should double check/create)
    WPC_SFC_DIR.mkdir(parents=True, exist_ok=True)
    
    filename = f"wpc_sfc_{dt_valid.strftime('%Y%m%d')}-{valid_hour:02d}0000.geojson"
    return WPC_SFC_DIR / filename


def get_latest_output_filepath() -> Path:
    """Get the filepath for the 'latest' surface analysis file.
    
    Returns:
        Path to the latest.geojson file
    """
    # Ensure directory exists
    WPC_SFC_DIR.mkdir(parents=True, exist_ok=True)
    return WPC_SFC_DIR / "latest.geojson"
