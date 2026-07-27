"""Downloader for WPC Coded Surface Analysis data."""

import urllib.request
import urllib.error
import ssl
from datetime import datetime, timezone, timedelta
from typing import Optional, Tuple
from pathlib import Path

from common.ingest.wpc.config import WPC_CODED_SFC_BASE_URL, VALID_HOURS
import util.file as fs
from util.io import IOManager

io_manager = IOManager("[WPC]")


def get_latest_valid_hour(dt: Optional[datetime] = None) -> Tuple[datetime, int]:
    dt = dt if dt is not None else datetime.now(timezone.utc)
    current_hour = dt.hour
    valid_hour = max([h for h in VALID_HOURS if h <= current_hour], default=21)
    if current_hour < VALID_HOURS[0]:
        valid_hour = VALID_HOURS[-1]
        dt = dt - timedelta(days=1)
    return dt, valid_hour


def build_url(dt: datetime, hour: int) -> str:
    date_str = dt.strftime("%Y%m%d")
    hour_str = f"{hour:02d}"
    filename = f"codsus{hour_str}_hr"
    return f"{WPC_CODED_SFC_BASE_URL}/{date_str}/{filename}"


def download_coded_surface(dt: Optional[datetime] = None) -> Optional[Tuple[str, datetime]]:
    ref_dt, valid_hour = get_latest_valid_hour(dt)
    url = build_url(ref_dt, valid_hour)
    io_manager.write_info(f"Downloading WPC surface analysis from: {url}")

    try:
        ctx = ssl.create_default_context()
        ctx.check_hostname = True
        ctx.verify_mode = ssl.CERT_REQUIRED

        with urllib.request.urlopen(url, timeout=30, context=ctx) as response:
            content = response.read().decode('utf-8', errors='replace')
            io_manager.write_info(f"Downloaded {len(content)} bytes")
            actual_time = ref_dt.replace(hour=valid_hour, minute=0, second=0, microsecond=0).replace(tzinfo=timezone.utc)
            return (content, actual_time)
    except urllib.error.HTTPError as e:
        io_manager.write_warning(f"HTTP error {e.code}: {e.reason}")
        return _try_fallback_download(ref_dt, valid_hour)
    except urllib.error.URLError as e:
        io_manager.write_error(f"URL error: {e.reason}")
        return None
    except Exception as e:
        io_manager.write_error(f"Download failed: {e}")
        return None


def _try_fallback_download(dt: datetime, failed_hour: int) -> Optional[Tuple[str, datetime]]:
    idx = VALID_HOURS.index(failed_hour) if failed_hour in VALID_HOURS else 0
    if idx > 0:
        fallback_hour = VALID_HOURS[idx - 1]
        fallback_dt = dt
    else:
        fallback_hour = VALID_HOURS[-1]
        fallback_dt = dt - timedelta(days=1)

    url = build_url(fallback_dt, fallback_hour)
    io_manager.write_info(f"Trying fallback URL: {url}")

    try:
        ctx = ssl.create_default_context()
        ctx.check_hostname = True
        ctx.verify_mode = ssl.CERT_REQUIRED

        with urllib.request.urlopen(url, timeout=30, context=ctx) as response:
            content = response.read().decode('utf-8', errors='replace')
            io_manager.write_info(f"Fallback downloaded {len(content)} bytes")
            actual_time = fallback_dt.replace(hour=fallback_hour, minute=0, second=0, microsecond=0).replace(tzinfo=timezone.utc)
            return (content, actual_time)
    except Exception as e:
        io_manager.write_error(f"Fallback download also failed: {e}")
        return None


def get_output_filepath(dt: Optional[datetime] = None) -> Path:
    wpc_sfc_dir = fs.WPC_SFC_DIR
    wpc_sfc_dir.mkdir(parents=True, exist_ok=True)
    if dt is None:
        dt = datetime.now(timezone.utc)
    dt_valid, valid_hour = get_latest_valid_hour(dt)
    filename = f"wpc_sfc_{dt_valid.strftime('%Y%m%d')}-{valid_hour:02d}0000.geojson"
    return wpc_sfc_dir / filename


def get_latest_output_filepath() -> Path:
    wpc_sfc_dir = fs.WPC_SFC_DIR
    wpc_sfc_dir.mkdir(parents=True, exist_ok=True)
    return wpc_sfc_dir / "latest.geojson"
