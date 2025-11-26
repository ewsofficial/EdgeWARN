from datetime import datetime, timezone, timedelta
from pathlib import Path
import re

def extract_timestamp(filepath, use_timezone_utc=False, round_to_minute=False, isoformat=False):
    """
    Compact timestamp extractor for MRMS (YYYYMMDD_HHMMSS) and GOES (sYYYYDDDHHMMSST).
    """
    fname = Path(filepath).name
    dt = None

    # MRMS: YYYYMMDD[-_]HHMMSS
    if m := re.search(r"(\d{8})[-_](\d{6})", fname):
        dt = datetime.strptime(f"{m.group(1)}{m.group(2)}", "%Y%m%d%H%M%S")
    
    # GOES: sYYYYDDDHHMMSST (T = tenths of second, ignored for dt)
    elif m := re.search(r"s(\d{4})(\d{3})(\d{2})(\d{2})(\d{2})(\d{1})", fname):
        y, d, h, mn, s, _ = map(int, m.groups())
        dt = datetime(y, 1, 1, h, mn, s) + timedelta(days=d-1)

    if not dt: return None

    if round_to_minute: dt = dt.replace(second=0, microsecond=0)
    if use_timezone_utc and dt.tzinfo is None: dt = dt.replace(tzinfo=timezone.utc)
    
    return dt.isoformat() if isoformat else dt
