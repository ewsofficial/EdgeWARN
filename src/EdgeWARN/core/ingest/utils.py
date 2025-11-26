import re
from datetime import datetime, timezone, timedelta

GOES_PATTERN = re.compile(r"s(\d{4})(\d{3})(\d{2})(\d{2})(\d{2})(\d{1})")
MRMS_PATTERN = re.compile(r"(\d{4})(\d{2})(\d{2})[-_](\d{2})(\d{2})(\d{2})")

def extract_timestamp(filepath: str) -> datetime:
    """
    Extract timestamp from filepath and return timezone-aware datetime object.

    Supports multiple formats:
    - MRMS: YYYYMMDD-HHMMSS or YYYYMMDD_HHMMSS
    - GOES: sYYYYDDDHHMMSSS (start time from GOES file naming convention)

    Returns a default timestamp if no pattern is found.

    Args:
        filepath (str): The filename/filepath string to search for timestamp

    Returns:
        datetime: A timezone-aware datetime object (UTC)
    """
    goes_match = GOES_PATTERN.search(filepath)
    if goes_match:
        year, day_of_year, hour, minute, second, _ = map(int, goes_match.groups())
        dt_aware = datetime(year, 1, 1, hour, minute, second, tzinfo=timezone.utc)
        return dt_aware + timedelta(days=day_of_year - 1)

    mrms_match = MRMS_PATTERN.search(filepath)
    if mrms_match:
        year, month, day, hour, minute, second = map(int, mrms_match.groups())
        return datetime(year, month, day, hour, minute, second, tzinfo=timezone.utc)

    return datetime.now(timezone.utc).replace(second=0, microsecond=0)
