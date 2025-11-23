import re
from datetime import datetime, timezone

def extract_timestamp(filepath):
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
    # Pattern for GOES format: sYYYYDDDHHMMSSS (e.g., s20243241234567)
    goes_pattern = r's(\d{4})(\d{3})(\d{2})(\d{2})(\d{2})(\d{1})'
    goes_match = re.search(goes_pattern, filepath)
    
    if goes_match:
        year = int(goes_match.group(1))
        day_of_year = int(goes_match.group(2))
        hour = int(goes_match.group(3))
        minute = int(goes_match.group(4))
        second = int(goes_match.group(5))
        
        # Convert Julian day to datetime
        dt_aware = datetime(year, 1, 1, hour, minute, second, 0, tzinfo=timezone.utc)
        # Add the day of year offset (subtract 1 because Jan 1 is day 1)
        from datetime import timedelta
        dt_aware = dt_aware + timedelta(days=day_of_year - 1)
        
        return dt_aware
    
    # Pattern for MRMS: YYYYMMDD-HHMMSS or YYYYMMDD_HHMMSS
    mrms_pattern = r'(\d{4})(\d{2})(\d{2})[-_](\d{2})(\d{2})(\d{2})'
    mrms_match = re.search(mrms_pattern, filepath)
    
    if mrms_match:
        year, month, day, hour, minute, second = map(int, mrms_match.groups())
        dt_aware = datetime(year, month, day, hour, minute, 0, 0, tzinfo=timezone.utc)
        return dt_aware
    
    # Return current time in UTC as default
    return datetime.now(timezone.utc).replace(second=0, microsecond=0)
