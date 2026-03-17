"""
Timestamp utilities for MRMS data processing.
"""
import datetime


def round_to_nearest_even_minute(ts: datetime.datetime) -> datetime.datetime:
    """
    Round a timestamp to the nearest even minute.
    
    Examples:
        23:59:30 → 00:00:00 (next day if at midnight boundary)
        23:59:00 → 00:00:00
        23:58:59 → 23:58:00
        23:57:30 → 23:58:00
        23:56:00 → 23:56:00
    
    Args:
        ts: Timezone-aware datetime object
        
    Returns:
        Datetime rounded to nearest even minute with seconds/microseconds zeroed
    """
    # First, zero out seconds and microseconds
    base = ts.replace(second=0, microsecond=0)
    
    # Calculate distance to previous and next even minute
    current_minute = base.minute
    
    if current_minute % 2 == 0:
        # Already on even minute, check if seconds push us to next
        if ts.second >= 30:
            # Round up to next even minute
            return base + datetime.timedelta(minutes=2)
        else:
            return base
    else:
        # Odd minute: decide whether to round down or up
        if ts.second >= 30:
            # Round up to next even minute
            return base + datetime.timedelta(minutes=1)
        else:
            # Round down to previous even minute
            return base - datetime.timedelta(minutes=1)
