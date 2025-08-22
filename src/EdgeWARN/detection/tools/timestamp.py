from pathlib import Path
import re
import datetime

def extract_timestamp_from_filename(filepath):
    """
    Extract timestamp from MRMS filename with multiple pattern support.
    """
    filename = Path(filepath).name
    print(f"DEBUG: Extracting timestamp from filename: {filename}")
    
    patterns = [
        r'MRMS_MergedReflectivityQC_3D_(\d{8})-(\d{6})',
        r'(\d{8})-(\d{6})_renamed',
        r'(\d{8}-\d{6})',
        r'.*(\d{8})-(\d{6}).*'
    ]
    
    for pattern_idx, pattern in enumerate(patterns):
        match = re.search(pattern, filename)
        if match:
            groups = match.groups()
            print(f"DEBUG: Pattern {pattern_idx+1} matched: {groups}")
            
            if len(groups) == 2:
                date_str, time_str = groups
            else:
                combined = groups[0]
                date_str, time_str = combined[:8], combined[9:]
            
            try:
                formatted_time = (f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}T"
                                 f"{time_str[:2]}:{time_str[2:4]}:{time_str[4:6]}")
                print(f"DEBUG: Extracted timestamp: {formatted_time}")
                return formatted_time
            except (IndexError, ValueError) as e:
                print(f"DEBUG: Error formatting timestamp: {e}")
                continue
    
    fallback = datetime.utcnow().isoformat()
    print(f"DEBUG: Using fallback timestamp: {fallback}")
    return fallback