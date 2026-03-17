"""Parser for WPC Coded Surface Analysis format.

The coded surface format uses 7-digit values for coordinates:
- First 3 digits: latitude in tenths of degrees (e.g., 338 = 33.8°N)
- Last 4 digits: longitude in tenths of degrees (e.g., 0787 = 78.7°W)

Note: Longitudes are stored as positive values but represent West longitude
for North American data.

Data can span multiple lines - continuation lines don't start with a keyword.
"""

from typing import List, Dict, Tuple, Optional
import re

# Keywords that start a new entry
KEYWORDS = {"VALID", "HIGHS", "LOWS", "COLD", "WARM", "STNRY", "OCFNT", "TROF"}


def decode_coordinate(code: str) -> Tuple[float, float]:
    """Decode a 7-digit coordinate code to (lat, lon).
    
    Args:
        code: 7-digit string like "3380787" (33.8°N, 78.7°W)
        
    Returns:
        Tuple of (latitude, longitude) in decimal degrees.
        Longitude is returned as negative (West) for standard GeoJSON.
    """
    if len(code) != 7:
        raise ValueError(f"Invalid coordinate code length: {code}")
    
    # First 3 digits = latitude in tenths
    lat_tenths = int(code[:3])
    lat = lat_tenths / 10.0
    
    # Last 4 digits = longitude in tenths
    lon_tenths = int(code[3:])
    lon = lon_tenths / 10.0
    
    # Convert to West longitude (negative) for GeoJSON standard
    lon = -lon
    
    return (lat, lon)


def _merge_continuation_lines(lines: List[str]) -> List[str]:
    """Merge continuation lines with their parent keyword lines.
    
    Continuation lines don't start with a known keyword and should be
    appended to the previous line.
    
    Args:
        lines: Raw lines from the file
        
    Returns:
        List of merged lines where each line starts with a keyword
    """
    merged = []
    current_line = ""
    
    for line in lines:
        line = line.strip()
        if not line:
            continue
        
        # Check if this line starts with a keyword
        first_word = line.split()[0] if line.split() else ""
        
        if first_word in KEYWORDS:
            # Save previous line if exists
            if current_line:
                merged.append(current_line)
            current_line = line
        elif current_line:
            # Continuation line - append to current
            current_line += " " + line
        # else: orphan line before any keyword, skip it
    
    # Don't forget the last line
    if current_line:
        merged.append(current_line)
    
    return merged


def parse_pressure_centers(tokens: List[str], center_type: str) -> List[Dict]:
    """Parse HIGH or LOW pressure center entries.
    
    Format: pressure coord pressure coord ...
    Example: 1027 3380787 1027 3791070 ...
    
    Args:
        tokens: List of tokens (pressure and coordinate values)
        center_type: Either "HIGH" or "LOW"
        
    Returns:
        List of pressure center dictionaries with lat, lon, pressure
    """
    centers = []
    
    i = 0
    while i < len(tokens) - 1:
        try:
            pressure = int(tokens[i])
            coord_code = tokens[i + 1]
            
            if len(coord_code) == 7 and coord_code.isdigit():
                lat, lon = decode_coordinate(coord_code)
                centers.append({
                    "type": center_type,
                    "pressure": pressure,
                    "lat": lat,
                    "lon": lon
                })
                i += 2
            else:
                i += 1
        except (ValueError, IndexError):
            i += 1
            continue
    
    return centers


def parse_front_coords(tokens: List[str]) -> List[Tuple[float, float]]:
    """Parse coordinate tokens into (lat, lon) pairs.
    
    Args:
        tokens: List of 7-digit coordinate strings
        
    Returns:
        List of (lat, lon) tuples representing the polyline vertices
    """
    coords = []
    
    for token in tokens:
        if len(token) == 7 and token.isdigit():
            try:
                lat, lon = decode_coordinate(token)
                coords.append((lat, lon))
            except ValueError:
                continue
    
    return coords


def parse_coded_surface(content: str) -> Dict:
    """Parse the full coded surface analysis content.
    
    Handles multi-line entries where data continues on the next line
    without a keyword prefix.
    
    Args:
        content: Full text content of the coded surface file
        
    Returns:
        Dictionary with parsed features:
        {
            "valid_time": str,
            "highs": [...],
            "lows": [...],
            "fronts": {
                "cold": [...],
                "warm": [...],
                "stationary": [...],
                "occluded": [...],
                "trough": [...]
            }
        }
    """
    result = {
        "valid_time": None,
        "highs": [],
        "lows": [],
        "fronts": {
            "cold": [],
            "warm": [],
            "stationary": [],
            "occluded": [],
            "trough": []
        }
    }
    
    # Split and merge continuation lines
    raw_lines = content.strip().split('\n')
    merged_lines = _merge_continuation_lines(raw_lines)
    
    for line in merged_lines:
        parts = line.split()
        if not parts:
            continue
        
        keyword = parts[0]
        tokens = parts[1:]  # Everything after the keyword
        
        # Extract valid time
        if keyword == "VALID":
            match = re.search(r'(\d{6})Z', line)
            if match:
                result["valid_time"] = match.group(1)
            continue
        
        # Parse pressure centers
        if keyword == "HIGHS":
            result["highs"].extend(parse_pressure_centers(tokens, "HIGH"))
            continue
            
        if keyword == "LOWS":
            result["lows"].extend(parse_pressure_centers(tokens, "LOW"))
            continue
        
        # Parse fronts and troughs
        if keyword == "COLD":
            coords = parse_front_coords(tokens)
            if len(coords) >= 2:
                result["fronts"]["cold"].append(coords)
            continue
            
        if keyword == "WARM":
            coords = parse_front_coords(tokens)
            if len(coords) >= 2:
                result["fronts"]["warm"].append(coords)
            continue
            
        if keyword == "STNRY":
            coords = parse_front_coords(tokens)
            if len(coords) >= 2:
                result["fronts"]["stationary"].append(coords)
            continue
            
        if keyword == "OCFNT":
            coords = parse_front_coords(tokens)
            if len(coords) >= 2:
                result["fronts"]["occluded"].append(coords)
            continue
            
        if keyword == "TROF":
            coords = parse_front_coords(tokens)
            if len(coords) >= 2:
                result["fronts"]["trough"].append(coords)
            continue
    
    return result
