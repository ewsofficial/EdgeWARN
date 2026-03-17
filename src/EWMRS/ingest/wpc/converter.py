"""Converter to transform parsed WPC data to GeoJSON format."""

import json
from datetime import datetime, timezone
from typing import Dict, List, Tuple, Optional

from EWMRS.ingest.wpc.config import FEATURE_TYPES


def coords_to_geojson_linestring(coords: List[Tuple[float, float]]) -> List[List[float]]:
    """Convert list of (lat, lon) tuples to GeoJSON coordinate array.
    
    GeoJSON uses [longitude, latitude] order.
    
    Args:
        coords: List of (lat, lon) tuples
        
    Returns:
        List of [lon, lat] pairs for GeoJSON
    """
    return [[lon, lat] for lat, lon in coords]


def create_front_feature(coords: List[Tuple[float, float]], feature_type: str) -> Dict:
    """Create a GeoJSON Feature for a front line.
    
    Args:
        coords: List of (lat, lon) tuples
        feature_type: Type of front (COLD, WARM, STNRY, OCFNT, TROF)
        
    Returns:
        GeoJSON Feature dictionary
    """
    type_info = FEATURE_TYPES.get(feature_type, {"name": feature_type, "color": "#000000"})
    
    return {
        "type": "Feature",
        "geometry": {
            "type": "LineString",
            "coordinates": coords_to_geojson_linestring(coords)
        },
        "properties": {
            "feature_type": feature_type,
            "name": type_info["name"],
            "color": type_info["color"]
        }
    }


def create_pressure_center_feature(center: Dict) -> Dict:
    """Create a GeoJSON Feature for a pressure center.
    
    Args:
        center: Dictionary with type, pressure, lat, lon
        
    Returns:
        GeoJSON Feature dictionary
    """
    center_type = center["type"]
    type_info = FEATURE_TYPES.get(center_type, {"name": center_type, "color": "#000000"})
    
    return {
        "type": "Feature",
        "geometry": {
            "type": "Point",
            "coordinates": [center["lon"], center["lat"]]
        },
        "properties": {
            "feature_type": center_type,
            "name": type_info["name"],
            "color": type_info["color"],
            "pressure": center["pressure"],
            "label": "H" if center_type == "HIGH" else "L"
        }
    }


def parsed_to_geojson(parsed_data: Dict, source_timestamp: Optional[datetime] = None) -> Dict:
    """Convert parsed WPC data to a GeoJSON FeatureCollection.
    
    Args:
        parsed_data: Output from parse_coded_surface()
        source_timestamp: Optional datetime for the analysis time
        
    Returns:
        GeoJSON FeatureCollection dictionary
    """
    features = []
    
    # Add fronts
    front_types = [
        ("cold", "COLD"),
        ("warm", "WARM"),
        ("stationary", "STNRY"),
        ("occluded", "OCFNT"),
        ("trough", "TROF")
    ]
    
    for key, feature_type in front_types:
        for coords in parsed_data["fronts"].get(key, []):
            if len(coords) >= 2:
                features.append(create_front_feature(coords, feature_type))
    
    # Add pressure centers
    for high in parsed_data.get("highs", []):
        features.append(create_pressure_center_feature(high))
    
    for low in parsed_data.get("lows", []):
        features.append(create_pressure_center_feature(low))
    
    # Build valid time string
    valid_time_str = None
    if source_timestamp:
        valid_time_str = source_timestamp.isoformat()
    elif parsed_data.get("valid_time"):
        # Parse the MMDDHHZ format
        valid_code = parsed_data["valid_time"]
        try:
            month = int(valid_code[:2])
            day = int(valid_code[2:4])
            hour = int(valid_code[4:6])
            now = datetime.now(timezone.utc)
            year = now.year
            # Handle year boundary
            if month == 12 and now.month == 1:
                year -= 1
            valid_time_str = datetime(year, month, day, hour, 0, 0, tzinfo=timezone.utc).isoformat()
        except (ValueError, IndexError):
            pass
    
    return {
        "type": "FeatureCollection",
        "properties": {
            "valid_time": valid_time_str,
            "source": "WPC",
            "product": "Surface Analysis"
        },
        "features": features
    }


def save_geojson(geojson: Dict, filepath: str) -> None:
    """Save GeoJSON to file.
    
    Args:
        geojson: GeoJSON dictionary
        filepath: Output file path
    """
    with open(filepath, 'w') as f:
        json.dump(geojson, f, indent=2)
