"""
GeoMapper CTAM Module

Maps NWS geocodes to precise polygon geometries using a local asset library.
Also cleans up unnecessary properties from the warning data.
"""

import json
from pathlib import Path
from typing import Dict, Any, List, Optional
from shapely.geometry import Polygon, MultiPolygon
from shapely.ops import unary_union

# Assets path (relative to project root)
# Path: src/EdgeWARN/core/ctam/modules/GeoMapper/__init__.py
# parents[6] = project root
_ASSETS_DIR = Path(__file__).resolve().parents[6] / "assets" / "nws_zones"

# Keys to remove from properties
JUNK_KEYS = [
    "references",
    "sender", 
    "parameters",
    "instruction",
    "response",
    "scope",
    "code",
    "language",
    "web",
    "eventCode",
]


class ZoneLookup:
    """Lazy-loading lookup for NWS zone polygons."""
    
    _cache: Dict[str, Dict[str, List]] = {}  # {state_code: {zone_code: polygon_coords}}
    
    @classmethod
    def get_polygon(cls, zone_code: str) -> Optional[List]:
        """
        Get polygon coordinates for a zone code.
        
        Args:
            zone_code: NWS zone code (e.g., 'FLC015', 'TXZ001')
            
        Returns:
            List of polygon coordinate rings, or None if not found.
        """
        if len(zone_code) < 2:
            return None
            
        state_code = zone_code[:2]
        
        # Load state data if not cached
        if state_code not in cls._cache:
            cls._load_state(state_code)
        
        return cls._cache.get(state_code, {}).get(zone_code)
    
    @classmethod
    def _load_state(cls, state_code: str) -> None:
        """Load zone data for a state into cache."""
        state_file = _ASSETS_DIR / state_code / "zones.json"
        
        if not state_file.exists():
            cls._cache[state_code] = {}
            return
        
        try:
            with open(state_file, "r", encoding="utf-8") as f:
                zones = json.load(f)
            
            cls._cache[state_code] = {
                zone["code"]: zone["Polygon"]
                for zone in zones
                if "code" in zone and "Polygon" in zone
            }
        except Exception:
            cls._cache[state_code] = {}


def extract_exterior_polygon(polygons: List[List]) -> List:
    """
    Compute the union of multiple polygons and return only exterior coordinates.
    
    Args:
        polygons: List of polygon coordinate lists (may be nested for multi-polygon)
        
    Returns:
        List of exterior coordinate rings (no holes).
        If multiple disjoint zones, returns a list of exterior rings.
    """
    if not polygons:
        return []
    
    shapely_polys = []
    
    for poly_coords in polygons:
        if not poly_coords:
            continue
        
        # Handle nested structure - could be single polygon or multi-polygon parts
        # Each poly_coords is a list of points [lon, lat]
        try:
            # Try to create a polygon from the coordinates
            if len(poly_coords) >= 3:
                # Check if it's a ring (list of [lon, lat] points)
                if isinstance(poly_coords[0], (list, tuple)) and len(poly_coords[0]) == 2:
                    shapely_polys.append(Polygon(poly_coords))
        except Exception:
            continue
    
    if not shapely_polys:
        return []
    
    # Union all polygons
    try:
        unified = unary_union(shapely_polys)
        
        # Merge touching polygons using buffer(0) to fix precision issues
        unified = unified.buffer(0)
        
        # Extract exterior coordinates
        if unified.geom_type == 'Polygon':
            return [list(unified.exterior.coords)]
        elif unified.geom_type == 'MultiPolygon':
            return [list(p.exterior.coords) for p in unified.geoms]
        else:
            return []
    except Exception:
        return []


def process_warning(feature: Dict[str, Any]) -> Dict[str, Any]:
    """
    Process a single NWS warning feature.
    
    1. Look up polygon(s) for geocodes
    2. Union and extract exterior if multiple
    3. Remove junk keys
    4. Add Polygon key
    
    Args:
        feature: GeoJSON feature dict from NWS API
        
    Returns:
        Processed feature dict
    """
    props = feature.get("properties", {})
    
    # Extract geocodes
    geocodes = []
    geocode_data = props.get("geocode", {})
    
    # Geocodes can be in different formats - try common ones
    # UGC format: ['TXC001', 'TXC002']
    # SAME format: ['048001', '048002']
    if isinstance(geocode_data, dict):
        # Prefer UGC (zone codes like TXC001)
        ugc_codes = geocode_data.get("UGC", [])
        if ugc_codes:
            geocodes = ugc_codes
    elif isinstance(geocode_data, list):
        geocodes = geocode_data
    
    # Collect all polygon coordinates from matching zones
    all_polygon_coords = []
    
    for code in geocodes:
        poly = ZoneLookup.get_polygon(code)
        if poly:
            all_polygon_coords.extend(poly)
    
    # Compute union and extract exterior
    if all_polygon_coords:
        exterior = extract_exterior_polygon(all_polygon_coords)
        if exterior:
            # Add polygon to feature (top level, not properties)
            feature["Polygon"] = exterior
    
    # Remove junk keys from properties
    for key in JUNK_KEYS:
        props.pop(key, None)
    
    # Also remove geocode since we've converted it
    props.pop("geocode", None)
    
    return feature


def process_file(input_path: Path, output_path: Path) -> int:
    """
    Process an entire NWS alerts file.
    
    Args:
        input_path: Path to raw NWS GeoJSON file
        output_path: Path to write processed output
        
    Returns:
        Number of features processed
    """
    with open(input_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    
    features = data.get("features", [])
    processed_features = []
    
    for feature in features:
        processed = process_warning(feature)
        processed_features.append(processed)
    
    # Write output
    output_data = {
        "type": "FeatureCollection",
        "features": processed_features
    }
    
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(output_data, f)
    
    return len(processed_features)
