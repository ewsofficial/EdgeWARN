
import json
from pathlib import Path
from typing import Dict, Any, List, Optional
from shapely.geometry import Polygon, MultiPolygon
from shapely.ops import unary_union

# NEW: GeoMapper Assets Path
# Path: src/EdgeWARN/core/ingest/nws/geomapper.py
# parents[5] = project root (EdgeWARN-Core)
_ASSETS_DIR = Path(__file__).resolve().parents[5] / "assets" / "nws_zones"

# Keys to remove from properties (from GeoMapper)
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
        """Get polygon coordinates for a zone code."""
        if len(zone_code) < 2:
            return None
            
        state_code = zone_code[:2]
        
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

def round_coords(coords: List[List[float]], precision: int = 4) -> List[List[float]]:
    """Round coordinates to a specified precision."""
    return [[round(float(c[0]), precision), round(float(c[1]), precision)] for c in coords]

def extract_exterior_polygon(polygons: List[List], tolerance: float = 0.03) -> List:
    """
    Compute the union of multiple polygons, simplify geometry, 
    and return only exterior coordinates with rounded precision.
    """
    if not polygons:
        return []
    
    shapely_polys = []
    
    for poly_coords in polygons:
        if not poly_coords:
            continue
        try:
            if len(poly_coords) >= 3:
                if isinstance(poly_coords[0], (list, tuple)) and len(poly_coords[0]) == 2:
                    shapely_polys.append(Polygon(poly_coords))
        except Exception:
            continue
    
    if not shapely_polys:
        return []
    
    try:
        unified = unary_union(shapely_polys)
        unified = unified.buffer(0)
        
        # Simplify geometry to reduce point count (especially for coastlines/rivers)
        if tolerance > 0:
            unified = unified.simplify(tolerance=tolerance, preserve_topology=True)
        
        if unified.geom_type == 'Polygon':
            return [round_coords(list(unified.exterior.coords))]
        elif unified.geom_type == 'MultiPolygon':
            return [round_coords(list(p.exterior.coords)) for p in unified.geoms]
        else:
            return []
    except Exception:
        return []

def round_geojson_coords(geometry: Dict[str, Any], precision: int = 4) -> Dict[str, Any]:
    """Round coordinates in a GeoJSON geometry object."""
    if not geometry or 'coordinates' not in geometry:
        return geometry
    
    def _round_recursive(coords):
        if isinstance(coords, (int, float)):
            return round(float(coords), precision)
        elif isinstance(coords, (list, tuple)):
            return [_round_recursive(c) for c in coords]
        return coords
    
    geometry['coordinates'] = _round_recursive(geometry['coordinates'])
    return geometry

def process_warning(feature: Dict[str, Any]) -> Dict[str, Any]:
    """Process a single NWS warning feature (Map Geocodes + Clean Props)."""
    props = feature.get("properties", {})
    
    # Check for original geometry (e.g. storm-based warnings)
    has_geometry_to_skip = False
    if feature.get("geometry") and feature.get("geometry", {}).get("coordinates"):
        # Round existing geometry coordinates but DO NOT simplify
        feature["geometry"] = round_geojson_coords(feature["geometry"])
        has_geometry_to_skip = True
    
    # Check if we already have a zone-mapped Polygon (prevents double simplification/mapping)
    if feature.get("Polygon"):
        # Coordinate rounding for safety
        feature["Polygon"] = [round_coords(p) for p in feature["Polygon"]]
        has_geometry_to_skip = True

    # If geometry exists, skip the zone-to-polygon mapping 
    # (prevents simplification of precise polygons into zone boundaries)
    if has_geometry_to_skip:
        props.pop("geocode", None)
        for key in JUNK_KEYS:
            props.pop(key, None)
        return feature

    # Extract geocodes for zone mapping
    geocodes = []
    geocode_data = props.get("geocode", {})
    
    if isinstance(geocode_data, dict):
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
        # Optimization: NWS alerts often cover the same sets of zones.
        # Cache the result of the union operation based on the sorted tuple of zone codes.
        # We need to extract just the codes to form a cache key.
        zone_codes_tuple = tuple(sorted(geocodes))
        
        exterior = _get_cached_union_exterior(zone_codes_tuple)
        if exterior:
            feature["Polygon"] = exterior
            
    # Remove "geocode" if valid geometry exists
    has_geometry = False
    if feature.get("geometry") and feature.get("geometry", {}).get("coordinates"):
        has_geometry = True
    if feature.get("Polygon"):
        has_geometry = True
        
    if has_geometry:
        props.pop("geocode", None)
    
    # Remove junk keys from properties
    for key in JUNK_KEYS:
        props.pop(key, None)
    
    return feature

# Helper for caching union operations
from functools import lru_cache

@lru_cache(maxsize=1024)
def _get_cached_union_exterior(zone_codes_tuple):
    """
    Cached helper to compute union of zones.
    Args:
        zone_codes_tuple: Sorted tuple of zone codes
    Returns:
        List of exterior coordinates
    """
    if not zone_codes_tuple:
        return []

    all_poly_coords = []
    for code in zone_codes_tuple:
        poly = ZoneLookup.get_polygon(code)
        if poly:
            all_poly_coords.extend(poly)
    
    if not all_poly_coords:
        return []
        
    return extract_exterior_polygon(all_poly_coords)
