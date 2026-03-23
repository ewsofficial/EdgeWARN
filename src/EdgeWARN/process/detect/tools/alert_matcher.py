"""
Alert-to-Cell Spatial Matching Module

Matches storm cells with active NWS alerts based on spatial intersection.
Only includes convective and flood-related alert types.

Usage:
    from EdgeWARN.process.detect.tools.alert_matcher import match_alerts_to_cells
    
    # Add alerts to cell entries
    entries_with_alerts = match_alerts_to_cells(cell_entries, registry_path)
"""

from typing import Dict, List, Any, Optional
from pathlib import Path
from shapely.geometry import Polygon, Point
from shapely.prepared import prep
from shapely.strtree import STRtree
import numpy as np
import json

from util.io import IOManager

io_manager = IOManager("[AlertMatcher]")

# Convective and flood-related alert events to include
CONVECTIVE_FLOOD_EVENTS = {
    # Convective events
    "Tornado Warning",
    "Severe Thunderstorm Warning",
    "Tornado Watch",
    "Severe Thunderstorm Watch",
    "Special Weather Statement",
    "Severe Weather Statement",
    # Flood events
    "Flash Flood Warning",
}


def load_active_alerts(registry_dir: Path, target_timestamp: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    Load active alerts from the AlertRegistry directory.
    Uses the closest snapshot in 'timestamps/' before or equal to the target_timestamp,
    and loads the corresponding feature files from 'ids/'.
    
    Args:
        registry_dir: Path to the official alerts directory containing ids/ and timestamps/
        target_timestamp: Optional ISO datetime string to find alerts active at that time.
                          If None, uses the latest snapshot.
        
    Returns:
        List of alert feature dictionaries
    """
    ts_dir = registry_dir / "timestamps"
    ids_dir = registry_dir / "ids"
    
    if not ts_dir.exists() or not ids_dir.exists():
        return []
    
    try:
        ts_files = sorted([f for f in ts_dir.glob("*.json") if not f.name.startswith(".tmp")])
        if not ts_files:
            return []
            
        selected_file = ts_files[-1]
        
        if target_timestamp:
            # Parse target_timestamp
            from datetime import datetime
            try:
                target_dt = datetime.fromisoformat(target_timestamp.replace('Z', '+00:00'))
                target_str = target_dt.strftime("%Y%m%d-%H%M%S")
            except Exception:
                target_str = target_timestamp.replace(":", "").replace("-", "").replace("T", "-").replace("Z", "")
                
            # Find closest file <= target
            valid_files = [f for f in ts_files if f.stem <= target_str]
            if valid_files:
                selected_file = valid_files[-1]
                
        with open(selected_file, 'r', encoding='utf-8') as f:
            ts_data = json.load(f)
            
        active_ids = ts_data.get("alerts", [])
        features = []
        
        for alert_id in active_ids:
            if isinstance(alert_id, dict):
                alert_id = alert_id.get("id") or alert_id.get("urn_oid")

            if not isinstance(alert_id, str):
                continue

            safe_id = alert_id.replace(":", "_").replace("/", "_") + ".json"
            feature_path = ids_dir / safe_id
            if feature_path.exists():
                try:
                    with open(feature_path, 'r', encoding='utf-8') as f:
                        alert_data = json.load(f)
                        if "feature" in alert_data:
                            features.append(alert_data["feature"])
                except Exception:
                    continue
        return features
        
    except Exception as e:
        io_manager.write_warning(f"Failed to load alerts from registry directory: {e}")
        return []


def filter_convective_flood_alerts(alerts: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Filter alerts to only include convective and flood-related events.
    
    Args:
        alerts: List of alert feature dictionaries
        
    Returns:
        Filtered list containing only convective/flood alerts
    """
    filtered = []
    for alert in alerts:
        props = alert.get("properties", {})
        event = props.get("event", "")
        if event in CONVECTIVE_FLOOD_EVENTS:
            filtered.append(alert)
    return filtered


def _extract_alert_id(feature: Dict[str, Any]) -> Optional[str]:
    """
    Extract unique alert ID from a feature.
    
    NWS alerts have an 'id' field that is a URL like:
    https://api.weather.gov/alerts/urn:oid:2.49.0.1.840.0...
    
    We extract just the URN portion for a cleaner ID.
    
    Args:
        feature: GeoJSON feature from NWS API
        
    Returns:
        Alert ID string or None if not found
    """
    alert_id = feature.get('id')
    
    if alert_id:
        if isinstance(alert_id, str) and '/alerts/' in alert_id:
            return alert_id.split('/alerts/')[-1]
        return alert_id
    
    # Fallback to properties['id']
    props = feature.get('properties', {})
    alert_id = props.get('id')
    
    if alert_id:
        if isinstance(alert_id, str) and '/alerts/' in alert_id:
            return alert_id.split('/alerts/')[-1]
        return alert_id
    
    return None


def _get_alert_polygon(alert: Dict[str, Any]) -> Optional[Polygon]:
    """
    Extract polygon geometry from an alert.
    
    Checks for:
    1. Pre-computed "Polygon" field from GeoMapper
    2. Geometry.coordinates from the alert
    
    Args:
        alert: Alert feature dictionary
        
    Returns:
        shapely Polygon or None if invalid
    """
    def _normalize_coords(coords):
        """Convert [-180, 180] coords to [0, 360] to match radar space."""
        return [[c[0] % 360, c[1]] for c in coords]
    
    # Check for GeoMapper's Polygon field
    if "Polygon" in alert:
        poly_data = alert["Polygon"]
        if poly_data and len(poly_data) > 0:
            # Polygon field contains list of rings, take the first one
            coords = poly_data[0]
            if len(coords) >= 3:
                try:
                    return Polygon(_normalize_coords(coords))
                except Exception:
                    pass
    
    # Check for standard geometry
    geom = alert.get("geometry")
    if geom and geom.get("type") == "Polygon":
        coords = geom.get("coordinates", [])
        if coords and len(coords[0]) >= 3:
            try:
                return Polygon(_normalize_coords(coords[0]))
            except Exception:
                pass
    
    return None


def _get_cell_centroid(cell: Dict[str, Any]) -> Optional[Point]:
    """
    Extract centroid from a cell entry.
    
    Args:
        cell: Cell entry dictionary
        
    Returns:
        shapely Point or None if invalid
    """
    centroid = cell.get("centroid")
    if centroid and len(centroid) == 2:
        try:
            # centroid is [lat, lon], shapely uses (x, y) = (lon, lat)
            return Point(centroid[1], centroid[0])
        except Exception:
            pass
    return None


def _get_cell_polygon(cell: Dict[str, Any]) -> Optional[Polygon]:
    """
    Extract polygon from a cell's bbox.
    
    Args:
        cell: Cell entry dictionary
        
    Returns:
        shapely Polygon or None if invalid
    """
    bbox = cell.get("bbox")
    if bbox and len(bbox) >= 3:
        try:
            # bbox is list of [lat, lon] points
            coords = [[pt[1], pt[0]] for pt in bbox]  # Convert to (lon, lat)
            return Polygon(coords)
        except Exception:
            pass
    return None


def match_alerts_to_cell(cell: Dict[str, Any], prepped_alerts: List[tuple]) -> List[str]:
    """
    Find all alert IDs that intersect with a given cell.
    
    Uses polygon-to-polygon intersection for precision, falling back to 
    centroid-based matching if cell polygon is unavailable.
    
    Args:
        cell: Cell entry dictionary with "centroid" and/or "bbox"
        prepped_alerts: List of (alert_id, prepared_polygon) tuples
        
    Returns:
        List of matching alert ID strings
    """
    matching_ids = []
    
    # Try to get cell polygon (bbox) first for better precision
    cell_geom = _get_cell_polygon(cell)
    
    # Fallback to centroid if no bbox
    if cell_geom is None:
        cell_geom = _get_cell_centroid(cell)
    
    if cell_geom is None:
        return matching_ids
    
    # Add a tiny buffer (approx 500m) to account for edge precision/rounding
    try:
        if isinstance(cell_geom, Polygon):
            # Using a slightly larger buffer for polygons to ensure overlap detection
            match_geom = cell_geom.buffer(0.005) 
        else:
            # For points, use a smaller buffer
            match_geom = cell_geom.buffer(0.01)
    except Exception:
        match_geom = cell_geom
    
    for alert_id, alert_poly_prepared in prepped_alerts:
        # Check if cell geometry intersects with alert polygon
        try:
            if alert_poly_prepared.intersects(match_geom):
                matching_ids.append(alert_id)
        except Exception:
            # Skip on geometry errors
            continue
    
    return matching_ids


def _build_match_geometry(cell: Dict[str, Any]):
    """Build matching geometry for a cell with precision buffer."""
    cell_geom = _get_cell_polygon(cell)
    if cell_geom is None:
        cell_geom = _get_cell_centroid(cell)

    if cell_geom is None:
        return None

    try:
        if isinstance(cell_geom, Polygon):
            return cell_geom.buffer(0.005)
        return cell_geom.buffer(0.01)
    except Exception:
        return cell_geom


def _normalize_tree_query_indices(query_result, geom_id_to_index):
    """Normalize STRtree query output to a list of integer indices."""
    if query_result is None:
        return []

    if isinstance(query_result, np.ndarray):
        if query_result.size == 0:
            return []
        if np.issubdtype(query_result.dtype, np.integer):
            return query_result.tolist()
        first_item = query_result[0]
        if isinstance(first_item, (int, np.integer)):
            return [int(item) for item in query_result.tolist()]
        return [geom_id_to_index.get(id(geom)) for geom in query_result if id(geom) in geom_id_to_index]

    if isinstance(query_result, list):
        if not query_result:
            return []
        if isinstance(query_result[0], (int, np.integer)):
            return [int(idx) for idx in query_result]
        return [geom_id_to_index.get(id(geom)) for geom in query_result if id(geom) in geom_id_to_index]

    return []


def _match_alerts_with_strtree(
    cell: Dict[str, Any],
    alert_ids: List[str],
    alert_polys: List[Polygon],
    alert_prepared: List[Any],
    spatial_index: STRtree,
    geom_id_to_index: Dict[int, int],
) -> List[str]:
    """Match alerts using STRtree candidate filtering before precise intersects."""
    match_geom = _build_match_geometry(cell)
    if match_geom is None:
        return []

    try:
        candidate_query = spatial_index.query(match_geom)
    except Exception:
        candidate_query = []

    candidate_indices = _normalize_tree_query_indices(candidate_query, geom_id_to_index)
    if not candidate_indices:
        return []

    matching_ids = []
    for idx in candidate_indices:
        if idx is None or idx < 0 or idx >= len(alert_polys):
            continue
        try:
            if alert_prepared[idx].intersects(match_geom):
                matching_ids.append(alert_ids[idx])
        except Exception:
            continue

    return matching_ids


def match_alerts_to_cells(
    cell_entries: List[Dict[str, Any]],
    registry_dir: Path,
    target_timestamp: Optional[str] = None
) -> List[Dict[str, Any]]:
    """
    Match active convective/flood alerts to each cell entry.
    
    Adds an "alerts" key to each cell containing a list of matching alert IDs.
    
    Args:
        cell_entries: List of cell entry dictionaries
        registry_dir: Path to the alerts registry directory
        target_timestamp: Optional timestamp of the cell detection to use for matching
        
    Returns:
        Cell entries with "alerts" key added to each
    """
    if not cell_entries:
        return []
    
    # Load and filter alerts
    all_alerts = load_active_alerts(registry_dir, target_timestamp)
    filtered_alerts = filter_convective_flood_alerts(all_alerts)
    
    if not filtered_alerts:
        # No active convective/flood alerts, add empty alerts list to each cell
        for cell in cell_entries:
            cell["alerts"] = []
        return cell_entries
    
    # Pre-process alert polygons and build spatial index.
    alert_ids: List[str] = []
    alert_polys: List[Polygon] = []
    alert_prepared = []
    for alert in filtered_alerts:
        alert_id = _extract_alert_id(alert)
        alert_poly = _get_alert_polygon(alert)
        if alert_id and alert_poly:
            alert_ids.append(alert_id)
            alert_polys.append(alert_poly)
            alert_prepared.append(prep(alert_poly))

    if not alert_polys:
        for cell in cell_entries:
            cell["alerts"] = []
        return cell_entries

    try:
        spatial_index = STRtree(alert_polys)
        geom_id_to_index = {id(geom): idx for idx, geom in enumerate(alert_polys)}
    except Exception:
        spatial_index = None
        geom_id_to_index = {}
        fallback_alerts = list(zip(alert_ids, alert_prepared))
    else:
        fallback_alerts = []

    # Match alerts to each cell
    for cell in cell_entries:
        if spatial_index is not None:
            matching_ids = _match_alerts_with_strtree(
                cell,
                alert_ids,
                alert_polys,
                alert_prepared,
                spatial_index,
                geom_id_to_index,
            )
        else:
            matching_ids = match_alerts_to_cell(cell, fallback_alerts)
        cell["alerts"] = matching_ids
    
    total_matches = sum(len(cell.get("alerts", [])) for cell in cell_entries)
    io_manager.write_info(
        f"Matched {total_matches} alert-cell pairs across {len(cell_entries)} cells "
        f"(from {len(filtered_alerts)} active convective/flood alerts)"
    )
    
    return cell_entries
