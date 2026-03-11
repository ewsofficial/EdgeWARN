"""
Alert-to-Cell Spatial Matching Module

Matches storm cells with active NWS alerts based on spatial intersection.
Only includes convective and flood-related alert types.

Usage:
    from EdgeWARN.core.process.detect.tools.alert_matcher import match_alerts_to_cells
    
    # Add alerts to cell entries
    entries_with_alerts = match_alerts_to_cells(cell_entries, registry_path)
"""

from typing import Dict, List, Any, Optional
from pathlib import Path
from shapely.geometry import Polygon, Point
from shapely.prepared import prep
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
    # Flood events
    "Flash Flood Warning",
    "Flood Warning",
    "Flash Flood Watch",
    "Flood Watch",
    "Flood Advisory",
    "Flash Flood Emergency",
}


def load_active_alerts(registry_path: Path) -> List[Dict[str, Any]]:
    """
    Load active alerts from the AlertRegistry.
    
    Args:
        registry_path: Path to the alerts_registry.json file
        
    Returns:
        List of alert feature dictionaries
    """
    if not registry_path.exists():
        return []
    
    try:
        with open(registry_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        alerts = data.get("alerts", {})
        return [
            alert_data["feature"]
            for alert_data in alerts.values()
            if "feature" in alert_data
        ]
    except Exception as e:
        io_manager.write_warning(f"Failed to load alerts from registry: {e}")
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
    # Check for GeoMapper's Polygon field
    if "Polygon" in alert:
        poly_data = alert["Polygon"]
        if poly_data and len(poly_data) > 0:
            # Polygon field contains list of rings, take the first one
            coords = poly_data[0]
            if len(coords) >= 3:
                try:
                    return Polygon(coords)
                except Exception:
                    pass
    
    # Check for standard geometry
    geom = alert.get("geometry")
    if geom and geom.get("type") == "Polygon":
        coords = geom.get("coordinates", [])
        if coords and len(coords[0]) >= 3:
            try:
                return Polygon(coords[0])
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


def match_alerts_to_cells(
    cell_entries: List[Dict[str, Any]],
    registry_path: Path
) -> List[Dict[str, Any]]:
    """
    Match active convective/flood alerts to each cell entry.
    
    Adds an "alerts" key to each cell containing a list of matching alert IDs.
    
    Args:
        cell_entries: List of cell entry dictionaries
        registry_path: Path to the alerts_registry.json file
        
    Returns:
        Cell entries with "alerts" key added to each
    """
    if not cell_entries:
        return []
    
    # Load and filter alerts
    all_alerts = load_active_alerts(registry_path)
    filtered_alerts = filter_convective_flood_alerts(all_alerts)
    
    if not filtered_alerts:
        # No active convective/flood alerts, add empty alerts list to each cell
        for cell in cell_entries:
            cell["alerts"] = []
        return cell_entries
    
    # Pre-process alert polygons for efficiency (prep once, check many)
    prepped_alerts = []
    for alert in filtered_alerts:
        alert_id = _extract_alert_id(alert)
        alert_poly = _get_alert_polygon(alert)
        if alert_id and alert_poly:
            prepped_alerts.append((alert_id, prep(alert_poly)))
    
    if not prepped_alerts:
        for cell in cell_entries:
            cell["alerts"] = []
        return cell_entries

    # Match alerts to each cell
    for cell in cell_entries:
        matching_ids = match_alerts_to_cell(cell, prepped_alerts)
        cell["alerts"] = matching_ids
    
    total_matches = sum(len(cell.get("alerts", [])) for cell in cell_entries)
    io_manager.write_info(
        f"Matched {total_matches} alert-cell pairs across {len(cell_entries)} cells "
        f"(from {len(filtered_alerts)} active convective/flood alerts)"
    )
    
    return cell_entries
