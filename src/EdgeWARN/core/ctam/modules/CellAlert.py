from typing import Dict, Any, Optional, List
import math
from shapely.geometry import Polygon, LineString, MultiPolygon
from shapely.ops import unary_union
from shapely.affinity import translate
from ..interface import AnalysisModule
from ..util.history import get_cell_history
from .CellAlert import config as cfg

class CellAlertModule(AnalysisModule):
    """
    CTAM Module to generate cell alert polygons.
    Projects the storm footprint 30m forward, expands for uncertainty,
    and buffers the result. Updates every 3 scans.
    """

    @property
    def name(self) -> str:
        return "CellAlert"

    def run(self, storm_entry: Dict[str, Any], environment: Optional[Dict[str, Any]] = None) -> None:
        """
        Calculate the alert polygon for a storm cell.
        """
        # --- 1. Check Update Frequency ---
        cell_id = storm_entry.get("id")
        history = get_cell_history(cell_id, limit=5)
        
        # history[0] is current (since we haven't saved it yet in integrate.py, 
        # but wait - history is loaded from disk. integrate.py updates history 
        # AFTER CTAM runs. So history contains past entries only.)
        scan_count = len(history)
        
        # If it's not the configured scan interval, we try to reuse the previous alert
        if scan_count % cfg.ALERT_UPDATE_INTERVAL != 0 and scan_count > 0:
            last_entry = history[0]
            if "modules" in last_entry and self.name in last_entry["modules"]:
                prev_alert = last_entry["modules"][self.name].get("alert_polygon")
                if prev_alert:
                    # Inject previous alert
                    # TODO: Optionally advect based on current motion
                    storm_entry.setdefault("modules", {})
                    storm_entry["modules"][self.name] = {
                        "alert_polygon": prev_alert,
                        "status": "reused"
                    }
                    return

        # --- 2. Projection & Expansion Logic ---
        # Retrieve base footprint
        footprint = storm_entry.get("polygon_obj")
        if footprint is None:
            # Fallback to creating it if FootprintModule didn't run or failed
            from EdgeWARN.core.process.integrate.utils import StormIntegrationUtils
            footprint = StormIntegrationUtils.create_cell_polygon(storm_entry)
            
        if footprint is None:
            return

        # Retrieve StormCast forecast
        stormcast = storm_entry.get("modules", {}).get("StormCast", {})
        cones = stormcast.get("forecast_cones", [])
        
        if not cones:
            # No forecast, just use buffered footprint
            self._set_result(storm_entry, footprint.buffer(0.02)) # Default small buffer
            return

        # Constants from config
        lead_time_seconds = cfg.ALERT_LEAD_TIME_SECONDS
        safety_buffer = cfg.ALERT_SAFETY_BUFFER_DEGREES
        
        # Filter cones by lead time
        relevant_cones = [c for c in cones if c.get("lead_time", 0) <= lead_time_seconds]
        
        # Current centroid
        centroid = footprint.centroid
        curr_lon, curr_lat = centroid.x, centroid.y
        
        polygons_to_union = [footprint]
        
        for cone in relevant_cones:
            # Cone center is (lat, lon)
            target_lat = cone["center"][0]
            target_lon = cone["center"][1]
            uncertainty_m = cone["radius"]
            
            # Displacement
            d_lon = target_lon - curr_lon
            d_lat = target_lat - curr_lat
            
            # Translate current shape
            projected_poly = translate(footprint, xoff=d_lon, yoff=d_lat)
            
            # Expand for uncertainty
            # Conv meters to degrees: 1 deg lat = 111,111m
            # 1 deg lon = 111,111 * cos(lat)
            lat_rad = math.radians(target_lat)
            deg_lat = uncertainty_m / 111111.0
            deg_lon = uncertainty_m / (111111.0 * math.cos(lat_rad))
            
            # Shapely buffer is usually uniform, we'll use avg degrees for simplicity
            # or we could use affinity.scale but buffer is better for "expanding"
            avg_deg = (deg_lat + deg_lon) / 2.0
            expanded_poly = projected_poly.buffer(avg_deg)
            
            polygons_to_union.append(expanded_poly)
            
        # --- 3. Construct Final Alert ---
        try:
            # Union all projected shapes
            merged = unary_union(polygons_to_union)
            
            # Apply final safety buffer
            # This helps smooth out the union of discrete footprints
            final_poly = merged.buffer(safety_buffer)
            
            # Cleanup
            if final_poly.is_valid and not final_poly.is_empty:
                self._set_result(storm_entry, final_poly)
            else:
                storm_entry["modules"][self.name] = {"status": "error", "error": "Invalid result geometry"}
                
        except Exception as e:
            storm_entry["modules"][self.name] = {"status": "error", "error": f"Geometry error: {e}"}

    def _set_result(self, storm_entry: Dict[str, Any], polygon: Any) -> None:
        """Helper to format and store the result."""
        # Ensure we have a single Polygon for GeoJSON compatibility
        if polygon.geom_type == 'MultiPolygon':
            # Use convex hull to return a single polygon representing the envelope
            polygon = polygon.convex_hull
            
        if polygon.is_empty or polygon.geom_type != 'Polygon':
            storm_entry.setdefault("modules", {})
            storm_entry["modules"][self.name] = {"status": "error", "error": f"Invalid final geometry type: {polygon.geom_type}"}
            return

        coords = list(polygon.exterior.coords)
        storm_entry.setdefault("modules", {})
        storm_entry["modules"][self.name] = {
            "alert_polygon": coords,
            "status": "updated"
        }
