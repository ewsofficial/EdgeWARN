"""
StormCast CTAM Module

Adapter for integrating StormCast core into the CTAM framework.
"""

from typing import Dict, Any, Optional, List
import dataclasses
from ...interface import AnalysisModule
from EdgeWARN.alerts import AlertManager
from EdgeWARN.alerts.schema import AlertPayload
import json
from pathlib import Path
from datetime import datetime, timedelta
import util.file as fs
from util.io import IOManager

# Re-export core components for external use
from .core import (
    StormCastEngine,
    ForecastResult,
    StormState,
    EnvironmentProfile,
    ForecastPoint,
    PRESSURE_LEVELS,
)



io_manager = IOManager("[StormCast]")


class StormCastModule(AnalysisModule):
    """
    CTAM adapter for StormCast forecasting.
    
    This module uses the StormCast core engine to run forecasts
    on storm cell entries. Results are stored in 
    storm_entry['modules']['StormCast'].
    """
    
    @property
    def name(self) -> str:
        return "StormCast"

    def run(self, storm_entry: Dict[str, Any], environment: Optional[Dict[str, Any]] = None, history_cache: Optional[Any] = None) -> None:
        """
        Run StormCast on a storm entry.
        
        Expects storm_entry['properties'] to contain:
            - x, y: position (meters)
            - dx, dy, dt: displacement since last observation
            - EchoTop30, EchoTop50: echo top heights (km AGL)
            - u850, v850, u700, v700, u500, v500, u250, v250: wind components
            
        Environment parameter (optional) can override extracted winds:
            - winds: dict mapping pressure level (int) to (u, v) tuple
            - reference_lat, reference_lon: reference coordinates
            
        Results stored in storm_entry['modules']['StormCast']:
            - u, v: predicted motion (m/s)
            - forecast_cones: list of forecast cone dicts
        """
        props = storm_entry.get("properties", {})
        
        # Extract required fields
        x = props.get("x", 0.0)
        y = props.get("y", 0.0)
        # dx, dy, dt are at top level (set by vecmath.py), not in properties
        dx = storm_entry.get("dx")
        dy = storm_entry.get("dy")
        dt = storm_entry.get("dt")
        # EchoTop30 comes from MRMS integration (p100EchoTop30)
        echo_top_30 = props.get("p100EchoTop30", 10.0)
        # EchoTop50 comes from ProbSevere integration (EchoTop_50 -> EchoTop50)
        echo_top_50 = props.get("EchoTop50", 8.0)
        
        # Check if we have enough data
        if dx is None or dy is None or dt is None or dt == 0:
            storm_entry["modules"][self.name] = {
                "status": "skipped",
                "reason": "Insufficient motion data (missing dx, dy, or dt)"
            }
            return
        
        # Build winds dict - first try environment, then extract from properties
        winds = {}
        
        # Extract reference lat/lon from storm's centroid
        # Centroid is available in storm_entry (top-level) as [lat, lon] with lon in 0-360 format
        centroid = storm_entry.get("centroid")
        if centroid and len(centroid) >= 2:
            ref_lat = centroid[0]
            # Convert longitude from 0-360 to -180-180 format
            ref_lon = centroid[1] if centroid[1] <= 180 else centroid[1] - 360
        else:
            # Fallback to defaults if centroid not available
            ref_lat = 35.0
            ref_lon = -97.0
        
        if environment is not None and "winds" in environment:
            # Use provided environment
            winds = environment["winds"]
            # Environment can override reference coordinates if provided
            ref_lat = environment.get("reference_lat", ref_lat)
            ref_lon = environment.get("reference_lon", ref_lon)
        else:
            # Extract wind data from properties (format: wind_field.u{level}/v{level})
            wind_field = props.get("wind_field", {})
            for level in PRESSURE_LEVELS:
                u_key = f"u{level}"
                v_key = f"v{level}"
                if u_key in wind_field and v_key in wind_field:
                    winds[level] = (wind_field[u_key], wind_field[v_key])
        
        # Check if we have wind data
        if not winds:
            storm_entry["modules"][self.name] = {
                "status": "skipped",
                "reason": "No wind data found (checked environment and properties)"
            }
            return
        
        try:
            # Build EnvironmentProfile
            env_profile = EnvironmentProfile(
                winds=winds,
                timestamp=None
            )
            
            # Initialize engine
            engine = StormCastEngine(reference_lat=ref_lat, reference_lon=ref_lon)
            engine.set_environment(env_profile)
            
            # Add observations (need at least 2 for velocity)
            # Use relative coordinates: centroid = (0, 0), previous = (-dx, -dy)
            # This aligns with the engine's expectation of meters relative to reference_lat/lon
            
            # Current position: at origin (0, 0) since reference is the centroid
            engine.add_observation(0.0, 0.0, dt_seconds=dt, echo_top_30=echo_top_30, echo_top_50=echo_top_50)
            
            # --- START HISTORY LOADING ---
            try:
                cell_id = storm_entry.get("id")
                if cell_id:
                    history_file = fs.CELL_DIR / f"{cell_id}.json"
                    if history_file.exists():
                        with open(history_file, "r") as f:
                            history_data = json.load(f)
                        
                        # Sort by timestamp to be safe
                        history_data.sort(key=lambda x: x.get("timestamp") or x.get("properties", {}).get("timestamp") or "")
                        
                        # We need to reconstruct relative positions for the engine.
                        # The engine expects (0,0) to be the *current* centroid.
                        # So previous points must be offset relative to current centroid.
                        # However, we don't have easy lat/lon -> meters conversion here without using the engine's internal methods
                        # or recreating the projection.
                        #
                        # BETTER APPROACH:
                        # The engine is initialized with the CURRENT reference lat/lon.
                        # We can add observations using their lat/lon if we convert them to meters relative to this reference.
                        # The engine has `_meters_to_latlon` but not the inverse publicly exposed, but we can replicate the flat-earth approx.
                        
                        import math
                        
                        # Current Reference (Centroid)
                        ref_lat_rad = math.radians(ref_lat)
                        cos_ref_lat = math.cos(ref_lat_rad)
                        
                        # Meters per degree
                        LAT_METERS = 111111.0
                        LON_METERS = 111111.0 * cos_ref_lat
                        
                        current_ts_str = storm_entry.get("timestamp")
                        
                        # Add historical points
                        # effective_history_count = 0
                        for hist_entry in history_data:
                            # Skip if it's the current entry (duplicate check by timestamp)
                            hist_ts = hist_entry.get("timestamp") or hist_entry.get("properties", {}).get("timestamp")
                            if hist_ts == current_ts_str:
                                continue
                                
                            # Get Lat/Lon
                            hist_centroid = hist_entry.get("centroid") or hist_entry.get("properties", {}).get("centroid")
                            if not hist_centroid:
                                continue
                                
                            h_lat = hist_centroid[0]
                            h_lon = hist_centroid[1] if hist_centroid[1] <= 180 else hist_centroid[1] - 360
                            
                            # Calculate relative position in meters to CURRENT reference
                            dy_meters = (h_lat - ref_lat) * LAT_METERS
                            dx_meters = (h_lon - ref_lon) * LON_METERS
                            
                            # We can't easily adhere to the strict `dt` sequence required by `add_observation` 
                            # if we just dump points in. `add_observation` calculates velocity from the *previous* point added.
                            # So we must add them in strict chronological order.
                            # But wait, we already added the "previous" (-dx, -dy) point from the current frame's motion vector!
                            #
                            # The `add_observation` logic:
                            # 1. Appends pos to history
                            # 2. If dt > 0, calculates velocity from (current - previous_in_history) / dt
                            #
                            # If we load history, we should probably NOT add the `-dx, -dy` synthetic point, 
                            # OR ensuring it aligns.
                            #
                            # actually, the `storm_entry` has `dx`, `dy` which are "displacement from previous frame".
                            # This is the most accurate instantaneous motion.
                            # Using historical lat/lon might be noisy if centroids jump around.
                            # 
                            # HYBRID APPROACH:
                            # 1. Initialize engine.
                            # 2. Load history. Convert to relative meters. Add to engine with correct dt.
                            # 3. Add current point (0,0).
                            #
                            # Issue: `add_observation` requires `dt` since *previous added observation*.
                            # We need timestamps to calculate `dt`.
                            
                            pass # Logic implemented below in replacement
                            
            except Exception as e:
                # print(f"Failed to load history for {cell_id}: {e}")
                pass
            # --- END HISTORY LOADING ---

            # RE-WRITE:
            # We will use a fresh approach.
            # 1. Collect all historical points + current point.
            # 2. Sort them.
            # 3. Feed them into engine sequentially.
            
            historical_points = []
            
            # 1. Load from file
            try:
                cell_id = storm_entry.get("id")
                if cell_id:
                    if history_cache is not None:
                        # cache get() returns highest-to-lowest, but we need lowest-to-highest for StormCast
                        hist_data = history_cache.get(cell_id)
                        hist_data = list(reversed(hist_data))
                    else:
                        history_file = fs.CELL_DIR / f"{cell_id}.json"
                        if history_file.exists():
                            with open(history_file, "r") as f:
                                hist_data = json.load(f)
                        else:
                            hist_data = []
                            
                    for h in hist_data:
                        h_ts = h.get("timestamp") or h.get("properties", {}).get("timestamp")
                        h_cent = h.get("centroid")
                        h_props = h.get("properties", {})
                        if h_ts and h_cent:
                            historical_points.append({
                                "ts": h_ts,
                                "lat": h_cent[0],
                                "lon": h_cent[1] if h_cent[1] <= 180 else h_cent[1] - 360,
                                "echo_top_30": h_props.get("p100EchoTop30", 10.0), # Schema check?
                                "echo_top_50": h_props.get("EchoTop50", 8.0)
                            })
            except Exception:
                pass
                
            # 2. Add current point
            from datetime import datetime
            
            # Function to parse ISO timestamp
            def parse_ts(t_str):
                try:
                    return datetime.fromisoformat(t_str.replace('Z', '+00:00'))
                except:
                    return datetime.now() # Fallback

            current_ts_str = storm_entry.get("timestamp")
            if not current_ts_str:
                 # Should have been caught by return earlier, but safe guard
                 current_ts_str = datetime.now().isoformat()

            current_ts_dt = parse_ts(current_ts_str)
                 
            historical_points.append({
                "ts": current_ts_str,
                "lat": ref_lat,
                "lon": ref_lon,
                "echo_top_30": echo_top_30,
                "echo_top_50": echo_top_50,
                "is_current": True
            })
            
            # Sort
            historical_points.sort(key=lambda x: parse_ts(x["ts"]))

            filtered_points = []
            future_point_count = 0
            for p in historical_points:
                point_dt = parse_ts(p["ts"])
                if point_dt > current_ts_dt and not p.get("is_current"):
                    future_point_count += 1
                    continue
                filtered_points.append(p)

            # Filter duplicates (by ts), preferring the synthetic current point
            unique_points = []
            index_by_ts = {}
            duplicate_count = 0
            replaced_current_count = 0
            for p in filtered_points:
                ts = p["ts"]
                existing_index = index_by_ts.get(ts)
                if existing_index is None:
                    index_by_ts[ts] = len(unique_points)
                    unique_points.append(p)
                    continue

                duplicate_count += 1
                existing_point = unique_points[existing_index]
                if p.get("is_current") and not existing_point.get("is_current"):
                    unique_points[existing_index] = p
                    replaced_current_count += 1

            if future_point_count > 0:
                io_manager.write_debug(
                    f"Cell {storm_entry.get('id', 'unknown')}: discarded {future_point_count} future history point(s) newer than current timestamp"
                )

            if duplicate_count > 0:
                io_manager.write_debug(
                    f"Cell {storm_entry.get('id', 'unknown')}: removed {duplicate_count} duplicate history point(s) by timestamp"
                )

            if replaced_current_count > 0:
                io_manager.write_debug(
                    f"Cell {storm_entry.get('id', 'unknown')}: retained current observation for {replaced_current_count} duplicate timestamp(s)"
                )
            
            # Add to engine
            import math
            LAT_METERS = 111111.0
            ref_lat_rad = math.radians(ref_lat)
            LON_METERS = 111111.0 * math.cos(ref_lat_rad)
            
            # Extract cell polygon
            current_polygon = None
            try:
                from EdgeWARN.process.integrate.utils import StormIntegrationUtils
                polygon_shape = StormIntegrationUtils.create_cell_polygon(storm_entry)
                if polygon_shape:
                    # Shapely exterior coords are usually (lon, lat) or (lat, lon) depending on usage
                    # StormIntegrationUtils returns (lon, lat). We need (lat, lon).
                    current_polygon = [
                        (lat, StormIntegrationUtils.normalize_longitude(lon))
                        for lon, lat in polygon_shape.exterior.coords
                    ]
            except Exception:
                pass
                
            prev_time = None
            
            # If we only have 1 point (current), try to use the dx/dy fallback
            if len(unique_points) < 2 and (dx is not None and dy is not None):
                 io_manager.write_debug(
                     f"Cell {storm_entry.get('id', 'unknown')}: insufficient history for multi-point StormCast run; "
                     f"using dx/dy fallback with dt={dt}"
                 )
                 # Fallback to single-frame logic
                 engine.add_observation(-dx, -dy, dt_seconds=0, echo_top_30=echo_top_30, echo_top_50=echo_top_50)
                 engine.add_observation(0.0, 0.0, dt_seconds=dt, echo_top_30=echo_top_30, echo_top_50=echo_top_50, polygon=current_polygon)
            else:
                # Use history
                for i, p in enumerate(unique_points):
                    # Calculate relative meters from current reference
                    rel_y = (p["lat"] - ref_lat) * LAT_METERS
                    rel_x = (p["lon"] - ref_lon) * LON_METERS
                    
                    dt_sec = 0.0
                    curr_time = parse_ts(p["ts"])
                    
                    if prev_time:
                        dt_sec = (curr_time - prev_time).total_seconds()
                    
                    # Ensure dt is non-negative and reasonable
                    if dt_sec < 0: dt_sec = 0
                    
                    # Pass polygon only for the current observation (last element)
                    poly_arg = current_polygon if p.get("is_current") else None
                    
                    # Add observation
                    engine.add_observation(
                        rel_x, 
                        rel_y, 
                        dt_seconds=dt_sec, 
                        echo_top_30=p.get("echo_top_30", 10.0),
                        echo_top_50=p.get("echo_top_50", 8.0),
                        timestamp=curr_time,
                        polygon=poly_arg
                    )
                    prev_time = curr_time
            
            # Generate forecast
            result = engine.generate_forecast()

            # Calculate tracking duration for diagnostics only.
            duration_min = 0.0
            if len(unique_points) >= 2:
                first_ts = parse_ts(unique_points[0]["ts"])
                last_ts = parse_ts(unique_points[-1]["ts"])
                duration_min = (last_ts - first_ts).total_seconds() / 60

            # Alert eligibility should begin as soon as a valid forecast polygon exists.
            # Tracking duration is preserved as metadata for downstream consumers/debugging.
            can_generate_alerts = bool(result.polygon_0_30m)
            alert_blockers = []
            if not current_polygon:
                alert_blockers.append("missing_current_polygon")
            if result.forecast_polygon_reason:
                alert_blockers.append(result.forecast_polygon_reason)

            if not can_generate_alerts:
                io_manager.write_info(
                    f"Cell {storm_entry.get('id', 'unknown')}: suppressing StormCast alert because forecast polygon_0_30m is unavailable"
                )
                if alert_blockers:
                    io_manager.write_debug(
                        f"Cell {storm_entry.get('id', 'unknown')}: alert blockers={','.join(dict.fromkeys(alert_blockers))}"
                    )
            
            
            # Store results
            storm_entry["modules"][self.name] = {
                "u": result.u,
                "v": result.v,
                "forecast_cones": result.forecast_cones,
                "forecast_polygons": result.forecast_polygons,
                "polygon_0_30m": result.polygon_0_30m,
                "status": "success",
                "can_generate_alerts": can_generate_alerts,
                "tracking_duration_min": round(duration_min, 2)
            }
            
        except Exception as e:
            storm_entry["modules"][self.name] = {
                "status": "error",
                "error": str(e)
            }

    # ------------------------------------------------------------------
    # Alert generation
    # ------------------------------------------------------------------
    def alerts(self, storm_entry: Dict[str, Any]) -> Optional[List[AlertPayload]]:
        """
        Build an alert from the 0-30m forecast polygon once the module has
        produced a valid forecast polygon for the cell.

        Alert cadence rule:
        - If no prior StormCast alert exists for the cell, emit immediately.
        - If a prior StormCast alert exists, wait at least 15 minutes after
          that alert's effective time before emitting a replacement polygon.
        """
        result = storm_entry.get("modules", {}).get(self.name, {})

        if result.get("status") != "success" or not result.get("can_generate_alerts"):
            return None

        polygon = result.get("polygon_0_30m")
        if not polygon:
            io_manager.write_warning(
                f"Cell {storm_entry.get('id', 'unknown')}: StormCast eligible for alert but polygon_0_30m is missing"
            )
            return None

        cell_id = storm_entry.get("id", "unknown_cell")

        # Parse timestamp for effective / expiry calculation
        ts_str = storm_entry.get("timestamp")
        if ts_str:
            try:
                effective = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
            except (ValueError, TypeError):
                effective = datetime.now()
        else:
            effective = datetime.now()

        previous_alert = AlertManager.load(self.name, cell_id)
        if previous_alert is not None:
            next_allowed_time = previous_alert.effective_time + timedelta(minutes=15)
            if effective < next_allowed_time:
                wait_minutes = (next_allowed_time - effective).total_seconds() / 60
                io_manager.write_info(
                    f"Cell {cell_id}: suppressing StormCast alert refresh because prior alert effective time "
                    f"{previous_alert.effective_time.isoformat()} requires 15-minute spacing; "
                    f"next eligible in {wait_minutes:.2f} min"
                )
                return None

        expiry = effective + timedelta(minutes=30)

        morphowind_result = storm_entry.get("modules", {}).get("MorphoWind", {})
        morphowind_severity = morphowind_result.get("severity_index", 0.0)
        tstm_wind = "true" if morphowind_severity > 0.6 else "false"

        return [
            AlertPayload(
                alert_type="TSTM",
                source=self.name,
                cell_id=cell_id,
                geometry=polygon,
                effective_time=effective,
                expiry_time=expiry,
                threats={
                    "tstm_wind": tstm_wind,
                },
            )
        ]
