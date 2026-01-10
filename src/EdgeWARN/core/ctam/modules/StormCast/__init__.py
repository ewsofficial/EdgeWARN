"""
StormCast CTAM Module

Adapter for integrating StormCast core into the CTAM framework.
"""

from typing import Dict, Any, Optional, List
import dataclasses
from ...interface import AnalysisModule

# Re-export core components for external use
from .core import (
    StormCastEngine,
    ForecastResult,
    StormState,
    EnvironmentProfile,
    ForecastPoint,
    PRESSURE_LEVELS,
)


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

    def run(self, storm_entry: Dict[str, Any], environment: Optional[Dict[str, Any]] = None) -> None:
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
        dx = props.get("dx")
        dy = props.get("dy")
        dt = props.get("dt")
        echo_top_30 = props.get("EchoTop30", 10.0)
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
        ref_lat = 35.0
        ref_lon = -97.0
        
        if environment is not None and "winds" in environment:
            # Use provided environment
            winds = environment["winds"]
            ref_lat = environment.get("reference_lat", ref_lat)
            ref_lon = environment.get("reference_lon", ref_lon)
        else:
            # Extract wind data from properties (format: u850, v850, etc.)
            for level in PRESSURE_LEVELS:
                u_key = f"u{level}"
                v_key = f"v{level}"
                if u_key in props and v_key in props:
                    winds[level] = (props[u_key], props[v_key])
        
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
            # First point: previous position
            prev_x = x - dx
            prev_y = y - dy
            engine.add_observation(prev_x, prev_y, dt_seconds=0, echo_top_30=echo_top_30, echo_top_50=echo_top_50)
            
            # Second point: current position
            engine.add_observation(x, y, dt_seconds=dt, echo_top_30=echo_top_30, echo_top_50=echo_top_50)
            
            # Generate forecast
            result = engine.generate_forecast()
            
            # Store results
            storm_entry["modules"][self.name] = {
                "u": result.u,
                "v": result.v,
                "forecast_cones": result.forecast_cones,
                "status": "success"
            }
            
        except Exception as e:
            storm_entry["modules"][self.name] = {
                "status": "error",
                "error": str(e)
            }
