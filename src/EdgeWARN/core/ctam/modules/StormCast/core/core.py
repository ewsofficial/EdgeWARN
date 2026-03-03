"""
StormCast Core Engine

Unified interface for storm motion prediction, integrating environmental diagnostics,
motion blending, Kalman filtering, and uncertainty quantification.
"""

from typing import List, Tuple, Dict, Optional, Any
from datetime import datetime
import dataclasses

from .types import (
    StormState, 
    EnvironmentProfile, 
    ForecastPoint, 
    MotionVector
)
from .config import (
    DEFAULT_LEAD_TIMES,
    BlendingWeights
)
from .diagnostics import (
    compute_storm_core_height,
    compute_adaptive_steering,
    compute_effective_shear,
    compute_bunkers_motion,
)
from .blending import (
    smooth_observed_motion,
    blend_motion,
    adjust_weights_for_maturity,
)
from .kalman import StormKalmanFilter
from .forecast import (
    generate_forecast_track,
    forecast_with_uncertainty,
    forecast_motion_cone,
)
from .uncertainty import (
    compute_velocity_covariance,
    compute_tracking_uncertainty
)


@dataclasses.dataclass
class ForecastResult:
    """Structured forecast output."""
    u: float
    v: float
    forecast_cones: List[Dict[str, Any]]
    forecast_polygons: List[List[Tuple[float, float]]]
    polygon_0_30m: Optional[List[Tuple[float, float]]] = None


class StormCastEngine:
    """
    Main entry point for StormCast predictions.
    
    Manages state history, environment data, and acts as the orchestrator
    for the forecast pipeline.
    """
    
    def __init__(
        self, 
        reference_lat: float = 35.0, 
        reference_lon: float = -97.0
    ):
        self.reference_lat = reference_lat
        self.reference_lon = reference_lon
        self.motion_history: List[Tuple[float, float]] = []  # List of (u, v) observations
        self.position_history: List[Tuple[float, float]] = [] # List of (x, y) observations
        self.environment: Optional[EnvironmentProfile] = None
        self.kalman_filter: StormKalmanFilter = StormKalmanFilter()
        
        # Current state cache
        self.last_update_time: Optional[datetime] = None
        self.current_h_core: float = 6.0  # Default moderate depth
        self.current_polygon: Optional[List[Tuple[float, float]]] = None
        self.current_echo_top_30: float = 10.0
        
    def set_environment(self, profile: EnvironmentProfile) -> None:
        """Update the environmental wind profile."""
        self.environment = profile
        
    def add_observation(
        self, 
        x: float, 
        y: float, 
        dt_seconds: float, 
        echo_top_30: float = 10.0,
        echo_top_50: float = 8.0,
        timestamp: Optional[datetime] = None,
        polygon: Optional[List[Tuple[float, float]]] = None
    ) -> None:
        """
        Add a new radar observation.
        
        Args:
            x, y: Position in meters
            dt_seconds: Time since last observation (for velocity calc)
            echo_top_30: Height of 30 dBZ echo top (km AGL)
            echo_top_50: Height of 50 dBZ echo top (km AGL)
            timestamp: Observation time
            polygon: List of (lat, lon) coordinates defining the storm footprint
        """
        self.current_polygon = polygon
        # Extract freezing level if environment is present
        fz_level = None
        if self.environment:
            fz_level = self.environment.freezing_level_km
            
        self.current_h_core = compute_storm_core_height(echo_top_30, echo_top_50, freezing_level_km=fz_level)
        self.last_update_time = timestamp or datetime.now()
        self.current_echo_top_30 = echo_top_30
        
        # 1. Update Position History
        self.position_history.append((x, y))
        
        # 2. Derive Observed Velocity
        if dt_seconds > 0:
            if len(self.position_history) >= 2:
                prev_x, prev_y = self.position_history[-2]
                u_obs = (x - prev_x) / dt_seconds
                v_obs = (y - prev_y) / dt_seconds
                self.motion_history.append((u_obs, v_obs))
            
            # 3. Update Kalman Filter (Predict step)
            self.kalman_filter.predict(dt=dt_seconds)
            
            # 4. Update Kalman Filter (Correction step)
            # We track history count to scale observation uncertainty
            track_len = len(self.motion_history)
            self.kalman_filter.update(
                observation=(x, y),
                track_history=track_len
            )
        else:
            # First point, just initialize KF state position
            # We can't infer velocity yet, so assume 0 or keep default
            self.kalman_filter.state[0] = x
            self.kalman_filter.state[1] = y
            
    def generate_forecast(
        self, 
        lead_times: Optional[List[float]] = None,
        confidence: float = 0.90
    ) -> ForecastResult:
        """
        Generate a comprehensive forecast based on current state.
        """
        if not self.environment:
            raise ValueError("Environment profile not set. Call set_environment() first.")
        
        if len(self.motion_history) < 1:
            raise ValueError("Insufficient motion history. Add at least 2 position observations.")

        track_history_len = len(self.motion_history)
        
        # --- A. Diagnostics & Environment ---
        # 1. Height-Adaptive Steering
        v_mean = compute_adaptive_steering(self.environment, self.current_h_core)
        
        # 2. Shear & Bunkers
        shear = compute_effective_shear(
            self.environment, 
            self.current_h_core, 
            echo_top_30=getattr(self, 'current_echo_top_30', 10.0)
        )
        try:
           v_bunkers = compute_bunkers_motion(self.environment, self.current_h_core, right_mover=True)
        except Exception:
           # Fallback if calculation fails (e.g. no shear)
           v_bunkers = v_mean
 
        # --- B. Motion Blending ---
        # 1. Smooth observations
        from .config import MOTION_SMOOTHING_WINDOW
        recent_history = self.motion_history[-MOTION_SMOOTHING_WINDOW:]
        v_obs_smooth = smooth_observed_motion(recent_history, method="exponential")
        
        # 2. Dynamic Weights
        shear_mag = (shear[0]**2 + shear[1]**2)**0.5
        weights = adjust_weights_for_maturity(
            h_core=self.current_h_core,
            track_history=track_history_len,
            shear_magnitude=shear_mag,
            mucape=self.environment.mucape
        )
        
        # 3. Blend
        v_final = blend_motion(v_obs_smooth, v_mean, v_bunkers, weights)
        
        # --- C. State Synthesis ---
        # Calculate motion jitter (velocity volatility) from history
        from .blending import calculate_motion_jitter
        jitter = calculate_motion_jitter(self.motion_history)
        
        storm = StormState(
            x=self.kalman_filter.x,
            y=self.kalman_filter.y,
            u=v_final[0],
            v=v_final[1],
            h_core=self.current_h_core,
            echo_top_30=getattr(self, 'current_echo_top_30', 10.0),
            track_history=track_history_len,
            motion_jitter=jitter,
            timestamp=self.last_update_time,
            polygon=self._latlon_to_meters_poly(self.current_polygon) if self.current_polygon else None
        )
        
        # --- D. Forecast & Uncertainty ---
        # 1. Generate Track Points with Uncertainty
        # Legacy behavior defaults to zero initial position uncertainty for "tight" tracks.
        # To use Kalman confidence, one would use: math.sqrt(kf_cov[i][i])
        initial_sigma_x, initial_sigma_y = 0.0, 0.0
 
        forecast_track = forecast_with_uncertainty(
            storm, 
            lead_times=lead_times,
            initial_sigma_pos=(initial_sigma_x, initial_sigma_y)
        )
        
        # Chi-Squared values for 2D at given confidence
        # 95%: 5.991, 90%: 4.605 (Point), 68%: 2.30, 90% Overlap (Tuned): 2.12
        chi2_map = {0.68: 2.30, 0.90: 2.12, 0.95: 5.991}
        chi2 = chi2_map.get(confidence, 2.12)
        import math
        scale = math.sqrt(chi2)

        cones = []
        polygons = []
        
        # Phase 5: 0-30m Encompassing Polygon
        # We need the 0m polygon (current footprint) and forecast polygons up to 30m
        from shapely.geometry import Polygon as ShapelyPolygon
        from shapely.ops import unary_union
        
        hull_shapes = []
        if storm.polygon:
            # Add 0-minute polygon (1:1 original size)
            hull_shapes.append(ShapelyPolygon(storm.polygon))
            
        for fp in forecast_track:
            # Expansion radius used for building the polygon
            radius = max(fp.sigma_x, fp.sigma_y) * scale
            
            # Convert center to lat/lon
            center_lat, center_lon = self._meters_to_latlon(fp.x, fp.y)
            
            cones.append({
                "center": (center_lat, center_lon),
                "radius": radius,
                "polygon_expansion": radius,
                "lead_time": fp.lead_time
            })

            if fp.polygon:
                # Add to hull if within 30 minutes (1800s)
                if fp.lead_time <= 1800:
                    hull_shapes.append(ShapelyPolygon(fp.polygon))
                
                # Convert back to lat/lon
                latlon_poly = [self._meters_to_latlon(px, py) for px, py in fp.polygon]
                polygons.append(latlon_poly)
            else:
                polygons.append([self._meters_to_latlon(fp.x, fp.y)])
                
        # Calculate 0-30m Encompassing Polygon (Quadrilateral / Trapezoid)
        polygon_0_30m = None
        if len(hull_shapes) >= 2:
            try:
                # To make an encompassing quadrilateral, we form a trapezoid aligned with storm motion.
                import math
                mag = math.hypot(v_final[0], v_final[1])
                
                # If storm is stationary, fallback to minimum rotated rectangle of the hull
                if mag < 0.1:
                    hull = unary_union(hull_shapes).convex_hull
                    quad = hull.minimum_rotated_rectangle
                    if not quad.is_empty and quad.geom_type == 'Polygon':
                        polygon_0_30m = [self._meters_to_latlon(px, py) for px, py in quad.exterior.coords]
                else:
                    # Forward and Right unit vectors
                    F = (v_final[0]/mag, v_final[1]/mag)
                    R = (v_final[1]/mag, -v_final[0]/mag)
                    
                    # Project T=0 polygon (hull_shapes[0])
                    pts_0 = list(hull_shapes[0].exterior.coords)
                    f_0 = [px*F[0] + py*F[1] for px, py in pts_0]
                    r_0 = [px*R[0] + py*R[1] for px, py in pts_0]
                    f_min0, r_min0, r_max0 = min(f_0), min(r_0), max(r_0)
                    
                    # Project T=30 polygon (hull_shapes[-1])
                    pts_30 = list(hull_shapes[-1].exterior.coords)
                    f_30 = [px*F[0] + py*F[1] for px, py in pts_30]
                    r_30 = [px*R[0] + py*R[1] for px, py in pts_30]
                    f_max30, r_min30, r_max30 = max(f_30), min(r_30), max(r_30)
                    
                    # 4 Corners of the Trapezoid in (F, R) space
                    corners_fr = [
                        (f_max30, r_min30), # Front-Left
                        (f_max30, r_max30), # Front-Right
                        (f_min0, r_max0),   # Rear-Right
                        (f_min0, r_min0)    # Rear-Left
                    ]
                    
                    # Convert back to (X, Y) and then Lat/Lon
                    quad_latlon = []
                    for cf, cr in corners_fr:
                        cx = cf * F[0] + cr * R[0]
                        cy = cf * F[1] + cr * R[1]
                        quad_latlon.append(self._meters_to_latlon(cx, cy))
                        
                    # Close the polygon
                    quad_latlon.append(quad_latlon[0])
                    polygon_0_30m = quad_latlon
            except Exception as e:
                # Fallback if math fails
                pass
        
        return ForecastResult(
            u=v_final[0],
            v=v_final[1],
            forecast_cones=cones,
            forecast_polygons=polygons,
            polygon_0_30m=polygon_0_30m
        )

    def _latlon_to_meters_poly(self, poly: List[Tuple[float, float]]) -> List[Tuple[float, float]]:
        """Convert lat/lon polygon to local meters."""
        import math
        meters_poly = []
        avg_lat_rad = math.radians(self.reference_lat)
        for lat, lon in poly:
            dy = (lat - self.reference_lat) * 111111.0
            dx = (lon - self.reference_lon) * (111111.0 * math.cos(avg_lat_rad))
            meters_poly.append((dx, dy))
        return meters_poly

    def _meters_to_latlon(self, x: float, y: float) -> Tuple[float, float]:
        """Convert local meters (x, y) to (lat, lon) using flat-earth approx."""
        import math
        # 1 deg lat ~ 111,111 m
        dlat = y / 111111.0
        new_lat = self.reference_lat + dlat
        
        # 1 deg lon ~ 111,111 * cos(lat) m
        avg_lat_rad = math.radians(self.reference_lat)
        dlon = x / (111111.0 * math.cos(avg_lat_rad))
        new_lon = self.reference_lon + dlon
        
        return (new_lat, new_lon)
