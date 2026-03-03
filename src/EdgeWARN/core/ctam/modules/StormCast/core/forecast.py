"""
Forecast Generation Module

Linear advection and forecast track generation with uncertainty.
"""

from typing import List, Tuple, Optional

from .config import DEFAULT_LEAD_TIMES
from .types import StormState, ForecastPoint
from .uncertainty import (
    compute_velocity_covariance,
    propagate_position_uncertainty,
)


def forecast_position(
    state: StormState,
    dt: float
) -> Tuple[float, float]:
    """
    Forecast storm position using linear advection.
    
    x(t + Δt) = x(t) + u × Δt
    y(t + Δt) = y(t) + v × Δt
    
    Args:
        state: Current storm state with position and velocity
        dt: Forecast lead time in seconds
        
    Returns:
        Forecast position (x, y) in meters
    """
    x_forecast = state.x + state.u * dt
    y_forecast = state.y + state.v * dt
    return (x_forecast, y_forecast)


def generate_forecast_track(
    state: StormState,
    lead_times: Optional[List[float]] = None
) -> List[ForecastPoint]:
    """
    Generate forecast positions at multiple lead times.
    
    Args:
        state: Current storm state
        lead_times: List of lead times in seconds. Default: [900, 1800, 2700, 3600]
        
    Returns:
        List of ForecastPoint objects
    """
    if lead_times is None:
        lead_times = list(DEFAULT_LEAD_TIMES)
    
    forecast_points = []
    
    for dt in lead_times:
        x, y = forecast_position(state, dt)
        forecast_points.append(ForecastPoint(
            x=x,
            y=y,
            lead_time=dt,
            sigma_x=0.0,
            sigma_y=0.0,
        ))
    
    return forecast_points


def forecast_with_uncertainty(
    state: StormState,
    lead_times: Optional[List[float]] = None,
    initial_sigma_pos: Tuple[float, float] = (0.0, 0.0),
    sigma_vel: Optional[Tuple[float, float]] = None
) -> List[ForecastPoint]:
    """
    Generate forecast track with growing uncertainty.
    
    Position uncertainty grows quadratically with lead time:
    P_pos(t + Δt) = P_pos(t) + P_v × Δt²
    
    Args:
        state: Current storm state
        lead_times: List of lead times in seconds
        initial_sigma_pos: Initial position uncertainty (sigma_x, sigma_y) in meters
        sigma_vel: Velocity uncertainty (sigma_u, sigma_v) in m/s. 
                   If None, computed from tracking history.
        
    Returns:
        List of ForecastPoint objects with uncertainty
    """
    if lead_times is None:
        lead_times = list(DEFAULT_LEAD_TIMES)
    
    # Compute velocity uncertainty if not provided
    if sigma_vel is None:
        sigma_u, sigma_v, _ = compute_velocity_covariance(
            n_samples=state.track_history,
            motion_jitter=state.motion_jitter
        )
        sigma_vel = (sigma_u, sigma_v)
    
    forecast_points = []
    sigma_pos = initial_sigma_pos
    prev_dt = 0.0
    
    # Pre-parse base polygon if available
    base_poly = None
    if state.polygon and len(state.polygon) >= 3:
        try:
            from shapely.geometry import Polygon
            base_poly = Polygon(state.polygon)
        except ImportError:
            pass
            
    import math
    
    for dt in sorted(lead_times):
        # Forecast position
        x, y = forecast_position(state, dt)
        
        # Propagate uncertainty from previous step
        delta_t = dt - prev_dt
        sigma_pos = propagate_position_uncertainty(sigma_pos, sigma_vel, delta_t)
        
        forecast_poly_coords = None
        if base_poly is not None:
            from shapely.affinity import translate
            # Translate from current storm position (state.x, state.y) to future position (x, y)
            dx = x - state.x
            dy = y - state.y
            translated_poly = translate(base_poly, xoff=dx, yoff=dy)
            
            # Proportional Expansion: Scale buffer by cell width
            # Small cells shouldn't balloon to 30km clusters
            minx, miny, maxx, maxy = base_poly.bounds
            width = max(maxx - minx, maxy - miny)
            ref_width = 20000.0 # 20km reference
            w_scale = min(1.0, max(0.1, width / ref_width))
            
            # Buffer by radius (max sigma * sqrt(2.12) for 90% overlap)
            # 2.12 is the tuned Chi-squared value for 2D 90% average overlap
            # Now scaled by w_scale to respect cell size
            radius = max(sigma_pos[0], sigma_pos[1]) * math.sqrt(2.12) * w_scale
            
            # Add a small 500m floor for tiny cells
            radius = max(radius, 500.0)
            
            buffered_poly = translated_poly.buffer(radius)
            
            if not buffered_poly.is_empty:
                # Use exterior coords
                forecast_poly_coords = list(buffered_poly.exterior.coords)
        
        forecast_points.append(ForecastPoint(
            x=x,
            y=y,
            lead_time=dt,
            sigma_x=sigma_pos[0],
            sigma_y=sigma_pos[1],
            polygon=forecast_poly_coords
        ))
        
        prev_dt = dt
    
    return forecast_points


def forecast_motion_cone(
    state: StormState,
    lead_times: Optional[List[float]] = None,
    confidence: float = 0.95,
    n_points: int = 36
) -> List[dict]:
    """
    Generate motion cone ellipses at each forecast time.
    
    Args:
        state: Current storm state
        lead_times: List of lead times in seconds
        confidence: Confidence level for ellipses (e.g., 0.95)
        n_points: Number of points per ellipse
        
    Returns:
        List of dicts with 'center', 'lead_time', and 'ellipse' (list of points)
    """
    from .uncertainty import generate_uncertainty_ellipse
    
    points = forecast_with_uncertainty(state, lead_times)
    
    cones = []
    for fp in points:
        ellipse_pts = generate_uncertainty_ellipse(
            fp.sigma_x, 
            fp.sigma_y, 
            confidence=confidence, 
            n_points=n_points
        )
        
        # Translate ellipse to forecast position
        ellipse_absolute = [(fp.x + ex, fp.y + ey) for ex, ey in ellipse_pts]
        
        cones.append({
            'center': (fp.x, fp.y),
            'lead_time': fp.lead_time,
            'ellipse': ellipse_absolute,
            'sigma_x': fp.sigma_x,
            'sigma_y': fp.sigma_y,
        })
    
    return cones
