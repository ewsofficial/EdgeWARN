"""
Motion Blending Module

Observed motion smoothing, propagation decomposition, and motion vector blending.
"""

from typing import List, Tuple, Optional, Union
import numpy as np
from scipy.signal import savgol_filter

from .config import DEFAULT_BLENDING_WEIGHTS, BlendingWeights
from .types import MotionVector


def _exponential_filter(
    values: List[Tuple[float, float]], 
    alpha: float = 0.3
) -> Tuple[float, float]:
    """Apply exponential smoothing to (u, v) vectors."""
    if not values:
        raise ValueError("Cannot filter empty sequence")
    u_smooth, v_smooth = values[0]
    for u, v in values[1:]:
        u_smooth = alpha * u + (1 - alpha) * u_smooth
        v_smooth = alpha * v + (1 - alpha) * v_smooth
    return (u_smooth, v_smooth)


def smooth_observed_motion(
    history: List[Tuple[float, float]],
    method: str = "exponential",
    alpha: float = 0.3,
    window_length: int = 7,
    polyorder: int = 2
) -> Tuple[float, float]:
    """
    Apply temporal smoothing to observed storm motion vectors.
    
    Args:
        history: List of (u, v) motion vectors in chronological order
        method: Smoothing method ('exponential', 'mean', or 'savgol')
        alpha: Exponential smoothing parameter (higher = more weight to recent)
        window_length: Window size for Savitzky-Golay filter (must be odd)
        polyorder: Polynomial order for Savitzky-Golay filter
        
    Returns:
        Smoothed (u, v) motion vector in m/s
        
    Raises:
        ValueError: If history is empty
    """
    if not history:
        raise ValueError("Cannot smooth empty motion history")
    
    if len(history) == 1:
        return history[0]
    
    if method == "exponential":
        return _exponential_filter(history, alpha)
    elif method == "mean":
        u_mean = sum(h[0] for h in history) / len(history)
        v_mean = sum(h[1] for h in history) / len(history)
        return (u_mean, v_mean)
    elif method == "savgol":
        if len(history) < window_length:
            # Fallback to mean if not enough samples
            return smooth_observed_motion(history, method="mean")
        
        u_vals = np.array([h[0] for h in history])
        v_vals = np.array([h[1] for h in history])
        
        u_smooth = savgol_filter(u_vals, window_length, polyorder)
        v_smooth = savgol_filter(v_vals, window_length, polyorder)
        
        return (float(u_smooth[-1]), float(v_smooth[-1]))
    else:
        raise ValueError(f"Unknown smoothing method: {method}")


def smooth_position_track(
    track: List[Tuple[float, float]],
    window_length: Optional[int] = None,
    polyorder: int = 2
) -> List[Tuple[float, float]]:
    """
    Smooth a sequence of (x, y) coordinates using Savitzky-Golay.
    
    Args:
        track: List of (x, y) coordinates
        window_length: Window size (must be odd). If None, uses min(7, len(track)) adjusted to odd.
        polyorder: Polynomial order
        
    Returns:
        List of smoothed (x, y) coordinates
    """
    if len(track) < 3:
        return track
    
    if window_length is None:
        window_length = min(7, len(track))
    
    # Savitzky-Golay window must be odd and > polyorder
    if window_length % 2 == 0:
        window_length -= 1
    
    window_length = max(window_length, polyorder + 1)
    if window_length % 2 == 0:
        window_length += 1
        
    if len(track) < window_length:
        return track
    
    x_vals = np.array([p[0] for p in track])
    y_vals = np.array([p[1] for p in track])
    
    x_smooth = savgol_filter(x_vals, window_length, polyorder)
    y_smooth = savgol_filter(y_vals, window_length, polyorder)
    
    return [(float(x), float(y)) for x, y in zip(x_smooth, y_smooth)]


def calculate_motion_jitter(history: List[Tuple[float, float]]) -> float:
    """
    Calculate storm motion jitter (velocity standard deviation).
    
    Args:
        history: List of (u, v) motion vectors in m/s
        
    Returns:
        Combined standard deviation of velocity components in m/s
    """
    if len(history) < 2:
        return 0.0
    
    import math
    u_vals = [h[0] for h in history]
    v_vals = [h[1] for h in history]
    
    def std_dev(data):
        mean = sum(data) / len(data)
        variance = sum((x - mean)**2 for x in data) / len(data)
        return math.sqrt(variance)
    
    # Combined jitter: sqrt(std_u^2 + std_v^2)
    return math.sqrt(std_dev(u_vals)**2 + std_dev(v_vals)**2)


def compute_propagation(
    v_obs: Tuple[float, float],
    v_mean_star: Tuple[float, float]
) -> Tuple[float, float]:
    """
    Compute storm propagation vector.
    
    V_prop = V_obs - V_mean*
    
    The propagation component represents storm-scale processes such as cold pool
    surges, boundary interactions, and updraft cycling.
    
    Args:
        v_obs: Observed (smoothed) storm motion (u, v) in m/s
        v_mean_star: Height-adaptive environmental steering (u, v) in m/s
        
    Returns:
        Propagation vector (u_prop, v_prop) in m/s
    """
    return (v_obs[0] - v_mean_star[0], v_obs[1] - v_mean_star[1])


def blend_motion(
    v_obs: Tuple[float, float],
    v_mean_star: Tuple[float, float],
    v_bunkers_star: Tuple[float, float],
    weights: Optional[BlendingWeights] = None
) -> Tuple[float, float]:
    """
    Compute blended storm motion vector.
    
    V_final = w_o × V_obs + w_m × V_mean* + w_b × V_bunkers*
    
    Args:
        v_obs: Smoothed observed storm motion (u, v) in m/s
        v_mean_star: Height-adaptive environmental steering (u, v) in m/s
        v_bunkers_star: Bunkers deviant motion (u, v) in m/s
        weights: Blending weights (defaults to DEFAULT_BLENDING_WEIGHTS)
        
    Returns:
        Blended motion vector (u_final, v_final) in m/s
    """
    if weights is None:
        weights = DEFAULT_BLENDING_WEIGHTS
    
    u_final = (
        weights.w_obs * v_obs[0] +
        weights.w_mean * v_mean_star[0] +
        weights.w_bunkers * v_bunkers_star[0]
    )
    v_final = (
        weights.w_obs * v_obs[1] +
        weights.w_mean * v_mean_star[1] +
        weights.w_bunkers * v_bunkers_star[1]
    )
    
    return (u_final, v_final)


def adjust_weights_for_maturity(
    h_core: float,
    track_history: int,
    shear_magnitude: float,
    base_weights: Optional[BlendingWeights] = None,
    mucape: Optional[float] = None
) -> BlendingWeights:
    """
    Dynamically adjust blending weights based on storm characteristics.
    
    - Newly initiated / shallow storms: favor environmental guidance
    - Mature, deep, well-tracked storms: favor observations
    
    Args:
        h_core: Storm core height in km AGL
        track_history: Number of valid tracking samples
        shear_magnitude: Vertical wind shear magnitude in m/s
        base_weights: Starting weights (defaults to DEFAULT_BLENDING_WEIGHTS)
        
    Returns:
        Adjusted BlendingWeights
    """
    if base_weights is None:
        base_weights = DEFAULT_BLENDING_WEIGHTS
    
    # Start with base weights
    w_obs = base_weights.w_obs
    w_mean = base_weights.w_mean
    w_bunkers = base_weights.w_bunkers
    
    # Adjust for storm maturity (track history)
    if track_history < 3:
        # New storm: rely more on environment
        w_obs -= 0.15
        w_mean += 0.10
        w_bunkers += 0.05
    elif track_history > 10:
        # Well-established: trust observations more
        w_obs += 0.10
        w_mean -= 0.05
        w_bunkers -= 0.05
    
    # Adjust for storm depth
    if h_core < 6.0:
        # Shallow: reduce Bunkers influence
        w_bunkers -= 0.10
        w_mean += 0.10
    elif h_core > 10.0:
        # Deep: slight increase in Bunkers for supercell behavior
        w_bunkers += 0.05
        w_mean -= 0.05
    
    # Adjust for shear magnitude
    if shear_magnitude > 25.0:
        # Strong shear: increase Bunkers influence
        w_bunkers += 0.05
        w_obs -= 0.05
        
    # Proxy Mode Classification: Stratiform
    if mucape is not None and mucape <= 250 and h_core <= 6.0:
        # Stratiform footprint is advection-dominant and lacks deep-layer rotation
        # Therefore Bunkers right-mover deviation should be completely negated.
        # Centroid tracking is also inherently more stable with large stratiform boundaries.
        w_bunkers = 0.0
        w_mean += 0.15
        w_obs -= 0.05
    
    # Ensure weights are non-negative and normalize
    w_obs = max(0.1, w_obs)
    w_mean = max(0.05, w_mean)
    w_bunkers = max(0.05, w_bunkers)
    
    total = w_obs + w_mean + w_bunkers
    
    return BlendingWeights(
        w_obs=w_obs / total,
        w_mean=w_mean / total,
        w_bunkers=w_bunkers / total,
    )
