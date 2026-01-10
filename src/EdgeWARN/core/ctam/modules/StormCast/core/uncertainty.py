"""
Uncertainty Quantification Module

Tracking confidence, velocity covariance, and forecast spread calculations.
"""

import math
from typing import Tuple

from .config import UNCERTAINTY_PARAMS


def compute_tracking_uncertainty(
    n_samples: int,
    motion_jitter: float = 0.0,
    sigma_min: float = None,
    sigma_range: float = None,
    alpha: float = None
) -> float:
    """
    Compute tracking-history-based velocity uncertainty.
    
    σ_hist = σ_min + σ_range / N^α
    
    Args:
        n_samples: Number of valid storm motion samples
        sigma_min: Minimum uncertainty floor (m/s). Default from config.
        sigma_range: Uncertainty for N=1 sample (m/s). Default from config.
        alpha: Rate of confidence increase with N. Default from config.
        
    Returns:
        Velocity uncertainty in m/s
        
    Examples:
        >>> compute_tracking_uncertainty(1)   # ~6.0 m/s
        >>> compute_tracking_uncertainty(16)  # ~2.25 m/s
    """
    if sigma_min is None:
        sigma_min = UNCERTAINTY_PARAMS.sigma_min
    if sigma_range is None:
        sigma_range = UNCERTAINTY_PARAMS.sigma_range
    if alpha is None:
        alpha = UNCERTAINTY_PARAMS.alpha_decay
    
    n = max(n_samples, 1)  # Prevent division by zero
    base_uncertainty = sigma_min + sigma_range / (n ** alpha)
    
    # Add jitter-based uncertainty
    return base_uncertainty + (motion_jitter * UNCERTAINTY_PARAMS.jitter_multiplier)


def compute_velocity_covariance(
    sigma_obs: float = None,
    sigma_env: float = None,
    sigma_hist: float = None,
    n_samples: int = None,
    motion_jitter: float = 0.0
) -> Tuple[float, float, float]:
    """
    Compute combined velocity covariance components.
    
    σ² = σ_obs² + σ_env² + σ_hist²
    
    Args:
        sigma_obs: Observational uncertainty (m/s). Default from config.
        sigma_env: Environmental uncertainty (m/s). Default from config.
        sigma_hist: Tracking history uncertainty (m/s). If None, computed from n_samples.
        n_samples: Number of tracking samples (used if sigma_hist is None).
        
    Returns:
        Tuple of (sigma_u, sigma_v, sigma_total) in m/s
        
    Note:
        Returns isotropic uncertainty (sigma_u = sigma_v).
        Anisotropic uncertainty would require storm motion direction.
    """
    if sigma_obs is None:
        sigma_obs = UNCERTAINTY_PARAMS.sigma_obs
    if sigma_env is None:
        sigma_env = UNCERTAINTY_PARAMS.sigma_env
    if sigma_hist is None:
        if n_samples is not None:
            sigma_hist = compute_tracking_uncertainty(n_samples, motion_jitter=motion_jitter)
        else:
            sigma_hist = UNCERTAINTY_PARAMS.sigma_min
    
    # Combined variance
    variance = sigma_obs**2 + sigma_env**2 + sigma_hist**2
    sigma_total = math.sqrt(variance)
    
    # Isotropic for now
    return (sigma_total, sigma_total, sigma_total)


def propagate_position_uncertainty(
    sigma_pos_current: Tuple[float, float],
    sigma_vel: Tuple[float, float],
    dt: float
) -> Tuple[float, float]:
    """
    Propagate position uncertainty forward in time.
    
    P_pos(t + Δt) = P_pos(t) + P_v × Δt²
    
    Args:
        sigma_pos_current: Current position uncertainty (sigma_x, sigma_y) in meters
        sigma_vel: Velocity uncertainty (sigma_u, sigma_v) in m/s
        dt: Time step in seconds
        
    Returns:
        New position uncertainty (sigma_x, sigma_y) in meters
    """
    # Variance propagation: σ²_pos_new = σ²_pos + σ²_vel × dt²
    var_x_new = sigma_pos_current[0]**2 + (sigma_vel[0] * dt)**2
    var_y_new = sigma_pos_current[1]**2 + (sigma_vel[1] * dt)**2
    
    return (math.sqrt(var_x_new), math.sqrt(var_y_new))


def generate_uncertainty_ellipse(
    sigma_x: float,
    sigma_y: float,
    confidence: float = 0.95,
    n_points: int = 36
) -> list:
    """
    Generate points for an uncertainty ellipse.
    
    Args:
        sigma_x: X-axis uncertainty (1-sigma) in meters
        sigma_y: Y-axis uncertainty (1-sigma) in meters
        confidence: Confidence level (0.95 = 95%)
        n_points: Number of points on the ellipse
        
    Returns:
        List of (x, y) points defining the ellipse (relative to center)
    """
    # Chi-squared value for 2D at given confidence
    # 95%: 5.991, 90%: 4.605, 68%: 2.30
    chi2_values = {0.68: 2.30, 0.90: 4.605, 0.95: 5.991, 0.99: 9.21}
    chi2 = chi2_values.get(confidence, 5.991)
    scale = math.sqrt(chi2)
    
    points = []
    for i in range(n_points):
        theta = 2 * math.pi * i / n_points
        x = scale * sigma_x * math.cos(theta)
        y = scale * sigma_y * math.sin(theta)
        points.append((x, y))
    
    return points
