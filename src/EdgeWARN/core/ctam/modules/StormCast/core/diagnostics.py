"""
Environmental Diagnostics Module

Height-adaptive steering, shear, and Bunkers motion calculations.
"""

import math
from typing import Dict, Tuple

from .config import (
    PRESSURE_LEVELS,
    GAUSSIAN_WEIGHT_PARAMS,
    BUNKERS_DEVIATION,
)
from .types import EnvironmentProfile, MotionVector

# Import math functions locally to avoid circular/path issues
import math

def _vector_magnitude(u: float, v: float) -> float:
    return math.sqrt(u * u + v * v)

def _unit_vector(u: float, v: float) -> tuple:
    mag = _vector_magnitude(u, v)
    if mag < 1e-10:
        return (0.0, 0.0)
    return (u / mag, v / mag)

def _rotate_vector_90(u: float, v: float, clockwise: bool = True) -> tuple:
    if clockwise:
        return (v, -u)
    return (-v, u)

def _gaussian(x: float, mu: float, sigma: float) -> float:
    z = (x - mu) / sigma
    return math.exp(-0.5 * z * z)

def _linear_interpolate(x: float, x1: float, x2: float, y1: float, y2: float) -> float:
    if x <= x1:
        return y1
    if x >= x2:
        return y2
    t = (x - x1) / (x2 - x1)
    return y1 + t * (y2 - y1)


def compute_height_weights(h_core: float) -> Dict[int, float]:
    """
    Compute height-dependent pressure level weights using Gaussian transitions.
    
    Each pressure level has a Gaussian activation centered at its characteristic height.
    Weights are normalized to sum to 1.0.
    
    Args:
        h_core: Storm core height in km AGL (typically EchoTop30 + EchoTop50) / 2
        
    Returns:
        Dictionary mapping pressure level (mb) to weight [0, 1]
        
    Example:
        >>> weights = compute_height_weights(6.0)
        >>> # Moderate depth: balanced 850, 700, 500 contribution
    """
    raw_weights = {}
    
    for level in PRESSURE_LEVELS:
        params = GAUSSIAN_WEIGHT_PARAMS[level]
        raw_weights[level] = _gaussian(h_core, params.mu, params.sigma)
    
    # Normalize weights to sum to 1.0
    total = sum(raw_weights.values())
    if total < 1e-10:
        # Fallback: equal weights if all activations are near zero
        n = len(PRESSURE_LEVELS)
        return {level: 1.0 / n for level in PRESSURE_LEVELS}
    
    return {level: w / total for level, w in raw_weights.items()}


def compute_adaptive_steering(
    profile: EnvironmentProfile,
    h_core: float
) -> Tuple[float, float]:
    """
    Compute height-adaptive environmental steering flow (V_mean*).
    
    V_mean* = Σ w_p(H_core) × V_p
    
    Args:
        profile: Environmental wind profile at multiple pressure levels
        h_core: Storm core height in km AGL
        
    Returns:
        Tuple (u_mean, v_mean) in m/s
    """
    weights = compute_height_weights(h_core)
    
    u_mean = 0.0
    v_mean = 0.0
    
    for level, weight in weights.items():
        if level in profile.winds:
            u, v = profile.winds[level]
            u_mean += weight * u
            v_mean += weight * v
    
    return (u_mean, v_mean)


def compute_shear(
    profile: EnvironmentProfile,
    base_level: int = 850,
    top_level: int = 500
) -> Tuple[float, float]:
    """
    Compute vertical wind shear vector.
    
    S_eff = V_top - V_base
    
    Args:
        profile: Environmental wind profile
        base_level: Base pressure level in mb (default 850)
        top_level: Top pressure level in mb (default 500)
        
    Returns:
        Shear vector (du, dv) in m/s
    """
    u_base, v_base = profile.get_wind(base_level)
    u_top, v_top = profile.get_wind(top_level)
    
    return (u_top - u_base, v_top - v_base)


def compute_effective_shear(
    profile: EnvironmentProfile,
    h_core: float,
    base_level: int = 850
) -> Tuple[float, float]:
    """
    Compute effective shear over the storm depth.
    
    V_top corresponds to the highest RAP level intersecting the storm core.
    
    Args:
        profile: Environmental wind profile
        h_core: Storm core height in km AGL
        base_level: Base level (default 850 mb)
        
    Returns:
        Effective shear vector (du, dv) in m/s
    """
    # Select top level based on storm height
    if h_core >= 10.0:
        top_level = 250
    elif h_core >= 5.5:
        top_level = 500
    elif h_core >= 3.0:
        top_level = 700
    else:
        top_level = 850  # Minimal shear for very shallow
    
    return compute_shear(profile, base_level, top_level)


def compute_deviation_magnitude(h_core: float) -> float:
    """
    Compute height-scaled Bunkers deviation magnitude.
    
    Scales linearly from D_shallow to D_deep between height thresholds.
    
    Args:
        h_core: Storm core height in km AGL
        
    Returns:
        Deviation magnitude D in m/s
    """
    return _linear_interpolate(
        h_core,
        BUNKERS_DEVIATION.h_shallow,
        BUNKERS_DEVIATION.h_deep,
        BUNKERS_DEVIATION.d_shallow,
        BUNKERS_DEVIATION.d_deep,
    )


def compute_bunkers_motion(
    profile: EnvironmentProfile,
    h_core: float,
    right_mover: bool = True
) -> Tuple[float, float]:
    """
    Compute Bunkers-type deviant storm motion estimate.
    
    V_bunkers* = V_mean* + D(H_core) × n_hat
    
    where n_hat is perpendicular to the shear vector (90° clockwise for right-movers).
    
    Args:
        profile: Environmental wind profile
        h_core: Storm core height in km AGL
        right_mover: If True, compute right-mover motion; else left-mover
        
    Returns:
        Bunkers motion vector (u_bunkers, v_bunkers) in m/s
    """
    # Height-adaptive mean wind
    u_mean, v_mean = compute_adaptive_steering(profile, h_core)
    
    # Effective shear
    shear_u, shear_v = compute_effective_shear(profile, h_core)
    shear_mag = _vector_magnitude(shear_u, shear_v)
    
    if shear_mag < 1e-10:
        # No shear: return mean wind
        return (u_mean, v_mean)
    
    # Unit vector perpendicular to shear
    # Right-mover: 90° clockwise rotation
    # Left-mover: 90° counter-clockwise rotation
    n_u, n_v = _rotate_vector_90(shear_u, shear_v, clockwise=right_mover)
    n_u, n_v = _unit_vector(n_u, n_v)
    
    # Height-scaled deviation magnitude
    d = compute_deviation_magnitude(h_core)
    
    # Bunkers motion
    u_bunkers = u_mean + d * n_u
    v_bunkers = v_mean + d * n_v
    
    return (u_bunkers, v_bunkers)


def compute_storm_core_height(echo_top_30: float, echo_top_50: float) -> float:
    """
    Compute storm core height from echo top diagnostics.
    
    H_core = (EchoTop30 + EchoTop50) / 2
    
    Args:
        echo_top_30: Height of 30 dBZ echo top in km AGL
        echo_top_50: Height of 50 dBZ echo top in km AGL
        
    Returns:
        Storm core height in km AGL
    """
    return (echo_top_30 + echo_top_50) / 2.0
