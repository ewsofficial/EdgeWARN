"""
Environmental Diagnostics Module

Height-adaptive steering, shear, and Bunkers motion calculations.
"""

import math
from typing import Dict, Tuple, Optional

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
    base_level: int = 850,
    echo_top_30: Optional[float] = None
) -> Tuple[float, float]:
    """
    Compute effective shear over the storm depth.
    
    V_top corresponds to the highest RAP level intersecting the storm core.
    
    Args:
        profile: Environmental wind profile
        h_core: Storm core height in km AGL
        base_level: Base level (default 850 mb)
        echo_top_30: Optional 30 dBZ echo top for capping shear layer in shallow storms
        
    Returns:
        Effective shear vector (du, dv) in m/s
    """
    # Select top level based on storm height
    top_level = 850
    if h_core >= 10.0:
        top_level = 250
    elif h_core >= 5.5:
        top_level = 500
    elif h_core >= 3.0:
        top_level = 700
    else:
        top_level = 850  # Minimal shear for very shallow
        
    # Cap shear for shallow storms
    if h_core <= 5.0 and top_level < 700:
        top_level = 700
        
    # If echo top 30 is provided, prevent top_level from exceeding physical storm height
    # Approximate standard atmosphere heights: 850mb~1.5km, 700mb~3km, 500mb~5.5km, 250mb~10km
    if echo_top_30 is not None:
        if echo_top_30 < 3.0 and top_level < 850:
            top_level = 850
        elif echo_top_30 < 5.5 and top_level < 700:
            top_level = 700
        elif echo_top_30 < 10.0 and top_level < 500:
            top_level = 500
            
    # Always ensure that top_level <= base_level
    if top_level > base_level:
        top_level = base_level
    
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

def compute_raw_bunkers_motion(
    profile: EnvironmentProfile,
    right_mover: bool = True
) -> Tuple[float, float]:
    """
    Compute Bunkers-type deviant storm motion estimate using the raw Bunkers et al. (2000) method.
    Fixed 0-6 km mean wind, 0-6 km shear, and constant 7.5 m/s deviation.
    """
    # 0-6 km mean (sfc to 500mb approximation using 850, 700, 500)
    u_vals, v_vals = [], []
    for level in [850, 700, 500]:
        if level in profile.winds:
            u, v = profile.winds[level]
            u_vals.append(u)
            v_vals.append(v)
            
    if not u_vals:
        return (0.0, 0.0)
        
    u_mean = sum(u_vals) / len(u_vals)
    v_mean = sum(v_vals) / len(v_vals)
    
    # 0-6 km shear (850 to 500mb approximation)
    u_base, v_base = profile.get_wind(850)
    u_top, v_top = profile.get_wind(500)
    shear_u = u_top - u_base
    shear_v = v_top - v_base
    
    shear_mag = _vector_magnitude(shear_u, shear_v)
    if shear_mag < 1e-10:
        return (u_mean, v_mean)
        
    n_u, n_v = _rotate_vector_90(shear_u, shear_v, clockwise=right_mover)
    n_u, n_v = _unit_vector(n_u, n_v)
    
    # Canonical deviation
    d = 7.5
    
    u_bunkers = u_mean + d * n_u
    v_bunkers = v_mean + d * n_v
    
    return (u_bunkers, v_bunkers)

def compute_corfidi_motion(
    profile: EnvironmentProfile,
    right_mover: bool = True
) -> Tuple[float, float]:
    """
    Compute Corfidi-type (MCS) deviant storm motion estimate.
    
    V_corfidi = V_mean(850-250) - V_llj(850)
    
    This is the "Downshear Corfidi" vector used for predicting the motion
    of mesoscale convective systems (MCSs).
    
    Args:
        profile: Environmental wind profile
        right_mover: Unused for standard Corfidi, kept for signature parity
        
    Returns:
        Corfidi motion vector (u_corfidi, v_corfidi) in m/s
    """
    # Use a deep-layer mean (850 to 250 mb)
    u_vals, v_vals = [], []
    for level in [850, 700, 500, 250]:
        if level in profile.winds:
            u, v = profile.winds[level]
            u_vals.append(u)
            v_vals.append(v)
    
    if not u_vals:
        return (0.0, 0.0)
        
    u_mean = sum(u_vals) / len(u_vals)
    v_mean = sum(v_vals) / len(v_vals)
    
    # Inflow/LLJ (850 mb)
    u_llj, v_llj = profile.get_wind(850)
    
    # Downshear Corfidi: V_mcs = V_mean - V_llj
    # Note: V_mean is effectively V_advection, V_llj is V_propagation
    u_corfidi = u_mean - u_llj
    v_corfidi = v_mean - v_llj
    
    return (u_corfidi, v_corfidi)


def compute_storm_core_height(
    echo_top_30: Optional[float], 
    echo_top_50: Optional[float],
    freezing_level_km: Optional[float] = None
) -> float:
    """
    Compute storm core height from echo top diagnostics.
    
    H_core = (EchoTop30 + EchoTop50) / 2
    
    Args:
        echo_top_30: Height of 30 dBZ echo top in km AGL
        echo_top_50: Height of 50 dBZ echo top in km AGL
        freezing_level_km: Optional freezing level limiting height
        
    Returns:
        Storm core height in km AGL
    """
    if (echo_top_30 is None or echo_top_30 == 0.0) and (echo_top_50 is None or echo_top_50 == 0.0):
        h_core = 0.0
    elif echo_top_50 is None or echo_top_50 < 2.0:
        # Fallback to 30 dBZ mainly if 50 dBZ is weak/absent
        h_core = float(echo_top_30) if echo_top_30 is not None else 0.0
    elif echo_top_30 is None or echo_top_30 == 0.0:
        h_core = float(echo_top_50)
    else:
        h_core = (echo_top_30 + echo_top_50) / 2.0
        
    # Thermal capping
    if freezing_level_km is not None and h_core > freezing_level_km + 1.0:
        h_core = freezing_level_km + 1.0
        
    return h_core
