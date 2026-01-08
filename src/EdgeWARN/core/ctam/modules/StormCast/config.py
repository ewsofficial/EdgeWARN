"""
StormCast Configuration Constants

All tunable parameters and default values for the StormCast framework.
"""

from dataclasses import dataclass
from typing import Dict, Tuple

# =============================================================================
# Pressure Levels (Section 3.2)
# =============================================================================
PRESSURE_LEVELS: Tuple[int, ...] = (850, 700, 500, 250)
"""Pressure levels in mb for environmental wind extraction."""

# Approximate heights AGL for reference
LEVEL_HEIGHTS: Dict[int, float] = {
    850: 1.5,   # km AGL
    700: 3.0,   # km AGL
    500: 5.5,   # km AGL
    250: 10.5,  # km AGL
}

# =============================================================================
# Gaussian Height Weight Parameters (Section 6.1)
# =============================================================================
@dataclass(frozen=True)
class GaussianWeightParams:
    """Parameters for Gaussian height-weight transitions."""
    mu: float    # Center height (km)
    sigma: float # Spread (km)

GAUSSIAN_WEIGHT_PARAMS: Dict[int, GaussianWeightParams] = {
    850: GaussianWeightParams(mu=3.0, sigma=3.0),
    700: GaussianWeightParams(mu=5.0, sigma=3.0),
    500: GaussianWeightParams(mu=8.0, sigma=3.0),
    250: GaussianWeightParams(mu=12.0, sigma=3.0),
}
"""Gaussian parameters for each pressure level's height-dependent weight."""

# =============================================================================
# Blending Weights (Section 6.3)
# =============================================================================
@dataclass(frozen=True)
class BlendingWeights:
    """Weights for motion vector blending."""
    w_obs: float      # Weight for smoothed observed motion
    w_mean: float     # Weight for height-adaptive mean wind
    w_bunkers: float  # Weight for Bunkers deviant motion

DEFAULT_BLENDING_WEIGHTS = BlendingWeights(
    w_obs=0.4,
    w_mean=0.3,
    w_bunkers=0.3,
)
"""Default blending weights for warm-season convection."""

# Dynamic weight presets
SHALLOW_STORM_WEIGHTS = BlendingWeights(w_obs=0.3, w_mean=0.3, w_bunkers=0.4)
MATURE_STORM_WEIGHTS = BlendingWeights(w_obs=0.6, w_mean=0.15, w_bunkers=0.25)

# =============================================================================
# Bunkers Motion Parameters (Section 4.3, 6.2)
# =============================================================================
BUNKERS_DEVIATION_FULL: float = 7.5  # m/s - canonical supercell deviation
BUNKERS_DEVIATION_SHALLOW: float = 3.0  # m/s - reduced for shallow convection
BUNKERS_HEIGHT_SHALLOW: float = 6.0  # km - below this, use reduced deviation
BUNKERS_HEIGHT_DEEP: float = 10.0  # km - above this, use full deviation

@dataclass(frozen=True)
class BunkersParams:
    """Bunkers deviation parameters."""
    d_shallow: float  # Deviation for shallow storms (m/s)
    d_deep: float     # Deviation for deep storms (m/s)
    h_shallow: float  # Height threshold for shallow (km)
    h_deep: float     # Height threshold for deep (km)

BUNKERS_DEVIATION = BunkersParams(
    d_shallow=3.0,
    d_deep=7.5,
    h_shallow=6.0,
    h_deep=10.0,
)

# =============================================================================
# Kalman Filter Parameters (Section 7)
# =============================================================================
@dataclass(frozen=True)
class KalmanParams:
    """Kalman filter configuration."""
    alpha: float           # Forecast smoothing coefficient (0-1)
    dt_default: float      # Default time step (seconds)
    sigma_pos: float       # Position observation uncertainty (m)
    sigma_vel: float       # Velocity process noise (m/s)
    q_pos: float           # Position process noise variance (m²)
    q_vel: float           # Velocity process noise variance (m²/s²)

KALMAN_PARAMS = KalmanParams(
    alpha=0.7,
    dt_default=300.0,      # 5 minutes
    sigma_pos=5000.0,      # meters
    sigma_vel=12.0,        # m/s
    q_pos=10000.0,         # (100 m)²
    q_vel=144.0,           # (12 m/s)²
)

# =============================================================================
# Uncertainty Parameters (Section 9)
# =============================================================================
@dataclass(frozen=True)
class UncertaintyParams:
    """Uncertainty quantification parameters."""
    sigma_min: float      # Minimum uncertainty floor (m/s)
    sigma_range: float    # Uncertainty for N=1 sample (m/s)
    alpha_decay: float    # Rate of confidence increase with N
    sigma_obs: float      # Observational centroid jitter (m/s)
    sigma_env: float      # Environmental (RAP) uncertainty (m/s)
    jitter_multiplier: float # Multiplier for velocity standard deviation

UNCERTAINTY_PARAMS = UncertaintyParams(
    sigma_min=1.2,
    sigma_range=2.5,
    alpha_decay=0.5,
    sigma_obs=4.0,
    sigma_env=2.0,
    jitter_multiplier=0.1,
)

# =============================================================================
# Forecast Parameters (Section 8)
# =============================================================================
DEFAULT_LEAD_TIMES: Tuple[float, ...] = (900.0, 1800.0, 2700.0, 3600.0)
"""Default forecast lead times in seconds (15, 30, 45, 60 min)."""

MAX_RELIABLE_LEAD_TIME: float = 3600.0  # 60 minutes
"""Beyond this, forecast skill degrades significantly."""

MIN_VELOCITY_THRESHOLD: float = 2.0  # m/s
MAX_VELOCITY_THRESHOLD: float = 50.0  # m/s
"""Thresholds for filtering stationary or unrealistically fast storm motions."""
