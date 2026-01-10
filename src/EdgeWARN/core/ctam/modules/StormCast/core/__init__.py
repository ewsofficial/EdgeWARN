"""
StormCast: Thunderstorm Motion Prediction Framework

A modular framework for predicting thunderstorm motion by blending
observed storm displacement vectors with RAP environmental wind profiles.
"""

from .types import StormState, EnvironmentProfile, ForecastPoint
from .config import (
    PRESSURE_LEVELS,
    DEFAULT_BLENDING_WEIGHTS,
    GAUSSIAN_WEIGHT_PARAMS,
    BUNKERS_DEVIATION,
    KALMAN_PARAMS,
    UNCERTAINTY_PARAMS,
)
from .diagnostics import (
    compute_height_weights,
    compute_adaptive_steering,
    compute_shear,
    compute_bunkers_motion,
)
from .blending import (
    smooth_observed_motion,
    compute_propagation,
    blend_motion,
)
from .kalman import StormKalmanFilter
from .uncertainty import (
    compute_tracking_uncertainty,
    compute_velocity_covariance,
    propagate_position_uncertainty,
)
from .forecast import (
    forecast_position,
    generate_forecast_track,
)
from .core import (
    StormCastEngine,
    ForecastResult,
)

__version__ = "0.1.0"
__all__ = [
    # Types
    "StormState",
    "EnvironmentProfile",
    "ForecastPoint",
    # Config
    "PRESSURE_LEVELS",
    "DEFAULT_BLENDING_WEIGHTS",
    "GAUSSIAN_WEIGHT_PARAMS",
    "BUNKERS_DEVIATION",
    "KALMAN_PARAMS",
    "UNCERTAINTY_PARAMS",
    # Diagnostics
    "compute_height_weights",
    "compute_adaptive_steering",
    "compute_shear",
    "compute_bunkers_motion",
    # Blending
    "smooth_observed_motion",
    "compute_propagation",
    "blend_motion",
    # Kalman
    "StormKalmanFilter",
    # Uncertainty
    "compute_tracking_uncertainty",
    "compute_velocity_covariance",
    "propagate_position_uncertainty",
    # Forecast
    "forecast_position",
    "generate_forecast_track",
    # Core
    "StormCastEngine",
    "ForecastResult",
]
