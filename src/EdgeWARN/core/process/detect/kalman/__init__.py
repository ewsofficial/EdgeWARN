"""
Kalman Filter Module for Storm Cell Tracking

This module provides Kalman filter-based tracking continuity for storm cells
when ProbSevere data temporarily fails to detect them.

Key Components:
- KalmanFilter: Core 6-dimensional Kalman filter
- StateVector: State representation (position, velocity, acceleration)
- CovarianceMatrix: Uncertainty representation
- ConfidenceCalculator: Confidence scoring for predicted cells
- PredictionState: Tracking state for cells in prediction mode

Usage:
    from EdgeWARN.core.process.detect.kalman import KalmanFilter, ConfidenceCalculator
    
    # Initialize filter from storm cell
    kf = KalmanFilter()
    kf.initialize_from_cell(storm_cell)
    
    # Predict forward
    predicted_state = kf.predict(dt=120.0)  # 2 minutes
    
    # Update with observation
    from EdgeWARN.core.process.detect.kalman import KalmanObservation
    obs = KalmanObservation(lat=33.5, lon=-97.2)
    updated_state = kf.update(obs)
"""

from .config import (
    KalmanConfig,
    TrackingConfig,
    DEFAULT_KALMAN_CONFIG,
    DEFAULT_TRACKING_CONFIG,
)

from .state import (
    StateVector,
    CovarianceMatrix,
    latlon_to_meters,
    meters_to_latlon,
    haversine_distance,
)

from .filter import (
    KalmanFilter,
    KalmanObservation,
)

from .confidence import (
    ConfidenceCalculator,
    PredictionState,
)

__all__ = [
    # Configuration
    'KalmanConfig',
    'TrackingConfig',
    'DEFAULT_KALMAN_CONFIG',
    'DEFAULT_TRACKING_CONFIG',
    
    # State
    'StateVector',
    'CovarianceMatrix',
    'latlon_to_meters',
    'meters_to_latlon',
    'haversine_distance',
    
    # Filter
    'KalmanFilter',
    'KalmanObservation',
    
    # Confidence
    'ConfidenceCalculator',
    'PredictionState',
]
