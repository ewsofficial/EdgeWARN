"""
Kalman Filter Module for Storm Cell Tracking

This module provides Kalman filter-based tracking continuity for storm cells
when ProbSevere data temporarily fails to detect them, along with advanced
measurement assignment using the Hungarian algorithm.

Key Components:
- KalmanFilter: Core 6-dimensional Kalman filter with Mahalanobis gating
- StateVector: State representation (position, velocity, acceleration)
- CovarianceMatrix: Uncertainty representation
- ConfidenceCalculator: Confidence scoring for predicted cells
- PredictionState: Tracking state for cells in prediction mode
- AssignmentConfig: Configuration for measurement assignment
- AssignmentCostCalculator: Cost function for track-detection assignment

Usage:
    from EdgeWARN.process.detect.kalman import KalmanFilter, ConfidenceCalculator
    
    # Initialize filter from storm cell
    kf = KalmanFilter()
    kf.initialize_from_cell(storm_cell)
    
    # Predict forward
    predicted_state = kf.predict(dt=120.0)  # 2 minutes
    
    # Update with observation
    from EdgeWARN.process.detect.kalman import KalmanObservation
    obs = KalmanObservation(lat=33.5, lon=-97.2)
    updated_state = kf.update(obs)
    
    # Check Mahalanobis distance for gating
    d_m = kf.get_mahalanobis_distance(lat=33.6, lon=-97.3)
    cfg = default_assignment_config()
    is_valid = kf.is_within_gate(
        lat=33.6, lon=-97.3,
        threshold=cfg.gating_threshold,
        min_radius_km=cfg.min_gating_radius_km,
    )
"""

from .config import (
    KalmanConfig,
    TrackingConfig,
    AssignmentConfig,
    FilterInternalsConfig,
    ConfidenceConfig,
    AssignmentCostsConfig,
    default_kalman_config,
    default_tracking_config,
    default_assignment_config,
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

from .assignment import (
    AssignmentCostCalculator,
    AssignmentResult,
    build_cost_matrix,
    build_filtered_cost_matrix,
    solve_assignment,
    run_hybrid_assignment,
    run_greedy_assignment,
)

__all__ = [
    # Configuration
    'KalmanConfig',
    'TrackingConfig',
    'AssignmentConfig',
    'FilterInternalsConfig',
    'ConfidenceConfig',
    'AssignmentCostsConfig',
    'default_kalman_config',
    'default_tracking_config',
    'default_assignment_config',

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
    
    # Assignment
    'AssignmentCostCalculator',
    'AssignmentResult',
    'build_cost_matrix',
    'build_filtered_cost_matrix',
    'solve_assignment',
    'run_hybrid_assignment',
    'run_greedy_assignment',
]
