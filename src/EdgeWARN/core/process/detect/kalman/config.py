"""
Kalman Filter Configuration

Configuration parameters for the Storm Cell Kalman Filter.
"""

from dataclasses import dataclass
from typing import Optional
import yaml
from pathlib import Path


@dataclass
class KalmanConfig:
    """Configuration for Kalman filter parameters."""
    
    # Process noise parameters
    process_noise_position: float = 0.1  # Process noise for position states
    process_noise_velocity: float = 0.5  # Process noise for velocity states
    process_noise_acceleration: float = 0.1  # Process noise for acceleration states
    
    # Measurement noise parameters
    measurement_noise_position: float = 0.5  # Measurement noise (km)
    
    @classmethod
    def from_yaml(cls, path: Optional[Path] = None) -> "KalmanConfig":
        """Load configuration from YAML file."""
        if path is None:
            # Default config path
            path = Path(__file__).parent.parent.parent.parent.parent.parent / "config" / "kalman.yaml"
        
        if not path.exists():
            return cls()
        
        with open(path, 'r') as f:
            data = yaml.safe_load(f)
        
        if data is None or 'kalman_filter' not in data:
            return cls()
        
        kalman_data = data['kalman_filter']
        process_noise = kalman_data.get('process_noise', {})
        measurement_noise = kalman_data.get('measurement_noise', {})
        
        return cls(
            process_noise_position=process_noise.get('position', 0.1),
            process_noise_velocity=process_noise.get('velocity', 0.5),
            process_noise_acceleration=process_noise.get('acceleration', 0.1),
            measurement_noise_position=measurement_noise.get('position', 0.5),
        )


@dataclass
class TrackingConfig:
    """Configuration for tracking parameters."""
    
    # Prediction limits
    max_prediction_time_minutes: float = 10.0  # Maximum time in prediction mode
    
    # Re-acquisition parameters
    reacquisition_radius_km: float = 5.0  # Maximum distance for re-acquisition
    
    # Confidence parameters
    confidence_threshold: float = 0.4  # Minimum confidence before termination
    confidence_decay_factor: float = 0.7  # Per-scan confidence decay
    
    @classmethod
    def from_yaml(cls, path: Optional[Path] = None) -> "TrackingConfig":
        """Load configuration from YAML file."""
        if path is None:
            path = Path(__file__).parent.parent.parent.parent.parent.parent / "config" / "kalman.yaml"
        
        if not path.exists():
            return cls()
        
        with open(path, 'r') as f:
            data = yaml.safe_load(f)
        
        if data is None or 'tracking' not in data:
            return cls()
        
        tracking_data = data['tracking']
        
        return cls(
            max_prediction_time_minutes=tracking_data.get('max_prediction_time_minutes', 10.0),
            reacquisition_radius_km=tracking_data.get('reacquisition_radius_km', 5.0),
            confidence_threshold=tracking_data.get('confidence_threshold', 0.4),
            confidence_decay_factor=tracking_data.get('confidence_decay_factor', 0.7),
        )


# Default configurations
DEFAULT_KALMAN_CONFIG = KalmanConfig()
DEFAULT_TRACKING_CONFIG = TrackingConfig()


@dataclass
class AssignmentConfig:
    """
    Configuration for measurement assignment algorithm.
    
    Controls the hybrid pre-filter + Hungarian algorithm approach
    for assigning detections to tracked storm cells.
    """
    
    # Pre-filtering (Stage 1)
    prefilter_radius_km: float = 16.0  # 10 miles - spatial pre-filter radius
    
    # Gating parameters (Stage 2)
    gating_threshold: float = 6.0  # Chi-squared 95% confidence (2 DOF)
    min_gating_radius_km: float = 2.0  # Floor for collapsed covariance
    
    # Cost function weights
    weight_position: float = 1.0  # Mahalanobis distance weight
    weight_velocity: float = 2.0  # Velocity direction consistency weight
    weight_shape: float = 0.5  # Size/reflectivity similarity weight
    
    # Algorithm selection (for A/B testing and fallback)
    method: str = "hybrid"  # 'hybrid', 'hungarian', or 'greedy'
    
    # Numerical stability
    covariance_regularization: float = 1e-6
    
    @classmethod
    def from_yaml(cls, path: Optional[Path] = None) -> "AssignmentConfig":
        """
        Load configuration from YAML file.
        
        Args:
            path: Path to YAML config file. If None, uses default path.
        
        Returns:
            AssignmentConfig instance
        """
        if path is None:
            # Default config path
            path = Path(__file__).parent.parent.parent.parent.parent.parent / "config" / "kalman.yaml"
        
        if not path.exists():
            return cls()
        
        with open(path, 'r') as f:
            data = yaml.safe_load(f)
        
        if data is None or 'assignment' not in data:
            return cls()
        
        assignment_data = data['assignment']
        weights = assignment_data.get('weights', {})
        
        return cls(
            prefilter_radius_km=assignment_data.get('prefilter_radius_km', 16.0),
            gating_threshold=assignment_data.get('gating_threshold', 6.0),
            min_gating_radius_km=assignment_data.get('min_gating_radius_km', 2.0),
            weight_position=weights.get('position', 1.0),
            weight_velocity=weights.get('velocity_direction', 2.0),
            weight_shape=weights.get('size_similarity', 0.5),
            method=assignment_data.get('method', 'hybrid'),
            covariance_regularization=assignment_data.get('covariance_regularization', 1e-6),
        )


# Default assignment configuration
DEFAULT_ASSIGNMENT_CONFIG = AssignmentConfig()
