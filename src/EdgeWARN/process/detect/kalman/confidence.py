"""
Confidence Calculator for Kalman Filter Tracking

Calculates and manages confidence scores for storm cells in prediction mode.
"""

from dataclasses import dataclass, field
from typing import List, Tuple, Optional
import numpy as np

from .config import TrackingConfig, default_tracking_config


@dataclass
class ConfidenceCalculator:
    """
    Calculates confidence scores for Kalman-predicted storm cells.
    
    Confidence decays over time while in prediction mode, and is affected
    by motion consistency and position uncertainty.
    """
    
    config: TrackingConfig = field(default_factory=default_tracking_config)
    
    # Base confidence when entering prediction mode
    base_confidence: float = 1.0
    
    def calculate(self, 
                  scans_predicted: int,
                  time_predicted_seconds: float,
                  velocity_variance: Optional[Tuple[float, float]] = None,
                  position_uncertainty_km: Optional[Tuple[float, float]] = None) -> float:
        """
        Calculate confidence score for a predicted storm cell.
        
        Args:
            scans_predicted: Number of consecutive scans in prediction mode
            time_predicted_seconds: Total time in prediction mode
            velocity_variance: Optional (var_u, var_v) from Kalman covariance
            position_uncertainty_km: Optional (std_lat_km, std_lon_km)
        
        Returns:
            Confidence score between 0.0 and 1.0
        """
        shape = self.config.confidence

        # Base decay from number of scans
        scan_confidence = self.base_confidence * (self.config.confidence_decay_factor ** scans_predicted)

        # Time-based decay (additional penalty for long predictions)
        max_time_seconds = self.config.max_prediction_time_minutes * 60
        time_factor = max(
            0.0,
            1.0 - (time_predicted_seconds / max_time_seconds) * shape.time_penalty_weight,
        )

        # Motion consistency factor (lower variance = higher confidence).
        # Keep this penalty gentle so a newly predicted track with a valid
        # Kalman state is not dropped immediately on the first missed scan.
        motion_factor = 1.0
        if velocity_variance is not None:
            var_u, var_v = velocity_variance
            # High velocity variance indicates uncertain motion.
            total_var = var_u + var_v
            if total_var > 0:
                motion_factor = 1.0 / (1.0 + total_var / shape.motion_factor_variance_denominator)
                motion_factor = max(shape.factor_floor, motion_factor)

        # Position uncertainty factor. Use a soft decay to preserve continuity
        # across temporary detection dropouts while still reducing confidence
        # as uncertainty grows.
        position_factor = 1.0
        if position_uncertainty_km is not None:
            std_lat, std_lon = position_uncertainty_km
            avg_std = (std_lat + std_lon) / 2
            if avg_std > shape.position_decay_onset_std:  # km
                position_factor = 1.0 / (
                    1.0 + (avg_std - shape.position_decay_onset_std) / shape.position_decay_scale
                )
                position_factor = max(shape.factor_floor, position_factor)

        # Combine factors
        confidence = scan_confidence * time_factor * motion_factor * position_factor
        
        # Clamp to [0, 1]
        return max(0.0, min(1.0, confidence))
    
    def should_terminate(self, 
                         confidence: float,
                         time_predicted_seconds: float,
                         scans_predicted: int) -> Tuple[bool, str]:
        """
        Determine if a predicted storm should be terminated.
        
        Args:
            confidence: Current confidence score
            time_predicted_seconds: Total time in prediction mode
            scans_predicted: Number of scans in prediction mode
        
        Returns:
            (should_terminate, reason) tuple
        """
        # Check confidence threshold
        if confidence < self.config.confidence_threshold:
            return True, f"Confidence {confidence:.2f} below threshold {self.config.confidence_threshold}"
        
        # Check time limit
        max_time_seconds = self.config.max_prediction_time_minutes * 60
        if time_predicted_seconds >= max_time_seconds:
            return True, f"Prediction time {time_predicted_seconds/60:.1f} min exceeds limit {self.config.max_prediction_time_minutes} min"
        
        return False, ""
    
@dataclass
class PredictionState:
    """
    Tracks the state of a storm cell in prediction mode.
    """
    
    # Number of consecutive scans in prediction mode
    scan_count: int = 0
    
    # Total time in prediction mode (seconds)
    total_time_seconds: float = 0.0
    
    # Current confidence score
    confidence: float = 1.0
    
    # Timestamp when prediction mode started
    start_timestamp: Optional[str] = None
    
    # Last update timestamp
    last_update_timestamp: Optional[str] = None
    
    # History of predicted positions
    predicted_positions: List[Tuple[float, float]] = None
    
    def __post_init__(self):
        if self.predicted_positions is None:
            self.predicted_positions = []
    
    def increment(self, dt_seconds: float, new_confidence: float,
                  predicted_position: Optional[Tuple[float, float]] = None) -> None:
        """
        Increment prediction state for a new scan.
        
        Args:
            dt_seconds: Time since last scan
            new_confidence: Updated confidence score
            predicted_position: Optional predicted (lat, lon) position
        """
        self.scan_count += 1
        self.total_time_seconds += dt_seconds
        self.confidence = new_confidence
        
        if predicted_position is not None:
            self.predicted_positions.append(predicted_position)
    
    def to_dict(self) -> dict:
        """Convert to dictionary for serialization."""
        return {
            'scan_count': self.scan_count,
            'total_time_seconds': self.total_time_seconds,
            'confidence': self.confidence,
            'start_timestamp': self.start_timestamp,
            'last_update_timestamp': self.last_update_timestamp,
            'predicted_positions': self.predicted_positions
        }
    
    @classmethod
    def from_dict(cls, data: dict) -> "PredictionState":
        """Create from dictionary."""
        return cls(
            scan_count=data.get('scan_count', 0),
            total_time_seconds=data.get('total_time_seconds', 0.0),
            confidence=data.get('confidence', 1.0),
            start_timestamp=data.get('start_timestamp'),
            last_update_timestamp=data.get('last_update_timestamp'),
            predicted_positions=data.get('predicted_positions', [])
        )
    
    def reset(self) -> None:
        """Reset prediction state when returning to active mode."""
        self.scan_count = 0
        self.total_time_seconds = 0.0
        self.confidence = 1.0
        self.start_timestamp = None
        self.last_update_timestamp = None
        self.predicted_positions = []
