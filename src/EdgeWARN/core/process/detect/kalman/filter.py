"""
Kalman Filter Implementation

Core Kalman filter for storm cell tracking with 6-dimensional state.
"""

from dataclasses import dataclass, field
from typing import Tuple, Optional, Dict, Any
import numpy as np
from datetime import datetime

from .state import StateVector, CovarianceMatrix, latlon_to_meters, meters_to_latlon
from .config import KalmanConfig, DEFAULT_KALMAN_CONFIG


@dataclass
class KalmanObservation:
    """Observation data for Kalman filter update."""
    
    lat: float
    lon: float
    timestamp: Optional[datetime] = None
    
    # Optional velocity observation (from StormCast or historical motion)
    u: Optional[float] = None
    v: Optional[float] = None


@dataclass
class KalmanFilter:
    """
    6-dimensional Kalman Filter for storm cell tracking.
    
    State vector: [lat, lon, u, v, a_lat, a_lon]^T
    - lat, lon: Position in degrees
    - u, v: Velocity in m/s
    - a_lat, a_lon: Acceleration in m/s²
    
    The filter uses a constant acceleration model for state transition.
    """
    
    # Configuration
    config: KalmanConfig = field(default_factory=KalmanConfig)
    
    # State
    state: StateVector = field(default_factory=StateVector)
    covariance: CovarianceMatrix = field(default_factory=CovarianceMatrix)
    
    # Reference point for coordinate conversion
    ref_lat: float = 35.0
    ref_lon: float = -97.0
    
    # Internal state (in meters for numerical stability)
    _x: float = 0.0  # Eastward position in meters
    _y: float = 0.0  # Northward position in meters
    _initialized: bool = False
    _last_timestamp: Optional[datetime] = None
    
    # Process noise matrix (Q)
    _Q: Optional[np.ndarray] = None
    
    # Measurement matrix (H) - observes position only
    _H: np.ndarray = field(default_factory=lambda: np.array([
        [1, 0, 0, 0, 0, 0],
        [0, 1, 0, 0, 0, 0]
    ], dtype=np.float64))
    
    # Measurement noise matrix (R)
    _R: Optional[np.ndarray] = None
    
    def __post_init__(self):
        """Initialize noise matrices."""
        self._initialize_noise_matrices()
    
    def _initialize_noise_matrices(self):
        """Initialize process and measurement noise matrices."""
        cfg = self.config
        
        # Process noise matrix Q (6x6)
        # For constant acceleration model with dt=1 (normalized)
        # Q models the uncertainty in the state transition
        self._Q = np.diag([
            cfg.process_noise_position,
            cfg.process_noise_position,
            cfg.process_noise_velocity,
            cfg.process_noise_velocity,
            cfg.process_noise_acceleration,
            cfg.process_noise_acceleration
        ], dtype=np.float64)
        
        # Measurement noise matrix R (2x2 for position-only observations)
        # Convert km to degrees (approximate)
        pos_noise_deg = cfg.measurement_noise_position / 111.0
        self._R = np.diag([
            pos_noise_deg**2,
            pos_noise_deg**2
        ], dtype=np.float64)
    
    def initialize(self, lat: float, lon: float, 
                   u: float = 0.0, v: float = 0.0,
                   position_std_km: float = 1.0,
                   timestamp: Optional[datetime] = None) -> None:
        """
        Initialize the Kalman filter with an initial state.
        
        Args:
            lat: Initial latitude in degrees
            lon: Initial longitude in degrees
            u: Initial eastward velocity in m/s (default 0)
            v: Initial northward velocity in m/s (default 0)
            position_std_km: Initial position uncertainty in km
            timestamp: Initial timestamp
        """
        self.state = StateVector(lat=lat, lon=lon, u=u, v=v)
        self.covariance = CovarianceMatrix.from_position_uncertainty(position_std_km)
        
        # Set reference point for coordinate conversion
        self.ref_lat = lat
        self.ref_lon = lon if lon <= 180 else lon - 360
        
        # Initialize internal meter coordinates
        self._x = 0.0
        self._y = 0.0
        
        self._initialized = True
        self._last_timestamp = timestamp
    
    def initialize_from_cell(self, cell: Dict[str, Any], 
                             timestamp: Optional[datetime] = None) -> None:
        """
        Initialize the Kalman filter from a storm cell entry.
        
        Args:
            cell: Storm cell dictionary with 'centroid' and optionally 'dx', 'dy', 'dt'
            timestamp: Initial timestamp
        """
        centroid = cell.get('centroid', [0, 0])
        lat, lon = centroid[0], centroid[1]
        
        # Try to get velocity from historical motion
        u, v = 0.0, 0.0
        dx = cell.get('dx')
        dy = cell.get('dy')
        dt = cell.get('dt')
        
        if dx is not None and dy is not None and dt is not None and dt > 0:
            u = dx / dt  # m/s
            v = dy / dt  # m/s
        
        # Try to get velocity from StormCast module
        modules = cell.get('modules', {})
        stormcast = modules.get('StormCast', {})
        if stormcast.get('status') == 'success':
            sc_u = stormcast.get('u')
            sc_v = stormcast.get('v')
            if sc_u is not None and sc_v is not None:
                u, v = sc_u, sc_v
        
        self.initialize(lat, lon, u, v, timestamp=timestamp)
    
    def predict(self, dt: float, 
                control_u: Optional[float] = None,
                control_v: Optional[float] = None) -> StateVector:
        """
        Perform prediction step of Kalman filter.
        
        Uses constant acceleration model to predict state forward.
        
        Args:
            dt: Time step in seconds
            control_u: Optional control input for velocity (from StormCast)
            control_v: Optional control input for velocity (from StormCast)
        
        Returns:
            Predicted state vector
        """
        if not self._initialized:
            return self.state
        
        # Build state transition matrix F
        F = self._build_transition_matrix(dt)
        
        # Get current state as array
        x = self.state.to_array()
        P = self.covariance.to_array()
        
        # Apply control input if provided (StormCast velocity)
        if control_u is not None and control_v is not None:
            # Update velocity in state to match StormCast prediction
            x[2] = control_u
            x[3] = control_v
        
        # Predict state: x' = F * x
        x_pred = F @ x
        
        # Predict covariance: P' = F * P * F^T + Q
        # Scale Q by dt for time-dependent process noise
        Q_scaled = self._Q * dt
        P_pred = F @ P @ F.T + Q_scaled
        
        # Update state and covariance
        self.state = StateVector.from_array(x_pred)
        self.covariance = CovarianceMatrix(_matrix=P_pred)
        
        return self.state
    
    def update(self, observation: KalmanObservation) -> StateVector:
        """
        Perform update step of Kalman filter with observation.
        
        Args:
            observation: Observation with lat/lon position
        
        Returns:
            Updated state vector
        """
        if not self._initialized:
            # Initialize with observation if not yet initialized
            self.initialize(observation.lat, observation.lon)
            return self.state
        
        # Get current state and covariance
        x = self.state.to_array()
        P = self.covariance.to_array()
        
        # Observation vector z (position only)
        z = np.array([observation.lat, observation.lon], dtype=np.float64)
        
        # Innovation (measurement residual): y = z - H * x
        y = z - self._H @ x
        
        # Innovation covariance: S = H * P * H^T + R
        S = self._H @ P @ self._H.T + self._R
        
        # Kalman gain: K = P * H^T * S^-1
        K = P @ self._H.T @ np.linalg.inv(S)
        
        # Updated state: x' = x + K * y
        x_updated = x + K @ y
        
        # Updated covariance: P' = (I - K * H) * P
        I = np.eye(6, dtype=np.float64)
        P_updated = (I - K @ self._H) @ P
        
        # Update state and covariance
        self.state = StateVector.from_array(x_updated)
        self.covariance = CovarianceMatrix(_matrix=P_updated)
        
        # Update timestamp
        if observation.timestamp:
            self._last_timestamp = observation.timestamp
        
        return self.state
    
    def _build_transition_matrix(self, dt: float) -> np.ndarray:
        """
        Build state transition matrix for constant acceleration model.
        
        The state transition follows:
        - Position: p' = p + v*dt + 0.5*a*dt²
        - Velocity: v' = v + a*dt
        - Acceleration: a' = a (constant)
        
        Args:
            dt: Time step in seconds
        
        Returns:
            6x6 state transition matrix
        """
        # Convert dt to a normalized time step
        # For numerical stability, we work in degrees for position
        # and m/s for velocity
        
        # Approximate conversion: 1 degree ~ 111 km ~ 111000 m
        # dt_norm = dt (in seconds)
        
        F = np.array([
            [1, 0, dt/111000, 0, 0.5*dt**2/111000, 0],          # lat' = lat + v_lat*dt + 0.5*a_lat*dt²
            [0, 1, 0, dt/111000, 0, 0.5*dt**2/111000],          # lon' = lon + v_lon*dt + 0.5*a_lon*dt²
            [0, 0, 1, 0, dt, 0],                                 # u' = u + a_lat*dt
            [0, 0, 0, 1, 0, dt],                                 # v' = v + a_lon*dt
            [0, 0, 0, 0, 1, 0],                                  # a_lat' = a_lat
            [0, 0, 0, 0, 0, 1]                                   # a_lon' = a_lon
        ], dtype=np.float64)
        
        return F
    
    def get_predicted_position(self, dt: float) -> Tuple[float, float]:
        """
        Get predicted position after dt seconds without updating state.
        
        Args:
            dt: Time step in seconds
        
        Returns:
            (lat, lon) predicted position
        """
        F = self._build_transition_matrix(dt)
        x = self.state.to_array()
        x_pred = F @ x
        return (float(x_pred[0]), float(x_pred[1]))
    
    def get_position_uncertainty_km(self) -> Tuple[float, float]:
        """
        Get current position uncertainty in km.
        
        Returns:
            (std_lat_km, std_lon_km)
        """
        return self.covariance.get_position_std_km(self.ref_lat)
    
    def get_state_dict(self) -> Dict[str, Any]:
        """
        Get state as dictionary for serialization.
        
        Returns:
            Dictionary with state components
        """
        return {
            'lat': self.state.lat,
            'lon': self.state.lon,
            'u': self.state.u,
            'v': self.state.v,
            'a_lat': self.state.a_lat,
            'a_lon': self.state.a_lon,
            'P': self.covariance.to_array().tolist()
        }
    
    @classmethod
    def from_state_dict(cls, state_dict: Dict[str, Any], 
                        config: Optional[KalmanConfig] = None) -> "KalmanFilter":
        """
        Create Kalman filter from state dictionary.
        
        Args:
            state_dict: Dictionary with state components
            config: Optional configuration
        
        Returns:
            KalmanFilter instance
        """
        kf = cls(config=config or KalmanConfig())
        
        kf.state = StateVector(
            lat=state_dict.get('lat', 0.0),
            lon=state_dict.get('lon', 0.0),
            u=state_dict.get('u', 0.0),
            v=state_dict.get('v', 0.0),
            a_lat=state_dict.get('a_lat', 0.0),
            a_lon=state_dict.get('a_lon', 0.0)
        )
        
        P = state_dict.get('P')
        if P is not None:
            kf.covariance = CovarianceMatrix(_matrix=np.array(P, dtype=np.float64))
        
        kf._initialized = True
        return kf