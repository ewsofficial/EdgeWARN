"""
StormCast Core Data Types

Data structures for storm state, environment profiles, and forecasts.
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, Tuple, Optional, List


@dataclass
class StormState:
    """
    Complete storm state representation.
    
    Attributes:
        x: Zonal position in meters (positive = east)
        y: Meridional position in meters (positive = north)
        u: Zonal velocity in m/s (positive = eastward)
        v: Meridional velocity in m/s (positive = northward)
        h_core: Storm core height in km AGL, computed as (EchoTop30 + EchoTop50) / 2
        track_history: Number of valid observation samples
        timestamp: Time of state estimate
    """
    x: float
    y: float
    u: float
    v: float
    h_core: float
    track_history: int = 1
    motion_jitter: float = 0.0
    timestamp: Optional[datetime] = None
    
    @property
    def speed(self) -> float:
        """Storm speed in m/s."""
        return (self.u**2 + self.v**2) ** 0.5
    
    @property
    def direction(self) -> float:
        """Storm motion direction in degrees from north (meteorological convention)."""
        import math
        return (270 - math.degrees(math.atan2(self.v, self.u))) % 360


@dataclass
class EnvironmentProfile:
    """
    Environmental wind profile from RAP analysis.
    
    Attributes:
        winds: Dictionary mapping pressure level (mb) to (u, v) wind components in m/s
        timestamp: Time of RAP analysis
    """
    winds: Dict[int, Tuple[float, float]]
    timestamp: Optional[datetime] = None
    
    def get_wind(self, level: int) -> Tuple[float, float]:
        """Get wind at specified pressure level."""
        if level not in self.winds:
            raise KeyError(f"Wind data not available for {level} mb")
        return self.winds[level]
    
    @property
    def levels(self) -> List[int]:
        """Available pressure levels."""
        return sorted(self.winds.keys(), reverse=True)


@dataclass
class ForecastPoint:
    """
    Single forecast position with uncertainty.
    
    Attributes:
        x: Forecast zonal position (m)
        y: Forecast meridional position (m)
        lead_time: Seconds from initial time
        sigma_x: Position uncertainty in x (m)
        sigma_y: Position uncertainty in y (m)
    """
    x: float
    y: float
    lead_time: float
    sigma_x: float = 0.0
    sigma_y: float = 0.0
    
    @property
    def position(self) -> Tuple[float, float]:
        """Position as (x, y) tuple."""
        return (self.x, self.y)
    
    @property
    def uncertainty(self) -> Tuple[float, float]:
        """Uncertainty as (sigma_x, sigma_y) tuple."""
        return (self.sigma_x, self.sigma_y)


@dataclass
class MotionVector:
    """
    Motion vector with optional metadata.
    
    Attributes:
        u: Zonal component (m/s)
        v: Meridional component (m/s)
        source: Description of vector source (e.g., 'observed', 'bunkers', 'blended')
    """
    u: float
    v: float
    source: str = ""
    
    @property
    def magnitude(self) -> float:
        """Vector magnitude in m/s."""
        return (self.u**2 + self.v**2) ** 0.5
    
    def __add__(self, other: "MotionVector") -> "MotionVector":
        return MotionVector(self.u + other.u, self.v + other.v)
    
    def __sub__(self, other: "MotionVector") -> "MotionVector":
        return MotionVector(self.u - other.u, self.v - other.v)
    
    def __mul__(self, scalar: float) -> "MotionVector":
        return MotionVector(self.u * scalar, self.v * scalar)
    
    def __rmul__(self, scalar: float) -> "MotionVector":
        return self.__mul__(scalar)


@dataclass
class KalmanState:
    """
    Full Kalman filter state including covariance.
    
    Attributes:
        state: State vector [x, y, u, v]
        covariance: 4x4 state covariance matrix
        timestamp: Time of state estimate
    """
    state: List[float] = field(default_factory=lambda: [0.0, 0.0, 0.0, 0.0])
    covariance: List[List[float]] = field(default_factory=lambda: [
        [10000.0, 0.0, 0.0, 0.0],
        [0.0, 10000.0, 0.0, 0.0],
        [0.0, 0.0, 25.0, 0.0],
        [0.0, 0.0, 0.0, 25.0],
    ])
    timestamp: Optional[datetime] = None
    
    @property
    def x(self) -> float:
        return self.state[0]
    
    @property
    def y(self) -> float:
        return self.state[1]
    
    @property
    def u(self) -> float:
        return self.state[2]
    
    @property
    def v(self) -> float:
        return self.state[3]
