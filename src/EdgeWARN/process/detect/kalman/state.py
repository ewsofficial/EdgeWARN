"""
Kalman Filter State Vector and Covariance Matrix

Defines the state vector and covariance matrix classes for the
6-dimensional Kalman filter used in storm cell tracking.
"""

from dataclasses import dataclass, field
from typing import Tuple, Optional, List
import numpy as np
from math import radians, cos, sin, sqrt, atan2, pi


# Earth radius in km
EARTH_RADIUS_KM = 6371.0

# Meters per km
KM_TO_M = 1000.0


@dataclass
class StateVector:
    """
    6-dimensional state vector for Kalman filter.
    
    State components:
        lat: Latitude position (degrees)
        lon: Longitude position (degrees, 0-360 format)
        u: Eastward velocity (m/s)
        v: Northward velocity (m/s)
        a_lat: Latitude acceleration (m/s²)
        a_lon: Longitude acceleration (m/s²)
    """
    
    lat: float = 0.0
    lon: float = 0.0
    u: float = 0.0
    v: float = 0.0
    a_lat: float = 0.0
    a_lon: float = 0.0
    
    def to_array(self) -> np.ndarray:
        """Convert state vector to numpy array."""
        # Note: We store lat/lon in degrees but convert to meters for Kalman operations
        return np.array([
            self.lat,
            self.lon,
            self.u,
            self.v,
            self.a_lat,
            self.a_lon
        ], dtype=np.float64)
    
    @classmethod
    def from_array(cls, arr: np.ndarray) -> "StateVector":
        """Create state vector from numpy array."""
        return cls(
            lat=float(arr[0]),
            lon=float(arr[1]),
            u=float(arr[2]),
            v=float(arr[3]),
            a_lat=float(arr[4]),
            a_lon=float(arr[5])
        )
    
    def get_position(self) -> Tuple[float, float]:
        """Get position as (lat, lon) tuple."""
        return (self.lat, self.lon)
    
    def get_velocity(self) -> Tuple[float, float]:
        """Get velocity as (u, v) tuple in m/s."""
        return (self.u, self.v)
    
    def get_speed(self) -> float:
        """Get speed magnitude in m/s."""
        return sqrt(self.u**2 + self.v**2)
    
    def get_bearing(self) -> float:
        """Get bearing in degrees (0=North, 90=East)."""
        bearing = atan2(self.u, self.v) * 180 / pi
        return bearing if bearing >= 0 else bearing + 360


@dataclass
class CovarianceMatrix:
    """
    6x6 covariance matrix for Kalman filter state uncertainty.
    
    The covariance matrix represents the uncertainty in the state estimate.
    Diagonal elements are variances of each state component.
    Off-diagonal elements are covariances between state components.
    """
    
    # Store as 6x6 numpy array
    _matrix: np.ndarray = field(default_factory=lambda: np.eye(6, dtype=np.float64))
    
    def __post_init__(self):
        """Ensure matrix is properly initialized."""
        if not isinstance(self._matrix, np.ndarray):
            self._matrix = np.eye(6, dtype=np.float64)
        elif self._matrix.shape != (6, 6):
            self._matrix = np.eye(6, dtype=np.float64)
    
    @classmethod
    def from_diagonal(cls, variances: List[float]) -> "CovarianceMatrix":
        """
        Create covariance matrix from diagonal variances.
        
        Args:
            variances: List of 6 variances [var_lat, var_lon, var_u, var_v, var_a_lat, var_a_lon]
        
        Returns:
            CovarianceMatrix with specified diagonal and zero off-diagonal elements
        """
        if len(variances) != 6:
            raise ValueError("Expected 6 variances for 6-dimensional state")
        
        return cls(_matrix=np.diag(variances).astype(np.float64))
    
    @classmethod
    def from_position_uncertainty(cls, position_std_km: float, ref_lat: float = 35.0) -> "CovarianceMatrix":
        """
        Create covariance matrix with position uncertainty only.
        
        Args:
            position_std_km: Standard deviation of position uncertainty in km
            ref_lat: Reference latitude in degrees for longitude scaling.
                     Callers should pass the actual cell latitude.
        
        Returns:
            CovarianceMatrix with position variance and default velocity/acceleration
        """
        # Convert km to degrees (approximate)
        lat_std = position_std_km / 111.0  # 1 degree lat ~ 111 km
        lon_std = position_std_km / (111.0 * cos(radians(ref_lat)))  # Adjust for latitude
        
        variances = [
            lat_std**2,      # var_lat
            lon_std**2,      # var_lon
            100.0,           # var_u (m/s)² - high initial velocity uncertainty
            100.0,           # var_v (m/s)²
            1.0,             # var_a_lat (m/s²)²
            1.0              # var_a_lon (m/s²)²
        ]
        return cls.from_diagonal(variances)
    
    def to_array(self, copy: bool = True) -> np.ndarray:
        """Get the covariance matrix as numpy array.

        copy defaults to True for external callers. Internal read-only
        callers may pass copy=False to avoid the per-call allocation; the
        returned array must not be mutated in that case.
        """
        return self._matrix.copy() if copy else self._matrix
    
    def get_position_variance(self) -> Tuple[float, float]:
        """Get position variances as (var_lat, var_lon) in degrees²."""
        return (self._matrix[0, 0], self._matrix[1, 1])
    
    def get_velocity_variance(self) -> Tuple[float, float]:
        """Get velocity variances as (var_u, var_v) in (m/s)²."""
        return (self._matrix[2, 2], self._matrix[3, 3])
    
    def get_position_std_km(self, ref_lat: float = 35.0) -> Tuple[float, float]:
        """
        Get position standard deviations in km.
        
        Args:
            ref_lat: Reference latitude for longitude conversion.
                     Callers should pass the actual cell latitude for accuracy.
        
        Returns:
            (std_lat_km, std_lon_km)
        """
        var_lat, var_lon = self.get_position_variance()
        std_lat_km = sqrt(var_lat) * 111.0
        std_lon_km = sqrt(var_lon) * 111.0 * cos(radians(ref_lat))
        return (std_lat_km, std_lon_km)


def latlon_to_meters(lat: float, lon: float, ref_lat: float, ref_lon: float) -> Tuple[float, float]:
    """
    Convert lat/lon to meters relative to a reference point.
    
    Uses local tangent plane approximation.
    
    Args:
        lat: Latitude in degrees
        lon: Longitude in degrees (0-360 or -180-180)
        ref_lat: Reference latitude in degrees
        ref_lon: Reference longitude in degrees
    
    Returns:
        (x, y) in meters where x is eastward and y is northward
    """
    # Convert longitude to -180 to 180 range if needed
    if lon > 180:
        lon = lon - 360
    if ref_lon > 180:
        ref_lon = ref_lon - 360
    
    # Approximate meters per degree
    meters_per_deg_lat = 111320.0  # meters per degree latitude
    meters_per_deg_lon = 111320.0 * cos(radians(ref_lat))  # meters per degree longitude
    
    x = (lon - ref_lon) * meters_per_deg_lon
    y = (lat - ref_lat) * meters_per_deg_lat
    
    return (x, y)


def meters_to_latlon(x: float, y: float, ref_lat: float, ref_lon: float) -> Tuple[float, float]:
    """
    Convert meters to lat/lon relative to a reference point.
    
    Args:
        x: Eastward distance in meters
        y: Northward distance in meters
        ref_lat: Reference latitude in degrees
        ref_lon: Reference longitude in degrees
    
    Returns:
        (lat, lon) in degrees (lon in same format as ref_lon)
    """
    # Approximate meters per degree
    meters_per_deg_lat = 111320.0
    meters_per_deg_lon = 111320.0 * cos(radians(ref_lat))
    
    lat = ref_lat + y / meters_per_deg_lat
    lon = ref_lon + x / meters_per_deg_lon
    
    # Convert to 0-360 format if reference is in that format
    if ref_lon > 180:
        lon = lon % 360
        if lon < 0:
            lon += 360
    
    return (lat, lon)


def haversine_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """
    Calculate the great circle distance between two points in km.
    
    Args:
        lat1, lon1: First point coordinates in degrees
        lat2, lon2: Second point coordinates in degrees
    
    Returns:
        Distance in km
    """
    # Convert to radians
    lat1_rad = radians(lat1)
    lat2_rad = radians(lat2)
    lon1_rad = radians(lon1 if lon1 <= 180 else lon1 - 360)
    lon2_rad = radians(lon2 if lon2 <= 180 else lon2 - 360)
    
    dlat = lat2_rad - lat1_rad
    dlon = lon2_rad - lon1_rad
    
    a = sin(dlat/2)**2 + cos(lat1_rad) * cos(lat2_rad) * sin(dlon/2)**2
    c = 2 * atan2(sqrt(a), sqrt(1-a))
    
    return EARTH_RADIUS_KM * c


def vectorized_haversine_distance(lat1: float, lon1: float, lats2: np.ndarray, lons2: np.ndarray) -> np.ndarray:
    """
    Calculate the great circle distance between one point and an array of points in km.
    
    Args:
        lat1, lon1: Origin point coordinates in degrees
        lats2, lons2: Arrays of target point coordinates in degrees
        
    Returns:
        Array of distances in km
    """
    # Convert to radians
    lat1_rad = np.radians(lat1)
    lats2_rad = np.radians(lats2)
    
    # Handle longitude mapping to -180 to 180
    lon1_mapped = lon1 if lon1 <= 180 else lon1 - 360
    lons2_mapped = np.where(lons2 <= 180, lons2, lons2 - 360)
    
    lon1_rad = np.radians(lon1_mapped)
    lons2_rad = np.radians(lons2_mapped)
    
    dlat = lats2_rad - lat1_rad
    dlon = lons2_rad - lon1_rad
    
    a = np.sin(dlat/2)**2 + np.cos(lat1_rad) * np.cos(lats2_rad) * np.sin(dlon/2)**2
    c = 2 * np.arctan2(np.sqrt(a), np.sqrt(1-a))
    
    return EARTH_RADIUS_KM * c