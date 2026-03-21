"""
Optimized grid indexing for meteorological datasets.

Provides O(1) lookups for regular lat/lon grids and O(log N) lookups
for irregular grids using k-d trees.
"""
import numpy as np
from typing import Tuple, Optional


class GridIndex:
    """
    Factory class that creates appropriate grid index based on grid type.
    
    Automatically detects whether a grid is regular (uniform lat/lon spacing)
    or irregular and returns the optimal indexer.
    """
    
    TOLERANCE = 1e-6  # Tolerance for detecting grid regularity
    
    @classmethod
    def create(cls, lat_vals: np.ndarray, lon_vals: np.ndarray) -> 'BaseGridIndexer':
        """
        Create appropriate indexer based on grid type.
        
        Args:
            lat_vals: 2D array of latitudes
            lon_vals: 2D array of longitudes
            
        Returns:
            RegularGridIndexer for regular grids, KDTreeGridIndexer for irregular
        """
        if RegularGridIndexer.is_regular(lat_vals, lon_vals, cls.TOLERANCE):
            return RegularGridIndexer(lat_vals, lon_vals)
        return KDTreeGridIndexer(lat_vals, lon_vals)


class BaseGridIndexer:
    """Base class for grid indexers."""
    
    def query(self, lat: float, lon: float) -> Optional[Tuple[int, int]]:
        """
        Get grid indices for a lat/lon coordinate.
        
        Args:
            lat: Latitude
            lon: Longitude
            
        Returns:
            Tuple of (lat_idx, lon_idx) or None if error
        """
        raise NotImplementedError


class RegularGridIndexer(BaseGridIndexer):
    """
    O(1) indexer for regular lat/lon grids.
    
    Regular grids have uniform spacing in latitude and longitude,
    allowing direct index calculation without distance searches.
    """
    
    @staticmethod
    def is_regular(lat_vals: np.ndarray, lon_vals: np.ndarray, tolerance: float = 1e-6) -> bool:
        """
        Check if grid is regular (uniform spacing in lat/lon).
        
        A regular 2D grid created with meshgrid(indexing='ij') has:
        - Latitude varies along axis 0 (rows), constant along axis 1 (cols)
        - Longitude varies along axis 1 (cols), constant along axis 0 (rows)
        
        Args:
            lat_vals: 2D array of latitudes
            lon_vals: 2D array of longitudes
            tolerance: Tolerance for variance checks
            
        Returns:
            True if grid is regular, False otherwise
        """
        if lat_vals.ndim != 2 or lon_vals.ndim != 2:
            return False
        
        # For a regular grid with indexing='ij':
        # - lat_grid[i, :] = constant (same lat across all columns in a row)
        # - lon_grid[:, j] = constant (same lon across all rows in a column)
        
        # Check: latitude should be constant across axis 1 (columns)
        # std along axis=1 should be ~0 (each row has constant lat)
        lat_std_across_cols = np.std(lat_vals, axis=1)
        lat_constant_across_cols = np.all(lat_std_across_cols < tolerance)
        
        # Check: longitude should be constant across axis 0 (rows)
        # std along axis=0 should be ~0 (each column has constant lon)
        lon_std_across_rows = np.std(lon_vals, axis=0)
        lon_constant_across_rows = np.all(lon_std_across_rows < tolerance)
        
        # Additionally verify they do vary along their primary axes
        lat_varies_along_rows = np.any(np.std(lat_vals, axis=0) > tolerance)
        lon_varies_along_cols = np.any(np.std(lon_vals, axis=1) > tolerance)
        
        return (lat_constant_across_cols and lon_constant_across_rows and
                lat_varies_along_rows and lon_varies_along_cols)
    
    def __init__(self, lat_vals: np.ndarray, lon_vals: np.ndarray):
        """
        Initialize regular grid indexer.
        
        Args:
            lat_vals: 2D array of latitudes
            lon_vals: 2D array of longitudes
        """
        if lat_vals.ndim != 2 or lon_vals.ndim != 2:
            raise ValueError("RegularGridIndexer requires 2D lat/lon arrays")
        
        self.shape = lat_vals.shape
        
        if lat_vals.shape[0] < 2 or lat_vals.shape[1] < 2:
            raise ValueError("Grid must have at least 2 points in each dimension")
        
        # Extract 1D coordinate arrays
        # For regular grid with indexing='ij':
        # - lat varies along axis 0, constant along axis 1 -> take first column
        # - lon varies along axis 1, constant along axis 0 -> take first row
        self.lat_coords = lat_vals[:, 0]
        self.lon_coords = lon_vals[0, :]
        
        # Validate we got reasonable arrays
        if len(self.lat_coords) < 2 or len(self.lon_coords) < 2:
            raise ValueError("Grid must have at least 2 points in each dimension")
        
        # Calculate step sizes (assumes regular spacing)
        self.lat_step = self.lat_coords[1] - self.lat_coords[0]
        self.lon_step = self.lon_coords[1] - self.lon_coords[0]
        
        # Handle negative step (descending coordinates)
        self.lat_step_abs = abs(self.lat_step)
        self.lon_step_abs = abs(self.lon_step)
        
        # Bounds
        self.lat_min = float(min(self.lat_coords[0], self.lat_coords[-1]))
        self.lon_min = float(min(self.lon_coords[0], self.lon_coords[-1]))
        self.lat_max = float(max(self.lat_coords[0], self.lat_coords[-1]))
        self.lon_max = float(max(self.lon_coords[0], self.lon_coords[-1]))
        
        # Direction flags (True if coordinates increase with index)
        self.lat_ascending = self.lat_step > 0
        self.lon_ascending = self.lon_step > 0
    
    def query(self, lat: float, lon: float) -> Tuple[int, int]:
        """
        Convert lat/lon to grid indices using direct calculation.
        
        Args:
            lat: Latitude coordinate
            lon: Longitude coordinate
            
        Returns:
            Tuple of (lat_idx, lon_idx) clamped to valid grid bounds
        """
        # Normalize longitude to -180 to 180 range
        if lon > 180:
            lon -= 360
        
        # Clamp lat/lon to grid bounds first
        lat = max(self.lat_min, min(lat, self.lat_max))
        lon = max(self.lon_min, min(lon, self.lon_max))
        
        # Calculate indices based on grid direction
        # For ascending: index = (value - min) / step
        # For descending: index = (max - value) / step
        if self.lat_ascending:
            lat_idx = int(round((lat - self.lat_coords[0]) / self.lat_step_abs))
        else:
            lat_idx = int(round((self.lat_coords[0] - lat) / self.lat_step_abs))
        
        if self.lon_ascending:
            lon_idx = int(round((lon - self.lon_coords[0]) / self.lon_step_abs))
        else:
            lon_idx = int(round((self.lon_coords[0] - lon) / self.lon_step_abs))
        
        # Clamp to valid range
        lat_idx = max(0, min(lat_idx, self.shape[0] - 1))
        lon_idx = max(0, min(lon_idx, self.shape[1] - 1))
        
        return (lat_idx, lon_idx)


class KDTreeGridIndexer(BaseGridIndexer):
    """
    O(log N) indexer using k-d tree for irregular/curvilinear grids.
    
    Uses scipy.spatial.cKDTree for efficient nearest-neighbor lookups.
    Suitable for grids where lat/lon don't vary uniformly along axes.
    """
    
    def __init__(self, lat_vals: np.ndarray, lon_vals: np.ndarray):
        """
        Initialize k-d tree indexer.
        
        Args:
            lat_vals: 2D array of latitudes
            lon_vals: 2D array of longitudes
        """
        from scipy.spatial import cKDTree
        
        self.shape = lat_vals.shape
        
        # Flatten 2D coordinates to Nx2 array of points
        points = np.column_stack([lat_vals.ravel(), lon_vals.ravel()])
        self.tree = cKDTree(points)
    
    def query(self, lat: float, lon: float) -> Tuple[int, int]:
        """
        Find nearest grid point using k-d tree.
        
        Args:
            lat: Latitude coordinate
            lon: Longitude coordinate
            
        Returns:
            Tuple of (lat_idx, lon_idx) for nearest grid point
        """
        # Normalize longitude
        if lon > 180:
            lon -= 360
        
        # Query k-d tree
        _, idx = self.tree.query([lat, lon])
        
        # Convert flat index back to 2D indices
        return np.unravel_index(idx, self.shape)
