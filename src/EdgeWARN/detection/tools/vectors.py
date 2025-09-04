import json
import math
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Tuple, Optional


class StormVectorCalculator:
    """
    A class to calculate and manage storm vectors for detected storm cells.
    """
    
    def __init__(self, min_magnitude_m: float = 9000.0):
        """
        Initialize the StormVectorCalculator.
        
        Args:
            min_magnitude_m (float): Minimum magnitude threshold for filtering vectors
        """
        self.min_magnitude_m = min_magnitude_m
    
    def calculate_storm_vectors(self, cells: List[Dict]) -> List[Dict]:
        """
        Calculate vector components for storm cells based on their movement history.
        
        Args:
            cells (list): List of storm cell dictionaries
            
        Returns:
            list: Vector information for each cell with valid history
        """
        vectors = []
        for cell in cells:
            vector_data = self._calculate_cell_vector(cell)
            if vector_data:
                vectors.append(vector_data)
        return vectors
    
    def _calculate_cell_vector(self, cell: Dict) -> Optional[Dict]:
        """
        Calculate vector components for a single storm cell.
        
        Args:
            cell (dict): Storm cell dictionary
            
        Returns:
            dict: Vector information or None if insufficient history
        """
        history = cell.get('storm_history', [])
        if len(history) < 2:
            return None
        
        # Sort history by timestamp (oldest to newest)
        history_sorted = sorted(history, key=lambda x: x['timestamp'])
        h0, h1 = history_sorted[-2], history_sorted[-1]
        
        # Extract centroids and timestamps
        c0 = h0['centroid']
        c1 = h1['centroid']
        t0 = h0['timestamp']
        t1 = h1['timestamp']
        
        # Parse timestamps
        dt0, dt1 = self._parse_timestamps(t0, t1)
        dt_seconds = (dt1 - dt0).total_seconds()
        
        # Calculate movement in meters
        dx, dy = self._calculate_movement(c0, c1, dt_seconds)
        
        # Add dx, dy, dt to the latest (h1) history entry
        h1['dx'] = dx
        h1['dy'] = dy
        h1['dt'] = dt_seconds
        
        return {
            'id': cell['id'],
            'dx': dx,
            'dy': dy,
            'dt': dt_seconds,
            't0': t0,
            't1': t1,
            'c0': c0,
            'c1': c1
        }
    
    def _parse_timestamps(self, t0: str, t1: str) -> Tuple[datetime, datetime]:
        """
        Parse timestamp strings into datetime objects.
        
        Args:
            t0 (str): First timestamp string
            t1 (str): Second timestamp string
            
        Returns:
            tuple: (datetime, datetime) objects for the two timestamps
        """
        try:
            dt0 = datetime.fromisoformat(t0)
            dt1 = datetime.fromisoformat(t1)
        except Exception:
            # Fallback to filename timestamp extraction if needed
            from . import timestamp  # Assuming this module exists
            dt0 = datetime.fromisoformat(timestamp.extract_timestamp_from_filename(t0))
            dt1 = datetime.fromisoformat(timestamp.extract_timestamp_from_filename(t1))
        
        return dt0, dt1
    
    def _calculate_movement(self, c0: List[float], c1: List[float], dt_seconds: float) -> Tuple[float, float]:
        """
        Calculate movement in meters between two points.
        
        Args:
            c0 (list): First centroid [lat, lon]
            c1 (list): Second centroid [lat, lon]
            dt_seconds (float): Time difference in seconds
            
        Returns:
            tuple: (dx, dy) movement in meters
        """
        avg_lat = (c0[0] + c1[0]) / 2
        deg_to_m_lat = 111320.0
        deg_to_m_lon = 111320.0 * math.cos(math.radians(avg_lat))
        
        dx = (c1[1] - c0[1]) * deg_to_m_lon
        dy = (c1[0] - c0[0]) * deg_to_m_lat
        
        return dx, dy
    
    def clean_vectors(self, cells: List[Dict]) -> List[Dict]:
        """
        Remove cells whose latest vector magnitude exceeds the threshold.
        
        Args:
            cells (list): List of cell dicts (modified in-place)
            
        Returns:
            list: Information about removed cells
        """
        removed = []
        kept = []
        
        for cell in cells:
            should_keep, removal_info = self._evaluate_cell(cell)
            if should_keep:
                kept.append(cell)
            elif removal_info:
                removed.append(removal_info)
        
        # Update the original list
        cells.clear()
        cells.extend(kept)
        
        return removed
    
    def _evaluate_cell(self, cell: Dict) -> Tuple[bool, Optional[Dict]]:
        """
        Evaluate whether a cell should be kept based on its vector magnitude.
        
        Args:
            cell (dict): Storm cell dictionary
            
        Returns:
            tuple: (should_keep, removal_info) where removal_info is None if keeping
        """
        hist = cell.get('storm_history', [])
        if not hist:
            return True, None
        
        # Assume history is sorted oldest->newest; take last
        latest = hist[-1]
        dx = latest.get('dx')
        dy = latest.get('dy')
        
        if dx is None or dy is None:
            # No vector recorded, keep the cell
            return True, None
        
        try:
            mag = math.hypot(float(dx), float(dy))
        except Exception:
            return True, None
        
        if mag > self.min_magnitude_m:
            return False, {'id': cell.get('id'), 'magnitude_m': mag}
        else:
            return True, None


def write_vectors():
    """
    Command-line interface function to calculate and write storm vectors.
    """
    import sys
    
    # Default path or from command line
    json_path = sys.argv[1] if len(sys.argv) > 1 else "stormcell_test.json"
    
    # Initialize the vector calculator
    calculator = StormVectorCalculator(min_magnitude_m=9000.0)
    
    # Load cells from JSON file
    with open(json_path, 'r') as f:
        cells = json.load(f)
    
    # Calculate vectors
    vectors = calculator.calculate_storm_vectors(cells)
    
    # Clean vectors with default threshold
    removed_cells = calculator.clean_vectors(cells)
    
    # Write updated cells back to file
    with open(json_path, 'w') as f:
        json.dump(cells, f, indent=4)
    
    # Print vectors
    for v in vectors:
        print(f"id: {v['id']}, dx: {v['dx']:.2f} m, dy: {v['dy']:.2f} m, dt: {v['dt']} s")
    
    # Print removed cells
    if removed_cells:
        print("\nRemoved cells due to high vector magnitude:")
        for r in removed_cells:
            print(f"id: {r['id']}, magnitude: {r['magnitude_m']:.2f} m")


if __name__ == "__main__":
    write_vectors()