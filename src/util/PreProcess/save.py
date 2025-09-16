from typing import List, Dict, Optional, Tuple
from datetime import datetime
import math
import json

class StormCellTracker:
    @staticmethod
    def update_storm_cell_history(existing_cell, new_data):
        """
        Update a storm cell's history, preventing duplicate timestamp entries
        
        Args:
            existing_cell (dict): The existing cell data with history
            new_data (dict): New data to potentially add to history
        
        Returns:
            bool: True if history was updated, False if duplicate was found
        """
        # Extract the timestamp from new data
        new_timestamp = new_data.get('timestamp')
        if not new_timestamp:
            return False  # No timestamp, can't add to history
        
        # Check if this timestamp already exists in history
        for history_entry in existing_cell.get('storm_history', []):
            if history_entry.get('timestamp') == new_timestamp:
                # Update the existing entry with new data instead of adding a duplicate
                history_entry.update(new_data)
                return True  # Entry was updated
        
        # If we get here, this is a new timestamp - add it to history
        if 'storm_history' not in existing_cell:
            existing_cell['storm_history'] = []
        
        existing_cell['storm_history'].append(new_data)
        return True  # New entry was added

    @staticmethod
    def process_matched_cell(existing_cell, new_cell_data, timestamp):
        """
        Process a matched cell: preserve the existing ID and history, 
        update all other properties with new data, and append new scan to history
        """
        # Store the existing ID and history before updating
        existing_id = existing_cell["id"]
        existing_history = existing_cell.get("storm_history", [])
        
        # Update ALL properties except ID and history with new data
        existing_cell.update({
            # DO NOT update the ID - keep the original tracking ID
            "num_gates": new_cell_data["num_gates"],
            "centroid": new_cell_data["centroid"],
            "bbox": new_cell_data.get("bbox", {}),
            "alpha_shape": new_cell_data.get("alpha_shape", []),
            "max_reflectivity_dbz": new_cell_data["max_reflectivity_dbz"],
            # Keep the existing storm_history intact
            "storm_history": existing_history
        })
        
        # Create the new snapshot for this scan
        new_snapshot = {
            "timestamp": timestamp,
            "max_reflectivity_dbz": new_cell_data["max_reflectivity_dbz"],
            "num_gates": new_cell_data["num_gates"],
            "centroid": new_cell_data["centroid"],
        }
        
        # Check if this timestamp already exists in history
        timestamp_exists = False
        for hist_entry in existing_history:
            if hist_entry.get("timestamp") == timestamp:
                # Update the existing entry with new data
                hist_entry.update(new_snapshot)
                timestamp_exists = True
                break
        
        # If timestamp doesn't exist, append new entry
        if not timestamp_exists:
            existing_history.append(new_snapshot)
        
        # Keep history sorted by timestamp
        existing_history.sort(key=lambda h: h["timestamp"])
        
        print(f"DEBUG: Updated cell ID {existing_id} with data from new cell ID {new_cell_data['id']}")
        return True
    
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
        Calculate vector components for storm cells and save them directly
        under the storm history entries as dx, dy, dt.
        """
        vectors = []
        for cell in cells:
            history = cell.get('storm_history', [])
            if len(history) < 2:
                continue  # Need at least 2 entries

            # Iterate through history pairs
            for i in range(1, len(history)):
                h0 = history[i-1]
                h1 = history[i]

                # Skip if this entry already has dx/dy/dt
                if all(k in h1 for k in ('dx', 'dy', 'dt')):
                    continue

                dt0, dt1 = self._parse_timestamps(h0['timestamp'], h1['timestamp'])
                dt_seconds = (dt1 - dt0).total_seconds()
                dx, dy = self._calculate_movement(h0['centroid'], h1['centroid'], dt_seconds)

                # Save directly under the history entry
                h1['dx'] = dx
                h1['dy'] = dy
                h1['dt'] = dt_seconds

                vectors.append({
                    'id': cell['id'],
                    'dx': dx,
                    'dy': dy,
                    'dt': dt_seconds,
                    't0': h0['timestamp'],
                    't1': h1['timestamp'],
                    'c0': h0['centroid'],
                    'c1': h1['centroid']
                })
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
        
        # Create a COPY of the latest history entry to add vector data
        # This preserves the original history data while adding vector info
        vector_info = h1.copy()
        vector_info.update({
            'dx': dx,
            'dy': dy,
            'dt': dt_seconds,
            'prev_centroid': c0,  # Store previous centroid for reference
            'prev_timestamp': t0  # Store previous timestamp for reference
        })
        
        # Return vector info without modifying the original cell history
        return {
            'id': cell['id'],
            'dx': dx,
            'dy': dy,
            'dt': dt_seconds,
            't0': t0,
            't1': t1,
            'c0': c0,
            'c1': c1,
            'vector_info': vector_info  # Include the enhanced history entry
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
            from ..PreProcess.data_utils import extract_timestamp_from_filename
            dt0 = datetime.fromisoformat(extract_timestamp_from_filename(t0))
            dt1 = datetime.fromisoformat(extract_timestamp_from_filename(t1))
        
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
        # Check if cell has vector data
        vectors = cell.get('vectors', [])
        if not vectors:
            return True, None  # No vector data, keep cell
        
        # Get the latest vector
        latest_vector = vectors[-1]
        dx = latest_vector.get('dx')
        dy = latest_vector.get('dy')
        
        if dx is None or dy is None:
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

def save_cells_to_json(cells, filepath, float_precision=4):
    """
    Save tracked storm cells to JSON (like detection.py).
    """
    json_data = []
    for cell in cells:
        cell_entry = {
            "id": int(cell["id"]),
            "num_gates": int(cell["num_gates"]),
            "centroid": [round(float(v), float_precision) for v in cell["centroid"]],
            "bbox": cell.get("bbox", {}),
            "alpha_shape": [
                [round(float(x), float_precision), round(float(y), float_precision)]
                for x, y in cell.get("alpha_shape", [])
            ],
            "storm_history": []
        }

        for hist_entry in cell.get("storm_history", []):
            cell_entry["storm_history"].append({
                "timestamp": hist_entry.get("timestamp", ""),
                "max_reflectivity_dbz": round(float(hist_entry.get("max_reflectivity_dbz", 0)), float_precision),
                "num_gates": int(hist_entry.get("num_gates", 0)),
                "centroid": [round(float(v), float_precision) for v in hist_entry.get("centroid", [0, 0])]
            })

        json_data.append(cell_entry)

    with open(filepath, 'w') as f:
        json.dump(json_data, f, indent=4)

    print(f"Saved {len(cells)} tracked cells to {filepath}")