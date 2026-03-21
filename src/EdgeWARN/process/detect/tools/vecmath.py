import json
from datetime import datetime
from math import radians, cos
import util.file as fs

class StormVectorCalculator:
    """
    Calculates storm motion vectors (dx, dy, dt) for storm cells
    by comparing current state with the last entry in persistent history.
    """

    @staticmethod
    def calculate_vectors(entries):
        """
        Updates each cell with dx, dy, dt by comparing against 
        the last recorded history entry in CELL_DIR.
        
        entries: list of current cell dictionaries
        """
        for cell in entries:
            # Skip if cell has no timestamp (unmatched/inactive)
            if "timestamp" not in cell:
                continue

            cell_id = cell.get('id')
            if not cell_id:
                continue

            # Try to load previous history
            history_path = fs.CELL_DIR / f"{cell_id}.json"
            if not history_path.exists():
                # New cell, no history yet
                continue

            try:
                with open(history_path, 'r') as f:
                    history = json.load(f)
            except Exception:
                continue

            if not history:
                continue

            # Get previous entry (T-1)
            prev_entry = history[-1]

            # Extract timestamps
            # Support both top-level (new) and properties (old)
            t1_str = prev_entry.get('timestamp') or prev_entry.get('properties', {}).get('timestamp')
            t2_str = cell.get('timestamp')

            if not t1_str or not t2_str:
                continue
            
            # If timestamps are identical, we haven't moved in time (re-processing?) -> skip
            if t1_str == t2_str:
                continue

            try:
                t1 = datetime.fromisoformat(t1_str)
                t2 = datetime.fromisoformat(t2_str)
                dt = (t2 - t1).total_seconds()
            except ValueError:
                continue

            if dt <= 0:
                continue

            # Extract centroid coordinates
            # prev_entry should have centroid
            c1 = prev_entry.get('centroid')
            c2 = cell.get('centroid')

            if not c1 or not c2:
                continue

            lat1, lon1 = c1
            lat2, lon2 = c2

            # Convert longitudinal difference to km accounting for latitude
            # Approximation: 1 degree lat ~ 111 km, 1 degree lon ~ 111*cos(lat) km
            dx = (lon2 - lon1) * 111 * cos(radians((lat1 + lat2) / 2)) * 1000  # East-West (meters)
            dy = (lat2 - lat1) * 111 * 1000 # North-South (meters)

            # Append motion vectors to CURRENT cell
            cell['dx'] = dx
            cell['dy'] = dy
            cell['dt'] = dt

        return entries
