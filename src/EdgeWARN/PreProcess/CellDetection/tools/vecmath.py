from datetime import datetime
from math import radians, cos

class StormVectorCalculator:
    """
    Calculates storm motion vectors (dx, dy, dt) for storm cells
    based on the last two entries in storm_history.
    """

    @staticmethod
    def calculate_vectors(entries):
        """
        Updates each cell's latest storm_history entry with dx, dy, dt.
        
        entries: list of cell dictionaries, each with 'storm_history' list
        """
        for cell in entries:
            storm_history = cell.get('storm_history', [])
            if len(storm_history) < 2:
                # Need at least two entries to calculate motion
                continue

            latest_entry = storm_history[-1]
            prev_entry = storm_history[-2]

            # Extract centroid coordinates
            lat1, lon1 = prev_entry['centroid']
            lat2, lon2 = latest_entry['centroid']

            # Convert longitudinal difference to km accounting for latitude
            # Approximation: 1 degree lat ~ 111 km, 1 degree lon ~ 111*cos(lat) km
            dx = (lon2 - lon1) * 111 * cos(radians((lat1 + lat2) / 2)) * 1000  # East-West
            dy = (lat2 - lat1) * 111 * 1000 # North-South

            # Compute time difference in seconds
            t1 = datetime.fromisoformat(prev_entry['timestamp'])
            t2 = datetime.fromisoformat(latest_entry['timestamp'])
            dt = (t2 - t1).total_seconds()

            # Append motion vectors to latest entry
            latest_entry['dx'] = dx
            latest_entry['dy'] = dy
            latest_entry['dt'] = dt

        return entries
