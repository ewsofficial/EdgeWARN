import json
from . import load
from . import timestamp
from pathlib import Path
from datetime import datetime
import math

"""
EdgeWARN Storm Vectors Module
Vectors provides EdgeWARN functions that determine vector components of detected storm cells
THIS IS NOT A STANDALONE SCRIPT
"""

def calculate_storm_vectors(cells):
	vectors = []
	for cell in cells:
		history = cell.get('storm_history', [])
		if len(history) < 2:
			continue
		# Sort history by timestamp (oldest to newest)
		history_sorted = sorted(history, key=lambda x: x['timestamp'])
		h0, h1 = history_sorted[-2], history_sorted[-1]
		c0 = h0['centroid']
		c1 = h1['centroid']
		t0 = h0['timestamp']
		t1 = h1['timestamp']
		try:
			dt0 = datetime.fromisoformat(t0)
			dt1 = datetime.fromisoformat(t1)
		except Exception:
			dt0 = datetime.fromisoformat(timestamp.extract_timestamp_from_filename(t0))
			dt1 = datetime.fromisoformat(timestamp.extract_timestamp_from_filename(t1))
		dt_seconds = (dt1 - dt0).total_seconds()
		avg_lat = (c0[0] + c1[0]) / 2
		deg_to_m_lat = 111320.0
		deg_to_m_lon = 111320.0 * math.cos(math.radians(avg_lat))
		dx = (c1[1] - c0[1]) * deg_to_m_lon
		dy = (c1[0] - c0[0]) * deg_to_m_lat
		# Add dx, dy, dt to the latest (h1) history entry
		h1['dx'] = dx
		h1['dy'] = dy
		h1['dt'] = dt_seconds
		vectors.append({
			'id': cell['id'],
			'dx': dx,
			'dy': dy,
			'dt': dt_seconds,
			't0': t0,
			't1': t1,
			'c0': c0,
			'c1': c1
		})
	return vectors

def clean_vectors(cells, min_magnitude_m=9000.0):
	"""
	Remove cells whose latest vector magnitude (sqrt(dx^2+dy^2)) exceeds min_magnitude_m.

	Args:
		cells (list): List of cell dicts (modified in-place if returned list is reassigned).
		min_magnitude_m (float): Minimum magnitude in meters used to decide removal. Default 0.0

	Returns:
		removed (list): List of dicts {'id': id, 'magnitude_m': mag} for removed cells.

	Behavior / assumptions:
	- Expects latest history entry to have 'dx' and 'dy' in meters (as produced by calculate_storm_vectors).
	- If a cell has no dx/dy in its latest history, it is ignored (not removed).
	- Removal is performed by returning a new list with the removed cells filtered out; caller can overwrite original list.
	"""
	removed = []
	kept = []
	for cell in cells:
		hist = cell.get('storm_history', [])
		if not hist:
			kept.append(cell)
			continue
		# assume history is sorted oldest->newest; take last
		latest = hist[-1]
		dx = latest.get('dx')
		dy = latest.get('dy')
		if dx is None or dy is None:
			# No vector recorded, keep the cell
			kept.append(cell)
			continue
		try:
			mag = math.hypot(float(dx), float(dy))
		except Exception:
			kept.append(cell)
			continue
		if mag > float(min_magnitude_m):
			removed.append({'id': cell.get('id'), 'magnitude_m': mag})
		else:
			kept.append(cell)

	# mutate list in-place by clearing and extending (caller may reassign instead)
	cells.clear()
	cells.extend(kept)
	return removed

def write_vectors():
    import sys
    # Default path or from command line
    json_path = sys.argv[1] if len(sys.argv) > 1 else "stormcell_test.json"
    with open(json_path, 'r') as f:
        cells = json.load(f)
    
    # Calculate vectors
    vectors = calculate_storm_vectors(cells)
    
    # Clean vectors with default threshold
    removed_cells = clean_vectors(cells, min_magnitude_m=9000.0)
    
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


	
