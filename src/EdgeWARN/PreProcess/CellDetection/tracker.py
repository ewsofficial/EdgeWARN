import json
from .detect import detect_cells
from pathlib import Path
# assumes your existing imports: detect, load, match_cells, process_matched_cell, vectors, plot_radar_and_cells
from util.PreProcess.data_utils import load_mrms_slice
from util.PreProcess.save import StormCellTracker, save_cells_to_json
from util.PreProcess.visualize import Visualizer

class StormCellDataManager:
    def __init__(self, storm_json: Path):
        self.storm_json = storm_json
        self.storm_data = []

    def load_or_create(self, cells_old):
        print("DEBUG: Loading existing storm data...")
        if self.storm_json.exists():
            with open(self.storm_json, "r") as f:
                storm_data = json.load(f)
            print(f"DEBUG: Loaded {len(storm_data)} existing cells: {[cell['id'] for cell in storm_data]}")

            # Deduplicate
            unique_cells = {}
            for cell in storm_data:
                cell_id = cell['id']
                if cell_id not in unique_cells:
                    unique_cells[cell_id] = cell
                else:
                    existing_cell = unique_cells[cell_id]
                    existing_timestamps = {entry["timestamp"] for entry in existing_cell["storm_history"]}
                    for history_entry in cell["storm_history"]:
                        if history_entry["timestamp"] not in existing_timestamps:
                            existing_cell["storm_history"].append(history_entry)
                    existing_last_time = existing_cell["storm_history"][-1]["timestamp"]
                    new_last_time = cell["storm_history"][-1]["timestamp"]
                    if new_last_time > existing_last_time:
                        existing_cell["num_gates"] = cell["num_gates"]
                        existing_cell["centroid"] = cell["centroid"]
                        existing_cell["bbox"] = cell.get("bbox", {})
                        existing_cell["alpha_shape"] = cell.get("alpha_shape", [])
                        existing_cell["max_reflectivity_dbz"] = cell["max_reflectivity_dbz"]

            self.storm_data = list(unique_cells.values())
            print(f"DEBUG: After deduplication: {len(self.storm_data)} unique cells: {[cell['id'] for cell in self.storm_data]}")
        else:
            print("DEBUG: No existing storm data found - creating with old scan cells")
            self.storm_data = cells_old.copy()
            save_cells_to_json(self.storm_data, self.storm_json)
            print(f"DEBUG: Created new JSON with {len(self.storm_data)} cells from old scan: {[cell['id'] for cell in self.storm_data]}")

        return self.storm_data

    def load(self):
        """Load existing storm data from JSON file."""
        if self.storm_json.exists() and self.storm_json.stat().st_size > 0:
            with open(self.storm_json, "r") as f:
                return json.load(f)
        return []
    
    def save(self):
        print("DEBUG: Saving updated JSON...")
        save_cells_to_json(self.storm_data, self.storm_json)


class CellDetector:
    def __init__(self, lat_limits, lon_limits):
        self.lat_limits = lat_limits
        self.lon_limits = lon_limits

    def detect(self, filepath):
        return detect_cells(filepath, self.lat_limits, self.lon_limits, plot=False)


class RadarHandler:
    def __init__(self, lat_limits, lon_limits):
        self.lat_limits = lat_limits
        self.lon_limits = lon_limits

    def load_reflectivity(self, filepath):
        return load_mrms_slice(filepath, self.lat_limits, self.lon_limits)

    def plot(self, refl, lat, lon, cells_old, cells_new, matches):
        # Use the Visualizer class from tracker
        Visualizer.plot_radar_and_cells(refl, lat, lon, cells_old, cells_new, matches)


class CellTracker:
    def __init__(self, storm_data, existing_cells):
        self.storm_data = storm_data
        self.existing_cells = existing_cells

    def process_matches(self, cells_old, cells_new, matches):
        print("DEBUG: Processing matched cells...")
        for match_idx, (i, j, cost) in enumerate(matches):
            old_cell = cells_old[i]
            new_cell = cells_new[j]
            current_timestamp = new_cell["storm_history"][0]["timestamp"]

            print(f"DEBUG: Match {match_idx + 1}: Old cell ID {old_cell['id']} -> New cell ID {new_cell['id']} (cost: {cost:.3f})")

            if old_cell['id'] in self.existing_cells:
                print(f"DEBUG:   Tracked cell ID {old_cell['id']} exists - updating with data from new cell {new_cell['id']}")
                existing_cell = self.existing_cells[old_cell['id']]
                # Use the StormCellTracker class from tracker
                updated = StormCellTracker.process_matched_cell(existing_cell, new_cell, current_timestamp)
                if updated:
                    print(f"DEBUG:   Updated tracked cell ID {old_cell['id']} with properties from scan {current_timestamp}")
                else:
                    print(f"DEBUG:   No update needed for ID {old_cell['id']} (duplicate timestamp)")
            else:
                print(f"DEBUG:   Cell ID {old_cell['id']} not found - adding old cell to maintain tracking")
                self.storm_data.append(old_cell)
                self.existing_cells[old_cell['id']] = old_cell
                print(f"DEBUG:   Added tracked cell ID {old_cell['id']} to storm data")

    def add_unmatched_new(self, cells_new, matches):
        print("DEBUG: Processing unmatched new cells...")
        matched_new_indices = {j for _, j, _ in matches}
        unmatched_count = 0
        for j, new_cell in enumerate(cells_new):
            if j not in matched_new_indices:
                print(f"DEBUG:   Found unmatched new cell ID {new_cell['id']} - adding as new detection")
                self.storm_data.append(new_cell)
                self.existing_cells[new_cell['id']] = new_cell
                unmatched_count += 1

        print(f"DEBUG: Added {unmatched_count} unmatched new cells")
        return unmatched_count