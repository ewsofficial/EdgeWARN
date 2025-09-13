import json
from pathlib import Path
# assumes your existing imports: detect, load, match_cells, process_matched_cell, vectors, plot_radar_and_cells
from . import detect
from util.detect_utils import StormCellTracker, Visualizer, CellProcessor, CellMatcher, write_vectors, load_mrms_slice

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
            detect.save_cells_to_json(self.storm_data, self.storm_json)
            print(f"DEBUG: Created new JSON with {len(self.storm_data)} cells from old scan: {[cell['id'] for cell in self.storm_data]}")

        return self.storm_data

    def save(self):
        print("DEBUG: Saving updated JSON...")
        detect.save_cells_to_json(self.storm_data, self.storm_json)


class CellDetector:
    def __init__(self, lat_limits, lon_limits):
        self.lat_limits = lat_limits
        self.lon_limits = lon_limits

    def detect(self, filepath):
        return detect.detect_cells(filepath, self.lat_limits, self.lon_limits, plot=False)


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


def main():
    filepath_old = Path(r"C:\input_data\nexrad_merged\MRMS_MergedReflectivityQC_max_20250913-003641.nc")
    filepath_new = Path(r"C:\input_data\nexrad_merged\MRMS_MergedReflectivityQC_max_20250913-004041.nc")
    storm_json = Path("stormcell_test.json")
    lat_limits = (45.3, 47.3)
    lon_limits = (256.6, 260.2)

    print("=== DEBUG: Starting tracking process ===")

    radar = RadarHandler(lat_limits, lon_limits)
    refl, lat_crop, lon_crop = radar.load_reflectivity(filepath_new)

    detector = CellDetector(lat_limits, lon_limits)

    print("DEBUG: Detecting cells from old scan...")
    cells_old, _ = detector.detect(filepath_old)
    print(f"DEBUG: Found {len(cells_old)} cells in old scan: {[cell['id'] for cell in cells_old]}")
    if cells_old:
        print(f"DEBUG: Old scan timestamp: {cells_old[0]['storm_history'][0]['timestamp']}")

    print("DEBUG: Detecting cells from new scan...")
    cells_new, _ = detector.detect(filepath_new)
    print(f"DEBUG: Found {len(cells_new)} cells in new scan: {[cell['id'] for cell in cells_new]}")
    if cells_new:
        print(f"DEBUG: New scan timestamp: {cells_new[0]['storm_history'][0]['timestamp']}")

    # Add area to cells before matching
    CellProcessor.add_area_to_cells(cells_old)
    CellProcessor.add_area_to_cells(cells_new)

    storm_manager = StormCellDataManager(storm_json)
    storm_data = storm_manager.load_or_create(cells_old)

    existing_cells = {cell['id']: cell for cell in storm_data}
    print(f"DEBUG: Existing cells index: {list(existing_cells.keys())}")

    print("DEBUG: Matching cells between scans...")
    # Use the CellMatcher class from tracker
    matches = CellMatcher.match_cells(cells_old, cells_new)
    print(f"DEBUG: Found {len(matches)} matches: {matches}")

    tracker = CellTracker(storm_data, existing_cells)
    tracker.process_matches(cells_old, cells_new, matches)
    unmatched_count = tracker.add_unmatched_new(cells_new, matches)

    storm_manager.save()

    print("DEBUG: Final storm data structure:")
    for cell in storm_data:
        print(f"  Cell ID {cell['id']}: {len(cell['storm_history'])} history entries")
        for hist_idx, hist_entry in enumerate(cell['storm_history']):
            print(f"    History {hist_idx + 1}: {hist_entry['timestamp']} - {hist_entry['num_gates']} gates")

    print(f"=== DEBUG: Completed tracking process ===")
    print(f"Updated {storm_json} with {len(matches)} matched pairs and {unmatched_count} new cells.")
    print(f"Total cells in database: {len(storm_data)}")

    write_vectors()
    radar.plot(refl, lat_crop, lon_crop, cells_old, cells_new, matches)

if __name__ == "__main__":
    main()