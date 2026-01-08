import json
from pathlib import Path
import sys
import os

# Add src to sys.path to allow imports from util
try:
    _SRC_DIR = Path(__file__).resolve().parents[3]
    _src_str = str(_SRC_DIR)
    if _src_str not in sys.path:
        sys.path.insert(0, _src_str)
except Exception:
    pass

import util.file as fs

def initialize_modules(scan_file_path: str):
    """
    Initializes the 'modules' key as an empty list [] in each cell entry
    of the given scan file, and also updates the corresponding per-cell
    history files in CELL_DIR.
    """
    scan_path = Path(scan_file_path)
    if not scan_path.exists():
        print(f"Error: Scan file not found at {scan_file_path}")
        return

    try:
        with open(scan_path, 'r') as f:
            data = json.load(f)
    except Exception as e:
        print(f"Error loading scan file: {e}")
        return

    if not isinstance(data, dict) or "features" not in data:
        print(f"Error: Invalid scan file format (missing 'features' key)")
        return

    features = data["features"]
    updated_cells_count = 0

    for cell in features:
        cell_id = cell.get("id")
        if cell_id is None:
            continue

        # Initialize modules in scan file entry
        if "modules" not in cell:
            cell["modules"] = {}
            updated_cells_count += 1
        elif not isinstance(cell["modules"], list):
            # If it exists but isn't a list, reset it to []
            cell["modules"] = {}
            updated_cells_count += 1

        # Update per-cell history file
        cell_history_path = fs.CELL_DIR / f"{cell_id}.json"
        if cell_history_path.exists():
            try:
                with open(cell_history_path, 'r') as f:
                    history = json.load(f)
                
                if isinstance(history, list):
                    history_updated = False
                    for entry in history:
                        if "modules" not in entry or not isinstance(entry["modules"], list):
                            entry["modules"] = {}
                            history_updated = True
                    
                    if history_updated:
                        with open(cell_history_path, 'w') as f:
                            json.dump(history, f, default=str)
            except Exception as e:
                print(f"Warning: Failed to update history for cell {cell_id}: {e}")

    # Save the updated scan file
    try:
        with open(scan_path, 'w') as f:
            json.dump(data, f, indent=2, default=str)
        print(f"Successfully initialized 'modules' key for {updated_cells_count} cells in {scan_path.name}")
    except Exception as e:
        print(f"Error saving updated scan file: {e}")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python3 init.py <path_to_stormcells_json>")
    else:
        initialize_modules(sys.argv[1])
