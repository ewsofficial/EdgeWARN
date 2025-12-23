import json
from pathlib import Path
from util.io import IOManager
import util.file as fs

class CellHistoryManager:
    def __init__(self, io_manager: IOManager):
        self.io_manager = io_manager
        # Ensure the cell history directory exists
        if not fs.CELL_DIR.exists():
            fs.CELL_DIR.mkdir(parents=True, exist_ok=True)
            self.io_manager.write_info(f"Created cell history directory at {fs.CELL_DIR}")

    def update_cell_histories(self, cells: list, timestamp: str = None):
        """
        Update the persistent history file for each cell in the list.
        Appends the current cell state safely to CELL_DIR/{cell_id}.json.
        """
        if not cells:
            return

        success_count = 0
        
        for cell in cells:
            cell_id = cell.get("id")
            if not cell_id:
                continue

            # Ensure we have a timestamp
            # Check if this cell is active (has a timestamp assigned by detection)
            if "timestamp" not in cell:
                # Unmatched (inactive) cells do not get a timestamp update in detect/track.py
                # Use this to skip history update entirely, preserving file mtime.
                continue

            # Prioritize cell timestamp (which is now guaranteed to be current if present)
            ts = cell.get("timestamp") or cell.get("properties", {}).get("timestamp")
            
            if not ts:
                self.io_manager.write_warning(f"Cell {cell_id} missing timestamp value, skipping history update.")
                continue

            # Move timestamp to top level (sanity check, usually redundant due to main.py logic now)
            if cell.get("timestamp") != ts:
                 cell["timestamp"] = ts

            # Remove from properties if present (legacy cleanup)
            if "properties" in cell and "timestamp" in cell["properties"]:
                cell["properties"].pop("timestamp")

            file_path = fs.CELL_DIR / f"{cell_id}.json"
            
            history = []
            
            # Load existing history
            if file_path.exists():
                try:
                    with open(file_path, 'r') as f:
                        data = json.load(f)
                        if isinstance(data, list):
                            history = data
                        else:
                            self.io_manager.write_warning(f"History file {file_path} invalid format, resetting.")
                            history = []
                except Exception as e:
                     self.io_manager.write_error(f"Failed to read history for {cell_id}: {e}")
                     history = []

            # Duplicate check
            is_duplicate = False
            if history:
                last_entry = history[-1]
                # Check top-level first, then properties (legacy support)
                last_ts = last_entry.get("timestamp") or last_entry.get("properties", {}).get("timestamp")
                
                # Compare timestamps
                if last_ts == ts:
                    is_duplicate = True
            
            if not is_duplicate:
                # Append current full cell state
                history.append(cell)
                
                # Write back
                try:
                    with open(file_path, 'w') as f:
                        json.dump(history, f, default=str)
                    success_count += 1
                except Exception as e:
                    self.io_manager.write_error(f"Failed to write history for {cell_id}: {e}")
            else:
                # Debug logging for duplicates might be noisy, verify later
                pass

        self.io_manager.write_debug(f"Updated history for {success_count} cells")