import json
from pathlib import Path
from datetime import datetime, timezone, timedelta
from util.io import IOManager
import util.file as fs


class APIIndexManager:
    """Manages index files for the API to track available resources."""
    
    def __init__(self, io_manager: IOManager, remove_old_cells=True):
        self.io_manager = io_manager
        self.stormcell_index_path = fs.STORMCELL_DIR / "stormcell_index.json"
        self.cell_index_path = fs.CELL_DIR / "cell_index.json"
        self.remove_old_cells = remove_old_cells
        
    def initialize_indexes(self):
        """
        Scan existing files and create initial index files.
        Called at server/pipeline startup.
        """
        self.io_manager.write_info("Initializing API indexes...")
        
        # Initialize stormcell index
        self._initialize_stormcell_index()
        
        # Initialize cell index
        self._initialize_cell_index()
        
        self.io_manager.write_info("API indexes initialized successfully")
    
    def _initialize_stormcell_index(self):
        """Scan STORMCELL_DIR and create/update stormcell_index.json"""
        if not fs.STORMCELL_DIR.exists():
            fs.STORMCELL_DIR.mkdir(parents=True, exist_ok=True)
        
        # Find all stormcells_*.json files
        stormcell_files = sorted(fs.STORMCELL_DIR.glob("stormcells_*.json"))
        
        # Extract timestamps from filenames
        timestamps = []
        for file in stormcell_files:
            # Format: stormcells_YYYYMMDD-HHMMSS.json
            name = file.stem  # Remove .json
            if name.startswith("stormcells_"):
                timestamp = name.replace("stormcells_", "")
                timestamps.append(timestamp)
        
        # Create index
        index_data = {
            "timestamps": sorted(timestamps),
            "lastUpdated": datetime.now(timezone.utc).isoformat()
        }
        
        # Write index
        with open(self.stormcell_index_path, 'w') as f:
            json.dump(index_data, f, indent=2)
        
    
    def _initialize_cell_index(self):
        """Scan CELL_DIR and create/update cell_index.json"""
        if not fs.CELL_DIR.exists():
            fs.CELL_DIR.mkdir(parents=True, exist_ok=True)
        
        # Find all {id}.json files
        cell_files = sorted(fs.CELL_DIR.glob("*.json"))
        
        # Extract cell IDs from filenames
        cell_ids = []
        for file in cell_files:
            name = file.stem  # Remove .json
            # Skip index file itself
            if name == "cell_index":
                continue
            try:
                cell_id = int(name)
                cell_ids.append(cell_id)
            except ValueError:
                self.io_manager.write_warning(f"Skipping non-numeric cell file: {name}")
        
        # Create index
        index_data = {
            "cellIds": sorted(cell_ids),
            "lastUpdated": datetime.now(timezone.utc).isoformat()
        }
        
        # Write index
        with open(self.cell_index_path, 'w') as f:
            json.dump(index_data, f, indent=2)
        
    
    def update_stormcell_index(self, timestamp: str):
        """
        Update stormcell_index.json by scanning the directory.
        This ensures the index matches the filesystem (files deleted by main.py are removed).
        
        Args:
            timestamp: Unused, kept for compatibility but we scan the dir anyway.
        """
        self._initialize_stormcell_index()

    def update_cell_index(self, cell_ids: list):
        """
        Update cell_index.json by scanning the directory.
        
        Args:
            cell_ids: Unused, kept for compatibility.
        """
        self._initialize_cell_index()
    
    def cleanup_inactive_cells(self):
        """
        Remove files older than 2 hours from CELL_DIR, then update index.
        """
        if self.remove_old_cells:
            # Files older than 120 minutes
            fs.clean_files_by_age(fs.CELL_DIR, max_age_minutes=120)
        
        # Update index to match reality
        self._initialize_cell_index()
