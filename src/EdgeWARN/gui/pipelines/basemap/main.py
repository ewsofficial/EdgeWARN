import sys
from pathlib import Path
from datetime import datetime
from util.io import IOManager
from util.file import clean_old_files, GUI_MAP_DIR

# Add src to path to import util
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from .data_loader import DataLoader
from .map_utils import MapUtils
from .output_utils import OutputUtils

def main():
    clean_old_files(GUI_MAP_DIR, 60)
    io_manager = IOManager("[BASEMAP]")
    data_loader = DataLoader(io_manager)
    stormcells = data_loader.load_stormcells()
    if stormcells is None:
        return

    # Extract timestamp from the first storm cell's data for timestamping
    mrms_timestamp = None
    if stormcells and 'storm_history' in stormcells[0] and stormcells[0]['storm_history']:
        try:
            # Get timestamp from first storm cell's history (MRMS CompRef timestamp)
            timestamp_str = stormcells[0]['storm_history'][0].get('timestamp')
            if timestamp_str:
                # Parse ISO timestamp to datetime object
                mrms_timestamp = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
        except (ValueError, TypeError) as e:
            io_manager.write_warning(f"Could not parse timestamp from storm data: {e}")
            io_manager.write_warning("Using current time instead.")

    map_utils = MapUtils(io_manager)
    m = map_utils.create_map()
    map_utils.add_storm_cells(m, stormcells)

    output_utils = OutputUtils(io_manager)
    output_utils.save_and_open_map(m, timestamp=mrms_timestamp)

if __name__ == "__main__":
    main()