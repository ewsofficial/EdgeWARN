import sys
from pathlib import Path
from util.io import IOManager

# Add src to path to import util
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from .data_loader import DataLoader
from .map_utils import MapUtils
from .output_utils import OutputUtils

def main():
    io_manager = IOManager("[BASEMAP]")
    data_loader = DataLoader(io_manager)
    stormcells = data_loader.load_stormcells()
    if stormcells is None:
        return

    map_utils = MapUtils(io_manager)
    m = map_utils.create_map()
    map_utils.add_storm_cells(m, stormcells)

    output_utils = OutputUtils(io_manager)
    output_utils.save_and_open_map(m)

if __name__ == "__main__":
    main()