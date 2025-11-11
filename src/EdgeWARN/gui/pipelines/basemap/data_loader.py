import json
from util.file import STORMCELL_JSON
from util.io import IOManager

class DataLoader:
    def __init__(self, io_manager=None):
        self.io_manager = io_manager or IOManager("DataLoader")

    def load_stormcells(self):
        if not STORMCELL_JSON.exists():
            self.io_manager.write_error(f"Stormcell file not found: {STORMCELL_JSON}")
            return None

        with open(STORMCELL_JSON, 'r') as f:
            stormcells = json.load(f)

        return stormcells