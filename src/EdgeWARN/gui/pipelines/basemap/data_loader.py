import json
from util.file import STORMCELL_JSON

class DataLoader:
    def __init__(self):
        pass

    def load_stormcells(self):
        if not STORMCELL_JSON.exists():
            print(f"Stormcell file not found: {STORMCELL_JSON}")
            return None

        with open(STORMCELL_JSON, 'r') as f:
            stormcells = json.load(f)

        return stormcells