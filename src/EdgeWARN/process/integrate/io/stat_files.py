import json
import os
import tempfile

import numpy as np
import xarray as xr

from .timestamps import extract_timestamp_from_filepath


class StatFileHandler:
    def __init__(self, io_manager):
        self.dataset = None
        self.file_path = None
        self.io_manager = io_manager

    def convert_lon_to_360(self, lon):
        return np.where(lon < 0, lon + 360, lon)

    def convert_lon_to_180(self, lon):
        return np.where(lon > 180, lon - 360, lon)

    def load_file(self, file_path):
        self.file_path = file_path

        try:
            self.dataset = xr.open_dataset(file_path, cache=False, decode_timedelta=True)
            self.io_manager.write_debug(f"Successfully loaded dataset from {file_path}")
            return self.dataset
        except Exception as e:
            self.io_manager.write_error(f"Could not load file {file_path}: {e}")
            return None

    def load_json(self, filepath):
        self.io_manager.write_debug(f"Loading JSON file {filepath}")
        with open(filepath, "r") as f:
            data = json.load(f)
        if not data:
            self.io_manager.write_error(f"{filepath} did not have any data")
            return None

        features = data["features"]
        latest_timestamp = data["latest_timestamp"]
        return features, latest_timestamp

    def write_json(self, data, filepath):
        self.io_manager.write_debug(f"Writing to JSON file {filepath}")
        target_path = os.fspath(filepath)
        target_dir = os.path.dirname(target_path) or "."
        temp_path = None

        try:
            with tempfile.NamedTemporaryFile("w", dir=target_dir, delete=False) as f:
                temp_path = f.name
                json.dump(data, f, indent=4, default=str)
                f.flush()
                os.fsync(f.fileno())

            os.replace(temp_path, target_path)
        except Exception:
            if temp_path and os.path.exists(temp_path):
                os.unlink(temp_path)
            raise

    def find_timestamp(self, filepath):
        return extract_timestamp_from_filepath(
            filepath=filepath,
            io_manager=self.io_manager,
            dataset=self.dataset,
        )
