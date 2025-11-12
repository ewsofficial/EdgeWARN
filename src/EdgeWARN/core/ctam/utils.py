import json
import xarray as xr
from pathlib import Path
from util.io import IOManager
from datetime import datetime

io_manager = IOManager(f"[CTAM]")

default_norm = { # 0 indicates that we haven't decided what value we use
    "CGFlashDensity": 0,
    "EchoTop18": 12.0,
    "EchoTop30": 10.0,
    "PrecipRate": 35,
    "VILDensity": 0,
    "RALA": 40,
    "VII": 15.0,
    "MLCAPE": 0,
    "MUCAPE": 0,
    "MLCIN": 0,
    "DCAPE": 0,
    "CAPE_M10M30": 0,
    "LCL": 0,
    "Wetbulb_0C_Hgt": 0,
    "LLLR": 0,
    "MLLR": 0,
    "EBShear": 0,
    "SRH01km": 0,
    "SRH02km": 0,
    "SRW46km": 0,
    "MeanWind_1-3kmAGL": 0,
    "LJA": 0.0,
    "MaxRef": 45,
    "Ref10": 40,
    "Ref20": 35,
    "RefUpper": 35,
    "MESH": 1,
    "H50_Above_0C": 0,
    "EchoTop50": 4,
    "VIL": 25.0,
    "MaxFED": 0,
    "MaxFCD": 0,
    "AccumFCD": 0,
    "MinFlashArea": 0,
    "TE@MaxFCD": 0,
    "FlashRate": 0.0,
    "FlashDensity": 0.0,
    "MaxLLAz": 0,
    "p98LLAz": 0,
    "p98MLAz": 0,
    "MaxRC_Emiss": 0.0,
    "ICP": 0,
    "PWAT": 1.5,
}

class DataLoader:
    
    @staticmethod
    def load_json(json_path):
        """
        Loads JSON file from a JSON path
        Args:
         - json_path: Path to JSON file

        Returns:
         - data: Contents of JSON file
        """
        path = Path(json_path)
        if path.exists():
            with open(json_path, 'r') as f:
                data = json.load(f)
            
            io_manager.write_debug("Successfully loaded JSON file")
            return data
        
        else:
            io_manager.write_error("JSON path doesn't exist")
            return None
    
    @staticmethod
    def load_ds(ds_path: Path, lat_limits=None, lon_limits=None):
        """
        Loads .grib2/.nc datasets
        Args:
         - ds_path: Pathlib Path() object of grib2 or netCDF dataset
         - lat_limits, lon_limits: 0-360 format of lat/lon limits (Only works on netCDF)
        
        Returns:
         - ds: Loaded dataset
        """
        
        io_manager.write_debug(f"Opening file: {ds_path} ...")

        try:
            if str(ds_path).endswith(".grib2") or str(ds_path).endswith(".grib"):
                if lat_limits or lon_limits:
                    IOManager.write_warning("lat/lon limits not supported with GRIB files, skipping ... ")
                
                ds = xr.open_dataset(ds_path, decode_timedelta=True)
                io_manager.write_debug(f"Successfully loaded dataset: {ds_path}")
                return ds
        
            if str(ds_path).endswith(".nc"):
                ds = xr.open_dataset(ds_path, decode_timedelta=True)

                if lat_limits and lon_limits:
                    # Latitude/Longitude variables: 'latitude', 'longitude'
                    ds = ds.sel(
                        latitude=slice(lat_limits[0], lat_limits[1]),
                        longitude=slice(lon_limits[0], lon_limits[1])
                    )
                    io_manager.write_debug(f"Loaded dataset subset with lat {lat_limits}, lon {lon_limits}")

                else:
                    io_manager.write_warning("lat/lon coordinates not specified, loading full dataset")

                io_manager.write_debug("Successfully loaded full dataset")
                return ds
        
        except Exception as e:
            IOManager.write_error(f"Failed to load dataset - {e}")
            return
    
class DataHandler:
    def __init__(self, stormcells):
        # Pre-index by ID for constant-time lookup
        self.stormcells = {str(cell["id"]): cell for cell in stormcells}

    @staticmethod
    def verify_norm_values(norm_values, default_norm):
        """
        Ensures that `norm_values` has the exact same (case-sensitive) keys as `default_norm`.
        Raises a ValueError if any keys are missing or extra.
        """
        default_keys = set(default_norm.keys())
        given_keys = set(norm_values.keys())

        missing = default_keys - given_keys
        extra = given_keys - default_keys

        if missing or extra:
            msg = []
            if missing:
                msg.append(f"Missing keys: {', '.join(missing)}")
            if extra:
                msg.append(f"Unexpected keys: {', '.join(extra)}")
            raise ValueError("Invalid normalization keys. " + " | ".join(msg))

    def find_top_level_key(self, cell_id, key):
        """
        Finds a top-level key in a specific storm cell by ID.

        Args:
            cell_id (str | int): The storm cell ID to search for.
            key (str): The top-level key to retrieve.

        Returns:
            The value associated with the key, or None if not found.
        """
        try:
            if key == 'storm_history':
                io_manager.write_error("Full storm history lookup not supported for find_top_level_key")
                return
            
            cell = self.stormcells.get(str(cell_id))
            if cell:
                return cell.get(key)
            return None

        except Exception as e:
            io_manager.write_error(f"Failed to obtain {key} - {e}")
    
    def find_latest_hist_key(self, cell_id, key):
        """
        Finds a key in a cell ID's storm_history entries

        Args:
            cell_id (str | int): The storm cell ID to search for
            key (str): The storm history key to retrieve
        
        Returns:
            list of (value, datetime_object) entries
        """
        entries = []

        # Find cell ID
        cell = self.stormcells.get(str(cell_id))
        if cell:
            if 'storm_history' in cell:
                for history in cell['storm_history']:
                    if key in history:
                        entries.append((history[key], datetime.fromisoformat(history['timestamp'])))
                return entries
            
            else:
                io_manager.write_error(f"storm_history not in cell {cell_id}")
                return []
        
        else:
            io_manager.write_error(f"Cell {cell_id} could not be found")
            return []
    
    def find_analysis_key(self, cell_id, key):
        pass