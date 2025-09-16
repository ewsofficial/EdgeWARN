from pathlib import Path
import platform
import numpy as np
import xarray as xr
import json
from pathlib import Path as PathLibPath
import re
from datetime import datetime
from typing import Dict, List, Optional

# ---------- PATH CONFIG ----------
BASE_DIR = Path("C:/input_data") if platform.system() == "Windows" else Path("data_ingest")
NEXRAD_L2_DIR = BASE_DIR / "nexrad_l2"
MRMS_3D_DIR = BASE_DIR / "nexrad_merged"
GOES_GLM_DIR = BASE_DIR / "goes_glm"
MRMS_LTNG_DIR = BASE_DIR / "mrms_lightning"
MRMS_NLDN_DIR = BASE_DIR / "mrms_nldn"
MRMS_ECHOTOP18_DIR = BASE_DIR / "mrms_echotop18"
MRMS_QPE15_DIR = BASE_DIR / "mrms_qpe15"
MRMS_PRECIPRATE_DIR = BASE_DIR / "mrms_preciprate"
MRMS_PROBSEVERE_DIR = BASE_DIR / "mrms_probsevere"
MRMS_COMBINED_DIR = BASE_DIR / "mrms_combined_strikes"
MRMS_RADAR_DIR = BASE_DIR / "nexrad_merged"
MRMS_FLASH_DIR = BASE_DIR / "mrms_flash"
MRMS_VIL_DIR = BASE_DIR / "mrms_vil_density"
MRMS_ROTATIONT_DIR = BASE_DIR / "mrms_rotationtrack"
THREDDS_RTMA_DIR = BASE_DIR / "rtma"
NOAA_RAP_DIR = BASE_DIR / "rap"
TEMP_DIR = BASE_DIR / "temp"
MRMS_COMBINED_DIR.mkdir(parents=True, exist_ok=True)

# NEW LATEST FILES FUNCTION
def latest_files(dir, n):
    """
    Return the n most recent files in a directory as a list (oldest to newest), excluding .idx files
    Inputs:
    - dir: Directory
    - n: Number of files
    Outputs:
    - List of files (oldest to newest) in the directory
    """
    if not dir.exists():
        print(f"WARNING: {dir} doesn't exist!")
        return
    files = sorted(
        [f for f in dir.glob("*") if f.is_file() and f.suffix.lower() != ".idx"],
        key=lambda f: f.stat().st_mtime
    )
    if len(files) < n:
        raise RuntimeError(f"Not enough files in {dir}")
    return [str(f) for f in files[-n:]]

def latest_nexrad(n):
    """Return the n most recent NEXRAD L2 files as a list (oldest to newest), excluding .idx files."""
    if not NEXRAD_L2_DIR.exists():
        print("WARNING: NEXRAD directory doesn't exist!")
        return
    files = sorted(
        [f for f in NEXRAD_L2_DIR.glob("*") if f.is_file() and f.suffix.lower() != ".idx"],
        key=lambda f: f.stat().st_mtime
    )
    if len(files) < n:
        raise RuntimeError(f"Not enough NEXRAD L2 files in {NEXRAD_L2_DIR}")
    return [str(f) for f in files[-n:]]


def latest_mosaic(n):
    """Return the n most recent MRMS Radar Mosaic files as a list (oldest to newest), excluding .idx files."""
    if not MRMS_RADAR_DIR.exists():
        print("WARNING: MRMS mosaic directory doesn't exist!")
        return
    files = sorted(
        [f for f in MRMS_RADAR_DIR.glob("*") if f.is_file() and f.suffix.lower() != ".idx"],
        key=lambda f: f.stat().st_mtime
    )
    if len(files) < n:
        raise RuntimeError(f"Not enough MRMS Mosaic files in {MRMS_RADAR_DIR}")
    return [str(f) for f in files[-n:]]


def latest_glm(n):
    """Return the n most recent GOES GLM files as a list (oldest to newest), excluding .idx files."""
    if not GOES_GLM_DIR.exists():
        print("WARNING: GOES GLM directory doesn't exist!")
        return
    files = sorted(
        [f for f in GOES_GLM_DIR.glob("*") if f.is_file() and f.suffix.lower() != ".idx"],
        key=lambda f: f.stat().st_mtime
    )
    if len(files) < n:
        raise RuntimeError(f"No GOES GLM files in {GOES_GLM_DIR}")
    return [str(f) for f in files[-n:]]


def latest_nldn(n):
    """Return the n most recent MRMS NLDN files as a list (oldest to newest), excluding .idx files."""
    if not MRMS_NLDN_DIR.exists():
        print("WARNING: MRMS NLDN directory doesn't exist!")
        return
    files = sorted(
        [f for f in MRMS_NLDN_DIR.glob("*") if f.is_file() and f.suffix.lower() != ".idx"],
        key=lambda f: f.stat().st_mtime
    )
    if len(files) < n:
        raise RuntimeError(f"No MRMS NLDN files in {MRMS_NLDN_DIR}")
    return [str(f) for f in files[-n:]]


def latest_echotop18(n):
    """Return the n most recent MRMS EchoTop18 files as a list (oldest to newest), excluding .idx files."""
    if not MRMS_ECHOTOP18_DIR.exists():
        print("WARNING: MRMS EchoTop18 directory doesn't exist!")
        return
    files = sorted(
        [f for f in MRMS_ECHOTOP18_DIR.glob("*") if f.is_file() and f.suffix.lower() != ".idx"],
        key=lambda f: f.stat().st_mtime
    )
    if len(files) < n:
        raise RuntimeError(f"No MRMS EchoTop18 files in {MRMS_ECHOTOP18_DIR}")
    return [str(f) for f in files[-n:]]


def latest_qpe15(n):
    """Return the n most recent MRMS QPE15 files as a list (oldest to newest), excluding .idx files."""
    if not MRMS_QPE15_DIR.exists():
        print("WARNING: MRMS QPE15 directory doesn't exist!")
        return
    files = sorted(
        [f for f in MRMS_QPE15_DIR.glob("*") if f.is_file() and f.suffix.lower() != ".idx"],
        key=lambda f: f.stat().st_mtime
    )
    if len(files) < n:
        raise RuntimeError(f"No MRMS QPE15 files in {MRMS_QPE15_DIR}")
    return [str(f) for f in files[-n:]]


def latest_preciprate(n):
    """Return the n most recent MRMS PrecipRate files as a list (oldest to newest), excluding .idx files."""
    if not MRMS_PRECIPRATE_DIR.exists():
        print("WARNING: MRMS PrecipRate directory doesn't exist!")
        return
    files = sorted(
        [f for f in MRMS_PRECIPRATE_DIR.glob("*") if f.is_file() and f.suffix.lower() != ".idx"],
        key=lambda f: f.stat().st_mtime
    )
    if len(files) < n:
        raise RuntimeError(f"No MRMS PrecipRate files in {MRMS_PRECIPRATE_DIR}")
    return [str(f) for f in files[-n:]]


def latest_vil_density(n):
    """Return the n most recent MRMS VIL Density files as a list (oldest to newest), excluding .idx files."""
    if not MRMS_VIL_DIR.exists():
        print("WARNING: MRMS VIL Density directory doesn't exist!")
        return
    files = sorted(
        [f for f in MRMS_VIL_DIR.glob("*") if f.is_file() and f.suffix.lower() != ".idx"],
        key=lambda f: f.stat().st_mtime
    )
    if len(files) < n:
        raise RuntimeError(f"No MRMS VIL Density files in {MRMS_VIL_DIR}")
    return [str(f) for f in files[-n:]]


def latest_ffg(n):
    """Return the n most recent FFG Guidance files as a list (oldest to newest), excluding .idx files."""
    if not MRMS_FLASH_DIR.exists():
        print("WARNING: MRMS FFG Guidance directory doesn't exist!")
        return
    files = sorted(
        [f for f in MRMS_FLASH_DIR.glob("*") if f.is_file() and f.suffix.lower() != ".idx"],
        key=lambda f: f.stat().st_mtime
    )
    if len(files) < n:
        raise RuntimeError(f"No MRMS FFG files in {MRMS_FLASH_DIR}")
    return [str(f) for f in files[-n:]]


def latest_rotationtrack(n):
    """Return the n most recent 30min rotation track files as a list (oldest to newest), excluding .idx files."""
    if not MRMS_ROTATIONT_DIR.exists():
        print("WARNING: MRMS RotationTrack30min directory doesn't exist!")
        return
    files = sorted(
        [f for f in MRMS_ROTATIONT_DIR.glob("*") if f.is_file() and f.suffix.lower() != ".idx"],
        key=lambda f: f.stat().st_mtime
    )
    if len(files) < n:
        raise RuntimeError(f"No MRMS RotationTrack30min in {MRMS_ROTATIONT_DIR}")
    return [str(f) for f in files[-n:]]


def latest_probsevere(n):
    """Return the n most recent MRMS ProbSevere files as a list (oldest to newest), excluding .idx files."""
    if not MRMS_PROBSEVERE_DIR.exists():
        print("WARNING: MRMS ProbSevere directory doesn't exist!")
        return
    files = sorted(
        [f for f in MRMS_PROBSEVERE_DIR.glob("*") if f.is_file() and f.suffix.lower() != ".idx"],
        key=lambda f: f.stat().st_mtime
    )
    if len(files) < n:
        raise RuntimeError(f"No MRMS ProbSevere files in {MRMS_PROBSEVERE_DIR}")
    return [str(f) for f in files[-n:]]


def latest_rtma(n):
    """Return the n most recent RTMA files as a list (oldest to newest), excluding .idx files."""
    if not THREDDS_RTMA_DIR.exists():
        print("WARNING: THREDDS RTMA directory doesn't exist!")
        return
    files = sorted(
        [f for f in THREDDS_RTMA_DIR.glob("*") if f.is_file() and f.suffix.lower() != ".idx"],
        key=lambda f: f.stat().st_mtime
    )
    if len(files) < n:
        raise RuntimeError(f"No RTMA files in {THREDDS_RTMA_DIR}")
    return [str(f) for f in files[-n:]]


def latest_rap(n):
    """Return the n most recent RAP files as a list (oldest to newest), excluding .idx files."""
    if not NOAA_RAP_DIR.exists():
        print("WARNING: NOAA RAP directory doesn't exist!")
        return
    files = sorted(
        [f for f in NOAA_RAP_DIR.glob("*") if f.is_file() and f.suffix.lower() != ".idx"],
        key=lambda f: f.stat().st_mtime
    )
    if len(files) < n:
        raise RuntimeError(f"No RAP files in {NOAA_RAP_DIR}")
    return [str(f) for f in files[-n:]]


def clean_idx_files(folders):
    """
    Remove IDX files in a specified list of folders.
    Inputs:
    - folders: list of folders you want to remove IDX files from
    """
    for folder in folders:
        if folder.exists():
            idx_files = list(folder.rglob("*.idx"))
            if len(idx_files) == 0:
                print(f"No IDX files in folder: {folder}")
                return
            else:
                for f in idx_files:
                    try:
                        f.unlink()
                        print(f"Deleted IDX file: {f}")
                    except Exception as e:
                        print(f"Failed to delete IDX file {f}: {e}")
        else:
            print(f"Folder not found: {folder}")

class StatFileHandler:
    def __init__(self):
        """
        Initialize the StatFileLoader for loading data files.
        """
        self.dataset = None
        self.file_path = None

    def convert_lon_to_360(self, lon):
        """
        Convert longitude from -180 to 180 range to 0 to 360 range.
        
        Args:
            lon (array-like): Longitude values in -180 to 180 range
            
        Returns:
            array-like: Longitude values converted to 0 to 360 range
        """
        return np.where(lon < 0, lon + 360, lon)
    
    def convert_lon_to_180(self, lon):
        """
        Convert longitude from 0 to 360 range to -180 to 180 range.
        
        Args:
            lon (array-like): Longitude values in 0 to 360 range
            
        Returns:
            array-like: Longitude values converted to -180 to 180 range
        """
        return np.where(lon > 180, lon - 360, lon)
        
    def load_file(self, file_path):
        """
        Load a radar data file using xarray.
        
        Args:
            file_path (str): Path to the radar data file
            
        Returns:
            xarray.Dataset: Loaded dataset or None if failed
        """
        self.file_path = file_path
        
        try:
            self.dataset = xr.open_dataset(file_path, cache=False)
            print(f"Successfully loaded dataset from {file_path}")
            return self.dataset
        except Exception as e:
            print(f"Error loading file {file_path}: {e}")
            return None
        
    def load_json(self, filepath):
        print(f"DEBUG: Loading JSON file {filepath}")
        with open(filepath, 'r') as f:
            data = json.load(f)
        if not data:
            print(f"ERROR: {filepath} did not have any data")
            return None
        else:
            return data
    
    def write_json(self, data, filepath):
        print(f"DEBUG: Writing to JSON file {filepath}")
        with open(filepath, 'w') as f:
            json.dump(data, f, indent=4)
        print(f"Successfully wrote to JSON file {filepath}")
    
    def find_timestamp(self, filepath):
        """
        Extract timestamp from meteorological file path using common naming patterns.
        
        Args:
            filepath (str): Path to the file
            
        Returns:
            datetime: Extracted timestamp or None if not found
        """
        filename = PathLibPath(filepath).name
        
        # Common timestamp patterns in meteorological files
        patterns = [
            # YYYYMMDD_HHMMSS pattern
            r'(\d{8}[_\.-]\d{6})',
            # YYYYMMDD_HHMM pattern
            r'(\d{8}[_\.-]\d{4})',
            # YYYYMMDD pattern
            r'(\d{8})',
            # Unix timestamp pattern
            r'(\d{10,})'
        ]
        
        for pattern in patterns:
            match = re.search(pattern, filename)
            if match:
                timestamp_str = match.group(1)
                
                try:
                    # Try to parse different timestamp formats
                    if len(timestamp_str) == 15 and ('_' in timestamp_str or '.' in timestamp_str or '-' in timestamp_str):
                        # YYYYMMDD_HHMMSS format
                        date_part, time_part = re.split(r'[_\.-]', timestamp_str)
                        if len(time_part) == 6:
                            return datetime.strptime(f"{date_part}{time_part}", "%Y%m%d%H%M%S")
                        elif len(time_part) == 4:
                            return datetime.strptime(f"{date_part}{time_part}00", "%Y%m%d%H%M%S")
                    
                    elif len(timestamp_str) == 14:
                        # YYYYMMDDHHMMSS format
                        return datetime.strptime(timestamp_str, "%Y%m%d%H%M%S")
                    
                    elif len(timestamp_str) == 12:
                        # YYYYMMDDHHMM format
                        return datetime.strptime(timestamp_str + "00", "%Y%m%d%H%M%S")
                    
                    elif len(timestamp_str) == 8:
                        # YYYYMMDD format
                        return datetime.strptime(timestamp_str + "000000", "%Y%m%d%H%M%S")
                    
                    elif len(timestamp_str) >= 10:
                        # Unix timestamp
                        return datetime.fromtimestamp(int(timestamp_str[:10]))
                        
                except (ValueError, TypeError) as e:
                    print(f"Warning: Could not parse timestamp '{timestamp_str}' from {filename}: {e}")
                    continue
        
        # If no pattern matched, try to extract from dataset if it's loaded
        if self.dataset is not None:
            try:
                # Check for common time coordinate names
                time_coords = ['time', 'valid_time', 'forecast_time', 'reference_time']
                for coord in time_coords:
                    if coord in self.dataset.coords:
                        time_data = self.dataset[coord].values
                        if len(time_data) > 0:
                            if hasattr(time_data[0], 'item'):
                                return datetime.utcfromtimestamp(time_data[0].item() / 1e9)
                            else:
                                return datetime.utcfromtimestamp(time_data[0] / 1e9)
            except Exception as e:
                print(f"Warning: Could not extract time from dataset: {e}")
        
        print(f"Warning: Could not find timestamp in filename: {filename}")
        return None
