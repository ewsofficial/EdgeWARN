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
MRMS_MESH_DIR = BASE_DIR / "mrms_mesh"
MRMS_PROBSEVERE_DIR = BASE_DIR / "mrms_probsevere"
MRMS_COMBINED_DIR = BASE_DIR / "mrms_combined_strikes"
MRMS_RADAR_DIR = BASE_DIR / "nexrad_merged"
THREDDS_RTMA_DIR = BASE_DIR / "rtma"
TEMP_DIR = BASE_DIR / "temp"
MRMS_COMBINED_DIR.mkdir(parents=True, exist_ok=True)

def latest_nexrad(n):
    """Return the n most recent NEXRAD L2 files as a list (oldest to newest)."""
    if not NEXRAD_L2_DIR.exists():
        print("WARNING: NEXRAD directory doesn't exist!")
        return
    files = sorted([f for f in NEXRAD_L2_DIR.glob("*") if f.is_file()], key=lambda f: f.stat().st_mtime)
    if len(files) < n:
        raise RuntimeError(f"Not enough NEXRAD L2 files in {NEXRAD_L2_DIR}")
    return [str(f) for f in files[-n:]]

def latest_mosaic(n):
    """Return the n most recent MRMS Radar Mosaic files as a list (oldest to newest)."""
    if not MRMS_RADAR_DIR.exists():
        print("WARNING: MRMS mosaic directory doesn't exist!")
        return
    files = sorted([f for f in MRMS_RADAR_DIR.glob("*") if f.is_file()], key=lambda f: f.stat().st_mtime)
    if len(files) < n:
        raise RuntimeError(f"Not enough MRMS Mosaic files in {MRMS_RADAR_DIR}")
    return [str(f) for f in files[-n:]]

def latest_glm(n):
    """Return the n most recent GOES GLM files as a list (oldest to newest)."""
    if not GOES_GLM_DIR.exists():
        print("WARNING: GOES GLM directory doesn't exist!")
        return
    files = sorted([f for f in GOES_GLM_DIR.glob("*") if f.is_file()], key=lambda f: f.stat().st_mtime)
    if len(files) < n:
        raise RuntimeError(f"No GOES GLM files in {GOES_GLM_DIR}")
    return [str(f) for f in files[-n:]]

def latest_ltng(n): #For Debug Purposes; use get_latest_mrms_lightning_combined_files for operational use
    """Return the n most recent MRMS Lightning files as a list (oldest to newest)."""
    if not MRMS_LTNG_DIR.exists():
        print("WARNING: MRMS Lightning directory doesn't exist!")
        return
    files = sorted([f for f in MRMS_LTNG_DIR.glob("*") if f.is_file()], key=lambda f: f.stat().st_mtime)
    if len(files) < n:
        raise RuntimeError(f"No MRMS Lightning files in {MRMS_LTNG_DIR}")
    return [str(f) for f in files[-n:]]

def latest_nldn(n): #For Debug Purposes; use get_latest_mrms_lightning_combined_files for operational use
    """Return the n most recent MRMS NLDN files as a list (oldest to newest)."""
    if not MRMS_NLDN_DIR.exists():
        print("WARNING: MRMS NLDN directory doesn't exist!")
        return
    files = sorted([f for f in MRMS_NLDN_DIR.glob("*") if f.is_file()], key=lambda f: f.stat().st_mtime)
    if len(files) < n:
        raise RuntimeError(f"No MRMS NLDN files in {MRMS_NLDN_DIR}")
    return [str(f) for f in files[-n:]]

def latest_echotop18(n):
    """Return the n most recent MRMS EchoTop18 files as a list (oldest to newest)."""
    if not MRMS_ECHOTOP18_DIR.exists():
        print("WARNING: MRMS EchoTop18 directory doesn't exist!")
        return
    files = sorted([f for f in MRMS_ECHOTOP18_DIR.glob("*") if f.is_file()], key=lambda f: f.stat().st_mtime)
    if len(files) < n:
        raise RuntimeError(f"No MRMS EchoTop18 files in {MRMS_ECHOTOP18_DIR}")
    return [str(f) for f in files[-n:]]

def latest_qpe15(n):
    """Return the n most recent MRMS QPE15 files as a list (oldest to newest)."""
    if not MRMS_QPE15_DIR.exists():
        print("WARNING: MRMS QPE15 directory doesn't exist!")
        return
    files = sorted([f for f in MRMS_QPE15_DIR.glob("*") if f.is_file()], key=lambda f: f.stat().st_mtime)
    if len(files) < n:
        raise RuntimeError(f"No MRMS QPE15 files in {MRMS_QPE15_DIR}")
    return [str(f) for f in files[-n:]]

def latest_preciprate(n):
    """Return the n most recent MRMS PrecipRate files as a list (oldest to newest)."""
    if not MRMS_PRECIPRATE_DIR.exists():
        print("WARNING: MRMS PrecipRate directory doesn't exist!")
        return
    files = sorted([f for f in MRMS_PRECIPRATE_DIR.glob("*") if f.is_file()], key=lambda f: f.stat().st_mtime)
    if len(files) < n:
        raise RuntimeError(f"No MRMS PrecipRate files in {MRMS_PRECIPRATE_DIR}")
    return [str(f) for f in files[-n:]]

"""
def latest_mesh(n=2):
    ""Return the n most recent MRMS MESH files as a list (oldest to newest).""
    files = sorted([f for f in MRMS_MESH_DIR.glob("*") if f.is_file()], key=lambda f: f.stat().st_mtime)
    if len(files) < n:
        raise RuntimeError(f"No MRMS MESH files in {MRMS_MESH_DIR}")
    return [str(f) for f in files[-n:]]
"""

def latest_probsevere(n=2):
    """Return the n most recent MRMS ProbSevere files as a list (oldest to newest)."""
    if not MRMS_PROBSEVERE_DIR.exists():
        print("WARNING: MRMS ProbSevere directory doesn't exist!")
        return
    files = sorted([f for f in MRMS_PROBSEVERE_DIR.glob("*") if f.is_file()], key=lambda f: f.stat().st_mtime)
    if len(files) < n:
        raise RuntimeError(f"No MRMS ProbSevere files in {MRMS_PROBSEVERE_DIR}")
    return [str(f) for f in files[-n:]]

def latest_rtma(n=1):
    """Return the n most recent RTMA files as a list (oldest to newest)."""
    if not THREDDS_RTMA_DIR.exists():
        print("WARNING: THREDDS RTMA directory doesn't exist!")
        return
    files = sorted([f for f in THREDDS_RTMA_DIR.glob("*") if f.is_file()], key=lambda f: f.stat().st_mtime)
    if len(files) < n:
        raise RuntimeError(f"No RTMA files in {THREDDS_RTMA_DIR}")
    return [str(f) for f in files[-n:]]

def latest_ltng_combined(n=2):
    """Return the n most recent combined MRMS Lightning/NLDN files as a list (oldest to newest)."""
    if not MRMS_LTNG_DIR.exists():
        print("WARNING: MRMS Lightning directory doesn't exist!")
        return
    if not MRMS_NLDN_DIR.exists():
        print("WARNING: MRMS NLDN directory doesn't exist!")
        return
    if not MRMS_COMBINED_DIR.exists():
        print("WARNING: MRMS Combined Strikes directory doesn't exist!")
        return
    files = sorted([f for f in MRMS_COMBINED_DIR.glob("*") if f.is_file()], key=lambda f: f.stat().st_mtime)
    if len(files) < n:
        raise RuntimeError(f"No combined MRMS Lightning/NLDN files in {MRMS_COMBINED_DIR}")
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
    
    def parse_probsevere_json(self, json_data: Dict) -> List[Dict]:
        """Parse ProbSevere JSON data and extract storm cell probabilities with coordinates."""
        storm_cells = []
        
        if not isinstance(json_data, dict) or 'features' not in json_data:
            return storm_cells
        
        for feature in json_data['features']:
            if feature.get('type') != 'Feature':
                continue
                
            cell_data = self._extract_probsevere_cell_data(feature)
            if cell_data:
                # Extract coordinates from GeoJSON geometry
                if 'geometry' in feature and feature['geometry']:
                    geometry = feature['geometry']
                    if geometry.get('type') == 'Polygon' and 'coordinates' in geometry:
                        coords = geometry['coordinates']
                        if coords and len(coords) > 0 and len(coords[0]) > 0:
                            # Calculate centroid from polygon coordinates
                            polygon_coords = np.array(coords[0])
                            centroid_lon = np.mean(polygon_coords[:, 0])
                            centroid_lat = np.mean(polygon_coords[:, 1])
                            
                            cell_data['lon'] = float(centroid_lon)
                            cell_data['lat'] = float(centroid_lat)
                            cell_data['polygon_coords'] = coords[0]  # Store full polygon for reference
                
                storm_cells.append(cell_data)
        
        return storm_cells

    def _extract_probsevere_cell_data(self, feature: Dict) -> Optional[Dict]:
        """Extract probability data from a single ProbSevere feature."""
        if not feature or 'properties' not in feature:
            return None
        
        try:
            properties = feature['properties']
            
            cell_data = {
                'id': properties.get('id', 'unknown'),  # Add cell ID if available
                'prob_severe': float(properties.get('ProbSevere', '0')),
                'prob_hail': float(properties.get('ProbHail', '0')),
                'prob_wind': float(properties.get('ProbWind', '0')),
                'prob_tornado': float(properties.get('ProbTor', '0')),
                'mesh': float(properties.get('MESH', '0')),
                'vil': float(properties.get('VIL', '0')),
                'flash_rate': float(properties.get('FLASH_RATE', '0')),
                'mucape': float(properties.get('MUCAPE', '0')),
                'mlcape': float(properties.get('MLCAPE', '0')),
                'mlcin': float(properties.get('MLCIN', '0')),
                'ebshear': float(properties.get('EBSHEAR', '0')),
                'srh_1km': float(properties.get('SRH01KM', '0')),
                'mean_wind_1_3km': float(properties.get('MEANWIND_1-3kmAGL', '0')),
            }
            
            return cell_data
            
        except (ValueError, KeyError, TypeError) as e:
            print(f"Error extracting ProbSevere cell data: {e}")
            return None
    
    def debug_probsevere_structure(self, probsevere_cells):
        """Debug function to understand the structure of ProbSevere data"""
        if not probsevere_cells:
            print("No ProbSevere cells to debug")
            return
        
        print("=== ProbSevere Data Structure Debug ===")
        print(f"Number of ProbSevere cells: {len(probsevere_cells)}")
        
        # Show first few cells
        for i, cell in enumerate(probsevere_cells[:3]):
            print(f"\nCell {i} keys: {list(cell.keys())}")
            for key, value in cell.items():
                if isinstance(value, (int, float, str)) and len(str(value)) < 50:
                    print(f"  {key}: {value}")
                else:
                    print(f"  {key}: {type(value)}")
        
        # Check for coordinates in first cell
        first_cell = probsevere_cells[0]
        coord_sources = ['lat', 'lon', 'latitude', 'longitude', 'centroid', 'geometry']
        found_coords = []
        
        for source in coord_sources:
            if source in first_cell:
                found_coords.append(f"{source}: {first_cell[source]}")
        
        print(f"\nCoordinate sources found: {found_coords}")
