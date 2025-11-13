# EdgeWARN Core Module API Reference

## Overview

This document provides a comprehensive API reference for the core EdgeWARN functionality: data ingestion, storm cell detection and tracking, composite threat analysis (CTAM), and storm cell data integration.

---

## CTAM Module (`src/EdgeWARN/core/ctam/`)

### Classes

#### `DataLoader`
**Purpose**: Static methods for loading meteorological data for threat analysis

**Methods**:
- `static load_json(json_path: Path) -> dict | None`
  - Loads JSON configuration or threat assessment metadata
  - Parameters: `json_path` - Path to JSON file
  - Returns: Parsed JSON data or None if file doesn't exist

- `static load_ds(ds_path: Path, lat_limits=None, lon_limits=None) -> xarray.Dataset | None`
  - Loads meteorological datasets (GRIB2 or NetCDF formats) for threat analysis
  - Supports spatial subsetting for focused threat assessment areas
  - Parameters: 
    - `ds_path` - Path to dataset file
    - `lat_limits` - Latitude bounds for threat assessment region
    - `lon_limits` - Longitude bounds for threat assessment region
  - Returns: Loaded xarray Dataset or None on failure

#### `DataHandler`
**Purpose**: Manages storm cell threat data with efficient lookup and threat calculation capabilities

**Constructor**:
- `__init__(stormcells: list)`
  - Pre-indexes storm cells by ID for O(1) threat lookup performance

**Methods**:
- `static verify_norm_values(norm_values: dict, default_norm: dict) -> bool`
  - Validates threat normalization parameters against expected thresholds

- `find_top_level_key(cell_id: str|int, key: str) -> any`
  - Retrieves threat-related metadata for specific storm cell

- `find_latest_hist_key(cell_id: str|int, key: str) -> list`
  - Finds historical threat assessment data for storm evolution tracking

---

## Ingest Module (`src/EdgeWARN/core/ingest/`)

### Classes

#### `FileFinder`
**Purpose**: Locates and identifies available MRMS files based on timestamps and modifiers

**Constructor**:
- `__init__(target_time, base_dir, max_time_window, max_entries, io_manager)`

**Methods**:
- `lookup_files(modifier) -> list`
  - Searches for files matching the target time and modifier
  - Returns list of (file_path, timestamp) tuples

#### `FileDownloader`
**Purpose**: Handles downloading, decompression, and file management

**Constructor**:
- `__init__(target_time, io_manager)`

**Methods**:
- `download_latest(files_with_timestamps, outdir) -> str | None`
  - Downloads the most recent file matching criteria
  - Returns path to downloaded file or None on failure

- `decompress_file(downloaded_file) -> None`
  - Automatically decompresses downloaded files
  - Supports common compression formats (gzip, etc.)

### Functions

#### `process_modifier(modifier, outdir, dt, max_time, max_entries) -> None`
**Purpose**: Processes a single MRMS modifier type
- Finds files matching target time
- Downloads and decompresses files
- Handles errors gracefully

#### `download_all_files(dt) -> None`
**Purpose**: Orchestrates concurrent downloading of all MRMS products
- Cleans old files
- Configures search parameters
- Downloads all products concurrently using ThreadPoolExecutor

---

## Process Module (`src/EdgeWARN/core/process/`)

### Detection Sub-module (`detect/`)

#### Functions

##### `detect_cells(radar_path, ps_path, preciptype_path, io_manager, lat_min, lat_max, lon_min, lon_max) -> list`
**Purpose**: Performs storm cell detection on single radar scan
- Loads radar, probability severe, and precipitation type data
- Maps radar gates to polygon boundaries
- Generates bounding boxes for detected regions
- Returns list of storm cell entries

##### `main(radar_old, radar_new, ps_old, ps_new, pt_old, pt_new, lat_bounds, lon_bounds, json_output) -> None`
**Purpose**: Orchestrates complete storm cell detection and tracking workflow
- Supports single-frame and dual-frame processing modes
- Handles cell tracking across time steps
- Calculates motion vectors
- Persists results to JSON

#### Classes

##### `StormCellTracker`
**Purpose**: Tracks storm cells across multiple time steps

**Constructor**:
- `__init__(ps_old, ps_new, io_manager)`

**Methods**:
- `update_cells(entries_old, entries_new) -> list`
  - Matches storm cells between old and new time steps
  - Updates cell metadata with tracking information
  - Returns combined entries with tracking history

##### `DetectionDataHandler`
**Purpose**: Manages loading and preprocessing of detection data

**Methods**:
- `load_subset() -> xarray.Dataset`
  - Loads radar data with geographic subsetting
  - Applies coordinate bounds filtering

- `load_probsevere() -> xarray.Dataset`
  - Loads probability severe data for storm cell validation

- `load_preciptype() -> xarray.Dataset`
  - Loads precipitation type data for storm characterization

##### `GateMapper`
**Purpose**: Maps radar gates to polygon boundaries for storm cell identification

**Constructor**:
- `__init__(radar_ds, ps_ds, io_manager, refl_threshold=40.0)`

**Methods**:
- `map_gates_to_polygons() -> xarray.Dataset`
  - Convert radar gate data to polygon boundaries
  - Apply reflectivity thresholding

- `expand_gates(mapped_ds) -> xarray.Dataset`
  - Expand initial polygons for comprehensive storm coverage

- `draw_bbox(expanded_ds, step=8) -> list`
  - Generate bounding boxes from expanded polygons

##### `CellDataSaver`
**Purpose**: Handles persistence of storm cell data and metadata

**Methods**:
- `create_entry() -> list`
  - Create storm cell entries from bounding box data
  - Generate unique identifiers and metadata

- `append_storm_history(entries, radar_path) -> list`
  - Append temporal history to storm cell entries
  - Integrate radar timestamp and analysis data

##### `StormVectorCalculator`
**Purpose**: Calculates storm motion vectors and physical characteristics

**Methods**:
- `calculate_vectors(entries) -> list`
  - Calculate storm displacement vectors between time steps
  - Compute velocity and direction information
  - Add vector metadata to entries

---

## Process Integration Sub-module (`integrate/`)

### Classes

#### `DataIntegrator`
**Purpose**: Integrates processed storm cell data with meteorological threat analysis

**Constructor**:
- `__init__(storm_cells, threat_data, io_manager)`

**Methods**:
- `integrate_threat_data() -> list`
  - Combines storm cell detection with CTAM threat analysis
  - Returns enhanced storm cell entries with threat assessments

- `calculate_threat_severity() -> dict`
  - Calculates overall threat severity for storm cells
  - Returns threat severity scores and classifications

---

## Schedule Module (`src/EdgeWARN/core/schedule/`)

### Classes

#### `MRMSUpdateChecker`
**Purpose**: Manages data update timing and synchronization across the EdgeWARN system

**Constructor**:
- `__init__(verbose=False, check_interval=60)`

**Methods**:
- `latest_common_minute_1h(modifiers) -> datetime | None`
  - Finds the latest timestamp where all MRMS products have data available within a 1-hour window
  - Parameters: `modifiers` - List of (modifier_path, output_directory) tuples
  - Returns: datetime object or None if no common timestamp found

---

## Configuration and Constants

### MRMS Data Products (`src/EdgeWARN/core/ingest/config.py`)

#### Complete Data Products
```python
mrms_modifiers = [
    ("2D/EchoTop_18/", MRMS_ECHOTOP18_DIR),           # 18 dBZ Echo Tops
    ("2D/EchoTop_30/", MRMS_ECHOTOP30_DIR),           # 30 dBZ Echo Tops
    ("2D/FLASH/QPE_FFG01H/", MRMS_FLASH_DIR),         # Flash Flood Guidance
    ("2D/NLDN_CG_005min_AvgDensity/", MRMS_NLDN_DIR), # Lightning Density
    ("2D/PrecipRate/", MRMS_PRECIPRATE_DIR),          # Precipitation Rate
    ("2D/RadarOnly_QPE_01H/", MRMS_QPE_DIR),          # Hourly QPE
    ("2D/RotationTrack30min/", MRMS_ROTATIONT_DIR),   # Rotation Tracks
    ("2D/VIL_Density/", MRMS_VIL_DIR),               # VIL Density
    ("ProbSevere/PROBSEVERE/", MRMS_PROBSEVERE_DIR),  # Probability Severe
    ("2D/MergedRhoHV/", MRMS_RHOHV_DIR),             # RhoHV Merged
    ("2D/PrecipFlag/", MRMS_PRECIPTYP_DIR),           # Precipitation Type
    ("2D/ReflectivityAtLowestAltitude/", MRMS_RALA_DIR), # Low-level Refl
    ("2D/MergedReflectivityQCComposite/", MRMS_COMPOSITE_DIR), # Composite Refl
    ("2D/VII/", MRMS_VII_DIR)                        # Vertically Integrated Ice
]

check_modifiers = [
    # Subset for validation (without flash guidance)
    # ... (same structure as mrms_modifiers without FLASH entry)
]
```

### Threat Assessment Thresholds (`src/EdgeWARN/core/ctam/utils.py`)

```python
default_norm = {
    "CGFlashDensity": 0,          # Lightning threat baseline
    "EchoTop18": 12.0,           # Storm depth threat indicator
    "EchoTop30": 10.0,           # Deep convection threat
    "PrecipRate": 35,            # Heavy precipitation threat
    "VILDensity": 0,             # In-cloud lightning threat
    "RALA": 40,                  # Low-level reflectivity threat
    "VII": 15.0,                 # Vertically integrated ice threat
    "MLCAPE": 0,                 # Atmospheric instability threat
    "MUCAPE": 0,                 # Unstable atmosphere threat
    "EBShear": 0,                # Environmental shear threat
    "SRH01km": 0,                # Low-level rotation threat
    "SRH02km": 0,                # Mid-level rotation threat
    "MeanWind_1-3kmAGL": 0,      # Mean wind threat
    "LJA": 0.0,                  # Low-level jet analysis
    "MESH": 1,                   # Maximum expected hail size
    "H50_Above_0C": 0,           # Hail above freezing level
    "EchoTop50": 4,              # Extreme echo top threat
    "VIL": 25.0,                 # Vertically integrated liquid threat
    "MaxFED": 0,                 # Maximum FED threat
    "MaxFCD": 0,                 # Maximum FCD threat
    "FlashRate": 0.0,            # Lightning frequency threat
    "FlashDensity": 0.0,         # Lightning density threat
    "MaxLLAz": 0,                # Max lightning azimuth threat
    "PWAT": 1.5,                 # Precipitable water threat
}
```

---

## Data Structures

### Storm Cell Entry Format
```python
{
    "id": "unique_cell_identifier",
    "bbox": [lat_min, lat_max, lon_min, lon_max],
    "center": [lat_center, lon_center],
    "max_reflectivity": float,
    "probsevere_value": float,
    "precipitation_type": str,
    "timestamp": "ISO8601_timestamp",
    "storm_history": [
        {
            "timestamp": "previous_timestamp",
            "position": [lat, lon],
            "intensity": float,
            "velocity": float,
            "direction": float
        }
    ],
    "motion_vector": {
        "u_component": float,
        "v_component": float,
        "speed": float,
        "direction": float
    },
    "threat_assessment": {
        "lightning_threat": float,
        "hail_threat": float,
        "tornado_threat": float,
        "wind_threat": float,
        "overall_threat": float,
        "threat_category": str
    }
}
```

### Threat Assessment Format
```python
{
    "cell_id": "unique_identifier",
    "threat_categories": {
        "lightning": {
            "cg_flash_density": float,
            "vil_density": float,
            "flash_rate": float,
            "threat_level": str
        },
        "hail": {
            "mesh": float,
            "h50_above_0c": float,
            "hail_probability": float,
            "threat_level": str
        },
        "tornado": {
            "lja": float,
            "srh_1km": float,
            "srh_2km": float,
            "ebshear": float,
            "tornado_probability": float,
            "threat_level": str
        },
        "wind": {
            "mean_wind_1_3km": float,
            "srw_4_6km": float,
            "wind_shear": float,
            "threat_level": str
        }
    },
    "composite_threat": {
        "overall_score": float,
        "threat_category": str,
        "confidence": float
    },
    "timestamp": "ISO8601_timestamp"
}
```

---

## Error Handling

### Common Exception Types

- **`ValueError`**: Invalid parameters or threat threshold configurations
- **`FileNotFoundError`**: Missing meteorological data files or directories
- **`IOError`**: File system or network access issues during data ingestion
- **`xarray.ValidationError`**: Invalid or corrupted meteorological data
- **`json.JSONDecodeError`**: Malformed JSON configuration files
- **`DataIntegrityError`**: Inconsistent data affecting threat assessment

### IOManager Logging Methods

All modules use `IOManager` for consistent logging:

- `write_debug(message)` - Debug level logging for development
- `write_info(message)` - Information level logging for normal operations
- `write_warning(message)` - Warning level logging for data issues
- `write_error(message)` - Error level logging for system failures

---

## Usage Patterns

### Complete Processing Workflow
```python
# 1. Check for new MRMS data
checker = MRMSUpdateChecker()
latest_common = checker.latest_common_minute_1h(check_modifiers)

# 2. Download data if available
if latest_common:
    download_all_files(latest_common)

# 3. Detect and track storm cells
main(radar_old, radar_new, ps_old, ps_new, pt_old, pt_new, 
     lat_bounds, lon_bounds, json_output)

# 4. Integrate with threat analysis
from EdgeWARN.core.process.integrate.main import main as integrate_main
integrate_main()
```

### Threat Analysis Only
```python
from EdgeWARN.core.ctam.utils import DataHandler

# Load storm cell data
handler = DataHandler(storm_cells_list)

# Calculate threat assessments
for cell in storm_cells_list:
    threat_data = handler.calculate_composite_threats(cell["id"])
    print(f"Cell {cell['id']} - Overall threat: {threat_data['overall_score']}")
```

### Data Ingestion Only
```python
from EdgeWARN.core.ingest.main import download_all_files
import datetime

# Download latest MRMS data
current_time = datetime.datetime.now(datetime.timezone.utc)
download_all_files(current_time)
```

### Custom Geographic Processing
```python
# Custom processing bounds
custom_bounds = {
    "lat_min": 35.0, "lat_max": 40.0,  # Southern region
    "lon_min": 280.0, "lon_max": 290.0  # Central US
}

# Process custom region
entries = detect_cells(
    radar_path="custom_radar.nc",
    ps_path="custom_probsevere.nc",
    preciptype_path="custom_preciptype.nc",
    io_manager=io_manager,
    lat_min=custom_bounds["lat_min"],
    lat_max=custom_bounds["lat_max"], 
    lon_min=custom_bounds["lon_min"],
    lon_max=custom_bounds["lon_max"]
)
```

---

## Dependencies

### Core Libraries
- **xarray**: Multi-dimensional data handling for meteorological datasets
- **numpy**: Numerical computations and array operations
- **concurrent.futures**: Thread pool management for concurrent data ingestion
- **pathlib**: Cross-platform path handling

### Data Formats
- **NetCDF (.nc)**: Primary meteorological data format
- **GRIB2 (.grib2)**: Alternative meteorological data format
- **JSON**: Configuration and output data format

### External Services
- **MRMS Data Feeds**: NOAA's Multi-Radar Multi-Sensor data servers
- **File Compression**: Support for gzip and other common formats

---

## Performance Considerations

### Memory Management
- **Chunked Processing**: Large datasets processed in manageable chunks
- **Subset Operations**: Geographic subsetting reduces memory usage
- **Automatic Cleanup**: Temporary files and data structures cleaned automatically

### Processing Efficiency
- **Vectorized Operations**: NumPy/XArray vectorized operations for fast calculations
- **Concurrent Processing**: Multi-threaded downloads and independent processing
- **Threat Calculation**: Optimized threat assessment algorithms

### Scalability
- **Horizontal Scaling**: Multiple geographic regions processed independently
- **Time Series**: Arbitrary length time series processing
- **Resource Management**: Automatic resource cleanup and error recovery
- **Threat Assessment**: Scalable threat calculation for large numbers of storm cells

---

## Integration Patterns

### Real-time Processing
- Continuous data ingestion from MRMS sources
- Real-time storm cell detection and threat assessment
- Automated processing pipeline with minimal human intervention

### Batch Processing
- Historical data processing for research and validation
- Large-scale threat analysis for climatological studies
- Post-event analysis and verification

### Hybrid Processing
- Real-time threat monitoring with historical context
- Incremental threat assessment updates
- Dynamic threat threshold adjustment based on conditions