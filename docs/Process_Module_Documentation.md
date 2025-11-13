# Process Module Documentation

## Overview

The Process module (`src/EdgeWARN/core/process/`) is the core computational engine of the EdgeWARN system. It handles storm cell detection, tracking, and analysis using radar and meteorological data. The module implements sophisticated algorithms for identifying severe weather patterns and maintaining storm cell histories across multiple time steps.

## Module Structure

```
process/
├── __init__.py
├── detect/                    # Storm cell detection algorithms
│   ├── __init__.py
│   ├── main.py               # Main detection pipeline orchestration
│   ├── detect.py             # Core detection algorithms
│   ├── track.py              # Storm cell tracking across time
│   └── tools/                # Detection utility tools
│       ├── __init__.py
│       ├── utils.py          # Data handling utilities
│       ├── gatemapper.py     # Gate-to-polygon mapping
│       ├── save.py           # Data persistence utilities
│       ├── vecmath.py        # Vector calculations
│       └── utils.py          # General utilities
└── integrate/                 # Data integration and analysis
    ├── __init__.py
    ├── main.py               # Integration pipeline
    ├── integrate.py          # Core integration logic
    └── utils.py              # Integration utilities
```

## Storm Cell Detection (`detect/`)

### Main Pipeline (`detect/main.py`)

The detection pipeline supports both single-frame and dual-frame processing modes:

#### Core Function: `main()`

**Purpose**: Orchestrates the complete storm cell detection and tracking workflow

**Parameters**:
```python
radar_old, radar_new      # Radar reflectivity data (old/new scans)
ps_old, ps_new           # Probability severe data (old/new)
pt_old, pt_new           # Precipitation type data (old/new)
lat_bounds, lon_bounds   # Geographic boundaries (lat_min, lat_max, lon_min, lon_max)
json_output              # Output file path for results
```

**Processing Modes**:

1. **Single-Frame Mode**: When `radar_new`, `ps_new`, or `pt_new` is None
   - Only analyzes the old scan
   - No tracking across time steps
   - Used for initial detection or when new data unavailable

2. **Dual-Frame Mode**: Full analysis with both old and new scans
   - Detects cells in both time steps
   - Performs cell matching and tracking
   - Calculates storm motion vectors
   - Maintains complete storm history

**Workflow**:
```python
# 1. Load or create previous entries
if json_output.exists() and not empty:
    entries_old = load_existing(json_output)
else:
    entries_old = detect_cells(radar_old, ps_old, pt_old)

# 2. Single-frame processing
if single_frame:
    entries = append_history(entries_old, radar_old)
    entries = calculate_vectors(entries)
    save_to_json(entries)
    return

# 3. Dual-frame processing
entries_new = detect_cells(radar_new, ps_new, pt_new)
entries = update_cells(entries_old, entries_new)  # Cell tracking
entries = append_history(entries, radar_new)
entries = calculate_vectors(entries)
save_to_json(entries)
```

### Core Detection Algorithm (`detect/detect.py`)

#### Function: `detect_cells()`

**Purpose**: Performs storm cell detection on single radar scan

**Parameters**:
- `radar_path` - Path to radar reflectivity data
- `ps_path` - Path to probability severe data
- `preciptype_path` - Path to precipitation type data
- `io_manager` - Logging and error handling
- Geographic boundaries (lat/lon min/max)

**Detection Pipeline**:
1. **Data Loading**: Load radar, probability severe, and precipitation type data
2. **Gate Mapping**: Convert radar gates to polygon boundaries
3. **Polygon Expansion**: Expand initial polygons for comprehensive coverage
4. **Bounding Box Generation**: Create bounding boxes for detected regions
5. **Cell Entry Creation**: Generate storm cell entries with metadata
6. **History Processing**: Add temporal context and analysis

**Example Usage**:
```python
from EdgeWARN.core.process.detect.detect import detect_cells

# Detect storm cells in single scan
entries = detect_cells(
    radar_path="radar.nc",
    ps_path="probsevere.nc", 
    preciptype_path="preciptype.nc",
    io_manager=io_manager,
    lat_min=42, lat_max=46,
    lon_min=287, lon_max=293
)
```

### Storm Cell Tracking (`detect/track.py`)

#### StormCellTracker Class

**Purpose**: Tracks storm cells across multiple time steps to analyze storm evolution and motion

#### Key Methods:

- **`__init__(ps_old, ps_new, io_manager)`**
  - Initialize tracker with probability severe data from consecutive scans
  - Prepares tracking parameters and matching algorithms

- **`update_cells(entries_old, entries_new) -> list`**
  - Matches storm cells between old and new time steps
  - Updates cell metadata with tracking information
  - Returns combined entries with tracking history

**Tracking Algorithm**:
1. **Spatial Matching**: Match cells based on geographic proximity
2. **Intensity Correlation**: Correlate intensity changes between time steps
3. **Motion Vector Calculation**: Calculate storm displacement and velocity
4. **History Integration**: Maintain continuous tracking across multiple scans

### Detection Tools (`detect/tools/`)

#### DetectionDataHandler (`tools/utils.py`)

**Purpose**: Manages loading and preprocessing of detection data

**Key Methods**:
- **`load_subset() -> xarray.Dataset`**
  - Loads radar data with geographic subsetting
  - Applies coordinate bounds filtering

- **`load_probsevere() -> xarray.Dataset`**
  - Loads probability severe data for storm cell validation
  - Integrates with detection algorithms

- **`load_preciptype() -> xarray.Dataset`**
  - Loads precipitation type data for storm characterization
  - Supports classification and analysis

#### GateMapper (`tools/gatemapper.py`)

**Purpose**: Maps radar gates to polygon boundaries for storm cell identification

**Key Methods**:
- **`__init__(radar_ds, ps_ds, io_manager, refl_threshold=40.0)`**
  - Initialize mapper with radar and probability severe data
  - Set reflectivity threshold for gate identification

- **`map_gates_to_polygons() -> xarray.Dataset`**
  - Convert radar gate data to polygon boundaries
  - Apply reflectivity thresholding

- **`expand_gates(mapped_ds) -> xarray.Dataset`**
  - Expand initial polygons for comprehensive storm coverage
  - Apply morphological operations

- **`draw_bbox(expanded_ds, step=8) -> list`**
  - Generate bounding boxes from expanded polygons
  - Return list of bounding box coordinates

#### CellDataSaver (`tools/save.py`)

**Purpose**: Handles persistence of storm cell data and metadata

**Key Methods**:
- **`create_entry() -> list`**
  - Create storm cell entries from bounding box data
  - Generate unique identifiers and metadata

- **`append_storm_history(entries, radar_path) -> list`**
  - Append temporal history to storm cell entries
  - Integrate radar timestamp and analysis data

#### StormVectorCalculator (`tools/vecmath.py`)

**Purpose**: Calculates storm motion vectors and physical characteristics

**Key Methods**:
- **`calculate_vectors(entries) -> list`**
  - Calculate storm displacement vectors between time steps
  - Compute velocity and direction information
  - Add vector metadata to entries

## Data Integration (`integrate/`)

### Integration Pipeline (`integrate/main.py`)

**Purpose**: Integrates processed storm cell data with meteorological analysis

**Workflow**:
1. Load storm cell detection results
2. Integrate with CTAM meteorological analysis
3. Calculate composite indices and metrics
4. Generate comprehensive storm reports

### Core Integration (`integrate/integrate.py`)

**Purpose**: Performs advanced integration of meteorological data with storm cell detections

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
    }
}
```

## Geographic Configuration

The module supports configurable geographic boundaries for storm cell detection:

### Default Bounds
- **Latitude**: 42°N to 46°N (Great Lakes region)
- **Longitude**: 287°W to 293°W (Central US)

### Custom Configuration
Bounds can be modified for different geographic regions:
```python
lat_bounds = (35.0, 38.0)  # Southern region
lon_bounds = (283.0, 285.0)  # Texas region
```

## Processing Parameters

### Radar Data Requirements
- **Format**: NetCDF (.nc) files
- **Variables**: 
  - Composite reflectivity
  - Probability severe values
  - Precipitation type classification

### Detection Thresholds
- **Reflectivity Threshold**: 40.0 dBZ (default)
- **Minimum Cell Size**: Configurable via polygon expansion parameters
- **Tracking Distance**: Maximum allowable displacement between time steps

### File Management
- **Output Format**: JSON with storm cell metadata
- **Data Retention**: Configurable history length
- **Cleanup**: Automatic removal of old detection files

## Error Handling

### Data Loading Errors
- Missing or corrupted radar files
- Invalid coordinate bounds
- Malformed NetCDF data

### Detection Errors
- Insufficient reflectivity for detection
- Invalid geographic boundaries
- Processing timeout or memory issues

### Tracking Errors
- Failed cell matching between time steps
- Invalid motion vector calculations
- Data continuity issues

## Performance Considerations

### Memory Management
- **Chunked Processing**: Large radar datasets processed in chunks
- **Subset Operations**: Geographic subsetting reduces memory usage
- **Cleanup**: Automatic cleanup of temporary data structures

### Processing Speed
- **Vectorized Operations**: Efficient NumPy/XArray operations
- **Parallel Processing**: Independent cell processing where possible
- **Caching**: Intermediate results cached when beneficial

### Scalability
- **Horizontal Scaling**: Can process multiple geographic regions
- **Time Series**: Supports arbitrary length time series processing
- **Data Volume**: Handles large-scale radar archive processing

## Integration Points

### Data Ingestion
- Consumes MRMS data from ingest module
- Validates data freshness and completeness
- Handles multiple data formats (GRIB2, NetCDF)

### Meteorological Analysis
- Integrates with CTAM module for advanced indices
- Provides storm cell context for composite analysis
- Supports real-time and archival processing

### Visualization
- Outputs structured data for GUI pipelines
- Supports real-time map generation
- Enables historical analysis and replay

### Scheduling
- Integrates with scheduler for automated processing
- Supports continuous monitoring workflows
- Handles data freshness validation

## Usage Examples

### Basic Storm Cell Detection
```python
from pathlib import Path
from EdgeWARN.core.process.detect.main import main

# Configure paths
radar_file = Path("mrms_composite_latest.nc")
ps_file = Path("mrms_probsevere_latest.nc")
pt_file = Path("mrms_preciptype_latest.nc")
output_file = Path("stormcells.json")

# Run detection
main(
    radar_old=None, radar_new=radar_file,
    ps_old=None, ps_new=ps_file,
    pt_old=None, pt_new=pt_file,
    lat_bounds=(42, 46), lon_bounds=(287, 293),
    json_output=output_file
)
```

### Dual-Frame Analysis
```python
# Process two consecutive scans
radar_old = Path("composite_20231113_120000.nc")
radar_new = Path("composite_20231113_123000.nc")
ps_old = Path("probsevere_20231113_120000.nc")
ps_new = Path("probsevere_20231113_123000.nc")

main(radar_old, radar_new, ps_old, ps_new, None, None, (42, 46), (287, 293), output_file)
```

### Custom Detection Parameters
```python
from EdgeWARN.core.process.detect.tools.gatemapper import GateMapper

# Custom reflectivity threshold
mapper = GateMapper(radar_ds, ps_ds, io_manager, refl_threshold=45.0)
```

## Dependencies

- **Data Processing**: xarray, numpy for multi-dimensional data
- **Geospatial**: Coordinate system utilities and geo libraries
- **I/O**: Path handling and file management utilities
- **Logging**: IOManager for consistent error handling
- **Time Management**: datetime for timestamp processing

## Configuration Files

The module relies on configuration from several sources:
- **File Paths**: Centralized path management in `util/file.py`
- **Geographic Bounds**: Configurable detection regions
- **Processing Parameters**: Thresholds and limits
- **Output Settings**: JSON formatting and persistence options