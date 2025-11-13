# CTAM (Composite Threat Analysis Module) Documentation

## Overview

The CTAM module (`src/EdgeWARN/core/ctam/`) is the Comprehensive Threat Analysis Module of the EdgeWARN system. It provides advanced meteorological analysis capabilities for severe weather threat assessment by processing multiple meteorological indices and combining them into comprehensive threat evaluations.

## Module Structure

```
ctam/
├── __init__.py
├── utils.py              # Core data loading and threat analysis utilities
└── comp_indices/         # Composite threat analysis indices
    ├── __init__.py
    ├── growth.py         # Storm growth and development threat indicators
    ├── hail.py           # Hail threat assessment parameters
    ├── intensity.py      # Storm intensity threat metrics
    ├── lightning.py      # Lightning activity threat analysis
    ├── tornado.py        # Tornado probability threat indices
    └── wind.py           # Wind shear and dynamics threat analysis
```

## Core Threat Analysis Classes

### DataLoader Class (`utils.py`)

Provides static methods for loading meteorological data for threat analysis.

#### Methods:

- **`load_json(json_path: Path) -> dict | None`**
  - Loads JSON configuration or threat assessment metadata
  - Parameters: `json_path` - Path to JSON file
  - Returns: Parsed JSON data or None if file doesn't exist

- **`load_ds(ds_path: Path, lat_limits=None, lon_limits=None) -> xarray.Dataset | None`**
  - Loads meteorological datasets (GRIB2 or NetCDF formats) for threat analysis
  - Supports spatial subsetting for focused threat assessment areas
  - Parameters:
    - `ds_path` - Path to dataset file
    - `lat_limits` - Latitude bounds for threat assessment region
    - `lon_limits` - Longitude bounds for threat assessment region
  - Returns: Loaded xarray Dataset or None on failure

### DataHandler Class (`utils.py`)

Manages storm cell threat data with efficient lookup and threat calculation capabilities.

#### Methods:

- **`__init__(stormcells: list)`**
  - Pre-indexes storm cells by ID for O(1) threat lookup performance

- **`verify_norm_values(norm_values: dict, default_norm: dict) -> bool`**
  - Validates threat normalization parameters against expected thresholds

- **`find_top_level_key(cell_id: str|int, key: str) -> any`**
  - Retrieves threat-related metadata for specific storm cell

- **`find_latest_hist_key(cell_id: str|int, key: str) -> list`**
  - Finds historical threat assessment data for storm evolution tracking

## Comprehensive Threat Analysis Indices

### Storm Growth Threat Indicators (`comp_indices/growth.py`)

Analyzes storm development patterns that indicate increasing threat levels:
- Storm growth rates and intensification trends
- Development trajectory assessment
- Maturity stage threat indicators

### Hail Threat Assessment (`comp_indices/hail.py`)

Processes parameters specifically for hail threat evaluation:
- **MESH** (Maximum Expected Hail Size) threat thresholds
- **H50_Above_0C** - Hail presence above freezing level threat zones
- Hail probability calculations based on environmental conditions

### Storm Intensity Threat Metrics (`comp_indices/intensity.py`)

Measures storm strength as primary threat indicators:
- **EchoTop18/30** - Echo top heights indicating storm depth and severity
- **MaxRef** - Maximum reflectivity as core threat intensity measure
- **PrecipRate** - Precipitation rate indicating storm severity
- **VIL** (Vertically Integrated Liquid) as overall storm threat metric

### Lightning Activity Threat Analysis (`comp_indices/lightning.py`)

Analyzes electrical activity as severe weather threat indicators:
- **CGFlashDensity** - Cloud-to-ground flash density as severe threat indicator
- **VILDensity** - In-cloud lightning density as storm intensity measure
- **FlashRate** - Lightning flash frequency indicating electrical activity
- **FlashDensity** - Spatial lightning distribution for threat mapping

### Tornado Probability Threat Indices (`comp_indices/tornado.py`)

Calculates tornado likelihood as critical threat assessment:
- **LJA** - Low-level jet analysis for tornado formation potential
- **SRH01km/02km** - Storm-relative helicity at key levels for rotation threat
- **EBShear** - Environmental bulk shear for tornado development potential

### Wind Threat Analysis (`comp_indices/wind.py`)

Processes wind-related parameters for severe weather threat assessment:
- **MeanWind_1-3kmAGL** - Low-level wind threat indicators
- **SRW46km** - Storm-relative wind at key levels for severe weather potential
- Wind shear calculations for storm organization and maintenance

## Threat Assessment Framework

### Threat Categories

The CTAM module categorizes threats into multiple levels:

1. **Lightning Threat**: Based on flash density and lightning activity
2. **Hail Threat**: Calculated from hail parameters and storm characteristics  
3. **Tornado Threat**: Derived from rotation and environmental indices
4. **Wind Threat**: Assessed from wind shear and storm organization
5. **Overall Severe Weather Threat**: Composite threat from all indicators

### Threat Normalization Framework

The module uses comprehensive default threat thresholds:

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
    "MESH": 1,                   # Maximum expected hail size
    "MaxRef": 45,                # Core reflectivity threat
    "VIL": 25.0,                 # Vertically integrated liquid threat
    "FlashRate": 0.0,            # Lightning frequency threat
    "PWAT": 1.5,                 # Precipitable water threat
}
```

## Threat Assessment Process

### Data Integration
1. **Meteorological Data Loading**: Load multiple data sources for comprehensive analysis
2. **Spatial Subsetting**: Focus threat assessment on specific geographic regions
3. **Temporal Analysis**: Track threat evolution over time for trend assessment

### Threat Calculation
1. **Index Calculation**: Compute individual threat indices from meteorological parameters
2. **Normalization**: Apply appropriate thresholds for threat classification
3. **Composite Scoring**: Combine multiple threat indicators into overall threat assessment
4. **Spatial Mapping**: Generate threat maps for visualization and decision support

### Threat Classification
- **Low Threat**: Minimal severe weather potential
- **Elevated Threat**: Conditions favorable for severe weather development
- **High Threat**: Significant severe weather probability
- **Extreme Threat**: Life-threatening severe weather conditions

## Usage Examples

### Loading Threat Assessment Data
```python
from pathlib import Path
from EdgeWARN.core.ctam.utils import DataLoader

# Load meteorological data for threat analysis
ds_path = Path("mrms_threat_data.nc")
ds = DataLoader.load_ds(ds_path, lat_limits=(40, 45), lon_limits=(280, 290))

# Load threat assessment configuration
threat_config = DataLoader.load_json(Path("threat_thresholds.json"))
```

### Storm Cell Threat Assessment
```python
from EdgeWARN.core.ctam.utils import DataHandler

# Initialize threat assessment with storm cell data
handler = DataHandler(storm_cells_list)

# Assess lightning threat for specific cell
lightning_threat = handler.find_latest_hist_key("cell_123", "cg_flash_density")

# Calculate composite threat metrics
overall_threat = handler.assess_composite_threat("cell_123")
```

### Custom Threat Thresholds
```python
from EdgeWARN.core.ctam.utils import DataHandler, default_norm

# Configure custom threat thresholds
custom_thresholds = {
    "MaxRef": 50,    # Elevated reflectivity threshold
    "MESH": 2.0,     # Enhanced hail size threshold
    "VIL": 30.0,     # Increased VIL threshold
    "SRH01km": 100   # Enhanced rotation threshold
}

# Validate custom threat parameters
DataHandler.verify_norm_values(custom_thresholds, default_norm)
```

## Integration with EdgeWARN System

### Storm Cell Detection Integration
- Receives storm cell detections from process module
- Applies threat analysis to detected cells
- Provides threat context for storm cell characterization

### Data Ingestion Integration
- Consumes MRMS data from ingest module
- Validates data quality for threat assessment
- Handles data freshness for real-time threat evaluation

### Threat Output Integration
- Provides threat assessments to downstream systems
- Supports real-time threat monitoring and alerting
- Enables historical threat analysis and pattern recognition

## Error Handling

The module includes robust error handling for:
- Missing or corrupted meteorological data
- Invalid threat threshold configurations
- Coordinate system mismatches in threat assessment
- Incomplete data sets affecting threat calculations
- Timestamp validation for threat evolution tracking

## Performance Considerations

### Efficient Threat Processing
- **Vectorized Operations**: Fast calculation of threat indices
- **Memory Optimization**: Efficient handling of large meteorological datasets
- **Parallel Processing**: Independent threat calculation for multiple cells

### Real-time Threat Assessment
- **Fast Calculation**: Rapid threat assessment for operational use
- **Incremental Updates**: Efficient updating of threat assessments
- **Caching**: Intelligent caching of threat calculations and thresholds

## Threat Assessment Validation

### Quality Control
- **Data Validation**: Verification of meteorological data quality
- **Threshold Verification**: Validation of threat threshold appropriateness
- **Temporal Consistency**: Ensuring consistent threat assessments over time

### Verification Methods
- **Historical Comparison**: Comparison with known severe weather events
- **Spatial Consistency**: Validation of threat spatial patterns
- **Temporal Evolution**: Tracking threat development and dissipation

## Dependencies

- **Meteorological Libraries**: xarray for multi-dimensional data analysis
- **Data Processing**: NumPy for numerical threat calculations
- **Coordinate Systems**: Geographic coordinate handling
- **File Formats**: Support for GRIB2, NetCDF, and JSON data formats

## Threat Assessment Applications

### Real-time Monitoring
- Continuous threat assessment for operational meteorology
- Early warning system support for severe weather events
- Decision support for weather service operations

### Post-event Analysis
- Historical threat assessment for event evaluation
- Verification of threat prediction accuracy
- Improvement of threat assessment algorithms

### Research Applications
- Severe weather climatology studies
- Threat assessment algorithm development
- Validation of meteorological forecasting techniques