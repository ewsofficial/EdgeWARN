# EdgeWARN Core Module Documentation

## Overview

The EdgeWARN Core Module (`src/EdgeWARN/core/`) provides the core functionality for the Early Detection and Warning of Anomalous Rainfall system. It includes data ingestion from MRMS sources, storm cell detection and tracking, composite threat analysis, and storm cell data integration.

## Architecture

The core module is organized into four main functional areas:

```
src/EdgeWARN/core/
├── ctam/           # Composite Threat Analysis Module
├── ingest/         # Data Ingestion from MRMS Sources
├── process/        # Storm Cell Detection and Tracking
└── schedule/       # Task Scheduling and Coordination
```

## Core Functionality

### 1. Data Ingestion (ingest/)
- **Purpose**: Downloads and manages MRMS meteorological data from NOAA NCEP servers
- **Key Features**:
  - Multi-threaded downloading of 14 MRMS data products
  - Support for GRIB2 and NetCDF formats
  - Automatic file compression/decompression
  - Data freshness validation
  - Time-bounded file lookup (6-hour lookback window)

### 2. Storm Cell Detection and Tracking (process/)
- **Purpose**: Identifies and tracks storm cells using radar data
- **Key Features**:
  - Single-frame and dual-frame detection modes
  - Storm cell tracking across multiple time steps
  - Vector calculations for storm motion
  - Integration with probability severe data
  - Geographic boundary filtering

### 3. Composite Threat Analysis (ctam/)
- **Purpose**: Comprehensive threat analysis using meteorological indices
- **Key Features**:
  - Lightning activity analysis (CGFlashDensity, VILDensity)
  - Storm intensity metrics (EchoTop, MaxRef, PrecipRate)
  - Atmospheric instability indicators (MLCAPE, MUCAPE)
  - Wind shear analysis (EBShear, SRH01km, SRH02km)
  - Hail detection parameters (MESH, H50_Above_0C)
  - Tornado indices and severe weather probability

### 4. Storm Cell Data Integration (process/integrate/)
- **Purpose**: Integrates processed storm cell data with meteorological analysis
- **Key Features**:
  - Composite meteorological analysis
  - Storm cell characterization
  - Historical data integration
  - Threat severity assessment

## Data Flow

1. **Data Ingestion**: MRMS data downloaded from NOAA sources using concurrent downloads
2. **Storm Detection**: Radar data analyzed to identify and track storm cells
3. **Threat Analysis**: CTAM module calculates comprehensive threat metrics
4. **Data Integration**: Storm cells characterized with meteorological context
5. **Scheduling**: Continuous monitoring ensures fresh data availability

## Dependencies

- **Data Formats**: GRIB2, NetCDF (.nc), JSON
- **Libraries**: xarray, concurrent.futures, datetime
- **Utilities**: Custom IO management, file system utilities
- **External Sources**: MRMS (Multi-Radar Multi-Sensor) data feeds

## Configuration

The module supports configurable parameters for:
- Data source directories and URLs (14 MRMS products)
- Processing boundaries (lat/lon limits)
- Update frequencies and time windows
- Detection thresholds and processing parameters
- Normalization values for meteorological indices

## Usage Examples

### Data Ingestion
```python
from EdgeWARN.core.ingest.main import download_all_files
import datetime

current_time = datetime.datetime.now(datetime.timezone.utc)
download_all_files(current_time)
```

### Storm Cell Detection
```python
from EdgeWARN.core.process.detect.main import main

# Configure detection parameters
radar_new = "path/to/latest/radar/file.nc"
ps_new = "path/to/prob_severe/file.nc"
json_output = "path/to/output/stormcells.json"

main(None, radar_new, None, ps_new, None, None, (42, 46), (287, 293), json_output)
```

### Threat Analysis
```python
from EdgeWARN.core.ctam.utils import DataHandler

# Analyze storm cell data with meteorological context
handler = DataHandler(storm_cells_list)
threat_metrics = handler.calculate_composite_threats()
```

## Integration Notes

- All modules use consistent logging via IOManager
- File paths managed through centralized utilities in src/util/
- Coordinate systems follow meteorological conventions
- Timestamps handled in UTC with ISO 8601 formatting
- Memory-efficient processing for large radar datasets

## Error Handling

The module includes comprehensive error handling for:
- Network connectivity issues during data download
- Invalid or corrupted data files
- Coordinate system mismatches
- Timestamp parsing and validation
- File system permissions and disk space