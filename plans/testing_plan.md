# EdgeWARN Comprehensive Testing Plan

## Overview

This plan outlines the strategy for adding comprehensive tests to the EdgeWARN codebase. The goal is to achieve high test coverage across all modules including unit tests, integration tests, and end-to-end tests.

## Current Test Coverage Analysis

### Existing Tests (✅)
- **API Integration**: [`test_index_manager.py`](tests/core/api_integration/test_index_manager.py)
- **CTAM Engine**: [`test_engine.py`](tests/core/ctam/test_engine.py)
- **METAR Ingest**: [`test_metar.py`](tests/core/ingest/test_metar.py)
- **MRMS Parse**: [`test_parse.py`](tests/core/ingest/mrms/test_parse.py)
- **MRMS Timestamp Utils**: [`test_timestamp_utils.py`](tests/core/ingest/mrms/test_timestamp_utils.py)
- **Process Detect**: [`test_detect_flow.py`](tests/core/process/detect/test_detect_flow.py), [`test_save.py`](tests/core/process/detect/test_save.py), [`test_track.py`](tests/core/process/detect/test_track.py), [`test_vecmath.py`](tests/core/process/detect/test_vecmath.py)
- **Process Integrate**: [`test_integrate_glm.py`](tests/core/process/integrate/test_integrate_glm.py), [`test_integrate_rap.py`](tests/core/process/integrate/test_integrate_rap.py), [`test_history.py`](tests/core/process/integrate/test_history.py)
- **Scheduler**: [`test_scheduler.py`](tests/core/schedule/test_scheduler.py), [`test_scheduler_fallback.py`](tests/core/schedule/test_scheduler_fallback.py)

### Coverage Gaps (❌)
- **API Layer (JavaScript)**: No tests for Express routes, validation, file reading
- **CTAM Modules**: StormCast core components lack tests
- **Ingest Modules**: NWS, Synoptic, MRMS downloader components
- **Utility Modules**: File handling, I/O management
- **Integration Tests**: Cross-module workflows
- **End-to-End Tests**: Full pipeline execution

---

## 1. API Layer Tests (JavaScript)

### 1.1 Utility Functions
**File**: `tests/api/utils/test_validation.js`

Test [`validateResourceType()`](src/EdgeWARN/api/utils/validation.js:10):
- Valid types: 'cell', 'list'
- Invalid types: null, undefined, empty string, random strings

Test [`validateTimestamp()`](src/EdgeWARN/api/utils/validation.js:19):
- Valid: '20231015-143000'
- Invalid: malformed dates, wrong length, non-numeric

Test [`validateCellId()`](src/EdgeWARN/api/utils/validation.js:31):
- Valid: positive integers as string/number
- Invalid: zero, negative, floats, non-numeric strings

**File**: `tests/api/utils/test_fileReader.js`

Test [`isSafeFilename()`](src/EdgeWARN/api/utils/fileReader.js:20):
- Safe: 'stormcells_20231015-143000.json'
- Unsafe: path traversal attempts, non-JSON extensions

Test [`readJsonFileSafe()`](src/EdgeWARN/api/utils/fileReader.js:33):
- Successfully reads valid JSON
- Throws on path traversal attempts
- Returns cached result on second call
- Throws ENOENT for missing files

Test [`readIndexFile()`](src/EdgeWARN/api/utils/fileReader.js:76):
- Reads index files with shorter TTL
- Returns cached result

### 1.2 Route Handlers
**File**: `tests/api/routes/test_health.js`

Test [`GET /health`](src/EdgeWARN/api/routes/health.js:13):
- Returns status 'OK'
- Includes CPU usage percentage
- Includes memory usage in MB
- Handles multiple requests (CPU diff calculation)

**File**: `tests/api/routes/test_data_fetch.js`

Test [`GET /data/fetch`](src/EdgeWARN/api/routes/data/fetch.js:45):
- Valid types: nws, metar, surface
- Invalid type returns 400
- Returns sorted timestamps
- Handles missing directories (ENOENT)
- Sets Cache-Control header

**File**: `tests/api/routes/test_data_download.js`

Test [`GET /data/download`](src/EdgeWARN/api/routes/data/download.js:16):
- Valid requests return JSON data
- Missing type returns 400
- Missing timestamp returns 400
- Invalid timestamp format returns 400
- Missing file returns 404
- Sets Cache-Control header

**File**: `tests/api/routes/test_features_fetch.js`

Test [`GET /features/fetch/resources`](src/EdgeWARN/api/routes/features/fetch.js:18):
- Valid type='list' returns timestamps
- Valid type='cell' returns cell IDs
- Invalid type returns 400
- Missing index file returns empty array

**File**: `tests/api/routes/test_features_download.js`

Test [`GET /features/download/resources`](src/EdgeWARN/api/routes/features/download.js:19):
- Valid cell ID returns cell data
- Valid timestamp returns stormcell list
- Invalid type returns 400
- Invalid timestamp format returns 400
- Invalid cell ID returns 400
- Missing resource returns 404

### 1.3 Server Configuration
**File**: `tests/api/test_config.js`

Test [`parseBaseDir()`](src/EdgeWARN/api/config.js:5):
- Parses --base-dir argument
- Parses --base-dir=value format
- Returns null when not provided

Test [`isDebugServer()`](src/EdgeWARN/api/config.js:19):
- Detects --debug_server flag

Test config resolution priority:
- CLI arg > environment variable > auto-detect

**File**: `tests/api/test_server.js`

Test middleware setup:
- Helmet security headers
- CORS configuration
- Rate limiting
- Compression
- JSON parsing

Test route mounting:
- /features mounted
- /data mounted
- /health mounted

---

## 2. Core Ingest Module Tests

### 2.1 NWS Ingest
**File**: `tests/core/ingest/nws/test_geomapper.py`

Test [`ZoneLookup.get_polygon()`](src/EdgeWARN/core/ingest/nws/geomapper.py:33):
- Returns polygon for valid zone code
- Returns None for invalid zone code
- Caches state data after first load

Test [`extract_exterior_polygon()`](src/EdgeWARN/core/ingest/nws/geomapper.py:66):
- Unions multiple polygons
- Returns exterior coordinates only
- Handles empty input

Test [`process_warning()`](src/EdgeWARN/core/ingest/nws/geomapper.py:99):
- Maps geocodes to polygons
- Cleans junk keys from properties
- Handles missing zones gracefully

**File**: `tests/core/ingest/nws/test_main.py`

Test [`download_alerts()`](src/EdgeWARN/core/ingest/nws/main.py:41):
- Downloads and filters alerts
- Applies GeoMapper processing
- Saves to correct filename format
- Handles network errors
- Cleans old files

Test [`download_alerts_async()`](src/EdgeWARN/core/ingest/nws/main.py:90):
- Async version works correctly
- Uses aiohttp for requests

### 2.2 Synoptic Ingest
**File**: `tests/core/ingest/synoptic/test_main.py`

Test [`download_rap()`](src/EdgeWARN/core/ingest/synoptic/main.py:13):
- Handles sync and async contexts
- Enforces file limit
- Returns file path on success

**File**: `tests/core/ingest/synoptic/test_downloader.py`

Test download logic:
- Constructs correct URLs
- Handles HTTP errors
- Validates downloaded files

### 2.3 MRMS Ingest
**File**: `tests/core/ingest/mrms/test_downloader.py`

Test S3 operations:
- Lists files in bucket
- Downloads files correctly
- Handles credentials

**File**: `tests/core/ingest/mrms/test_https_client.py`

Test HTTPS operations:
- Downloads from MRMS HTTPS endpoint
- Handles redirects
- Validates checksums

**File**: `tests/core/ingest/mrms/test_utils.py`

Test utility functions:
- File path construction
- Timestamp extraction from filenames

### 2.4 METAR Ingest (Additional Tests)
**File**: `tests/core/ingest/test_metar_additional.py`

Test [`_load_station_database()`](src/EdgeWARN/core/ingest/metar.py:20):
- Loads from cache if available
- Downloads from API if cache missing
- Handles network failures

Test [`download_metar()`](src/EdgeWARN/core/ingest/metar.py:50):
- Downloads and parses METAR data
- Associates stations with coordinates
- Saves to correct directory

---

## 3. Core Process Module Tests

### 3.1 Detect Module
**File**: `tests/core/process/detect/test_gatemapper.py`

Test [`GateMapper.map_gates_to_polygons()`](src/EdgeWARN/core/process/detect/tools/gatemapper.py:18):
- Rasterizes ProbSevere polygons correctly
- Handles negative longitudes
- Returns empty grid for missing data

Test [`GateMapper.expand_gates()`](src/EdgeWARN/core/process/detect/tools/gatemapper.py:81):
- Expands high reflectivity areas
- Respects polygon boundaries
- Handles edge cases

Test [`GateMapper.draw_bbox()`](src/EdgeWARN/core/process/detect/tools/gatemapper.py:100):
- Creates bounding boxes for cells
- Samples points correctly

**File**: `tests/core/process/detect/test_utils.py`

Test [`DetectionDataHandler`](src/EdgeWARN/core/process/detect/tools/utils.py:19):
- Loads radar data with subsetting
- Loads ProbSevere GeoJSON with filtering
- Loads precipitation type data

### 3.2 Integrate Module
**File**: `tests/core/process/integrate/test_utils.py`

Test [`RAPFileHandler.get_isobaric_dataset()`](src/EdgeWARN/core/process/integrate/utils.py:25):
- Finds isobaric dataset with u/v components
- Handles alternative variable names
- Returns None on failure

Test [`StormIntegrationUtils.create_cell_polygon()`](src/EdgeWARN/core/process/integrate/utils.py:100):
- Creates Shapely polygon from cell bbox
- Handles various bbox formats

**File**: `tests/core/process/integrate/test_integrate.py`

Test main integrate workflow:
- Orchestrates GLM and RAP integration
- Handles missing data sources
- Returns updated storm cells

---

## 4. CTAM Module Tests

### 4.1 StormCast Core
**File**: `tests/core/ctam/modules/stormcast/test_blending.py`

Test [`smooth_observed_motion()`](src/EdgeWARN/core/ctam/modules/StormCast/core/blending.py:27):
- Exponential smoothing with alpha parameter
- Mean smoothing method
- Raises ValueError for empty history

Test [`blend_motion()`](src/EdgeWARN/core/ctam/modules/StormCast/core/blending.py:50):
- Blends observed and environmental motion
- Adjusts weights for storm maturity

**File**: `tests/core/ctam/modules/stormcast/test_kalman.py`

Test [`StormKalmanFilter`](src/EdgeWARN/core/ctam/modules/StormCast/core/kalman.py:14):
- Initialize with default state
- Initialize with custom state
- Predict step updates state
- Update step corrects with observation

**File**: `tests/core/ctam/modules/stormcast/test_forecast.py`

Test [`forecast_position()`](src/EdgeWARN/core/ctam/modules/StormCast/core/forecast.py:17):
- Linear advection calculation
- Correct position after dt seconds

Test [`generate_forecast_track()`](src/EdgeWARN/core/ctam/modules/StormCast/core/forecast.py:39):
- Generates positions at multiple lead times
- Default lead times [900, 1800, 2700, 3600]
- Custom lead times

Test [`forecast_with_uncertainty()`](src/EdgeWARN/core/ctam/modules/StormCast/core/forecast.py:60):
- Includes uncertainty bounds
- Generates forecast cone

**File**: `tests/core/ctam/modules/stormcast/test_diagnostics.py`

Test [`compute_storm_core_height()`](src/EdgeWARN/core/ctam/modules/StormCast/core/diagnostics.py:47):
- Calculates core height from echo tops

Test [`compute_adaptive_steering()`](src/EdgeWARN/core/ctam/modules/StormCast/core/diagnostics.py:60):
- Computes height-weighted steering flow

Test [`compute_effective_shear()`](src/EdgeWARN/core/ctam/modules/StormCast/core/diagnostics.py:75):
- Calculates bulk wind shear

Test [`compute_bunkers_motion()`](src/EdgeWARN/core/ctam/modules/StormCast/core/diagnostics.py:90):
- Computes Bunkers storm motion vector

**File**: `tests/core/ctam/modules/stormcast/test_uncertainty.py`

Test [`compute_tracking_uncertainty()`](src/EdgeWARN/core/ctam/modules/StormCast/core/uncertainty.py:13):
- Decreases with more samples
- Applies minimum floor
- Includes jitter component

Test [`compute_velocity_covariance()`](src/EdgeWARN/core/ctam/modules/StormCast/core/uncertainty.py:30):
- Returns 2x2 covariance matrix

**File**: `tests/core/ctam/modules/stormcast/test_core.py`

Test [`StormCastEngine`](src/EdgeWARN/core/ctam/modules/StormCast/core/core.py:55):
- Initialize with reference coordinates
- Set environment profile
- Add observations
- Generate forecast with uncertainty cone

### 4.2 CTAM Infrastructure
**File**: `tests/core/ctam/test_interface.py`

Test [`AnalysisModule`](src/EdgeWARN/core/ctam/interface.py:4) abstract class:
- Cannot instantiate directly
- Subclasses must implement name and run

**File**: `tests/core/ctam/test_registry.py`

Test [`ModuleRegistry`](src/EdgeWARN/core/ctam/registry.py:12):
- Register modules
- Get module by name
- Get all modules
- List names
- Clear registry

---

## 5. Utility Module Tests

**File**: `tests/util/test_io.py`

Test [`TimestampedOutput`](src/util/io.py:5):
- Adds timestamps to messages
- Skips empty lines

Test [`IOManager`](src/util/io.py:31):
- write_info, write_debug, write_warning, write_error
- get_args parses CLI arguments
- Validates lat/lon limits
- Converts longitude to 0-360 range

**File**: `tests/util/test_handler.py`

Test [`extract_timestamp()`](src/util/handler.py:13):
- MRMS format: YYYYMMDD_HHMMSS
- GOES format: sYYYYDDDHHMMSST
- Returns None for unknown formats

Test [`FileHandler.load_dataset()`](src/util/handler.py:40):
- Loads GRIB2 files with cfgrib
- Loads NetCDF files with netcdf4
- Loads JSON files
- Returns None on failure

Test [`FileHandler.subset_dataset()`](src/util/handler.py:93):
- Subsets by lat/lon limits
- Handles coordinate naming variations

**File**: `tests/util/test_file.py`

Test [`_define_paths()`](src/util/file.py:22):
- Creates all directory paths
- Uses correct base directory

Test [`initialize_filesystem()`](src/util/file.py:61):
- Sets custom base directory
- Defaults to platform-specific location

Test [`latest_files()`](src/util/file.py:84):
- Returns n most recent files
- Excludes .idx files
- Returns empty list if directory missing

Test [`clean_files_by_age()`](src/util/file.py:100):
- Removes files older than max_age
- Preserves newer files

Test [`clean_old_files()`](src/util/file.py:120):
- Enforces maximum file count
- Removes oldest files first

---

## 6. Integration Tests

**File**: `tests/integration/test_detect_to_integrate.py`

Test detection to integration workflow:
- Detect cells from radar data
- Integrate GLM flash data
- Integrate RAP wind data
- Verify final cell properties

**File**: `tests/integration/test_ingest_to_detect.py`

Test ingest to detection workflow:
- Download MRMS radar data
- Download ProbSevere data
- Run detection algorithm
- Verify cell detection

**File**: `tests/integration/test_ctam_pipeline.py`

Test CTAM analysis pipeline:
- Initialize storm cells
- Run analysis modules
- Verify module outputs in cell data

**File**: `tests/integration/test_api_to_backend.py`

Test API to backend integration:
- API requests trigger data fetching
- Index files updated correctly
- Cell history accessible via API

---

## 7. End-to-End Tests

**File**: `tests/e2e/test_full_pipeline.py`

Test complete processing pipeline:
1. Ingest MRMS, NWS, METAR data
2. Detect storm cells
3. Track cells over time
4. Integrate environmental data
5. Run CTAM analysis
6. Verify API can serve results

**File**: `tests/e2e/test_api_server.py`

Test API server end-to-end:
1. Start server with test data
2. Query health endpoint
3. Fetch available resources
4. Download cell data
5. Verify response formats

---

## Test Infrastructure

### Dependencies to Add

**package.json** (JavaScript tests):
```json
{
  "devDependencies": {
    "jest": "^29.7.0",
    "supertest": "^6.3.3",
    "@jest/globals": "^29.7.0"
  }
}
```

**environment.yml** (Python tests - likely already have pytest):
```yaml
- pytest-asyncio>=0.21.0
- pytest-mock>=3.11.0
- pytest-cov>=4.1.0
- httpx>=0.25.0  # For async HTTP tests
```

### Test Configuration

**jest.config.js**:
```javascript
export default {
  testEnvironment: 'node',
  coverageDirectory: 'coverage',
  collectCoverageFrom: [
    'src/EdgeWARN/api/**/*.js',
    '!src/EdgeWARN/api/**/*.test.js'
  ],
  testMatch: ['**/tests/api/**/*.test.js'],
  transform: {},
  moduleNameMapper: {
    '^(\\.{1,2}/.*)\\.js$': '$1'
  }
};
```

**pytest.ini**:
```ini
[pytest]
testpaths = tests
python_files = test_*.py
python_classes = Test*
python_functions = test_*
addopts = -v --tb=short --strict-markers
markers =
    unit: Unit tests
    integration: Integration tests
    e2e: End-to-end tests
    slow: Slow tests
```

### Directory Structure

```
tests/
├── conftest.py                    # Shared fixtures
├── unit/                          # Unit tests
│   ├── api/                       # JavaScript API tests
│   │   ├── utils/
│   │   └── routes/
│   ├── core/
│   │   ├── api_integration/
│   │   ├── ctam/
│   │   ├── ingest/
│   │   ├── process/
│   │   └── schedule/
│   └── util/
├── integration/                   # Integration tests
│   ├── test_detect_to_integrate.py
│   ├── test_ingest_to_detect.py
│   ├── test_ctam_pipeline.py
│   └── test_api_to_backend.py
└── e2e/                          # End-to-end tests
    ├── test_full_pipeline.py
    └── test_api_server.py
```

---

## Implementation Priority

### Phase 1: Critical Unit Tests
1. API validation utilities
2. API route handlers
3. File reading utilities
4. CTAM StormCast core functions

### Phase 2: Module Unit Tests
1. NWS ingest (geomapper, main)
2. Synoptic ingest
3. MRMS ingest utilities
4. Process detect utilities
5. Process integrate utilities

### Phase 3: Infrastructure Tests
1. Utility modules (io, handler, file)
2. CTAM registry and interface
3. Scheduler components

### Phase 4: Integration & E2E
1. Cross-module integration tests
2. Full pipeline end-to-end tests
3. API server end-to-end tests

---

## Success Criteria

- **Unit Test Coverage**: 80%+ for all new code
- **Integration Tests**: All major workflows covered
- **E2E Tests**: Critical user journeys tested
- **Test Execution**: All tests pass in CI/CD
- **Documentation**: Each test file has module docstring

---

## Notes

- Use mocking for external dependencies (S3, HTTP APIs, filesystem)
- Use temporary directories for file operations
- Use pytest fixtures for shared test data
- Use parameterized tests for multiple similar cases
- Keep tests independent and idempotent
