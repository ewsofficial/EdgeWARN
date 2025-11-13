# Ingest Module Documentation

## Overview

The Ingest module (`src/EdgeWARN/core/ingest/`) handles the download, management, and preprocessing of MRMS (Multi-Radar Multi-Sensor) meteorological data from NOAA's NCEP servers. It provides a robust data ingestion pipeline with automatic error handling, file cleanup, and concurrent downloading capabilities.

## Module Structure

```
ingest/
├── __init__.py
├── config.py         # Data source configuration and directory mapping
├── download.py       # File finding and downloading utilities
└── main.py           # Main ingestion pipeline and orchestration
```

## Configuration (`config.py`)

### Data Source Configuration

The module is configured to download from NOAA's MRMS servers:

**Base Directory**: `https://mrms.ncep.noaa.gov/`

### MRMS Data Products

The module supports the following MRMS data products:

#### Complete Data Products (`mrms_modifiers`)
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
```

#### Validation Products (`check_modifiers`)
A subset of products used for data freshness validation and timestamp synchronization.

## Core Classes and Functions

### FileFinder Class (`download.py`)

Locates and identifies available MRMS files based on timestamps and modifiers.

#### Constructor
```python
FileFinder(target_time, base_dir, max_time_window, max_entries, io_manager)
```

**Parameters:**
- `target_time` - Target timestamp for file lookup
- `base_dir` - Base URL for MRMS servers
- `max_time_window` - Time window for file search (default: 6 hours)
- `max_entries` - Maximum number of files to check per source
- `io_manager` - Logging and error handling instance

#### Methods
- **`lookup_files(modifier) -> list`**
  - Searches for files matching the target time and modifier
  - Returns list of (file_path, timestamp) tuples
  - Handles time zone considerations and file naming conventions

### FileDownloader Class (`download.py`)

Handles downloading, decompression, and file management.

#### Constructor
```python
FileDownloader(target_time, io_manager)
```

#### Methods
- **`download_latest(files_with_timestamps, outdir) -> str | None`**
  - Downloads the most recent file matching criteria
  - Returns path to downloaded file or None on failure
  - Implements retry logic and error handling

- **`decompress_file(downloaded_file)`**
  - Automatically decompresses downloaded files
  - Supports common compression formats (gzip, etc.)
  - Cleans up compressed files after extraction

## Main Pipeline Functions

### `process_modifier()` Function

Processes a single MRMS modifier type:

```python
def process_modifier(modifier, outdir, dt, max_time, max_entries):
    # 1. Ensure minute precision (ignore seconds)
    dt_minute_precision = dt.replace(second=0, microsecond=0)
    
    # 2. Initialize finder and downloader
    finder = FileFinder(dt_minute_precision, base_dir, max_time, max_entries, io_manager)
    downloader = FileDownloader(dt_minute_precision, io_manager)
    
    # 3. Look up and download files
    files_with_timestamps = finder.lookup_files(modifier)
    if not files_with_timestamps:
        return
    
    # 4. Download and decompress
    downloaded = downloader.download_latest(files_with_timestamps, outdir)
    if downloaded:
        downloader.decompress_file(downloaded)
```

### `download_all_files()` Function

Orchestrates concurrent downloading of all MRMS products:

```python
def download_all_files(dt):
    # 1. Cleanup old files
    folders = [modifier[1] for modifier in mrms_modifiers]
    for f in folders:
        fs.clean_old_files(f, max_age_minutes=20)
    fs.wipe_temp()
    
    # 2. Configure search parameters
    max_time = datetime.timedelta(hours=6)   # 6-hour lookback
    max_entries = 10                         # Max files per source
    
    # 3. Concurrent downloading
    with ThreadPoolExecutor(max_workers=len(mrms_modifiers) + 2) as executor:
        futures = [
            executor.submit(process_modifier, modifier, outdir, dt, max_time, max_entries)
            for modifier, outdir in mrms_modifiers
        ]
        
        for future in as_completed(futures):
            future.result()
```

## Main Execution (`main.py`)

The main execution function demonstrates the complete workflow:

### Standalone Mode
```python
if __name__ == "__main__":
    # 1. Initialize update checker
    checker = MRMSUpdateChecker(verbose=True)
    last_processed = None
    
    # 2. Find latest common timestamp
    latest_common = checker.latest_common_minute_1h(check_modifiers)
    
    if latest_common:
        # 3. Validate all products have data
        latest_common_minute = latest_common.replace(second=0, microsecond=0)
        
        if latest_common_minute != last_processed:
            # Verify all modifiers have files
            all_have_files = True
            for modifier, outdir in check_modifiers:
                dt_minute_precision = latest_common_minute.replace(second=0, microsecond=0)
                finder = FileFinder(dt_minute_precision, base_dir, datetime.timedelta(hours=6), 10, io_manager)
                files = finder.lookup_files(modifier, verbose=False)
                if not files:
                    all_have_files = False
            
            # 4. Download if validated
            if all_have_files:
                dt = latest_common_minute
                download_all_files(dt)
                last_processed = latest_common_minute
```

## Data Flow Architecture

1. **Timestamp Discovery**: Find latest common timestamp across all MRMS products
2. **File Validation**: Verify all required products have data at target timestamp
3. **Concurrent Download**: Download all products simultaneously using ThreadPoolExecutor
4. **File Processing**: Decompress and organize downloaded files
5. **Cleanup**: Remove old files and temporary data
6. **Error Handling**: Comprehensive error handling and logging

## File Management

### Directory Structure
The module uses standardized directory paths from `util.file`:
- **MRMS_COMPOSITE_DIR** - Composite reflectivity data
- **MRMS_ECHOTOP18_DIR** - 18 dBZ echo top heights
- **MRMS_ECHOTOP30_DIR** - 30 dBZ echo top heights
- **MRMS_NLDN_DIR** - Lightning density data
- **MRMS_PRECIPRATE_DIR** - Precipitation rate data
- **MRMS_QPE_DIR** - Quantitative precipitation estimates
- **MRMS_ROTATIONT_DIR** - Rotation track data
- **MRMS_VIL_DIR** - Vertically integrated liquid
- **MRMS_PROBSEVERE_DIR** - Probability severe data
- **MRMS_PRECIPTYP_DIR** - Precipitation type data
- **MRMS_RALA_DIR** - Low-level reflectivity
- **MRMS_VII_DIR** - Vertically integrated ice

### Cleanup Policies
- **Old Files**: Automatically removed after 20 minutes
- **Temporary Files**: Cleaned up after each download cycle
- **Failed Downloads**: Removed to prevent disk space issues

## Error Handling and Logging

The module includes comprehensive error handling:

### File Finding Errors
- Network connectivity issues
- Invalid timestamp formats
- File availability validation
- Time window boundary checks

### Download Errors
- Network timeouts and retries
- Insufficient disk space
- File corruption detection
- Permission issues

### Data Validation Errors
- Incomplete data sets
- Timestamp mismatches
- File format validation
- Decompression failures

## Performance Considerations

### Concurrent Processing
- **Thread Pool**: Configurable worker count (len(mrms_modifiers) + 2)
- **Resource Management**: Proper cleanup of threads and connections
- **Rate Limiting**: Respectful downloading to avoid server overload

### Memory Efficiency
- **Streaming Downloads**: Large files downloaded in chunks
- **Temporary Storage**: Minimal memory footprint during processing
- **Cleanup**: Aggressive cleanup of temporary files

### Network Optimization
- **Connection Reuse**: Persistent connections when possible
- **Compression**: Server-side compression support
- **Selective Downloading**: Only download most recent files

## Integration Points

### Scheduler Integration
- **MRMSUpdateChecker**: Time synchronization with scheduling module
- **Data Freshness**: Validates data availability before processing
- **Timestamp Matching**: Ensures data consistency across products

### File System Integration
- **Path Management**: Uses centralized path utilities from `util.file`
- **Cleanup Policies**: Integrates with file age management
- **Output Organization**: Standardized directory structure

## Usage Examples

### Basic Data Download
```python
from EdgeWARN.core.ingest.main import download_all_files
import datetime

# Download all MRMS products for current time
current_time = datetime.datetime.now(datetime.timezone.utc)
download_all_files(current_time)
```

### Processing Specific Product
```python
from EdgeWARN.core.ingest.download import FileFinder, FileDownloader
from EdgeWARN.core.ingest.config import base_dir
from util.io import IOManager
import datetime

# Download specific product
target_time = datetime.datetime.now(datetime.timezone.utc)
io_manager = IOManager("[CustomIngest]")

finder = FileFinder(target_time, base_dir, datetime.timedelta(hours=6), 10, io_manager)
downloader = FileDownloader(target_time, io_manager)

# Find and download composite reflectivity
modifier = "2D/MergedReflectivityQCComposite/"
files = finder.lookup_files(modifier)
if files:
    downloaded = downloader.download_latest(files, "/path/to/output/")
    if downloaded:
        downloader.decompress_file(downloaded)
```

## Dependencies

- **Network Libraries**: HTTP/HTTPS client for file downloads
- **Compression**: Support for gzip and other compression formats
- **Threading**: concurrent.futures for parallel processing
- **File System**: Path manipulation and file management
- **Time Handling**: datetime for timestamp management

## Configuration Notes

### Server Specifications
- **Base URL**: https://mrms.ncep.noaa.gov/
- **Data Latency**: Typically 2-5 minutes behind real-time
- **Update Frequency**: Varies by product (1-60 minutes)
- **Data Retention**: Files available for 6-24 hours

### Download Parameters
- **Lookback Window**: 6 hours default for file searching
- **Max Entries**: 10 files checked per product source
- **Cleanup Age**: 20 minutes for automatic file removal
- **Worker Threads**: Configurable based on available products