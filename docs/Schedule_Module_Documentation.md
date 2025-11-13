# Schedule Module Documentation

## Overview

The Schedule module (`src/EdgeWARN/core/schedule/`) provides automated coordination and timing control for the EdgeWARN data ingestion and processing workflows. It ensures that data is processed at appropriate intervals and maintains synchronization between different data sources and processing stages.

## Module Structure

```
schedule/
├── __init__.py
└── scheduler.py         # Main scheduling and synchronization logic
```

## Core Functionality

### MRMSUpdateChecker Class (`scheduler.py`)

The `MRMSUpdateChecker` class is the central component for managing data update timing and synchronization across the EdgeWARN system.

#### Constructor

```python
MRMSUpdateChecker(verbose=False, check_interval=60)
```

**Parameters:**
- `verbose` - Enable verbose logging for debugging
- `check_interval` - Time interval between checks in seconds (default: 60)

#### Key Methods

##### `latest_common_minute_1h(modifiers) -> datetime | None`

**Purpose**: Finds the latest timestamp where all MRMS products have data available within a 1-hour window

**Parameters:**
- `modifiers` - List of (modifier_path, output_directory) tuples representing MRMS products to check

**Returns:**
- `datetime` object representing the latest common timestamp, or `None` if no common timestamp found

**Workflow:**
1. **Time Window Setup**: Define 1-hour lookback window from current time
2. **Product Validation**: Check data availability for each MRMS product
3. **Timestamp Matching**: Find timestamps present in all products
4. **Latest Selection**: Return the most recent common timestamp

**Example Usage:**
```python
from EdgeWARN.core.schedule.scheduler import MRMSUpdateChecker

checker = MRMSUpdateChecker(verbose=True)
latest_common = checker.latest_common_minute_1h(check_modifiers)

if latest_common:
    print(f"Latest common timestamp: {latest_common}")
else:
    print("No common timestamp found in last hour")
```

### Scheduling Integration

The Schedule module integrates with other EdgeWARN components to provide:

#### Data Freshness Monitoring
- **Continuous Checking**: Monitors MRMS data availability every 60 seconds
- **Common Timestamps**: Ensures all products are synchronized before processing
- **Data Validation**: Validates data completeness before triggering workflows

#### Workflow Orchestration
- **Trigger Conditions**: Only processes when all required data is available
- **Duplicate Prevention**: Avoids processing same timestamp multiple times
- **Error Recovery**: Handles missing data scenarios gracefully

### Integration Points

#### With Ingest Module
- **Timestamp Coordination**: Shares timestamp information with data ingestion
- **Processing Triggers**: Triggers data downloads when fresh data becomes available
- **Data Validation**: Validates downloaded data before marking as processed

#### With Process Module
- **Processing Scheduling**: Coordinates storm cell detection with data availability
- **Time Synchronization**: Ensures consistent timestamps across detection workflows
- **History Management**: Maintains processing history to avoid duplicates

### Configuration Parameters

#### Update Checking
- **Check Interval**: Time between data availability checks (configurable)
- **Lookback Window**: How far back to search for common timestamps (1 hour default)
- **Timeout Handling**: Graceful handling of slow or missing data sources

#### Validation Parameters
- **Product Count**: Number of MRMS products that must have matching timestamps
- **File Existence**: Validation of actual file availability in directories
- **Timestamp Precision**: Minute-level precision for timestamp matching

## Scheduling Logic

### Timestamp Discovery Algorithm

The `latest_common_minute_1h` function implements a sophisticated timestamp matching algorithm:

```python
def latest_common_minute_1h(modifiers):
    # 1. Define time window (current time - 1 hour)
    current_time = datetime.now(timezone.utc)
    one_hour_ago = current_time - timedelta(hours=1)
    
    # 2. Collect available timestamps for each product
    product_timestamps = {}
    for modifier, outdir in modifiers:
        timestamps = collect_product_timestamps(outdir, one_hour_ago, current_time)
        product_timestamps[modifier] = set(timestamps)
    
    # 3. Find intersection of all timestamp sets
    common_timestamps = set.intersection(*product_timestamps.values())
    
    # 4. Return most recent common timestamp
    if common_timestamps:
        return max(common_timestamps)
    return None
```

### File Timestamp Extraction

The scheduler analyzes file timestamps to determine data availability:

#### Supported File Formats
- **NetCDF (.nc)**: Extracts timestamps from file metadata
- **GRIB2 (.grib2)**: Analyzes timestamp in filename or metadata
- **Compressed Files**: Handles timestamp extraction from compressed archives

#### Timestamp Sources
1. **File Creation Time**: File system creation/modification times
2. **Metadata Timestamps**: Internal data timestamps from files
3. **Filename Patterns**: Timestamps embedded in file naming conventions

## Error Handling

### Data Availability Issues
- **Missing Products**: Handles cases where some MRMS products are unavailable
- **Corrupted Files**: Detects and reports file integrity issues
- **Network Problems**: Manages connectivity issues with data sources

### Timing Synchronization
- **Clock Drift**: Handles minor time synchronization differences
- **Processing Delays**: Manages cases where processing takes longer than expected
- **Concurrent Access**: Handles multiple processes accessing same data

### Recovery Mechanisms
- **Retry Logic**: Automatic retry for temporary failures
- **Fallback Options**: Alternative timestamp sources when primary unavailable
- **Notification**: Alerts for persistent scheduling issues

## Performance Considerations

### Efficiency Optimizations
- **Cached Results**: Avoids re-scanning directory structures unnecessarily
- **Incremental Updates**: Only checks for new timestamps since last scan
- **Parallel Validation**: Concurrent checking of multiple products

### Resource Management
- **Memory Usage**: Efficient timestamp storage and comparison
- **File System Access**: Minimized directory scanning and file operations
- **Network Usage**: Conservative checking to avoid overwhelming servers

### Scalability
- **Product Scaling**: Handles additional MRMS products without performance degradation
- **Time Window Scaling**: Configurable lookback periods for different use cases
- **Concurrent Processing**: Support for multiple simultaneous scheduling operations

## Integration Examples

### Standalone Usage
```python
from EdgeWARN.core.schedule.scheduler import MRMSUpdateChecker
from EdgeWARN.core.ingest.config import check_modifiers

# Initialize checker
checker = MRMSUpdateChecker(verbose=True)

# Find latest common timestamp
latest_common = checker.latest_common_minute_1h(check_modifiers)

if latest_common:
    # Trigger processing workflow
    from EdgeWARN.core.ingest.main import download_all_files
    download_all_files(latest_common)
```

### Continuous Monitoring
```python
import time
from datetime import datetime, timedelta

# Initialize components
checker = MRMSUpdateChecker(verbose=True)
last_processed = None

while True:
    try:
        # Check for new data
        latest_common = checker.latest_common_minute_1h(check_modifiers)
        
        if latest_common and latest_common != last_processed:
            print(f"Processing new data: {latest_common}")
            
            # Trigger processing workflow
            # ... processing logic here ...
            
            last_processed = latest_common
        else:
            print("No new data available")
            
        # Wait before next check
        time.sleep(60)  # Check every minute
        
    except KeyboardInterrupt:
        print("Monitoring stopped by user")
        break
    except Exception as e:
        print(f"Scheduling error: {e}")
        time.sleep(30)  # Wait 30 seconds before retry
```

### Custom Validation
```python
# Custom modifiers for specific products
custom_modifiers = [
    ("2D/MergedReflectivityQCComposite/", MRMS_COMPOSITE_DIR),
    ("ProbSevere/PROBSEVERE/", MRMS_PROBSEVERE_DIR),
    ("2D/PrecipRate/", MRMS_PRECIPRATE_DIR)
]

# Check specific subset of products
checker = MRMSUpdateChecker(verbose=True)
latest_custom = checker.latest_common_minute_1h(custom_modifiers)
```

## Dependencies

- **Time Management**: datetime and timedelta for timestamp handling
- **File System**: Directory scanning and file timestamp extraction
- **Data Validation**: File format recognition and metadata extraction
- **Logging**: Verbose output for debugging and monitoring

## Configuration

### Environment Variables
- **Check Interval**: Override default check frequency
- **Timeout Settings**: Configure network timeout values
- **Debug Level**: Enable detailed logging for troubleshooting

### File Paths
- **Product Directories**: Configuration of MRMS product storage locations
- **Log Files**: Scheduling activity logging and monitoring
- **State Files**: Persistence of scheduling state between runs

## Monitoring and Observability

### Logging Levels
- **INFO**: Standard scheduling operations and timestamp discoveries
- **DEBUG**: Detailed timestamp checking and validation processes
- **WARNING**: Data availability issues and recovery actions
- **ERROR**: Scheduling failures and system errors

### Metrics Collection
- **Timestamp Discovery Rate**: How often new timestamps are found
- **Processing Latency**: Time from data availability to processing trigger
- **Error Frequency**: Rate of scheduling and data availability errors

### Health Checks
- **Data Source Status**: Health of individual MRMS product feeds
- **Clock Synchronization**: Validation of system time accuracy
- **Processing Pipeline**: Status of downstream processing components