# Ingest Module Documentation

The **Ingest Module** (`src/EdgeWARN/core/ingest`) is responsible for downloading meteorological data from various sources. It is divided into several sub-modules:
- **MRMS** (`src/EdgeWARN/core/ingest/mrms`): Handles radar, ProbSevere, and GOES data.
- **Synoptic** (`src/EdgeWARN/core/ingest/synoptic`): Handles larger-scale atmospheric data (RAP).
- **METAR** (`src/EdgeWARN/core/ingest/metar.py`): Handles surface observation data (METAR).
- **NWS** (`src/EdgeWARN/core/ingest/nws`): Handles NWS Watch/Warning/Advisory data.

## Overview

The module provides both synchronous and asynchronous interfaces for downloading data. It is designed to be robust, supporting strict timestamp matching with fallback mechanisms to ensure data availability.

### Key Features

*   **Dual-Mode Operation**: Supports both `asyncio` (for high performance) and synchronous (fallback/legacy) operations.
*   **Strict Timestamp Matching**: Attempts to download files matching a specific minute-precision timestamp to ensure data consistency across products.
*   **Fallback Mechanism**: If a file with the exact timestamp is not found, it automatically falls back to the latest available file within the search window.
*   **Multi-File Merging**: (For NetCDF/GLM) Capable of downloading a sequence of files (target + previous) and merging them into a single dataset.
*   **Thread Offloading**: Heavy synchronous operations (like GLM merging or NWS GeoJSON processing) are offloaded to a thread pool via `run_in_executor`.
*   **Automatic Decompression**: Handles `.gz` compression automatically.

## Core Components

#### MRMS & GOES (`src/EdgeWARN/core/ingest/mrms/main.py`)
*   `download_all_files(dt, remove_old_files=True)`: Main entry point used by the scheduler. Orchestrates downloads for all configured MRMS and GOES products for the given datetime `dt`.
*   `download_goes_product(...)`: Helper for downloading a single GOES product synchronously.

#### Synoptic (`src/EdgeWARN/core/ingest/synoptic/main.py`)
*   `download_rap(dt)`: Entry point for downloading RAP (Rapid Refresh) data for a specific timestamp.
*   Uses `aioboto3` for async S3 downloads from `noaa-rap-pds`.

#### METAR (`src/EdgeWARN/core/ingest/metar.py`)
*   `ingest_metars()`: Synchronous entry point.
*   `ingest_metars_async()`: Asynchronous entry point.
*   **Functionality**:
    - Fetches the current and previous 2 hours of METAR cycle files (`HHZ.TXT`) from the NWS TG FTP server.
    - Parses raw METAR strings into structured JSON objects using regex.
    - Resolves station coordinates using a cached station database.
    - Saves parsed data to `METAR_YYYYMMDD-HHz.json`.

#### NWS Alerts (`src/EdgeWARN/core/ingest/nws/main.py`)
*   `download_alerts(dt)`: Synchronous entry point.
*   `download_alerts_async(dt)`: Asynchronous entry point.
*   **Functionality**:
    - Fetches active alerts from `https://api.weather.gov/alerts/active`.
    - Streams and filters the GeoJSON response using `ijson` to minimize memory usage.
    - Applies the **GeoMapper** (`geomapper.py`) to map NWS zone codes (UGC) to actual polygons.
    - Saves the processed GeoJSON to `alerts_active_YYYYMMDD-HHMMSS.json`.

### 2. Configuration (`config.py`)

Defines the S3 buckets and product modifiers.

*   `mrms_modifiers`: List of MRMS products to download (Region, Product, Output Directory).
*   `goes_modifiers`: List of GOES products to download (Currently GLM-L2-LCFA).
*   `RAP_BUCKET`: `noaa-rap-pds`.

### 3. Downloader Logic (`downloader.py`)

Orchestrates the download process for specific modifiers.

*   `download_modifier_async`: Async function to handle lookup and download for a single product.
*   `download_modifier_sync`: Synchronous equivalent.

### 4. S3 Interaction (`s3_async.py` & `s3_sync.py`)

Low-level classes for interacting with S3.

*   **FileFinder**: Searches for files in S3 buckets based on timestamp and lookback windows.
*   **FileDownloader**: Handles the actual file download, with automatic fallback to the latest file if the exact timestamp is missing.

### 5. Merging (`utils.py`)

Handles merging of NetCDF files, specifically for GLM (Geostationary Lightning Mapper) data from GOES-19.

#### Functions:

*   **`merge_glm_files(file_list, io_manager)`**: Merges multiple GLM L2 LCFA NetCDF files into a single consolidated file.
    *   **Workflow**:
        1.  **Load Files**: Opens all GLM NetCDF files in `file_list`.
        2.  **Concatenate**: Combines the datasets along the `number_of_flashes` dimension.
        3.  **Save**: Writes the consolidated dataset to `GOES_GLM_DIR`.
        4.  **Cleanup**: Deletes all original (unmerged) GLM files.

## Usage

### Basic Usage

```python
from datetime import datetime, timezone
from EdgeWARN.core.ingest.mrms.main import download_all_files
from EdgeWARN.core.ingest.metar import ingest_metars
from EdgeWARN.core.ingest.nws.main import download_alerts

# Download all data for the current time
now = datetime.now(timezone.utc)

# MRMS/GOES
download_all_files(now)

# METAR
ingest_metars()

# NWS Alerts
download_alerts(now)
```

## Data Flow

1.  **Scheduler** (in `run.py`) triggers ingestion for a specific timestamp `dt`.
2.  **Async Wrappers** attempt to download data concurrently:
    - **MRMS/GOES**: S3 lookup and download.
    - **RAP**: S3 lookup and download.
    - **NWS**: HTTP fetch from API + GeoJSON processing.
    - **METAR**: HTTP fetch from NWS FTP + Text parsing.
3.  **Fallback**: If async methods fail, synchronous fallbacks are executed to ensure data delivery.
4.  **Post-Processing**:
    - GLM files are merged.
    - NWS alerts are mapped to polygons.
    - METARs are parsed to JSON.