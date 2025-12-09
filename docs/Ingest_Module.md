# Ingest Module Documentation

The **Ingest Module** (`src/EdgeWARN/core/ingest`) is responsible for downloading meteorological data from NOAA S3 buckets. It supports both MRMS (Multi-Radar Multi-Sensor) and GOES-19 (Geostationary Operational Environmental Satellite) data products.

## Overview

The module provides both synchronous and asynchronous interfaces for downloading data. It is designed to be robust, supporting strict timestamp matching with fallback mechanisms to ensure data availability.

### Key Features

*   **Dual-Mode Operation**: Supports both `asyncio` (for high performance) and synchronous (fallback/legacy) operations.
*   **Strict Timestamp Matching**: Attempts to download files matching a specific minute-precision timestamp to ensure data consistency across products.
*   **Fallback Mechanism**: If a file with the exact timestamp is not found, it automatically falls back to the latest available file within the search window.
*   **Multi-File Merging**: (For NetCDF/GLM) Capable of downloading a sequence of files (target + previous) and merging them into a single dataset.
*   **Automatic Decompression**: Handles `.gz` compression automatically.

## Core Components

### 1. Entry Points (`main.py`)

The primary interface for external calls.

*   `download_all_files(dt)`: Main entry point used by the scheduler. Orchestrates downloads for all configured MRMS and GOES products for the given datetime `dt`. **Also performs cleanup of old files (both MRMS and GOES) older than 60 minutes.**
*   `download_goes_product(...)`: Helper for downloading a single GOES product synchronously.
*   `download_all_goes_files(...)`: Helper for downloading all GOES products.

### 2. Configuration (`config.py`)

Defines the S3 buckets and product modifiers.

*   `bucket`: MRMS S3 bucket (`noaa-mrms-pds`).
*   `goes_bucket`: GOES-19 S3 bucket (`noaa-goes19`).
*   `mrms_modifiers`: List of MRMS products to download (Region, Product, Output Directory).
*   `goes_modifiers`: List of GOES products to download.

### 3. Downloader Logic (`downloader.py`)

Orchestrates the download process for specific modifiers.

*   `download_modifier_async`: Async function to handle lookup and download for a single product.
*   `download_modifier_sync`: Synchronous equivalent.

### 4. S3 Interaction (`s3_async.py` & `s3_sync.py`)

Low-level classes for interacting with S3.

*   **FileFinder**: Searches for files in S3 buckets based on timestamp and lookback windows.
    *   `lookup_files`: Returns a list of `(s3_path, timestamp)` tuples, sorted by time (latest first).
*   **FileDownloader**: Handles the actual file download.
    *   `download_matching(file_list, outdir)`:
        1.  Searches `file_list` for a file matching the target `dt` (minute precision).
        2.  If found, downloads it.
        3.  If **not found**, logs a warning and falls back to the **latest** file in the list.

### 5. Merging (`merge.py`)

Handles merging of NetCDF files (e.g., for GLM data).

*   `merge_netcdf_files(file_paths, output_path)`: Uses `xarray` to merge multiple NetCDF files into a single output file.

## Usage

### Basic Usage

```python
from datetime import datetime, timezone
from EdgeWARN.core.ingest.main import download_all_files

# Download all data for the current time
now = datetime.now(timezone.utc)
download_all_files(now)
```

### Downloading Specific GOES Product

```python
from EdgeWARN.core.ingest.main import download_goes_product
from util.file import GOES_GLM_DIR

download_goes_product(
    product="GLM-L2-LCFA",
    outdir=GOES_GLM_DIR,
    dt=now
)
```

## Data Flow

1.  **Scheduler** calls `download_all_files(dt)`.
2.  **Downloader** initiates async tasks for each configured product (MRMS & GOES).
3.  **FileFinder** generates S3 prefixes based on `dt` and looks up available files.
4.  **FileDownloader** receives the list of files.
    *   It tries to find a file matching `dt`.
    *   If missing, it picks the latest one.
5.  **FileDownloader** downloads the selected file to the output directory.
6.  (Optional) If compression is detected (`.gz`), it is decompressed.
7.  (Optional) For specific products, multiple files may be downloaded and merged via `merge.py`.
