# Process Integration Module Documentation

## Overview
The Process Integration module is responsible for enriching detected storm cells with additional meteorological data. It takes the storm cells identified by the detection module and integrates data from various MRMS products (e.g., Echo Tops, VIL, Precip Rate) and ProbSevere updates.

## Core Components

### 1. `integrate.py`
This module contains the `StormCellIntegrator` class, which performs the heavy lifting of data extraction and integration.

#### Classes:

- **`StormCellIntegrator`**
    - **`__init__(self, io_manager)`**: Initializes the integrator with an IO manager for logging.
    - **`integrate_ds_via_max(self, dataset_path, storm_cells, output_key)`**:
        - **Functionality**: Integrates a gridded dataset (NetCDF/GRIB2) into the storm cells by finding the maximum value within each cell's polygon.
        - **Steps**:
            1.  **Load Dataset**: Loads the entire dataset into memory using xarray. Handles `.grib2` and standard NetCDF formats.
            2.  **Validate**: Checks if the dataset is empty or missing variables.
            3.  **Coordinate Extraction**: Extracts latitude and longitude arrays (handles both 1D and 2D coordinates).
            4.  **Process Cells**: Iterates through each storm cell:
                - Creates a polygon for the cell using `StormIntegrationUtils.create_cell_polygon`.
                - Creates a boolean mask to select data points within the cell's bounding box.
                - Extracts the subset of data values falling within the mask.
                - Finds the maximum value (`np.nanmax`) in the subset.
                - Updates the latest entry in the cell's `storm_history` with this maximum value under `output_key`.
        - **Error Handling**: Catches memory errors and general exceptions, logging them and marking the cell data as error codes (e.g., "MEMORY_ERROR", "PROCESSING_ERROR").
        - **Returns**: The updated list of storm cells.

    - **`integrate_probsevere(self, probsevere_data, storm_cells)`**:
        - **Functionality**: Merges ProbSevere JSON data into the storm cells based on matching IDs.
        - **Steps**:
            1.  **Index Features**: Creates a lookup dictionary of ProbSevere features keyed by their ID for O(1) access.
            2.  **Map Fields**: Defines a mapping from ProbSevere property names to the internal keys used in `storm_history`.
            3.  **Process Cells**: Iterates through each storm cell:
                - Looks up the cell ID in the ProbSevere feature map.
                - If a match is found, it iterates through the field map and copies the values from the ProbSevere feature to the cell's latest `storm_history` entry.
        - **Returns**: The updated list of storm cells.

### 2. `integrate_glm.py`
This module handles the integration of Geostationary Lightning Mapper (GLM) data from GOES-19.

#### Functions:

- **`integrate_glm(storm_cells, glm_file_path=None)`**
    - **Functionality**: Performs point-in-polygon checks for GLM lightning flashes.
    - **Output**: Adds `GLM_FLASH_COUNT` and `GLM_TOTAL_ENERGY` to cell properties.
    - **Returns**: Updated storm cells.
    - **Steps**:
        1. **Load Dataset**: Opens the GLM L2 LCFA NetCDF file from GOES-19.
        2. **Filter Active Cells**: Selects only cells active at the latest timestamp.
        3. **Spatial Query**:
            - Uses a bounding box check for initial filtering (performance optimization).
            - Performs a precise point-in-polygon check for flashes within the cell boundaries using `shapely`.
        4. **Aggregation**:
            - Counts the number of flashes within each cell polygon (`GLM_FLASH_COUNT`).
            - Sums the flash energy values for all flashes in the cell (`GLM_TOTAL_ENERGY`).
        5. **Data Storage**: Appends the computed values to the cell's `properties` dictionary.
    - **Returns**: Updated storm cells with GLM data appended to the `properties` dictionary.

### 3. `integrate_rap.py`
This module integrates RAP (Rapid Refresh) meteorological data, specifically wind components.

#### Functions:

- **`integrate_rap_winds(storm_cells, rap_file_path, io_manager)`**
    - **Functionality**: Extracts U and V wind components at four isobaric levels: 850, 700, 500, and 250mb.
    - **Method**: Uses nearest-neighbor lookup to map wind vectors from the GRIB2 grid to the storm cell centroid.
    - **Output**: Adds keys such as `u850`, `v850`, `u700`, `v700`, etc., to the cell's `properties` dictionary.
    - **Returns**: Updated storm cells.

### 4. `history.py`
This module manages persistent per-cell history tracking, storing each cell's state over time in individual JSON files.

#### Classes:

- **`CellHistoryManager`**
    - **`__init__(self, io_manager)`**: Initializes the history manager and creates the cell history directory (`CELL_DIR`) if it doesn't exist.
    - **`update_cell_histories(self, cells, timestamp=None)`**:
        - **Functionality**: Updates the persistent history file for each active cell in the list.
        - **Per-Cell Files**: Each cell ID gets its own JSON file at `CELL_DIR/{cell_id}.json` containing a list of historical states.
        - **Active Cell Detection**: Only processes cells that have a `timestamp` field (set by the detection/tracking pipeline). Cells without timestamps are considered inactive/unmatched and are **skipped entirely**, preserving their file modification times.
        - **Duplicate Prevention**: Checks if the last entry in the history has the same timestamp as the current entry. If so, skips appending to avoid duplicates.
        - **History Structure**: Each history file is a JSON array where each element is a complete cell state snapshot including all fields (`id`, `timestamp`, `centroid`, `bbox`, `max_refl`, `properties`, etc.).
        - **Timestamp Normalization**: Ensures the `timestamp` field is at the cell's top level and removes it from nested `properties` if present (legacy cleanup).
        - **File Cleanup**: Inactive cell history files (not updated for more than 1 hour) are automatically deleted by a separate cleanup process in the main pipeline.
        - **Returns**: Nothing (updates files on disk).

### 5. `main.py`
This script defines the integration workflow, specifying which datasets to process and running the integration.

#### Functions:

- **`main(stormcells_json, remove_old_cells=True)`**
    - **Functionality**: Orchestrates the integration of multiple datasets into storm cell files.
    - **Parameters**:
        - `stormcells_json`: Path to the stormcell list JSON file
        - `remove_old_cells`: If `True` (default), cleans up old cell history files not updated for over 1 hour
    - **Steps**:
        1.  **Setup**: Initializes `StatFileHandler`, `StormCellIntegrator`, and `CellHistory` manager. Loads the storm cell list from the provided JSON file.
        2.  **Integrate Gridded Data**: Iterates through a predefined list of datasets (NLDN, EchoTop18, EchoTop30, PrecipRate, VIL, RALA, VII).
            - Finds the latest file for each product.
            - Calls `integrator.integrate_ds_via_max` to add the data to the cells.
        3.  **Integrate ProbSevere**: Finds the latest ProbSevere JSON file and calls `integrator.integrate_probsevere`.
        4.  **Update Cell Histories**: Saves enriched data to individual cell history JSON files in `CELL_DIR`.
        5.  **Cleanup**: If `remove_old_cells=True`, removes inactive cell history files (not updated for over 1 hour).

### 5. `utils.py`
Contains utility classes for file handling and geometry operations.

#### Classes:

- **`StatFileHandler`**
    - **`load_file(self, file_path)`**: Loads a dataset using xarray.
    - **`load_json(self, filepath)`**: Loads a JSON file.
    - **`write_json(self, data, filepath)`**: Writes data to a JSON file.
    - **`find_timestamp(self, filepath)`**: robustly extracts a datetime object from a filename using various common meteorological naming patterns (regex).
    - **`convert_lon_to_360` / `convert_lon_to_180`**: Helper methods for longitude conversion.

- **`StormIntegrationUtils`**
    - **`create_coordinate_grids(dataset)`**: Extracts lat/lon coordinates from a dataset and ensures they are 2D grids (using `meshgrid` if necessary).
    - **`create_cell_polygon(cell, min_size=0.0)`**: Converts a cell's bounding box or centroid into a `shapely.geometry.Polygon`. This is crucial for spatial queries.
    - **`create_polygon_mask(polygon, lat_grid, lon_grid)`**: Creates a boolean mask over the coordinate grids corresponding to the polygon's bounding box.

## Usage Examples

### Running the Integration Pipeline
The `main.py` script is designed to be run as a standalone process or imported.

```python
from EdgeWARN.core.process.integrate.main import main
from pathlib import Path

# This will load the stormcell list JSON, integrate all defined datasets,
# and save cell histories to individual JSON files in CELL_DIR.
# Set remove_old_cells=False to prevent cleanup of inactive cells.
if __name__ == "__main__":
    stormcells_file = Path("path/to/stormcells_20251230-150000.json")
    main(stormcells_file, remove_old_cells=True)
```

### Custom Integration Example
You can use the `StormCellIntegrator` to integrate a custom dataset.

```python
from EdgeWARN.core.process.integrate.integrate import StormCellIntegrator
from util.io import IOManager
import json

# Setup
io = IOManager("CustomIntegration")
integrator = StormCellIntegrator(io)

# Load cells
with open("my_cells.json", "r") as f:
    cells = json.load(f)

# Integrate a specific file
dataset_path = "/path/to/my/data.nc"
output_key = "MyVariableMax"
cells = integrator.integrate_ds_via_max(dataset_path, cells, output_key)

# Save
with open("my_cells_enriched.json", "w") as f:
    json.dump(cells, f, indent=2)
```
