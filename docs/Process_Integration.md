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

### 2. `main.py`
This script defines the integration workflow, specifying which datasets to process and running the integration.

#### Functions:

- **`main()`**
    - **Functionality**: Orchestrates the integration of multiple datasets into the storm cell JSON file.
    - **Steps**:
        1.  **Setup**: Initializes `StatFileHandler` and `StormCellIntegrator`. Loads the initial storm cell data from `stormcell_test.json`.
        2.  **Integrate Gridded Data**: Iterates through a predefined list of datasets (NLDN, EchoTop, PrecipRate, VIL, RALA, VII).
            - Finds the latest file for each product.
            - Calls `integrator.integrate_ds_via_max` to add the data to the cells.
        3.  **Integrate ProbSevere**: Finds the latest ProbSevere JSON file and calls `integrator.integrate_probsevere`.
        4.  **Save**: Writes the fully enriched storm cell data back to `stormcell_test.json`.

### 3. `utils.py`
Contains utility classes for file handling and geometry operations.

#### Classes:

- **`StatFileHandler`**
    - **`load_file(self, file_path)`**: Loads a dataset using xarray.
    - **`load_json(self, filepath)`**: Loads a JSON file.
    - **`write_json(self, data, filepath)`**: Writes data to a JSON file.
    - **`find_timestamp(self, filepath)`**: robustly extracts a datetime object from a filename using various common meteorological naming patterns (regex).
    - **`convert_lon_to_360` / `convert_lon_to_180`**: Helper methods for longitude conversion.
    - **`convert_geos_to_latlon(x, y, ...)`**: Converts GOES fixed grid coordinates (scan angles in radians) to latitude/longitude. Supports normalization to 0.01° grid.

- **`StormIntegrationUtils`**
    - **`create_coordinate_grids(dataset)`**: Extracts lat/lon coordinates from a dataset and ensures they are 2D grids (using `meshgrid` if necessary).
    - **`create_cell_polygon(cell, min_size=0.0)`**: Converts a cell's bounding box or centroid into a `shapely.geometry.Polygon`. This is crucial for spatial queries.
    - **`create_polygon_mask(polygon, lat_grid, lon_grid)`**: Creates a boolean mask over the coordinate grids corresponding to the polygon's bounding box.

## Usage Examples

### Running the Integration Pipeline
The `main.py` script is designed to be run as a standalone process or imported.

```python
from EdgeWARN.core.process.integrate.main import main

# This will load 'stormcell_test.json', integrate all defined datasets, 
# and save the result back to the same file.
if __name__ == "__main__":
    main()
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
