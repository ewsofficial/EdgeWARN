# Process Detection Module Documentation

## Overview
The Process Detection module is responsible for identifying and tracking storm cells using radar data (MRMS Composite Reflectivity), ProbSevere data, and Precipitation Type data. It detects cells in the current scan, matches them with cells from the previous scan to maintain continuity, and calculates storm motion vectors.

## Core Components

### 1. `detect.py`
This module handles the core logic for detecting storm cells from a single time step.

#### Functions:

- **`detect_cells(radar_path, ps_path, preciptype_path, io_manager, lat_min, lat_max, lon_min, lon_max)`**
    - **Functionality**: Orchestrates the detection process for a single timeframe.
    - **Robustness**: If `preciptype_path` is invalid or loading fails, the pipeline will log a warning and continue. In this case, the `hail_core` bounding box list for detected cells will be empty, but cell detection and tracking will otherwise function normally.
    - **Steps**:
        1.  Initializes a `DetectionDataHandler` to load and subset radar, ProbSevere, and Precipitation Type data.
        2.  Loads the data subsets.
        3.  Initializes a `GateMapper` to identify storm cells based on reflectivity thresholds.
        4.  Maps radar gates to polygons (`map_gates_to_polygons`).
        5.  Expands the identified gates (`expand_gates`).
        6.  Draws bounding boxes around the cells (`draw_bbox`).
        7.  Initializes a `CellDataSaver` to format the detected cell data.
        8.  Creates initial cell entries and appends storm history.
    - **Returns**: A list of dictionaries, where each dictionary represents a detected storm cell with its properties and history.

### 2. `track.py`
This module manages the tracking of storm cells across consecutive time steps.

#### Classes:

- **`StormCellTracker`**
    - **`__init__(self, ps_old, ps_new, io_manager)`**: Initializes the tracker with old and new ProbSevere data.
    - **`update_cells(self, entries, updated_data, timestamp=None)`**:
        - **Functionality**: Updates the list of storm cells based on new detection data.
        - **Parameters**:
            - `entries`: List of existing storm cells from the previous scan
            - `updated_data`: List of newly detected cells from the current scan
            - `timestamp`: Optional ISO-format timestamp string for the current scan
        - **Logic**:
            - Maps `updated_data` (new detections) by cell ID for O(1) lookup.
            - Iterates through existing `entries` (previous cells):
                - **Matched cells**: If a cell ID exists in the new data, it updates the cell's main fields (`num_gates`, `centroid`, `max_refl`, `bbox`) while preserving its `storm_history`. If `timestamp` is provided, assigns it to the cell (marking it as active).
                - **Unmatched cells**: If a cell ID is missing in the new data, it is **not** updated and **removed** from tracking (not returned in the output list).
            - Adds any **new** cells found in `updated_data` that were not in `entries`. If `timestamp` is provided, it is assigned to new cells as well.
        - **Timestamp Behavior**:
            - **Matched cells**: Receive the current `timestamp`, indicating they are active/current.
            - **New cells**: Receive the current `timestamp` upon creation.
            - **Unmatched cells**: Do NOT receive a timestamp update and are removed from tracking.
            - This timestamp-based approach enables downstream processes (like history tracking) to identify which cells were active in the current scan by checking for the presence of the `timestamp` field.
        - **Returns**: A filtered and updated list of storm cell dictionaries containing only matched and new cells.

### 3. `main.py`
This is the entry point for the detection pipeline. It coordinates the loading of data, detection, tracking, and saving of results.

#### Functions:

- **`main(radar_old, radar_new, ps_old, ps_new, pt_old, pt_new, lat_bounds, lon_bounds, json_output)`**
    - **Functionality**: Runs the full detection and tracking workflow.
    - **Logic**:
        1.  **Single-frame Fallback**: If new data is missing, it defaults to single-frame detection using only the old data.
        2.  **Load Previous Data**: Attempts to load existing cell data from `json_output`. If it fails or doesn't exist, it runs `detect_cells` on the old data to establish a baseline.
        3.  **Extract Exact Timestamp**: Uses `DetectionDataHandler.find_timestamp()` to extract the exact timestamp (including seconds) from the composite reflectivity file metadata.
        4.  **Single-frame Mode**: If running in single-frame mode, applies the extracted timestamp to all cells and saves the stormcell list to `json_output`.
        5.  **Dual-frame Mode**:
            - Runs `detect_cells` on the **new** data.
            - Loads ProbSevere data for both old and new timestamps.
            - Uses `StormCellTracker` to update the old entries with new detections.
            - Appends the new storm history using `CellDataSaver` with the exact timestamp.
            - Calculates storm motion vectors using `StormVectorCalculator`.
            - Saves the stormcell list to `json_output` (filename includes exact timestamp in YYYYMMDD-HHMMSS format).

## Tools (`tools/`)
The `tools` directory contains helper classes and functions used by the core detection logic.

-   **`utils.py`**: Contains `DetectionDataHandler` for loading and subsetting xarray datasets (Radar, ProbSevere, PrecipType).
-   **`gatemapper.py`**: Contains `GateMapper` for identifying storm cells from radar reflectivity.
    - **Method**: Uses a **Watershed algorithm** (via `skimage`) instead of simple Voronoi expansion.
    - **Connectivity**: Enforces strict spatial connectivity by using a high-reflectivity mask ($\ge$ 37.5 dBZ). This prevents "gap jumping," where a cell might incorrectly claim a distant, unconnected storm.
    - **Mergers**: Uses a negative Euclidean Distance Transform (EDT) as an "elevation" map. This ensures that when two cells merge, the boundary is drawn naturally along the "ridge" between their respective intensity cores.
-   **`morphology.py`**: Contains `MorphologyEngine` for computing geometric features of storm cells.
    - **Solidity**: Contour Area / Convex Hull Area (lower = more non-convex/bowed)
    - **Aspect Ratio**: MinAreaRect width/height (higher = more linear)
    - **Convexity Defects**: Depth and bearing of the deepest "notch" in the contour
    - **Skeletonization**: `linearity` metric (skeleton length / complexity) and `branching_factor` (junctions)
-   **`save.py`**: Contains `CellDataSaver` for structuring the detected cell data into a standardized dictionary format and managing the `storm_history` list.
-   **`vecmath.py`**: Contains `StormVectorCalculator` for computing motion vectors (speed and bearing) based on centroid displacement over time.

## Usage Examples

### Running the Detection Pipeline
The `main.py` script can be run directly to execute the pipeline. It automatically finds the latest files and runs the detection.

```python
from EdgeWARN.core.process.detect.main import main
from pathlib import Path
import util.file as fs

# Define bounds
lat_bounds = (36, 46)
lon_bounds = (277, 297)
output_file = Path("storm_cells.json")

# Get file paths (example using util.file helper)
radar_files = fs.latest_files(fs.MRMS_COMPOSITE_DIR, 2)
ps_files = fs.latest_files(fs.MRMS_PROBSEVERE_DIR, 2)
pt_files = fs.latest_files(fs.MRMS_PRECIPTYP_DIR, 2)

radar_old, radar_new = radar_files[-2], radar_files[-1]
ps_old, ps_new = ps_files[-2], ps_files[-1]
pt_old, pt_new = pt_files[-2], pt_files[-1]

# Run pipeline
main(
    radar_old, radar_new,
    ps_old, ps_new,
    pt_old, pt_new,
    lat_bounds, lon_bounds,
    output_file
)
```
