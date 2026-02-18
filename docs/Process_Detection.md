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
This module manages the tracking of storm cells across consecutive time steps with Kalman filter support for tracking continuity.

#### Classes:

- **`StormCellTracker`**
    - **`__init__(self, ps_old, ps_new, io_manager, tracking_config=None)`**: Initializes the tracker with old and new ProbSevere data and optional tracking configuration.
    - **`update_cells(self, entries, updated_data, timestamp=None, dt_seconds=120.0)`**:
        - **Functionality**: Updates the list of storm cells based on new detection data with Kalman filter prediction for unmatched cells.
        - **Parameters**:
            - `entries`: List of existing storm cells from the previous scan
            - `updated_data`: List of newly detected cells from the current scan
            - `timestamp`: Optional ISO-format timestamp string for the current scan
            - `dt_seconds`: Time since last scan in seconds (default: 120.0)
        - **Tracking Modes**:
            - **active**: Normal tracking with ProbSevere observations
            - **predicted**: Kalman-only prediction mode (ProbSevere dropped)
            - **terminated**: Storm removed from tracking
        - **Logic**:
            - Maps `updated_data` (new detections) by cell ID for O(1) lookup.
            - Iterates through existing `entries` (previous cells):
                - **Matched cells**: Updates cell's main fields and resets to active mode.
                - **Unmatched cells**: Enters prediction mode using Kalman filter instead of immediate removal.
            - Checks for re-acquisition: New cells within 5km of predicted cells are merged, preserving the original storm ID and history.
            - Terminates predicted cells when confidence drops below threshold or time limit exceeded.
        - **Re-acquisition**:
            - New ProbSevere detections within 5km of a predicted cell are automatically merged.
            - The new cell receives the old cell's ID, preserving storm history.
            - Confidence resets to 1.0 and mode returns to active.

#### Kalman Filter Module (`kalman/`)

The Kalman filter module provides prediction-based tracking continuity when ProbSevere temporarily drops detection.

##### Components:

- **`filter.py`**: `KalmanFilter` class with 6-dimensional state vector (position, velocity, acceleration)
- **`state.py`**: `StateVector` and `CovarianceMatrix` classes for state representation
- **`confidence.py`**: `ConfidenceCalculator` and `PredictionState` for confidence scoring
- **`config.py`**: Configuration classes for Kalman and tracking parameters

##### Configuration (`config/kalman.yaml`):

| Parameter | Default | Description |
|-----------|---------|-------------|
| max_prediction_time_minutes | 10 | Maximum time in prediction mode |
| reacquisition_radius_km | 5.0 | Maximum distance for re-acquisition |
| confidence_threshold | 0.4 | Minimum confidence before termination |
| confidence_decay_factor | 0.7 | Per-scan confidence decay |

##### Usage Example:

```python
from EdgeWARN.core.process.detect.kalman import KalmanFilter, KalmanObservation

# Initialize from storm cell
kf = KalmanFilter()
kf.initialize(lat=33.5, lon=-97.2, u=12.5, v=-6.7)

# Predict forward (2 minutes)
predicted_state = kf.predict(dt=120.0)
print(f"Predicted position: {predicted_state.lat}, {predicted_state.lon}")

# Update with observation
obs = KalmanObservation(lat=33.52, lon=-97.18)
updated_state = kf.update(obs)
```
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
