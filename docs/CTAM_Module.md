# CTAM Module Documentation

## Overview
The Cellular Tracking Analysis Module (CTAM) is a modular framework for running analysis modules on storm cell data. It provides a plugin-based architecture where modules can be registered and automatically executed on each storm cell.

## Core Components

### 1. `run.py`
The main entry point for CTAM processing.

#### Functions:

- **`run_ctam(cells)`**
    - **Functionality**: Runs all registered CTAM modules on the provided storm cells.
    - **Parameters**:
        - `cells`: List of storm cell dictionaries, each with a 'properties' key.
    - **Steps**:
        1. Discovers all registered modules from the `ModuleRegistry`.
        2. Initializes the `modules` dict in each cell.
        3. Runs each module on each cell, catching and storing any errors.
        4. Logs progress with timing information.
    - **Returns**: The same list of cells with `modules` populated by each registered module.
    - **Console Output**: Prints debug information including:
        - `[CTAM] Starting CTAM pipeline...`
        - `[CTAM] Discovered N registered module(s): [names]`
        - `[CTAM] Processing N storm cell(s)...`
        - Per-cell module timing
        - `[CTAM] Pipeline complete: X success, Y error(s) in Z.XXXs`

### 2. `registry.py`
Central registry for CTAM analysis modules.

#### Classes:

- **`ModuleRegistry`**
    - **`register(module)`**: Register an analysis module instance.
    - **`get(name)`**: Get a specific module by name.
    - **`get_all()`**: Get all registered modules as a dict.
    - **`list_names()`**: List all registered module names.
    - **`clear()`**: Clear all registered modules (for testing).

### 3. `interface.py`
Defines the abstract base class for analysis modules.

#### Classes:

- **`AnalysisModule`** (Abstract Base Class)
    - **`name`** (property): Returns the module's unique name.
    - **`run(storm_entry, environment=None)`**: Runs analysis on a single storm entry.

### 4. `engine.py`
Provides utility functions for module initialization.

#### Functions:

- **`initialize_modules(cell, module_names)`**: Initializes the `modules` dict in a cell with empty entries for each module.

## Registered Modules

### StormCast
A storm motion forecasting module that predicts storm cell trajectories.

**Location**: `modules/StormCast/`

**Input Requirements** (from storm_entry):
- `dx`, `dy`, `dt` (top-level): Displacement since last observation
- `properties.EchoTop30`, `properties.EchoTop50`: Echo top heights
- `properties.u850`, `properties.v850`, etc.: Wind components at pressure levels

**Output** (stored in `storm_entry['modules']['StormCast']`):
- `status`: "success", "skipped", or "error"
- `u`, `v`: Predicted motion components (m/s) if successful
- `forecast_cones`: List of forecast cone dictionaries
- `reason` or `error`: Explanation if skipped or failed

## Data Structure

After CTAM processing, each storm cell has a `modules` key:

```json
{
    "id": 151282,
    "timestamp": "2026-01-10T16:58:41",
    "centroid": [33.45, 275.82],
    "dx": 1500.0,
    "dy": -800.0,
    "dt": 120.0,
    "properties": { ... },
    "modules": {
        "StormCast": {
            "status": "success",
            "u": 12.5,
            "v": -6.7,
            "forecast_cones": [ ... ]
        }
    }
}
```

## Usage Examples

### Running CTAM in the Pipeline
CTAM is automatically called in the integration pipeline:

```python
from EdgeWARN.core.ctam import run_ctam

# cells is a list of storm cell dictionaries
cells = run_ctam(cells)
```

### Creating a Custom Module
To create a custom CTAM module:

```python
from EdgeWARN.core.ctam.interface import AnalysisModule
from EdgeWARN.core.ctam.registry import ModuleRegistry

class MyModule(AnalysisModule):
    @property
    def name(self) -> str:
        return "MyCustomModule"
    
    def run(self, storm_entry, environment=None):
        props = storm_entry.get("properties", {})
        
        # Your analysis logic here
        result = perform_analysis(props)
        
        # Store results
        storm_entry["modules"][self.name] = {
            "status": "success",
            "result": result
        }

# Register the module (typically done at module import)
ModuleRegistry.register(MyModule())
```

## Module Status Values

Each module stores a `status` key indicating the result:

| Status    | Description                                              |
|-----------|----------------------------------------------------------|
| `success` | Module completed successfully                            |
| `skipped` | Module skipped due to missing data (see `reason` key)    |
| `error`   | Module encountered an error (see `error` key)            |
