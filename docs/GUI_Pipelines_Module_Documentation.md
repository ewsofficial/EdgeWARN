# GUI Pipelines Module Documentation

## Overview

The GUI Pipelines module (`src/EdgeWARN/core/gui_pipelines/`) provides comprehensive visualization and mapping capabilities for the EdgeWARN system. It handles the generation of interactive maps, data transformation, layer management, and visual output generation.

## Module Structure

```
gui_pipelines/
├── __init__.py
├── basemap/              # Base map generation and configuration
│   ├── __init__.py
│   ├── config.py         # Map configuration parameters
│   ├── coordinate_utils.py # Coordinate system utilities
│   ├── data_loader.py    # Storm cell data loading
│   ├── main.py           # Main basemap generation pipeline
│   ├── map_utils.py      # Map creation and manipulation utilities
│   ├── output_utils.py   # Map output and persistence utilities
│   └── tooltip_utils.py  # Interactive tooltip generation
└── transform/            # Data transformation and rendering
    ├── __init__.py
    ├── config.py         # Transformation configuration
    ├── main.py           # Main transformation pipeline
    ├── render.py         # Layer rendering utilities
    └── tools.py          # Overlay and manifest management tools
```

## Core Functionality

### Basemap Generation (`basemap/`)

The basemap submodule creates interactive maps for visualizing storm cell data and meteorological information.

#### Main Pipeline (`basemap/main.py`)

**Key Function: `main()`**
- **Purpose**: Orchestrates the complete basemap generation process
- **Workflow**:
  1. Clean old map files (60-minute retention)
  2. Load storm cell data from JSON files
  3. Extract timestamp from MRMS data for map timestamping
  4. Create base map using `MapUtils`
  5. Overlay storm cell information
  6. Save and open the generated map

**Example Usage:**
```python
from EdgeWARN.core.gui_pipelines.basemap.main import main

# Generate storm cell visualization map
main()
```

#### Configuration (`basemap/config.py`)
- Map projection settings
- Coordinate system parameters
- Visual styling and rendering options
- Data layer configurations

#### Data Management (`basemap/data_loader.py`)
- **Storm Cell Loading**: Loads storm cell data from JSON files
- **Data Validation**: Ensures data integrity and completeness
- **Timestamp Extraction**: Parses MRMS timestamps from storm history

#### Map Utilities (`basemap/map_utils.py`)
- **Map Creation**: Initializes map canvas with appropriate projections
- **Storm Cell Overlay**: Adds storm cell data to map visualization
- **Interactive Features**: Enables user interaction capabilities

#### Output Management (`basemap/output_utils.py`)
- **Map Persistence**: Saves maps in various formats (PNG, HTML, etc.)
- **File Organization**: Manages output directory structure
- **Automatic Opening**: Opens generated maps in default viewer

### Data Transformation (`transform/`)

The transform submodule processes and converts meteorological data for visualization.

#### Main Pipeline (`transform/main.py`)

**Key Function: `main()`**
- **Purpose**: Transforms data layers and generates PNG representations
- **Workflow**:
  1. Load transformation configuration from `config.py`
  2. Iterate through configured data files
  3. Apply colormap transformations
  4. Convert data to PNG format using `GUILayerRenderer`
  5. Update overlay manifest with new layer information

**Configuration Structure:**
```python
file_list = [
    {
        'name': 'layer_name',
        'colormap_key': 'colormap_identifier', 
        'filepath': 'path/to/data/file',
        'outdir': 'output/directory'
    }
]
```

#### Rendering (`transform/render.py`)
- **GUILayerRenderer Class**: Converts data files to PNG format
- **Colormap Application**: Applies color mapping based on configuration
- **Timestamp Handling**: Preserves temporal information in transformations

#### Tool Management (`transform/tools.py`)
- **OverlayManifestUtils**: Manages layer manifest for web interface
- **Layer Organization**: Tracks available visualization layers
- **Metadata Management**: Stores layer properties and timestamps

## Key Classes and Methods

### DataLoader Class (`basemap/data_loader.py`)

#### Methods:
- **`load_stormcells() -> list`** 
  - Loads storm cell data from JSON files
  - Returns list of storm cell dictionaries with metadata

### MapUtils Class (`basemap/map_utils.py`)

#### Methods:
- **`create_map() -> map_object`**
  - Creates base map with appropriate projection and styling
  - Configures interactive features and controls

- **`add_storm_cells(map_object, stormcells: list)`**
  - Overlays storm cell data on map
  - Adds interactive elements and tooltips

### GUILayerRenderer Class (`transform/render.py`)

#### Constructor:
- **`__init__(filepath: str, outdir: str, colormap_key: str, name: str)`**
  - Initializes renderer for specific data file
  - Configures output settings and colormap

#### Methods:
- **`convert_to_png() -> (png_file, timestamp)`**
  - Converts data file to PNG format
  - Returns path to generated PNG and timestamp

### OverlayManifestUtils Class (`transform/tools.py`)

#### Methods:
- **`add_layer(name: str, colormap_key: str, png_path: str, timestamp: str)`**
  - Adds new layer to manifest
  - Records layer metadata and properties

- **`save_to_json(manifest_path: str)`**
  - Saves manifest to JSON file for web interface consumption

## Configuration Parameters

### Basemap Configuration (`basemap/config.py`)
- Map projection settings
- Coordinate bounds (latitude/longitude ranges)
- Visual styling options
- Storm cell display parameters

### Transform Configuration (`transform/config.py`)
- File processing list
- Colormap definitions
- Output directory structure
- Layer naming conventions

## Data Flow

1. **Storm Cell Data Loading**: JSON data loaded and validated
2. **Map Initialization**: Base map created with appropriate settings
3. **Data Overlay**: Storm cells added to map with visual styling
4. **Layer Transformation**: Data files converted to PNG for web interface
5. **Manifest Generation**: Layer information compiled for frontend consumption
6. **Output Persistence**: Maps saved and made available for viewing

## Integration Points

- **Storm Detection**: Consumes output from `process/detect/main.py`
- **Meteorological Data**: Uses data processed by CTAM module
- **File Management**: Integrates with `src/util/file.py` for path management
- **Logging**: Uses `IOManager` for consistent logging across modules

## Error Handling

The module includes comprehensive error handling for:
- Missing or corrupted storm cell data files
- Invalid coordinate bounds or projections
- File path validation and permissions
- Colormap configuration errors
- Timestamp parsing and validation

## Dependencies

- **Mapping Libraries**: Mapping/GIS libraries for map generation
- **Image Processing**: PNG generation and manipulation tools
- **Data Formats**: Support for JSON, NetCDF, and meteorological data formats
- **File Management**: Path handling and file system utilities

## Usage Examples

### Generating Storm Cell Maps
```python
from EdgeWARN.core.gui_pipelines.basemap.main import main

# Simple map generation
main()

# Map generation with custom storm cells
from EdgeWARN.core.gui_pipelines.basemap.data_loader import DataLoader
loader = DataLoader(io_manager)
stormcells = loader.load_stormcells()
```

### Data Transformation Pipeline
```python
from EdgeWARN.core.gui_pipelines.transform.main import main

# Run transformation pipeline
main()
```

### Custom Layer Rendering
```python
from EdgeWARN.core.gui_pipelines.transform.render import GUILayerRenderer
from EdgeWARN.core.gui_pipelines.transform.tools import OverlayManifestUtils

renderer = GUILayerRenderer("data.nc", "output/", "radar", "Radar")
png_file, timestamp = renderer.convert_to_png()

manifest = OverlayManifestUtils()
manifest.add_layer("Radar", "radar", str(png_file), timestamp)
manifest.save_to_json("manifest.json")
```

## File Management

The module manages several key directories:
- **GUI Map Directory**: Temporary map storage with automatic cleanup
- **Output Directories**: Persistent storage for generated maps and layers
- **Manifest Files**: JSON metadata for web interface integration
- **Temporary Files**: Processing artifacts with automatic cleanup

## Performance Considerations

- **Memory Efficiency**: Large datasets processed in chunks
- **File Cleanup**: Automatic removal of old files (60-minute retention)
- **Concurrent Processing**: Multi-threaded layer transformation
- **Caching**: Manifest-based caching for web interface optimization