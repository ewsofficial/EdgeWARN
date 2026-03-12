# FLOHAR Module

FLOHAR (FLOod HAzaRds) is a CTAM Grid Module for grid-based flash flood detection using MRMS FLASH products. It operates independently of storm cells to identify flood threat regions from radar-derived rainfall and hydrologic data.

## Overview

The FLOHAR module is a GridAnalysisModule implementation that:
1. Loads MRMS FLASH GRIB files from disk
2. Computes composite flood threat scores using a multi-pillar scoring system
3. Extracts contiguous flood threat regions
4. Generates GeoJSON output with severity classification
5. Produces alert payloads for high-threat regions

## Module Structure

```
FLOHAR/
├── __init__.py        # Module entry point (exports FLOHARModule)
├── config.py          # Algorithm configuration constants
├── engine.py          # Core threat scoring algorithm
├── flohar_module.py   # Main module implementation (extends GridAnalysisModule)
├── main.py            # Standalone entry point for testing/debugging
└── regions.py         # Region extraction and polygonization logic
```

## Configuration

The FLOHAR algorithm uses the following key configuration parameters from `config.py`:

### Pillar Weights
```python
PILLAR_WEIGHTS = {
    "rainfall": 0.25,
    "hydro": 0.45,
    "ffg": 0.30,
}
```

### Severity Tiers
```python
SEVERITY_TIERS = [
    (80, "emergency"),  # Flash Flood Warning
    (55, "warning"),    # Flash Flood Watch
    (30, "advisory"),   # Advisory (monitor only)
    (0, "none"),
]
```

### Region Extraction
```python
THREAT_THRESHOLD = 30               # Minimum score to include in a region
MIN_REGION_AREA_KM2 = 10.0          # Minimum region area to keep
POLYGON_SIMPLIFY_TOLERANCE = 0.005  # Polygon simplification tolerance
CONNECTIVITY = 8                    # 8-connectivity for region labeling
MAX_REGIONS = 1000                  # Maximum number of regions to return
```

## Algorithm

The FLOHAR algorithm computes a composite threat score by blending three pillars:

1. **Rainfall Extremity**: Uses ARI (Annual Return Interval) values from MRMS FLASH products
2. **Hydrologic Response**: Combines streamflow and soil saturation data
3. **Guidance Exceedance**: Compares FFG (Flash Flood Guidance) ratios

### Scoring Process

1. Load all required MRMS FLASH GRIB files
2. Normalize and weight each pillar
3. Compute composite threat score grid
4. Threshold and extract regions
5. Polygonize and classify severity

### Alert Generation

The module generates `AlertPayload` objects for regions meeting specific severity thresholds:
- **Emergency (score ≥ 80)**: Flash Flood Warning (Extreme severity)
- **Warning (score 55-79)**: Flash Flood Watch (Moderate severity)
- **Advisory (score 30-54)**: No alert (monitor only)

## Usage

### Standalone Execution (for Testing)

```python
from EdgeWARN.core.ctam.modules.FLOHAR.main import run_flohar

result = run_flohar()
print(f"FLOHAR processing complete: {result['metadata']}")
```

### CTAM Integration

The module is registered in `src/EdgeWARN/core/ctam/modules/__init__.py` and automatically included in the CTAM pipeline.

## Output Format

The `run()` method returns a dictionary with:

```python
{
    "features": {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [...]
                },
                "properties": {
                    "region_id": 1,
                    "peak_score": 95,
                    "mean_score": 82.5,
                    "severity": "emergency",
                    "area_km2": 15.2,
                    "centroid": [35.0, -97.0],
                    "pillar_peaks": {
                        "rainfall": 0.95,
                        "hydro": 0.85,
                        "ffg": 0.78
                    },
                    "timestamp": "2023-10-01T12:00:00Z"
                }
            }
        ]
    },
    "metadata": {
        "region_count": 1,
        "max_threat_score": 95,
        "grid_shape": [1000, 1000]
    },
    "timestamp": "2023-10-01T12:00:00Z"
}
```

GeoJSON files are saved to the `FLASH_FLOOD_DIR` directory in the format: `flohar_YYYYMMDD_HHMMSS.json`

## Requirements

- Python 3.13+
- NumPy, SciPy
- Shapely, Rasterio
- xarray, cfgrib
- concurrent.futures (for parallel grid loading)

## Performance Notes

- Uses ThreadPoolExecutor for parallel GRIB file loading
- Vectorized computations with NumPy for efficiency
- Bounding box slicing for spatial optimization
- Limit max_workers to 2 to prevent excessive eccodes allocations

## References

MRMS FLASH products used:
- CREST Streamflow
- HP Streamflow
- ARI (Annual Return Interval)
- FFG (Flash Flood Guidance) Ratio
- Soil Saturation
- RQI (Radar Quality Index)
