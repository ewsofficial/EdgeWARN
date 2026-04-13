# StormCast Module

StormCast is a CTAM AnalysisModule that provides thunderstorm motion prediction by blending observed storm displacement vectors with RAP environmental wind profiles. It generates forecast tracks and polygons with uncertainty quantification.

## Overview

StormCast uses a modular framework to:
1. Extract storm observations and environmental wind profiles
2. Apply Kalman filtering for state estimation
3. Blend observed motion with steering flow
4. Generate forecast tracks and polygons
5. Quantify forecast uncertainty

## Module Structure

```
StormCast/
├── __init__.py        # Module entry point (exports StormCastModule)
└── core/
    ├── __init__.py    # Core engine exports
    ├── types.py       # Data classes (StormState, EnvironmentProfile, ForecastPoint)
    ├── config.py      # Configuration constants (pressure levels, weights, parameters)
    ├── diagnostics.py # Diagnostic calculations (shear, steering, height weights)
    ├── blending.py    # Motion blending algorithms
    ├── kalman.py      # Kalman filter implementation
    ├── uncertainty.py # Uncertainty quantification
    ├── forecast.py    # Forecast generation
    └── core.py        # Main engine (StormCastEngine, ForecastResult)
```

## Core Components

### StormCastEngine

Main orchestrator that:
1. Manages storm observations
2. Sets up environment profile
3. Runs Kalman filtering
4. Generates forecasts

### EnvironmentProfile

Encapsulates wind data at different pressure levels and timestamp.

### ForecastResult

Container for forecast output:
- Forecasted motion (u, v)
- Forecast cones (with confidence levels)
- Forecast polygons
- 0-30 minute forecast polygon for alerting

## Configuration

Key configuration from `core/config.py`:

```python
PRESSURE_LEVELS = [250, 500, 700, 850]  # Wind levels used for steering

DEFAULT_BLENDING_WEIGHTS = {
    "observed": 0.6,    # Weight for observed motion
    "steering": 0.4     # Weight for steering flow
}

KALMAN_PARAMS = {
    "process_noise": 0.01,
    "measurement_noise": 0.1,
    "initial_covariance": 0.1
}

UNCERTAINTY_PARAMS = {
    "velocity_error": 1.0,
    "position_error": 0.5,
    "time_horizon": 30
}
```

## Algorithm

### Observation Extraction

1. Current position (from storm entry centroid)
2. Motion vectors (from storm entry dx, dy, dt)
3. Wind profile (from environment or properties)
4. Echo top heights (from MRMS integration)

### History Loading

Loads historical observations from:
1. History cache (if available)
2. Cell history file (JSON format)

### Engine Initialization

```python
# Build environment profile
env_profile = EnvironmentProfile(winds=wind_data, timestamp=None)

# Initialize engine
engine = StormCastEngine(reference_lat=35.0, reference_lon=-97.0)
engine.set_environment(env_profile)

# Add observations
engine.add_observation(x, y, dt_seconds=dt, echo_top_30=10.0, echo_top_50=8.0)

# Generate forecast
result = engine.generate_forecast()
```

### Forecast Generation

1. Kalman state estimation using historical observations
2. Observed motion smoothing
3. Steering flow computation from wind profile
4. Motion blending using configured weights
5. Position forecasting
6. Uncertainty quantification
7. Cone and polygon generation

## Usage

### CTAM Integration

The module is registered in `src/EdgeWARN/ctam/modules/__init__.py` and automatically included in the CTAM pipeline. Results are stored in:
```python
storm_entry["modules"]["StormCast"]
```

### Output Structure

```python
{
    "u": 10.5,
    "v": 2.3,
    "forecast_cones": [
        {
            "time": 10,
            "radius": 5.2,
            "confidence": 0.68
        }
    ],
    "forecast_polygons": [
        {
            "time": 30,
            "coordinates": [...]
        }
    ],
    "polygon_0_30m": [...],  # 0-30 minute forecast polygon
    "status": "success",
    "can_generate_alerts": True
}
```

### Alert Generation

Generates alerts if:
1. Module ran successfully
2. Cell has been tracked for at least 15 minutes
3. 0-30 minute forecast polygon is available

## Requirements

- Python 3.13+
- NumPy, SciPy
- Shapely (for polygon handling)
- Kalman filtering implementation

## Performance

- Optimized for real-time operation
- Caches history for repeated calls
- Handles missing data gracefully
- Efficient wind profile processing

## References

- Thunderstorm motion prediction using wind profiles
- Kalman filtering for state estimation
- Uncertainty quantification in short-term forecasting
- Blending observed and environmental motion vectors
