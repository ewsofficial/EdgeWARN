# StormCast Module

StormCast is the reserved CTAM built-in that provides thunderstorm motion
prediction by blending observed storm displacement vectors, storm history,
Bunkers-style deviant motion, and RAP environmental wind profiles. It generates
forecast tracks, forecast-polygon metadata, a 0-30 minute alert polygon when
enough geometry is available, and uncertainty information.

## Overview

StormCast uses a modular framework to:

1. Extract storm observations, cell history, current footprint geometry, and environmental wind profiles
2. Apply Kalman filtering for state estimation
3. Compute adaptive steering, effective shear, and Bunkers deviant motion
4. Blend observed motion, mean steering flow, and Bunkers motion with dynamic maturity-aware weights
5. Generate forecast cones, forecast-polygon metadata, and a 0-30 minute corridor polygon

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

Key configuration from `core/config.py`. Every grouped parameter set is a
`@dataclass(frozen=True)` *instance*, not a dict — access is by attribute
(`KALMAN_PARAMS.alpha`), and assignment raises.

```python
PRESSURE_LEVELS = tuple(range(1000, 75, -25))  # 1000mb through 100mb, 25mb steps

DEFAULT_BLENDING_WEIGHTS = BlendingWeights(
    w_obs=0.6,
    w_mean=0.2,
    w_bunkers=0.2,
)

SHALLOW_STORM_WEIGHTS = BlendingWeights(w_obs=0.3, w_mean=0.3, w_bunkers=0.4)
MATURE_STORM_WEIGHTS = BlendingWeights(w_obs=0.6, w_mean=0.15, w_bunkers=0.25)
MOTION_SMOOTHING_WINDOW = 10

BUNKERS_DEVIATION = BunkersParams(
    d_shallow=3.0,   # m/s, reduced deviation for shallow convection
    d_deep=7.5,      # m/s, canonical supercell deviation
    h_shallow=6.0,   # km, below this use the reduced deviation
    h_deep=10.0,     # km, above this use the full deviation
)

KALMAN_PARAMS = KalmanParams(
    alpha=0.97,
    dt_default=300.0,
    sigma_pos=800.0,
    sigma_vel=12.0,
    q_pos=500.0,
    q_vel=7.2,
)

UNCERTAINTY_PARAMS = UncertaintyParams(
    sigma_min=1.2,
    sigma_range=2.5,
    alpha_decay=0.5,
    sigma_obs=4.0,
    sigma_env=2.0,
    jitter_multiplier=0.1,
)

DEFAULT_LEAD_TIMES = (900.0, 1800.0, 2700.0, 3600.0)
MAX_RELIABLE_LEAD_TIME = 3600.0        # skill degrades sharply past 60 min
MIN_VELOCITY_THRESHOLD = 2.0           # m/s, filters stationary cells
MAX_VELOCITY_THRESHOLD = 50.0          # m/s, filters unrealistic motion
```

`LEVEL_HEIGHTS` and `GAUSSIAN_WEIGHT_PARAMS` are derived per pressure level
rather than written out: heights come from the standard-atmosphere relation
`44.3308 * (1 - (p/1013.25)**0.190263)`, and each level's Gaussian weight is
centered on its own height with a fixed `sigma` of `2.0` km.

The scalar Bunkers constants `BUNKERS_DEVIATION_FULL`,
`BUNKERS_DEVIATION_SHALLOW`, `BUNKERS_HEIGHT_SHALLOW`, and
`BUNKERS_HEIGHT_DEEP` also exist and carry the same four values as
`BUNKERS_DEVIATION`. Prefer the dataclass; the loose scalars are duplicates.

## Algorithm

### Observation Extraction

1. Current position from the storm entry centroid, with longitudes converted from `0..360` to `-180..180` for local projection math
2. Motion vectors from top-level `dx`, `dy`, and `dt` values written by detection vector math
3. Wind profile from an explicit environment override or from `properties.wind_field.u{level}` / `v{level}` entries
4. Echo top heights from MRMS/ProbSevere integration fields, primarily `p100EchoTop30` and `EchoTop50`
5. Current storm footprint polygon when available, used to build forecast-polygon products

### History Loading

Loads historical observations from:

1. The shared CTAM history cache when provided by the framework
2. The per-cell history file under `data/cells/{id}.json` when no cache is provided

Historical centroids are converted to local meter offsets relative to the current centroid, sorted chronologically, and fed into the engine before the current observation so observed motion smoothing and Kalman updates can use prior samples.

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
2. Observed motion smoothing over the configured recent history window
3. Adaptive steering and effective shear computation from the RAP wind profile
4. Bunkers motion computation with shallow/deep deviation parameters
5. Dynamic blending using observed, mean-wind, and Bunkers components
6. Position forecasting for 15, 30, 45, and 60 minute lead times by default
7. Cone, forecast-polygon metadata, and 0-30 minute corridor generation

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
            "center": [35.123, -97.456],
            "radius": 1250.0,
            "polygon_expansion": 1250.0,
            "lead_time": 900.0
        }
    ],
    "forecast_polygons": [
        {
            "center": [35.234, -97.345],
            "expansion_ratio": 1.12,
            "lead_time": 1800.0
        }
    ],
    "polygon_0_30m": [...],
    "forecast_polygon_reason": null,
    "status": "success",
    "can_generate_alerts": True
}
```

If a forecast corridor cannot be produced, `forecast_polygon_reason` is set to a reason such as `missing_current_polygon` or `insufficient_hull_shapes`.

### Alert Generation

Generates alerts if:

1. Module execution succeeds
2. The cell has enough history/motion to produce a forecast
3. `polygon_0_30m` is available

The CTAM adapter publishes these alerts through `AlertManager`, using the 0-30 minute corridor polygon as the alert geometry.

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
