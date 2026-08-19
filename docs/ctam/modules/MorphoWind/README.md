# MorphoWind Module

MorphoWind is a CTAM AnalysisModule that provides physics-based morphological wind risk assessment for storm cells. It detects QLCS/Bow Echoes and Microburst potential using data-driven fuzzy logic and physics-based triggers.

## Overview

The MorphoWind module analyzes storm cell morphology and environmental conditions to assess wind risk. It uses Gaussian CDF-based scoring to smooth transitions between risk levels and incorporates physics-based triggers for enhanced accuracy.

## Module Structure

```
MorphoWind/
├── __init__.py        # Module entry point (exports MorphoWindModule)
├── config.py          # Algorithm configuration constants (Gaussian thresholds)
└── morphowind.py      # Main module implementation (extends AnalysisModule)
```

## Configuration

Key configuration parameters from `config.py` (Gaussian thresholding):

### QLCS / Bow Echo Detection
```python
QLCS_SOLIDITY_MEAN = 0.75      # Solidity mean (lower = riskier)
QLCS_SOLIDITY_SIGMA = 0.1      # Solidity sigma

QLCS_ASPECT_RATIO_MEAN = 3.0   # Aspect ratio mean (higher = riskier)
QLCS_ASPECT_RATIO_SIGMA = 1.0  # Aspect ratio sigma

QLCS_SHEAR_MEAN = 4.0          # AzShear mean (higher = riskier)
QLCS_SHEAR_SIGMA = 2.0         # AzShear sigma

QLCS_DEFECT_DEPTH_MEAN = 5.0   # Rear inflow notch depth mean (higher = riskier)
QLCS_DEFECT_DEPTH_SIGMA = 2.0  # Rear inflow notch depth sigma
```

### Microburst Detection
```python
MB_VIL_DENSITY_MEAN = 3.5      # VIL density mean (higher = riskier)
MB_VIL_DENSITY_SIGMA = 1.5     # VIL density sigma

MB_ECHOTOP_MEAN = 6.0          # Echo Top mean (lower = riskier)
MB_ECHOTOP_SIGMA = 2.0         # Echo Top sigma
```

### Physics Triggers
```python
COLLAPSE_VIL_RATE_THRESHOLD = -1.0  # VIL collapse rate (kg/m^2 per 5 min)
COLLAPSE_ET_RATE_THRESHOLD = -1.5   # Echo Top collapse rate (km per 5 min)

BOOKEND_VORTEX_SHEAR_THRESHOLD = 5.0
BOOKEND_VORTEX_LINEARITY_THRESHOLD = 0.6
BOOKEND_MAX_BRANCHING = 2
```

### Environmental Calibration
```python
FL_MEAN = 4.0             # reference freezing level (km), standard mid-latitude
FL_SIGMA = 1.5            # transition width (km)
FL_MAX_CORRECTION = 1.0   # max VIL-density mean reduction at high FL (g/m^3)

DD_MEAN = 12.0            # dewpoint depression (C) where P(correction) = 0.5
DD_SIGMA = 5.0            # transition width (C)
DD_MAX_CORRECTION = 1.0   # max VIL-density mean reduction in very dry air (g/m^3)
```

These do not score directly. Each produces a Gaussian CDF multiplier that
*lowers* the effective `MB_VIL_DENSITY_MEAN`, so a high freezing level or a dry
sub-cloud layer makes the same observed VIL density score as riskier. Both
corrections replaced an older set of hard thresholds, which no longer exist.

## Algorithm

### Feature Extraction

The module extracts and calculates key morphological and physical features:

1. **Morphology**: Solidity, aspect ratio, rear inflow notch depth/bearing
2. **Kinematics**: Azimuthal shear, storm motion
3. **Dynamics**: VIL density, echo top height
4. **Temporal**: VIL and echo top collapse rates
5. **Environment**: Freezing level height, dewpoint depression

### Scoring System

Uses Gaussian CDF scoring:
```python
def _gaussian_score(self, value, mean, sigma, invert=False):
    """Calculate probability score (0.0 - 1.0) using Gaussian CDF."""
    z = (value - mean) / (sigma * math.sqrt(2)) if not invert else (mean - value) / (sigma * math.sqrt(2))
    return 0.5 * (1 + math.erf(z))
```

### Physics Triggers

1. **Collapse Detection**: Rapid decreases in VIL or echo top height
2. **Rear Inflow Notch**: Kinematic verification based on storm motion
3. **Bookend Vortex**: High shear, linear, simple structures

### Risk Aggregation

```python
qlcs_risk = (score_solidity + score_aspect + score_shear + (notch_score * 1.5)) / 4.5
qlcs_risk = min(1.0, qlcs_risk + bookend_bonus)

mb_risk = ((score_vil_density + score_et) / 2.0) * 0.7 + (collapse_score * 0.3)
if collapse_score > 0.8:
    mb_risk = max(mb_risk, 0.9)
```

Two adjustments follow the weighted sums and both matter:

- A detected bookend vortex adds a flat `bookend_bonus` of `0.5` to
  `qlcs_risk`, clamped to `1.0`. That bonus alone clears the `0.5`
  classification threshold, so a confirmed bookend vortex is sufficient to
  classify a cell as QLCS regardless of its morphology scores.
- `collapse_score` accumulates `0.5` for a VIL collapse and `0.5` for an echo-top
  collapse, so it is `1.0` only when both trigger. In that case the
  `collapse_score > 0.8` branch floors `mb_risk` at `0.9` — a dual collapse
  cannot be scored down by weak VIL-density or echo-top terms.

## Usage

### CTAM Integration

The module is registered in `src/EdgeWARN/ctam/modules/__init__.py` and automatically included in the CTAM pipeline. Results are stored in:
```python
storm_entry["modules"]["MorphoWind"]
```

### Output Structure

```python
{
    "risk_type": "QLCS",
    "confidence": 0.75,
    "physics_triggers": ["REAR_INFLOW_NOTCH", "BOOKEND_VORTEX"],
    "severity_index": 0.75,
    "scores": {
        "qlcs": 0.75,
        "microburst": 0.45
    },
    "physics": {
        "vil_density": 4.2,
        "collapse_score": 0.0,
        "vil_change": 0.1,
        "et_change": -0.2,
        "defect_bearing": 180,
        "linearity": 0.8
    }
}
```

## Risk Classification

| Risk Type | Condition | Description |
|-----------|-----------|-------------|
| QLCS | `qlcs_risk > 0.5` and `qlcs_risk >= mb_risk` | Quasi-Linear Convective System (bow echo) |
| Microburst | otherwise, `mb_risk > 0.5` | Downburst/microburst potential |
| None | neither score exceeds `0.5` | No significant wind threat |

The two are mutually exclusive — a cell gets one `risk_type`, never both — and
QLCS wins an exact tie because its test uses `>=`. `confidence` is the winning
score.

`severity_index` is `max(qlcs_risk, mb_risk)` and is always populated, so a cell
classified `None` still reports a non-zero severity when a score sits just under
`0.5`. Read `risk_type` rather than thresholding `severity_index` yourself.

## Key Features

1. **Gaussian Smoothing**: Eliminates hard thresholds for smoother risk transitions
2. **Physics-Based Triggers**: Enhanced accuracy from temporal and kinematic features
3. **Environmental Calibration**: Freezing level and dewpoint depression corrections
4. **History Integration**: Uses cell history for collapse detection
5. **Multi-Component Scoring**: Combines multiple morphological indicators

## Requirements

- Python 3.13+
- Python standard library `math` (`math.erf`)
- Storm cell properties with morphology fields

## References

- QLCS/Bow Echo morphology characteristics from radar meteorology
- Microburst physics and VIL density relationships
- Gaussian CDF scoring for fuzzy logic applications
