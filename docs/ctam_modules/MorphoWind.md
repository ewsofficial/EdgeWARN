# MorphoWind Algorithm Documentation

**Module**: `MorphoWind`  
**Package**: `src.EdgeWARN.core.ctam.modules.MorphoWind`  
**Config**: `src.EdgeWARN.core.ctam.modules.MorphoWind.config`

## Overview
MorphoWind is a Cell Tracking and Monitoring (CTAM) module designed to classify severe wind risks—specifically **QLCS (Quasi-Linear Convective Systems / Bow Echoes)** and **Microbursts**—using deterministic, physics-based logic and morphological analysis.

Unlike threshold-based systems, MorphoWind uses **Gaussian Smoothing (Fuzzy Logic)** for continuous risk probabilities, combined with **physics-based triggers** for high-confidence detections.

---

## Detection Regimes

### 1. Microburst (Pulse-Collapse Regime)
Driven by **Precipitation Loading** and **Evaporative Cooling**.

| Feature | Description |
|:--------|:------------|
| **VIL Density** | $VIL / EchoTop_{18}$. High values (> 3.5 g/m³) indicate heavy, suspended cores. |
| **Pre-Condition** | Requires historical max VIL Density ≥ 3.0 before collapse is meaningful. |
| **Collapse Detection** | Flags `VIL_COLLAPSE` / `ET_COLLAPSE` if VIL or EchoTop drops rapidly. |
| **Freezing Level Correction** | Adjusts thresholds based on `environment.freezing_level_height`. Higher FL = more sensitive. |
| **Dry Air Correction** | Adjusts thresholds based on `environment.dewpoint_depression`. High DD = more sensitive. |

### 2. QLCS / Bow Echo (Organized Linear Regime)
Driven by **Rear Inflow Jets (RIJ)** and **Bookend Vortices**.

| Feature | Description |
|:--------|:------------|
| **Solidity** | Low values (~0.5) indicate non-convex, bowed shapes. |
| **Aspect Ratio** | High values (> 3.0) indicate linear structure. |
| **Linearity** | Skeletonization metric. High values = simple line; Low = complex cluster. |
| **Rear Inflow Notch** | Convexity defect in rear quadrant (> 135° from motion) triggers `REAR_INFLOW_NOTCH`. |
| **Bookend Vortex** | Linear + Low Branching + High AzShear triggers `BOOKEND_VORTEX`. |

---

## Inputs

### Geometric Features (from `morphology.py`)
| Field | Source | Description |
|:------|:-------|:------------|
| `solidity` | `morphology` | Contour Area / Hull Area |
| `aspect_ratio` | `morphology` | MinAreaRect W/H (normalized ≥ 1) |
| `defect_max_depth` | `morphology` | Largest convexity defect depth (pixels) |
| `defect_bearing` | `morphology` | Bearing of deepest defect from centroid (0-360°) |
| `linearity` | `morphology` | Skeleton Length / (Endpoints + Junctions) |
| `branching_factor` | `morphology` | Number of skeleton junctions |

### Microphysical Features (from `integrate_multi_stats`)
| Field | Source | Description |
|:------|:-------|:------------|
| `p95VIL` | `properties` | 95th Percentile VIL (kg/m²) |
| `p95EchoTop18` | `properties` | 95th Percentile 18 dBZ Echo Top (km) |
| `p95AzShearLow` | `properties` | 95th Percentile Low-Level Azimuthal Shear (×10⁻³ s⁻¹) |

### Environment (optional)
| Field | Type | Description |
|:------|:-----|:------------|
| `freezing_level_height` | `float` (km) | Height of 0°C isotherm. Standard = 4.0 km. |
| `dewpoint_depression` | `float` (°C) | T - T_d. High values indicate dry air. |

---

## Configuration (`config.py`)

### Gaussian Parameters
| Feature | Mean (μ) | Sigma (σ) | Direction |
|:--------|:---------|:----------|:----------|
| **Solidity** | 0.75 | 0.1 | Lower = Risky |
| **Aspect Ratio** | 3.0 | 1.0 | Higher = Risky |
| **AzShear** | 4.0 | 2.0 | Higher = Risky |
| **Notch Depth** | 5.0 | 2.0 | Higher = Risky |
| **VIL Density** | 3.5 | 1.5 | Higher = Risky |
| **Echo Top** | 6.0 | 2.0 | Lower = Risky |

### Physics Thresholds
| Parameter | Value | Description |
|:----------|:------|:------------|
| `COLLAPSE_VIL_RATE_THRESHOLD` | -1.0 | VIL change per 5 min (trigger if below) |
| `COLLAPSE_ET_RATE_THRESHOLD` | -1.5 | EchoTop change per 5 min (trigger if below) |
| `BOOKEND_VORTEX_SHEAR_THRESHOLD` | 5.0 | AzShear for Bookend Vortex |
| `BOOKEND_VORTEX_LINEARITY_THRESHOLD` | 0.6 | Linearity for Bookend Vortex |
| `BOOKEND_MAX_BRANCHING` | 2 | Max skeleton junctions for "clean line" |

### Environmental Corrections (Gaussian Smoothed)
VIL of the Day corrections now use Gaussian CDF smoothing instead of hard thresholds:

```
correction = gaussian_cdf(value, mean, sigma) × max_correction
adjusted_vil_mean = MB_VIL_DENSITY_MEAN - fl_correction - dp_correction
```

| Parameter | Mean (μ) | Sigma (σ) | Max Correction |
|:----------|:---------|:----------|:---------------|
| **Freezing Level** | 4.0 km | 1.5 km | 1.0 g/m³ |
| **Dewpoint Depression** | 12.0°C | 5.0°C | 1.0 g/m³ |

**VIL of the Day Table** (Freezing Level → Effective VIL Threshold):
| FL (km) | VIL Threshold (g/m³) |
|:--------|:---------------------|
| 2.0 | 3.41 |
| 4.0 | 3.00 |
| 6.0 | 2.59 |
| 8.0 | 2.50 |

---

## Output
**Location**: `cell['properties']['morphowind']`

```json
{
    "risk_type": "QLCS",
    "confidence": 0.85,
    "severity_index": 0.85,
    "physics_triggers": ["REAR_INFLOW_NOTCH", "BOOKEND_VORTEX"],
    "scores": {
        "qlcs": 0.85,
        "microburst": 0.12
    },
    "physics": {
        "vil_density": 3.2,
        "collapse_score": 0.0,
        "vil_change": 0.5,
        "et_change": -0.2,
        "defect_bearing": 270.0,
        "linearity": 0.75
    }
}
```

### Physics Triggers
| Trigger | Meaning |
|:--------|:--------|
| `VIL_COLLAPSE` | Rapid VIL drop detected (with pre-condition met) |
| `ET_COLLAPSE` | Rapid Echo Top drop detected (with pre-condition met) |
| `REAR_INFLOW_NOTCH` | Rear-sector convexity defect confirmed |
| `BOOKEND_VORTEX` | Linear structure with high shear at ends |

---

## Usage
The module runs automatically in the CTAM pipeline after cell detection and integration.

```python
from EdgeWARN.core.ctam.modules.MorphoWind import MorphoWindModule

module = MorphoWindModule()
module.run(storm_entry, environment={"freezing_level_height": 4.5})
```
