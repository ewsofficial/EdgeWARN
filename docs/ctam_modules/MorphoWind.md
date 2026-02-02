# MorphoWind Algorithm Documentation

**Module**: `MorphoWind`  
**Class**: `src.EdgeWARN.core.ctam.modules.morphowind.MorphoWindModule`  
**Config**: `src.EdgeWARN.core.ctam.modules.morphowind_config.py`

## Overview
MorphoWind is a Cell Tracking and Monitoring (CTAM) module designed to classify severe wind risks—specifically **QLCS (Quasi-Linear Convective Systems)** and **Microbursts**—by analyzing the morphology (shape) and microphysics of storm cells.

Unlike traditional threshold-based systems, MorphoWind uses **Gaussian Smoothing (Fuzzy Logic)** to calculate continuous risk probabilities.

## Inputs
The module consumes data from the following integration pipelines:

1.  **Geometric Features** (from `detect_cells`):
    *   **Solidity**: Ratio of Contour Area to Hull Area. (Lower = more non-convex/linear).
    *   **Aspect Ratio**: Major Axis / Minor Axis. (Higher = more linear).
    *   **Convexity Defect Depth**: Normalized depth of the largest defect. (Higher = Rear Inflow Notch signature).

2.  **Microphysical Features** (from `integrate_multi_stats`):
    *   **p95AzShearLow**: 95th Percentile of Low-Level Azimuthal Shear ($0-2km$). (Proxi for vortices/leading edge shear).
    *   **p95VILDensity**: 95th Percentile of Vertically Integrated Liquid Density. (Proxi for hail core/water loading).
    *   **p95EchoTop18**: 95th Percentile of 18 dBZ Echo Top Height. (Proxi for updraft strength/core depth).

## Algorithm: Gaussian Scoring
Risk scores for each feature are calculated using the Cumulative Distribution Function (CDF) of the Gaussian distribution. This maps any input value to a probability $P \in [0, 1]$.

$$ P(x) = 0.5 \cdot \left( 1 + \text{erf}\left( \frac{x - \mu}{\sigma\sqrt{2}} \right) \right) $$

Where:
*   $\mu$ (`MEAN`): The inflection point where risk probability is 0.5.
*   $\sigma$ (`SIGMA`): Controls the width of the transition zone (fuzziness).

### Risk Categories

#### 1. QLCS / Bow Echo
Risk is the average of 4 indicators:
*   **Solidity**: Inverted Gaussian (Lower is riskier).
*   **Aspect Ratio**: Standard Gaussian (Higher is riskier).
*   **AzShear Low**: Standard Gaussian (Higher is riskier).
*   **Rear Inflow Notch**: Standard Gaussian (Higher is riskier).

#### 2. Microburst
Risk is the average of 2 indicators:
*   **VIL Density**: Standard Gaussian (Higher is riskier).
*   **Echo Top 18**: Inverted Gaussian (Lower is riskier - indicates shallow or collapsing core).

## Configuration
All Gaussian parameters are tunable in `morphowind_config.py`.

### Default Constants
| Feature | Mean ($\mu$) | Sigma ($\sigma$) | Direction |
| :--- | :--- | :--- | :--- |
| **Solidity** | 0.75 | 0.1 | Lower = Risky |
| **Aspect Ratio** | 3.0 | 1.0 | Higher = Risky |
| **AzShear** | 4.0 ($10^{-3} s^{-1}$) | 2.0 | Higher = Risky |
| **Notch Depth** | 5.0 | 2.0 | Higher = Risky |
| **VIL Density** | 2.5 $g/m^3$ | 1.0 | Higher = Risky |
| **Echo Top** | 6.0 km | 2.0 | Lower = Risky |

## Usage
The module runs automatically in the pipeline.
**Output Location**: `cell['properties']['morphowind']`

**Example JSON Output**:
```json
"morphowind": {
    "risk_type": "QLCS",
    "confidence": 0.85, 
    "flags": ["linear_shape", "shear_detected"],
    "scores": {
        "qlcs": 0.85, 
        "microburst": 0.12
    }
}
```
