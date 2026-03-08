# Flash Flood Detection Algorithm

## Background

EdgeWARN currently ingests seven NOAA MRMS FLASH products from S3 but does **not** run any flash-flood analysis. This plan adds a **standalone grid-based flash flood processor** that operates directly on the FLASH GRIB grids — independent of storm cells — since flash flood footprints do not align with convective storm cell boundaries.

The processor:
1. Loads the seven FLASH grids and computes a per-pixel composite threat score (0–100).
2. Thresholds the score grid to identify contiguous flood threat regions.
3. Extracts region polygons with metadata (peak score, severity, area).
4. Saves the result as a GeoJSON feature collection to a dedicated output directory.

---

## Product Reference

| Product | Grid Key | What it measures | Units / Range |
|---------|----------|-----------------|---------------|
| `FLASH_CREST_MAXUNITSTREAMFLOW_00.00` | `crest_streamflow` | CREST hydrologic model max unit streamflow | 0–∞ (m³/s/km²) |
| `FLASH_QPE_ARIMAX_00.00` | `ari_max` | Maximum ARI of QPE (all durations) | years (≥1 = noteworthy, ≥5 = rare) |
| `FLASH_QPE_ARI30M_00.00` | `ari_30m` | ARI for 30-minute QPE | years |
| `FLASH_QPE_ARI01H_00.00` | `ari_01h` | ARI for 1-hour QPE | years |
| `FLASH_HP_MAXUNITSTREAMFLOW_00.00` | `hp_streamflow` | HP hydrologic model max unit streamflow | 0–∞ (m³/s/km²) |
| `FLASH_SAC_MAXSOILSAT_00.00` | `soil_sat` | SAC model max soil saturation fraction | 0–1 (fraction) — confirmed via sample GRIBs |
| `FLASH_QPE_FFGMAX_00.00` | `ffg_ratio` | QPE-to-FFG ratio — values ≥1 = QPE exceeds guidance | ratio (0–∞) |
| `RadarQualityIndex_00.00` | `rqi` | Radar Quality Index — higher values indicate better data quality | 0–1 (fraction) |

---

## Proposed Changes

### 1. Grid-Based Flash Flood Processor

#### [NEW] FlashFlood/ (src/EdgeWARN/core/process/flashflood/)

New CTAM Grid Module directory containing:

| File | Purpose |
|------|---------|
| `__init__.py` | Package init with GridModuleRegistry registration |
| `config.py` | Thresholds, weights, severity tiers, minimum region area |
| `engine.py` | Per-pixel scoring engine (vectorized with numpy) |
| `regions.py` | Connected-component labeling → polygon extraction |
| `flashflood_module.py` | CTAM GridAnalysisModule implementation |
| `main.py` | Standalone entry point for testing/debugging |

---

### 2. Output Directory

#### [MODIFY] file.py (src/util/file.py)

Add a new directory constant:

```python
FLASH_FLOOD_DIR = DATA_DIR / "FlashFlood"
```

---

### 3. Pipeline Integration

The flash flood processor runs as a **CTAM Grid Module** per the architecture defined in [`plans/CTAM_Grid_Module_Architecture.md`](CTAM_Grid_Module_Architecture.md).

Key integration points:
- Uses `GridModuleRegistry` for registration
- Inherits from `GridAnalysisModule` base class
- Operates on raw FLASH GRIB files (not storm cells)
- Produces GeoJSON FeatureCollection output

---

## Algorithm Design

The algorithm operates **per-pixel** on aligned FLASH grids, fusing seven indicators into a composite threat score (0–100) using three pillars:

### Pillar 1 — Rainfall Extremity (Weight: 0.40)

How extreme is the precipitation relative to climatology?

| Indicator | Sub-weight | Normalization |
|-----------|-----------|---------------|
| `ari_max` | 0.50 | Logarithmic: `min(log10(ARI) / log10(200), 1.0)` — ARI of 200yr → 1.0 |
| `ari_30m` | 0.25 | Same log scale |
| `ari_01h` | 0.25 | Same log scale |

### Pillar 2 — Hydrologic Response (Weight: 0.35)

Is the rainfall generating dangerous runoff?

| Indicator | Sub-weight | Normalization |
|-----------|-----------|---------------|
| `crest_streamflow` | 0.45 | Sigmoid: `1 / (1 + exp(-k * (x - x0)))` — midpoint `x0 = 1.5`, steepness `k = 2.0` (per CREST literature baseline) |
| `hp_streamflow` | 0.45 | Same sigmoid — midpoint `x0 = 1.5`, steepness `k = 2.0` (per HP literature baseline) |
| `soil_sat` | 0.30 | Linear: saturated soil (≥0.90) → 1.0, dry (≤0.40) → 0.0. **Confirmed 0–1 fraction** from GRIB inspection. Increased weight to reflect its importance in predicting flash flood potential |

Soil saturation acts as a **conditioning factor** — it amplifies the pillar score when soil is already near saturation:

```
hydro_score = (streamflow_blend * 0.70) + (soil_factor * 0.30)
if soil_sat > 0.85:
    hydro_score *= 1.0 + 0.25 * ((soil_sat - 0.85) / 0.15)  # up to +25% boost
```

### Pillar 3 — Guidance Exceedance (Weight: 0.25)

Has the QPE exceeded Flash Flood Guidance?

| Indicator | Sub-weight | Normalization |
|-----------|-----------|---------------|
| `ffg_ratio` | 1.00 | Piecewise: ratio < 0.75 → 0; 0.75–1.0 → ramp 0–0.5; 1.0–2.0 → ramp 0.5–1.0; ≥ 2.0 → 1.0 |

### Quality Control (Radar Quality Index)

The Radar Quality Index (RQI) acts as a multiplicative factor to adjust the composite score based on the quality of the radar data. Higher RQI values indicate better data quality:

- RQI ≥ 0.8 → Full weight (1.0)
- 0.3 ≤ RQI < 0.8 → Linear ramp from 0.0 to 1.0
- RQI < 0.3 → Hard mask (0.0) — data is considered unreliable

```python
def rqi_weight(rqi: float) -> float:
    if rqi >= 0.8:
        return 1.0
    elif rqi >= 0.3:
        return (rqi - 0.3) * (1.0 / 0.5)
    else:
        return 0.0
```

### Temporal Persistence (Score History)

To account for the temporal dimension, the algorithm tracks threat score history across cycles. A persistence multiplier is applied to the current score based on how long the score has been above the advisory threshold:

- 0–1 cycles: No multiplier (1.0)
- 2–3 cycles: 10% boost (1.10)
- 4–5 cycles: 20% boost (1.20)
- ≥6 cycles: 30% boost (1.30)

```python
def persistence_multiplier(score_history: np.ndarray) -> float:
    """
    Calculate persistence multiplier based on score history (last 6 cycles)
    
    Args:
        score_history: Array of threat scores from the last 6 cycles
        
    Returns:
        Persistence multiplier (1.0 - 1.30)
    """
    # Count number of cycles with score ≥ advisory threshold
    active_cycles = np.sum(score_history >= 25)
    
    if active_cycles <= 1:
        return 1.0
    elif active_cycles <= 3:
        return 1.10
    elif active_cycles <= 5:
        return 1.20
    else:
        return 1.30
```

### Composite Score

```
composite = (rainfall_score * 0.40) + (hydro_score * 0.35) + (ffg_score * 0.25)
adjusted_composite = composite * rqi_weight(rqi) * persistence_multiplier(score_history)
threat_score = round(adjusted_composite * 100)  # 0–100
```

### Severity Tiers

| Score Range | Severity | Description |
|------------|----------|-------------|
| 0–24 | `none` | No significant flash flood threat |
| 25–49 | `advisory` | Elevated risk — monitor conditions |
| 50–74 | `warning` | Likely flash flooding occurring or imminent |
| 75–100 | `emergency` | Extreme / life-threatening flash flooding |

> [!IMPORTANT]
> These severity tiers are based on initial expert judgment and should be calibrated using historical data before operational use. A recommended approach:
> 
> 1. Collect 10–15 historical flash flood events with NWS LSR ground truth
> 2. Plot score distributions of verified flood pixels vs. non-flood pixels
> 3. Adjust thresholds using ROC analysis to optimize CSI (Critical Success Index)
> 4. Validate on a held-out test set of 3–5 events
> 
> Expected calibration range: Advisory (20–30), Warning (45–55), Emergency (70–80)

---

## Region Extraction Pipeline

After computing the per-pixel threat grid:

1. **Threshold** — mask pixels where `threat_score >= 25` (advisory or higher).
2. **Static mask** — zero-out pixels that fall within a permanent water body mask (large rivers, lakes, reservoirs) loaded from a static GeoJSON/shapefile to prevent false regions.
3. **Connected-component labeling** — use `scipy.ndimage.label` with an **explicit 8-connectivity structuring element** (`np.ones((3,3))`) to correctly connect diagonally adjacent flood pixels.
4. **Filter by area** — discard regions smaller than a configurable minimum (e.g., 4 km²) to remove noise.
5. **Polygonize** — convert each region's pixel mask to a simplified polygon using `rasterio.features.shapes` or contour tracing.
6. **Compute region metadata**:
   - `peak_score`: max threat score in the region
   - `mean_score`: average threat score
   - `severity`: tier from peak score
   - `area_km2`: region area
   - `centroid`: [lat, lon]
   - `pillar_peaks`: peak value of each pillar within the region
7. **Output** — save as GeoJSON feature collection to `FLASH_FLOOD_DIR/flashflood_{timestamp}.json`.

---

## Output Format for Alerting Integration

The algorithm outputs both GeoJSON for visualization and a structured list for alerting integration:

### GeoJSON Output (flashflood_{timestamp}.json)

```json
{
  "type": "FeatureCollection",
  "features": [
    {
      "type": "Feature",
      "geometry": {
        "type": "Polygon",
        "coordinates": [[[lon, lat], [lon, lat], ...]]
      },
      "properties": {
        "region_id": 1,
        "peak_score": 85,
        "mean_score": 62.4,
        "severity": "warning",
        "area_km2": 125.7,
        "centroid": [40.5, -95.2],
        "pillar_peaks": {
          "rainfall": 0.92,
          "hydro": 0.78,
          "ffg": 0.65
        }
      }
    }
  ]
}
```

### Alert-Ready List Output (for programmatic integration)

The module also provides a simplified list format suitable for direct alerting integration:

```python
def get_alert_regions() -> List[Dict[str, Any]]:
    """
    Returns a list of flood threat regions formatted for alerting.
    
    Each region contains:
        - geometry: Polygon coordinates (list of [lon, lat] pairs)
        - severity_score: 0-100 threat score
        - severity_tier: 'none' | 'advisory' | 'warning' | 'emergency'
        - area_km2: Region area in square kilometers
        - centroid: [latitude, longitude] of region center
    """
    # Returns list ready for AlertManager integration
```

#### Data Structure for Alerting

```python
# Type definition for alert regions
AlertRegion = {
    "region_id": int,           # Unique identifier for this region
    "geometry": List[List[float]],  # Polygon as [[lon, lat], ...]
    "severity_score": int,      # 0-100 composite threat score
    "severity_tier": str,       # 'none' | 'advisory' | 'warning' | 'emergency'
    "area_km2": float,          # Region area
    "centroid_lat": float,      # Centroid latitude
    "centroid_lon": float,      # Centroid longitude
    "pillar_scores": {          # Individual pillar scores (0-1)
        "rainfall": float,
        "hydrologic": float,
        "ffg_exceedance": float
    },
    "timestamp": str            # ISO 8601 timestamp
}
```

#### Integration with Alert Manager

The `FlashFloodModule.alerts()` method generates alert payloads:

```python
def alerts(self, features: List[Dict]) -> Optional[List[AlertPayload]]:
    """
    Generate alert payloads for regions above advisory threshold.
    
    Alert priority:
        - Emergency (score >= 75): High priority alert
        - Warning (score 50-74): Medium priority alert  
        - Advisory (score 25-49): Low priority alert
    """
    alerts = []
    for feature in features:
        props = feature.get('properties', {})
        score = props.get('peak_score', 0)
        severity = props.get('severity', 'none')
        
        if severity == 'emergency':
            alerts.append(AlertPayload(
                event="Flash Flood Warning",
                severity="Extreme",
                geometry=feature['geometry'],
                metadata={...}
            ))
        elif severity == 'warning':
            alerts.append(AlertPayload(
                event="Flash Flood Watch",
                severity="Moderate",
                geometry=feature['geometry'],
                metadata={...}
            ))
    return alerts
```

---

## File Details

### [NEW] config.py (flashflood/config.py)

```python
# Pillar weights (must sum to 1.0)
PILLAR_WEIGHTS = {
    "rainfall": 0.40,
    "hydro": 0.35,
    "ffg": 0.25,
}

# ARI log normalization ceiling (years)
ARI_CEILING_YEARS = 200

# Streamflow sigmoid parameters
# Literature-based defaults from CREST/HP model documentation.
# These represent the unit streamflow (m³/s/km²) at which threat probability
# reaches 50%. Will be refined via post-deployment calibration (see below).
CREST_SIGMOID = {"x0": 1.5, "k": 2.0}   # midpoint, steepness
HP_SIGMOID    = {"x0": 1.5, "k": 2.0}

# Soil saturation conditioning (confirmed 0–1 fraction from GRIB inspection)
# Increased weight to reflect its importance as a predictor of flash flood potential
SOIL_SAT_LOW  = 0.40   # below → 0
SOIL_SAT_HIGH = 0.90   # above → 1
SOIL_BOOST_THRESHOLD = 0.85
SOIL_BOOST_MAX = 0.25  # +25% max (increased from 15%)

# FFG ratio piecewise breakpoints
FFG_RAMP_START = 0.75
FFG_RAMP_MID   = 1.0
FFG_RAMP_END   = 2.0

# RQI quality control parameters
RQI_FULL_WEIGHT_THRESHOLD = 0.8    # RQI >= this → full weight (1.0)
RQI_MIN_WEIGHT_THRESHOLD = 0.3    # RQI <= this → hard mask (0.0)
RQI_MIN_WEIGHT = 0.0              # Minimum weight to apply (hard mask)

# Severity tiers (checked in descending order)
SEVERITY_TIERS = [
    (75, "emergency"),
    (50, "warning"),
    (25, "advisory"),
    (0,  "none"),
]

# Region extraction
THREAT_THRESHOLD = 25            # minimum score to form a region
MIN_REGION_AREA_KM2 = 4.0       # discard regions smaller than this
POLYGON_SIMPLIFY_TOLERANCE = 0.005  # degrees, for polygon simplification
CONNECTIVITY = 8                 # 8-connectivity for diagonal flood pixel connection
MAX_REGIONS = 1000              # maximum number of regions to extract per cycle

# Temporal persistence
MAX_HISTORY_LENGTH = 6          # number of cycles to track for persistence
PERSISTENCE_MULTIPLIERS = {
    1: 1.0,    # 0-1 active cycles
    3: 1.10,   # 2-3 active cycles
    5: 1.20,   # 4-5 active cycles
    6: 1.30    # 6+ active cycles
}

# Static water body mask (enabled by default)
# Path to the bundled coarse water mask included with the algorithm
WATER_BODY_MASK_PATH = Path(__file__).parent / "data" / "water_bodies_coarse.geojson"
```

### [NEW] engine.py (flashflood/engine.py)

Vectorized numpy scoring engine that operates on 2D arrays:

```python
def compute_threat_grid(
    ari_max: np.ndarray,
    ari_30m: np.ndarray,
    ari_01h: np.ndarray,
    crest_streamflow: np.ndarray,
    hp_streamflow: np.ndarray,
    soil_sat: np.ndarray,
    ffg_ratio: np.ndarray,
    rqi: np.ndarray,
) -> Tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    """
    Compute per-pixel threat scores from aligned FLASH grids.
    
    Returns:
        threat_grid: 2D int array (0-100)
        rainfall_grid: 2D float array (pillar 1 score)
        hydro_grid: 2D float array (pillar 2 score)
        ffg_grid: 2D float array (pillar 3 score)
    """
```

Key design decisions:
- **Vectorized** — all operations use numpy broadcasting, no Python loops over pixels.
- **NaN/sentinel handling** — missing data (NaN, −999) is masked; sub-weights redistribute within each pillar. If an entire pillar is NaN at a pixel, its weight redistributes to the other pillars.
- **Logarithmic ARI** avoids over-saturation from extreme ARI values.
- **Sigmoid streamflow** — configurable midpoints per deployment.

### [NEW] regions.py (flashflood/regions.py)

```python
import numpy as np
from scipy.ndimage import label
from shapely.geometry import shape
import rasterio.features

# Explicit 8-connectivity structuring element — flash flood regions
# frequently connect diagonally and must not be split artificially.
CONNECTIVITY_STRUCTURE = np.ones((3, 3), dtype=int)

# Maximum number of regions to extract (prevents overload in widespread events)
MAX_REGIONS = 1000

def extract_regions(
    threat_grid: np.ndarray,
    lat_coords: np.ndarray,
    lon_coords: np.ndarray,
    pillar_grids: dict,
    threshold: int = 25,
    min_area_km2: float = 4.0,
    water_body_mask: np.ndarray | None = None,
    max_regions: int = MAX_REGIONS,
) -> List[dict]:
    """
    Extract contiguous flood threat regions from the threat grid with scalability safeguards.

    Steps:
        1. Threshold the grid.
        2. Apply static water body mask (if provided) to zero-out permanent water.
        3. Label connected components using 8-connectivity.
        4. Filter by minimum area.
        5. Limit to maximum number of regions (largest regions first)
        6. Polygonize and compute metadata.
    
    Returns list of region dicts with:
        - geometry: list of (lat, lon) polygon coordinates
        - peak_score, mean_score, severity
        - area_km2, centroid
        - pillar_peaks: {rainfall, hydro, ffg}
    """
```

### [NEW] main.py (flashflood/main.py)

Standalone entry point for testing/debugging the flash flood processor outside of CTAM:

```python
def run_flash_flood():
    """
    Standalone entry point for flash flood processing.
    For CTAM integration, use FlashFloodModule in flashflood_module.py instead.
    """
    from .flashflood_module import FlashFloodModule
    module = FlashFloodModule()
    result = module.run()
    print(f"Flash flood processing complete: {result['metadata']}")
    return result
```

See [`plans/CTAM_Grid_Module_Architecture.md`](CTAM_Grid_Module_Architecture.md) for the full CTAM Grid Module implementation.

---

## Verification Plan

### Automated Tests

Test file: `tests/core/process/test_flash_flood.py`

```bash
pytest tests/core/process/test_flash_flood.py -v
```

Test cases for `engine.py`:
1. **All zeros** → all pixels score 0
2. **Uniform extreme values** → all pixels score ≈ 100
3. **ARI-only scenario** (high ARI, zero streamflow/FFG) → moderate rainfall score, low hydro/ffg
4. **FFG ratio edge cases**: 0.5 → 0, 0.75 → 0, 1.0 → 0.5, 2.0 → 1.0, 3.0 → 1.0
5. **NaN handling**: grids with NaN pixels produce valid output (NaN redistributes weight)
6. **All NaN** → threat score 0
7. **Soil saturation boost**: verify amplification when soil_sat > 0.85
8. **Severity tier boundaries**: 24 → none, 25 → advisory, 50 → warning, 75 → emergency
9. **RQI quality control**: 
   - RQI = 1.0 → full score
   - RQI = 0.8 → full score
   - RQI = 0.65 → 70% of score
   - RQI = 0.3 → 0% of score (hard mask)
   - RQI = 0.2 → 0% of score (hard mask)

Test cases for `regions.py`:
9. **Single region extraction**: synthetic grid with one cluster above threshold
10. **Multiple disjoint regions**: verify correct count and separation
11. **Small region filtering**: region below `MIN_REGION_AREA_KM2` is discarded
12. **Polygon coordinates**: verify output polygon coords are valid lat/lon

### Manual Verification

1. Run `python src/process_historical.py --start <date> --end <date>` on a known flash flood event
2. Inspect the output GeoJSON in `FLASH_FLOOD_DIR/` for the corresponding timestamp
3. Verify that extracted regions align geographically with the actual flood-affected areas
4. Check that `threat_score` and `severity` values are reasonable
5. Confirm that 8-connectivity produces coherent region shapes (no artificial splits along diagonals)
6. Confirm that the water body mask (if enabled) suppresses false detections over lakes/large rivers

### Post-Deployment Calibration Plan

> [!IMPORTANT]
> The sigmoid midpoints and pillar weights are literature-informed defaults. A structured calibration must be performed before declaring the system operational.

**Sigmoid midpoint calibration** (`CREST_SIGMOID.x0`, `HP_SIGMOID.x0`):
1. Select 5–10 recent flash flood case studies with NWS Local Storm Reports (LSR) ground truth.
2. For each event, extract the raw CREST/HP streamflow distributions from the FLASH GRIBs.
3. Plot the cumulative distribution of streamflow values inside vs. outside verified flood areas.
4. Set `x0` at the value where the probability of flooding reaches ~50% (the natural sigmoid midpoint).
5. Adjust steepness `k` so the sigmoid transition spans a physically reasonable range (e.g., 0.5–3.0 m³/s/km²).

**Pillar weight calibration** (`PILLAR_WEIGHTS`):
1. Using the same case studies, compute binary hit/miss tables (detected vs. verified) for threat scores 25–100.
2. Generate ROC curves and compute CSI, POD, and FAR at each severity tier.
3. Use grid-search or Bayesian optimization over the three weights (constrained to sum to 1.0) to maximize CSI.
4. Validate on a held-out set of 3–5 events to guard against overfitting.

---

---

## Future Enhancements

> [!NOTE]
> These are **not** required for the initial deployment but are recommended improvements.

- **Temporal persistence / rate-of-rise**: Track score changes across cycles (e.g., "score increased 30+ points in last 10 min") to boost confidence in rapidly developing events. Currently acceptable because the FLASH products themselves incorporate maximum values over recent accumulation windows.
- **Urban vs. rural differentiation**: Weight adjustments based on land-use (impervious surface fraction) to account for faster runoff in urban areas.
- **Ensemble weighting**: Dynamically adjust pillar weights based on regional climatology or season.
