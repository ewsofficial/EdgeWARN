# Mesocyclone Module

Mesocyclone is a CTAM `GridAnalysisModule` that detects rotating storm signatures from MRMS azimuthal shear grids, verifies them against composite reflectivity, associates low- and mid-level detections into vertically coherent features, tracks them over time, and writes timestamped JSON sidecar output.

## Overview

The module runs independently of the storm-cell object pipeline and produces a mesocyclone snapshot for each execution cycle. Its processing stages are:

1. Load the latest MRMS low-level azimuthal shear, mid-level azimuthal shear, and composite reflectivity grids.
2. Normalize azimuthal shear units when needed and harmonize the grids onto the low-level azimuthal shear coordinates.
3. Preprocess azimuthal shear grids with noise filtering, Gaussian smoothing, and optional morphological cleanup.
4. Extract contiguous low-level and mid-level azimuthal shear objects.
5. Gate detections using reflectivity overlap or proximity to a reflectivity core.
6. Associate low- and mid-level detections into vertically paired mesocyclones.
7. Score and classify each detection, then update persistent track IDs and motion vectors.
8. Save a timestamped JSON payload under the runtime Mesocyclone output directory.

## Module Structure

```text
Mesocyclone/
├── __init__.py      # Exports MesocycloneModule
├── associate.py     # Low/mid vertical association logic
├── config.py        # Detection, gating, scoring, and tracking constants
├── detect.py        # Object extraction and shape metrics
├── gate.py          # Reflectivity gating and reflectivity metrics
├── loader.py        # MRMS file loading, unit normalization, grid alignment
├── module.py        # GridAnalysisModule implementation and pipeline orchestration
├── output.py        # JSON payload construction and persistence
├── preprocess.py    # AzShear cleanup, smoothing, and tile-based preprocessing
├── score.py         # Strength labels, rank, and confidence scoring
└── track.py         # Track continuity, IDs, and motion vectors
```

## Runtime Inputs

The module loads the most recent files from the runtime MRMS directories referenced by `util.file`:

- low-level azimuthal shear (`MRMS_AZSHEARLOW_DIR`)
- mid-level azimuthal shear (`MRMS_AZSHEARMID_DIR`)
- composite reflectivity (`MRMS_COMPOSITE_DIR`)

`loader.py` uses the low-level azimuthal shear grid as the reference grid. Mid-level azimuthal shear is harmonized onto that grid with nearest-neighbor coordinate mapping. Reflectivity must already cover the same spatial extent.

## Key Configuration

Important thresholds in `src/EdgeWARN/ctam/modules/Mesocyclone/config.py`:

```python
NOISE_FLOOR = 0.0035
DETECTION_THRESHOLD = 0.006
MIN_OBJECT_PIXELS = 6

REFLECTIVITY_THRESHOLD_DBZ = 38.0
REFLECTIVITY_CORE_DISTANCE_KM = 8.0

VERTICAL_ASSOCIATION_DISTANCE_KM = 7.5
VERTICAL_ASSOCIATION_MIN_OVERLAP_RATIO = 0.05
TRACK_MATCH_DISTANCE_KM = 15.0
TRACK_MEMORY = timedelta(minutes=10)

MAX_COMPONENT_ASPECT_RATIO = 8.0

STRENGTH_BINS = [
    (0.025, "violent"),
    (0.015, "strong"),
    (0.008, "moderate"),
    (0.006, "weak"),
]

RANK_MAX = 0.03
MAX_STRENGTH_RANK = 25

CONFIDENCE_WEIGHTS = {
    "azshear": 0.65,
    "depth": 0.2,
    "reflectivity": 0.15,
}
```

Azimuthal shear inputs are also normalized when the raw values appear to be scaled integers:

```python
AZSHEAR_UNIT_SCALE_THRESHOLD = 1.0
AZSHEAR_UNIT_DIVISOR = 1000.0
```

If the maximum absolute value in an azimuthal shear grid exceeds `1.0`, the loader divides the grid by `1000.0` before detection.

## Processing Details

### 1. Input Loading and Normalization

`loader.py`:

- finds the latest low, mid, and reflectivity GRIB files
- loads all three grids in parallel
- normalizes azimuthal shear units when needed
- harmonizes the mid-level grid onto the low-level grid
- extracts a timestamp from the input filename when possible

### 2. Preprocessing

`preprocess.py`:

- replaces non-finite values with zero
- removes values below the noise floor
- applies Gaussian smoothing (`SMOOTHING_SIGMA = 1.0`)
- optionally performs binary opening and closing around the detection threshold
- can operate in active tiles to reduce work on sparse grids

### 3. Detection

`detect.py` identifies contiguous azimuthal shear objects where values are at least `DETECTION_THRESHOLD`.

For each object, the module computes:

- centroid latitude and longitude
- peak and mean azimuthal shear
- area in square kilometers
- eccentricity
- compactness
- aspect ratio
- local azimuthal shear maxima (`maxima`)
- bounding-box and source pixel indices for later gating

Objects smaller than the configured minimum native-grid area are dropped. Extremely elongated components with `aspect_ratio > 8.0` are also rejected as likely linear shear artifacts before association.

### 4. Reflectivity Gating

`gate.py` filters azimuthal shear detections against composite reflectivity.

A detection is kept when either of the following is true:

- at least one mapped reflectivity pixel overlapping the detection is at or above `38.0 dBZ`
- the detection centroid is within `8.0 km` of any reflectivity core pixel at or above `38.0 dBZ`

The gating step also adds:

- `reflectivity_overlap_pixels`
- `reflectivity_max`
- `reflectivity_mean`
- `distance_to_reflectivity_core_km`

### 5. Vertical Association

`associate.py` pairs low-level and mid-level detections by centroid distance and footprint overlap.

- candidate pairs are allowed only when centroid distance is within `VERTICAL_ASSOCIATION_DISTANCE_KM` (`7.5 km`)
- candidate pairs must share at least one footprint pixel and meet `VERTICAL_ASSOCIATION_MIN_OVERLAP_RATIO` (`0.05`) relative to the smaller footprint
- pairs are chosen greedily by highest overlap ratio, then shortest distance, then strongest azimuthal shear signal
- unmatched low-level detections become `shallow`
- unmatched mid-level detections become `mid-level`
- matched low+mid detections become `deep`

This stage sets:

- `depth_flag`
- `association_distance_km`
- `association_overlap_pixels` (deep pairs only)
- `association_overlap_ratio` (deep pairs only)

### 6. Scoring and Classification

`score.py` uses the maximum of the low-level and mid-level azimuthal shear values as the primary strength signal.

Derived fields include:

- `strength_label`: categorical label from `STRENGTH_BINS`
- `strength_rank`: integer from 1 to 25 based on linear scaling between `DETECTION_THRESHOLD` and `0.03`
- `confidence_score`: weighted combination of azimuthal shear, depth, and reflectivity

Confidence is computed as:

```python
az_component = min(1.0, max_azshear / RANK_MAX)
depth_bonus = 1.0 if depth_flag == "deep" else 0.55
ref_component = min(1.0, reflectivity_max / 60.0)

confidence = (
    0.65 * az_component
    + 0.2 * depth_bonus
    + 0.15 * ref_component
)
```

The result is clipped to the range `[0.0, 1.0]` and rounded to three decimals.

### 7. Tracking

`track.py` maintains persistent IDs across runs.

- tracks expire after `10 minutes`
- detections are matched to existing tracks with Hungarian assignment
- matches farther than `15 km` are rejected
- new detections get new integer IDs
- motion vectors are reported in meters per second as `{"u": ..., "v": ...}`

The output record uses the low-level centroid when present, otherwise the mid-level centroid.

## Output Format

The module persists JSON files using this pattern:

```text
<BASE_DIR>/data/Mesocyclones/mesocyclones_YYYYMMDD-HHMMSS.json
```

The payload has this top-level structure:

```json
{
  "type": "MesocycloneDetectionCollection",
  "source": "Mesocyclone",
  "timestamp": "2026-04-14T17:00:00+00:00",
  "metadata": {
    "detection_count": 3,
    "low_candidate_count": 8,
    "mid_candidate_count": 6,
    "low_gated_count": 4,
    "mid_gated_count": 3
  },
  "detections": [
    {
      "id": 1,
      "time": "2026-04-14T17:00:00+00:00",
      "lat": 35.12345,
      "lon": -97.12345,
      "motion_vector": {"u": 5.2, "v": 1.4},
      "azshear_low": 27.9,
      "azshear_mid": 19.4,
      "depth_flag": "deep",
      "reflectivity_max": 53.5,
      "strength_rank": 23,
      "confidence_score": 0.861,
      "area": 9.094,
      "eccentricity": 0.884,
      "compactness": 0.551,
      "strength_label": "violent",
      "multi_peak_count_low": 1,
      "multi_peak_count_mid": 1,
      "association_distance_km": 2.317,
      "association_overlap_pixels": 8,
      "association_overlap_ratio": 0.615
    }
  ]
}
```

## Detection Field Definitions

Each exported detection record contains the following fields:

| Field | Type | Meaning |
| --- | --- | --- |
| `id` | integer | Persistent mesocyclone track ID. |
| `time` | string | Detection timestamp in ISO 8601 UTC form. |
| `lat` / `lon` | float | Primary centroid location used for the track record. |
| `motion_vector` | object | Track motion in m/s with east-west `u` and north-south `v`. |
| `azshear_low` | float | Peak low-level azimuthal shear exported in units of `0.001 s^-1`. |
| `azshear_mid` | float | Peak mid-level azimuthal shear exported in units of `0.001 s^-1`. |
| `depth_flag` | string | Vertical association class: `deep`, `shallow`, or `mid-level`. |
| `reflectivity_max` | float | Maximum mapped composite reflectivity in dBZ. |
| `strength_rank` | integer | Strength rank from 1 to 25 derived from peak azimuthal shear. |
| `confidence_score` | float | Weighted confidence score in the range 0.0 to 1.0. |
| `area` | float | Detection footprint area in square kilometers. |
| `eccentricity` | float | Shape elongation metric from 0.0 to 1.0. |
| `compactness` | float | Shape compactness metric from 0.0 to 1.0. |
| `strength_label` | string | Strength category from the configured azimuthal shear bins. |
| `multi_peak_count_low` | integer | Number of local azimuthal shear maxima inside the low-level object. |
| `multi_peak_count_mid` | integer | Number of local azimuthal shear maxima inside the mid-level object. |
| `association_distance_km` | float or `null` | Low/mid centroid distance for vertically paired detections; `null` when unpaired. |
| `association_overlap_pixels` | integer or `null` | Count of overlapping low/mid footprint pixels for deep pairs. |
| `association_overlap_ratio` | float or `null` | Overlap fraction relative to the smaller low/mid footprint for deep pairs. |

## CTAM Integration

`MesocycloneModule.run()` returns lightweight metadata to CTAM and writes the full sidecar payload to disk. The returned structure looks like:

```python
{
    "features": {"type": "FeatureCollection", "features": []},
    "metadata": {
        "detection_count": ...,
        "low_candidate_count": ...,
        "mid_candidate_count": ...,
        "low_gated_count": ...,
        "mid_gated_count": ...,
    },
    "timestamp": "2026-04-14T17:00:00+00:00",
    "output_path": ".../mesocyclones_20260414-170000.json",
    "attach_to_stormcells": False,
}
```

The module does not attach its results to storm-cell records. Mesocyclone snapshots are consumed separately through the filesystem and API layer.

## API Exposure

The EdgeWARN API exposes persisted snapshots through:

- `GET /api/v2/features/mesocyclones`

Without a `timestamp`, the route lists available snapshot timestamps. With `?timestamp=YYYYMMDD-HHMMSS`, it returns the corresponding persisted mesocyclone JSON payload.

## Performance Notes

- low, mid, and reflectivity inputs are loaded concurrently
- low-level detection runs in parallel with mid-level preprocessing
- tile-based preprocessing avoids smoothing empty areas of sparse grids
- tracking state is held in memory inside the module instance

## Limitations and Current Behavior

- detection depends entirely on MRMS azimuthal shear and composite reflectivity availability
- reflectivity is used only as a gate and intensity summary, not as the primary detector
- the exported payload is JSON sidecar data rather than GeoJSON features
- track continuity is process-local; restarting the worker resets in-memory track IDs
