# Storm Tracking, Lineage Detection, and Termination Logic

This document provides comprehensive technical documentation for the storm cell tracking system in EdgeWARN-Core, covering three major subsystems:

1. **Storm Tracking** - Kalman filter-based motion prediction and measurement assignment
2. **Lineage Detection** - Merge and split event detection with hysteresis buffering
3. **Termination Logic** - Confidence-based cell termination with decay monitoring

---

## Table of Contents

1. [Storm Tracking](#1-storm-tracking)
   - [1.1 Overview](#11-overview)
   - [1.2 Kalman Filter Architecture](#12-kalman-filter-architecture)
   - [1.3 Measurement Assignment](#13-measurement-assignment)
   - [1.4 Prediction Mode](#14-prediction-mode)
   - [1.5 Configuration](#15-configuration)

2. [Lineage Detection (Merger/Splitter)](#2-lineage-detection-mergersplitter)
   - [2.1 Overview](#21-overview)
   - [2.2 Event Types](#22-event-types)
   - [2.3 Spatial Analysis](#23-spatial-analysis)
   - [2.4 Hysteresis Buffering](#24-hysteresis-buffering)
   - [2.5 Detection Algorithm](#25-detection-algorithm)
   - [2.6 Dominant Cell Selection](#26-dominant-cell-selection)

3. [Termination Logic](#3-termination-logic)
   - [3.1 Overview](#31-overview)
   - [3.2 Termination Criteria](#32-termination-criteria)
   - [3.3 Confidence Calculation](#33-confidence-calculation)
   - [3.4 State Machine Model](#34-state-machine-model)
   - [3.5 Decay Monitoring](#35-decay-monitoring)

4. [Integration](#4-integration)
   - [4.1 Processing Pipeline](#41-processing-pipeline)
   - [4.2 Data Flow](#42-data-flow)
   - [4.3 API Reference](#43-api-reference)

---

## 1. Storm Tracking

### 1.1 Overview

The storm tracking subsystem maintains continuous identity of storm cells across radar scans using a Kalman filter-based approach. The system handles:

- **Motion Prediction**: Predicting cell positions between scans using velocity estimates
- **Measurement Assignment**: Matching new ProbSevere detections to existing tracks
- **Continuity Tracking**: Maintaining cell identity during temporary detection dropouts
- **Re-acquisition**: Recovering tracks when detection resumes after prediction mode

The primary implementation is in [`StormCellTracker`](src/EdgeWARN/core/process/detect/track.py:38).

### 1.2 Kalman Filter Architecture

The system uses a 6-dimensional Kalman filter with constant acceleration model:

```
State Vector: x = [lat, lon, v_lat, v_lon, a_lat, a_lon]
```

Where:
- `lat, lon`: Geographic position (degrees)
- `v_lat, v_lon`: Velocity components (m/s)
- `a_lat, a_lon`: Acceleration components (m/s²)

#### State Transition Model

The state transition follows a constant acceleration model:

```
x(k+1) = F * x(k) + B * u(k) + w(k)
```

Where:
- `F`: State transition matrix (6x6)
- `B`: Control input matrix (for StormCast velocity)
- `u(k)`: Control vector (StormCast u, v velocities)
- `w(k)`: Process noise

#### Process Noise Configuration

Process noise parameters control how much the state is expected to change between predictions:

| Parameter | Default | Description |
|-----------|---------|-------------|
| `process_noise_position` | 0.1 | Position process noise (degrees) |
| `process_noise_velocity` | 0.5 | Velocity process noise (m/s) |
| `process_noise_acceleration` | 0.1 | Acceleration process noise (m/s²) |

#### Measurement Model

Observations come from ProbSevere detections:

```
z(k) = H * x(k) + v(k)
```

Where:
- `z(k)`: Measurement vector [lat, lon]
- `H`: Measurement matrix (2x6)
- `v(k)`: Measurement noise

Measurement noise is configured via `measurement_noise_position` (default: 0.5 km).

### 1.3 Measurement Assignment

The measurement assignment algorithm matches new ProbSevere detections to existing tracked cells. The system implements a hybrid approach:

#### Stage 1: Spatial Pre-filtering

Detections beyond `prefilter_radius_km` (default: 16 km / 10 miles) from a track are excluded from consideration. This reduces computational cost for the Hungarian algorithm.

#### Stage 2: Mahalanobis Gating

The Mahalanobis distance validates potential matches:

```python
d_m = sqrt((z - z_pred)^T * S^-1 * (z - z_pred))
```

Where:
- `z`: Observed position
- `z_pred`: Predicted position
- `S`: Innovation covariance

A detection is considered valid if `d_m < gating_threshold` (default: 6.0, corresponding to ~95% confidence for 2 degrees of freedom).

#### Stage 3: Cost Matrix Construction

The assignment cost matrix combines multiple factors:

```
cost = w1 * d_position + w2 * d_velocity + w3 * d_shape
```

| Component | Weight | Description |
|-----------|--------|-------------|
| `d_position` | 1.0 | Mahalanobis distance (scale-normalized) |
| `d_velocity` | 2.0 | Velocity direction consistency penalty |
| `d_shape` | 0.5 | Size/reflectivity similarity |

#### Stage 4: Hungarian Algorithm

The Hungarian algorithm solves the optimal assignment problem:

```python
from scipy.optimize import linear_sum_assignment
row_ind, col_ind = linear_sum_assignment(cost_matrix)
```

#### Fallback: Greedy Assignment

If the Hungarian algorithm fails or is disabled, a greedy nearest-neighbor approach is used:

```python
def run_greedy_assignment(tracks, detections, kalman_filters, config, dt):
    # Sort by Mahalanobis distance and assign greedily
    # Respects gating threshold and one-to-one matching
```

### 1.4 Prediction Mode

When a tracked cell loses ProbSevere detection, it enters **Prediction Mode**:

#### Entry Conditions

- No spatial overlap with any new detection
- No valid assignment from Hungarian/greedy algorithm
- Cell was previously in ACTIVE state

#### Prediction Process

1. **Velocity Retrieval**: Get StormCast velocity (u, v) if available
2. **State Prediction**: Kalman filter predicts forward position
3. **Confidence Decay**: Confidence decreases each scan
4. **Position Update**: Cell centroid updated to predicted position

```python
def _handle_unmatched_cell(self, cell, cell_id, timestamp, dt_seconds):
    # Get StormCast velocity as control input
    control_u, control_v = self._get_stormcast_velocity(cell)
    
    # Perform Kalman prediction
    predicted_state = kf.predict(dt_seconds, control_u, control_v)
    
    # Calculate confidence
    confidence = self.confidence_calc.calculate(
        scans_predicted=pred_state.scan_count,
        time_predicted_seconds=pred_state.total_time_seconds,
        velocity_variance=vel_var,
        position_uncertainty_km=pos_unc
    )
```

#### Re-acquisition

A predicted cell can be re-acquired if a new detection appears within `reacquisition_radius_km` (default: 5 km) of the predicted position:

```python
def _check_reacquisition(self, new_cell, predicted_cells, timestamp):
    for cell in predicted_cells:
        pred_centroid = cell.get('kalman_predicted_centroid')
        distance = haversine_distance(new_lat, new_lon, pred_lat, pred_lon)
        
        if distance <= self.tracking_config.reacquisition_radius_km:
            # Re-acquire: update fields and reset prediction state
            return best_match
```

### 1.5 Configuration

All tracking parameters are configurable via [`config/kalman.yaml`](config/kalman.yaml:1):

```yaml
kalman_filter:
  process_noise:
    position: 0.1
    velocity: 0.5
    acceleration: 0.1
  measurement_noise:
    position: 0.5

tracking:
  max_prediction_time_minutes: 6
  reacquisition_radius_km: 5.0
  confidence_threshold: 0.4
  confidence_decay_factor: 0.7

assignment:
  prefilter_radius_km: 16.0
  gating_threshold: 6.0
  min_gating_radius_km: 2.0
  weights:
    position: 1.0
    velocity_direction: 2.0
    size_similarity: 0.5
  method: hybrid
```

---

## 2. Lineage Detection (Merger/Splitter)

### 2.1 Overview

Lineage detection identifies merge and split events between storm cells across scans. This is critical for:

- Maintaining accurate storm history
- Tracking parent-child relationships
- Handling ProbSevere ID instability
- Providing event logging for meteorological analysis

The lineage detection system is implemented in the [`lineage`](src/EdgeWARN/core/process/detect/lineage) module.

### 2.2 Event Types

The [`LineageEvent`](src/EdgeWARN/core/process/detect/lineage/events.py:13) enum defines four event types:

| Event | Description |
|-------|-------------|
| `MERGE` | Multiple parent cells combining into a single child cell |
| `SPLIT` | Single parent cell dividing into multiple child cells |
| `ACTIVE` | Normal continuation with no lineage change |
| `DISSIPATED` | Cell removed without merging (ceased to exist) |

#### Merge Event Structure

```python
@dataclass
class MergeEvent:
    child_id: int                    # ID of resulting child cell
    parent_ids: List[int]            # IDs of merged parent cells
    dominant_parent: int             # ID of dominant parent (inherits ID)
    overlap_ratios: Dict[int, float] # Parent_id -> overlap ratio mapping
```

#### Split Event Structure

```python
@dataclass
class SplitEvent:
    parent_id: int                   # ID of parent cell that split
    child_ids: List[int]             # IDs of resulting child cells
    dominant_child: int              # ID of dominant child (inherits ID)
    overlap_ratios: Dict[int, float] # Child_id -> overlap ratio mapping
```

### 2.3 Spatial Analysis

Spatial analysis is implemented in [`spatial.py`](src/EdgeWARN/core/process/detect/lineage/spatial.py:1).

#### Overlap Ratio Calculation

The overlap ratio determines what fraction of a parent polygon overlaps with a child polygon:

```python
def calculate_overlap_ratio(parent_bbox, child_bbox) -> float:
    """
    Calculate the ratio of parent area that overlaps with child area.
    
    Returns: Float between 0.0 and 1.0
    """
    # Build shapely Polygons
    parent_poly = Polygon([(lon, lat) for lat, lon in parent_bbox])
    child_poly = Polygon([(lon, lat) for lat, lon in child_bbox])
    
    # Handle invalid polygons
    if not parent_poly.is_valid:
        parent_poly = make_valid(parent_poly)
    
    # Calculate intersection ratio
    intersection = parent_poly.intersection(child_poly)
    return intersection.area / parent_poly.area
```

#### Spatial Index

The spatial index enables efficient overlap queries:

```python
def build_spatial_index(cells) -> Dict[int, Dict]:
    """
    Build spatial index for O(1) cell lookup by ID.
    
    Returns: Dict mapping cell_id to:
        - bbox: Polygon coordinates
        - min_lat, max_lat, min_lon, max_lon: Bounding extents
        - centroid: [lat, lon]
        - max_refl: Maximum reflectivity
        - num_gates: Number of gates
    """
```

#### Two-Stage Overlap Detection

For efficiency, overlap detection uses a two-stage approach:

1. **Bounding Box Pre-filter**: Fast axis-aligned bounding box check
2. **Precise Polygon Intersection**: Shapely geometry calculation

```python
def find_overlapping_cells(target_cell, cell_index, overlap_threshold):
    # Stage 1: Fast bounding box check
    if not bounds_overlap(target_bounds, cell_data):
        continue
    
    # Stage 2: Precise polygon overlap
    overlap_ratio = calculate_overlap_ratio(cell_data['bbox'], target_bbox)
    
    if overlap_ratio >= overlap_threshold:
        overlapping.append((cell_id, overlap_ratio))
```

### 2.4 Hysteresis Buffering

Hysteresis buffering prevents false positives from ProbSevere ID instability. The [`LineageBuffer`](src/EdgeWARN/core/process/detect/lineage/buffer.py:107) requires multiple consecutive detections before confirming an event.

#### Buffer Structure

```python
class LineageBuffer:
    min_confirmations: int = 2       # Scans needed to confirm
    max_pending: int = 100           # Memory limit
    prune_after_scans: int = 5       # Inactivity threshold
    
    pending_merges: Dict[int, PendingMerge]   # child_id -> PendingMerge
    pending_splits: Dict[int, PendingSplit]   # parent_id -> PendingSplit
    confirmed_merges: Set[int]                # Confirmed this scan
    confirmed_splits: Set[int]                # Confirmed this scan
```

#### Pending Event Tracking

```python
@dataclass
class PendingMerge:
    child_id: int
    parent_ids: Set[int]
    count: int = 1                   # Consecutive scan count
    first_seen: float                # Timestamp
    last_seen: float                 # Timestamp
    dominant_parent: int             # Updated each detection
```

#### Confirmation Process

```python
def record_potential_merge(self, child_id, parent_ids, dominant_parent) -> bool:
    """
    Record a potential merge and check if confirmed.
    
    Returns: True if count >= min_confirmations
    """
    if child_id in self.pending_merges:
        pending = self.pending_merges[child_id]
        pending.count += 1
        pending.parent_ids = set(parent_ids)
    else:
        self.pending_merges[child_id] = PendingMerge(...)
    
    if self.pending_merges[child_id].count >= self.min_confirmations:
        self.confirmed_merges.add(child_id)
        return True
    return False
```

#### Persistence

The buffer persists to disk since `StormCellTracker` is re-instantiated each scan:

```python
BUFFER_FILE = "lineage_buffer.json"

@classmethod
def load(cls, stormcell_dir: Path) -> 'LineageBuffer':
    """Load buffer state from disk."""

def save(self, stormcell_dir: Path) -> bool:
    """Persist buffer state to disk."""
```

### 2.5 Detection Algorithm

The [`LineageDetector`](src/EdgeWARN/core/process/detect/lineage/detector.py:31) implements the core detection algorithm:

```python
def detect(self, old_cells, new_cells) -> LineageResult:
    """
    Detect merge and split events between cell sets.
    
    Process:
    1. Build spatial indices for both cell sets
    2. Detect merges (multiple old -> single new)
    3. Detect splits (single old -> multiple new)
    4. Apply hysteresis buffer for confirmation
    5. Build lineage result with event classifications
    """
```

#### Merge Detection

For each new cell, find overlapping old cells:

```python
# For each new cell
for new_cell in new_cells:
    overlapping = find_overlapping_cells(new_cell, old_index, threshold)
    
    if len(overlapping) > 1:
        # Multiple old cells overlap -> potential MERGE
        parent_ids = [pid for pid, _ in overlapping]
        dominant_parent = select_dominant_parent(parent_ids, old_index)
        
        confirmed = buffer.record_potential_merge(new_id, parent_ids, dominant_parent)
        
        if confirmed:
            result.merges.append(MergeEvent(...))
```

#### Split Detection

For each old cell, find overlapping new cells:

```python
# For each old cell
for old_cell in old_cells:
    if old_id in matched_old:
        continue  # Already matched in merge detection
    
    overlapping = find_overlapping_cells(old_cell, new_index, threshold)
    
    if len(overlapping) > 1:
        # Single old overlaps multiple new -> potential SPLIT
        child_ids = [cid for cid, _ in overlapping]
        dominant_child = select_dominant_child(child_ids, new_index)
        
        confirmed = buffer.record_potential_split(old_id, child_ids, dominant_child)
        
        if confirmed:
            result.splits.append(SplitEvent(...))
```

### 2.6 Dominant Cell Selection

When multiple cells merge or split, one cell is designated as "dominant" and inherits the tracking history:

#### Dominant Parent Selection (Merge)

```python
def select_dominant_parent(parent_ids, cell_index) -> int:
    """
    Select dominant parent from merge candidates.
    
    Criteria:
    1. Highest max_refl (maximum reflectivity)
    2. Tiebreaker: largest num_gates
    """
    best_id = parent_ids[0]
    best_refl = cell_index[best_id]['max_refl']
    best_gates = cell_index[best_id]['num_gates']
    
    for pid in parent_ids[1:]:
        refl = cell_index[pid]['max_refl']
        gates = cell_index[pid]['num_gates']
        
        if refl > best_refl or (refl == best_refl and gates > best_gates):
            best_id = pid
            best_refl = refl
            best_gates = gates
    
    return best_id
```

#### Dominant Child Selection (Split)

Uses identical criteria: highest `max_refl`, tiebreaker `num_gates`.

---

## 3. Termination Logic

### 3.1 Overview

The termination logic determines when a tracked storm cell should be removed from the system. The implementation balances:

- **Sensitivity**: Detecting true dissipation promptly
- **Specificity**: Avoiding premature termination during temporary fluctuations

The termination system uses a dual-criterion approach requiring both reflectivity decay and prediction exhaustion.

### 3.2 Termination Criteria

A storm cell is terminated when **BOTH** criteria are met:

#### Criterion 1: Reflectivity Decay

```
max_refl < 30 dBZ for 3+ consecutive scans
```

- **Threshold**: 30 dBZ (configurable)
- **Hysteresis**: 3 consecutive scans (configurable)
- **Rationale**: Below 30 dBZ, convection has typically lost severe potential

#### Criterion 2: Prediction Exhaustion

```
confidence < 0.4 OR time_in_prediction > max_prediction_time
```

- **Confidence Threshold**: 0.4 (configurable)
- **Time Limit**: 6 minutes (configurable)
- **Rationale**: Extended prediction without re-acquisition indicates true loss

### 3.3 Confidence Calculation

The [`ConfidenceCalculator`](src/EdgeWARN/core/process/detect/kalman/confidence.py:15) computes confidence scores for predicted cells:

#### Confidence Formula

```python
confidence = scan_confidence * time_factor * motion_factor * position_factor
```

#### Component Calculations

**Scan-based Decay:**
```python
scan_confidence = base_confidence * (decay_factor ** scans_predicted)
# Example: 1.0 * (0.7 ** 3) = 0.343 after 3 scans
```

**Time-based Decay:**
```python
time_factor = max(0.0, 1.0 - (time_predicted / max_time) * 0.3)
# Example: 1.0 - (180/360) * 0.3 = 0.85 after 3 minutes
```

**Motion Consistency Factor:**
```python
if velocity_variance is not None:
    total_var = var_u + var_v
    motion_factor = max(0.5, 1.0 - total_var / 500.0)
# Higher velocity variance = lower confidence
```

**Position Uncertainty Factor:**
```python
if position_uncertainty_km is not None:
    avg_std = (std_lat + std_lon) / 2
    if avg_std > 5.0:
        position_factor = max(0.5, 1.0 - (avg_std - 5.0) / 20.0)
# Higher position uncertainty = lower confidence
```

#### Termination Decision

```python
def should_terminate(self, confidence, time_predicted_seconds, scans_predicted):
    # Check confidence threshold
    if confidence < self.config.confidence_threshold:
        return True, f"Confidence {confidence:.2f} below threshold"
    
    # Check time limit
    if time_predicted_seconds >= self.config.max_prediction_time_minutes * 60:
        return True, f"Prediction time exceeds limit"
    
    return False, ""
```

### 3.4 State Machine Model

The tracking state machine defines valid state transitions:

```
┌─────────────────────────────────────────────────────────────────────┐
│                                                                     │
│   ┌─────────────┐      ProbSevere      ┌─────────────────┐         │
│   │   ACTIVE    │ ◄─────────────────── │   PREDICTED     │         │
│   └──────┬──────┘                      └────────┬────────┘         │
│          │                                      │                   │
│          │ max_refl < 30 dBZ                    │                   │
│          ▼                                      ▼                   │
│   ┌─────────────┐      Both criteria    ┌─────────────────┐        │
│   │   DECAYING  │ ────────────────────► │   TERMINATED    │        │
│   └─────────────┘      met              └─────────────────┘        │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

#### State Descriptions

| State | Description | Entry Condition | Exit Conditions |
|-------|-------------|-----------------|-----------------|
| `ACTIVE` | Normal tracking with ProbSevere detection | New cell or re-acquisition | ProbSevere lost → PREDICTED; max_refl < threshold → DECAYING |
| `PREDICTED` | Kalman prediction mode without ProbSevere | ProbSevere detection lost | Re-acquisition → ACTIVE; Both criteria met → TERMINATED |
| `DECAYING` | Reflectivity below threshold, monitoring decay | max_refl < threshold for 1 scan | max_refl ≥ threshold → ACTIVE; Both criteria met → TERMINATED |
| `TERMINATED` | Cell removed from tracking | Both decay + prediction criteria met | N/A - terminal state |

#### Transition Logic

```
State: ACTIVE
├── ProbSevere match AND max_refl ≥ threshold → remain ACTIVE
├── ProbSevere match AND max_refl < threshold → transition to DECAYING
├── ProbSevere lost AND max_refl ≥ threshold → transition to PREDICTED
└── ProbSevere lost AND max_refl < threshold → DECAYING + PREDICTED

State: PREDICTED
├── Re-acquired → ACTIVE
├── Confidence < threshold AND decay_count ≥ hysteresis → TERMINATED
└── Time limit exceeded AND decay_count ≥ hysteresis → TERMINATED

State: DECAYING
├── max_refl ≥ threshold → ACTIVE (reset decay count)
└── Both criteria met → TERMINATED
```

### 3.5 Decay Monitoring

Decay monitoring tracks reflectivity decline for each cell:

#### Tracked Fields

| Field | Type | Description |
|-------|------|-------------|
| `decay_scan_count` | int | Consecutive scans below reflectivity threshold |
| `decay_start_timestamp` | str | ISO timestamp when decay monitoring began |
| `decay_max_refl_history` | List[float] | Recent max_refl values for analysis |

#### Decay Event Logging

```
[CellDetection] Cell {id} entered decay monitoring (max_refl: {value} dBZ)
[CellDetection] Cell {id} decay count: {n}/{threshold} (max_refl: {value} dBZ)
[CellDetection] Cell {id} terminated: reflectivity decay + prediction exhaustion
```

---

## 4. Integration

### 4.1 Processing Pipeline

The complete tracking pipeline processes each scan cycle:

```
┌─────────────────────────────────────────────────────────────────────┐
│                      STORM TRACKING PIPELINE                        │
└─────────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────────┐
│ 1. LOAD PREVIOUS STATE                                              │
│    - Load previous scan cells from disk                             │
│    - Load lineage buffer from disk                                  │
│    - Initialize Kalman filters for existing tracks                  │
└─────────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────────┐
│ 2. LINEAGE DETECTION                                                │
│    - Build spatial indices for old and new cells                    │
│    - Detect merges (multiple old → single new)                      │
│    - Detect splits (single old → multiple new)                      │
│    - Apply hysteresis buffer for confirmation                       │
└─────────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────────┐
│ 3. PRIMARY ASSIGNMENT (Overlap-based)                               │
│    - Match cells with spatial overlap                               │
│    - Process confirmed merge events                                 │
│    - Process confirmed split events                                 │
└─────────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────────┐
│ 4. SECONDARY ASSIGNMENT (Kalman-based)                              │
│    - Build cost matrix for unmatched tracks/detections              │
│    - Run Hungarian algorithm (or greedy fallback)                   │
│    - Apply Mahalanobis gating                                       │
└─────────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────────┐
│ 5. PREDICTION MODE HANDLING                                         │
│    - Enter prediction for unmatched tracks                          │
│    - Calculate confidence scores                                    │
│    - Check termination criteria                                     │
│    - Update predicted positions                                     │
└─────────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────────┐
│ 6. NEW CELL INITIALIZATION                                          │
│    - Create new tracks for unmatched detections                     │
│    - Initialize Kalman filters                                      │
│    - Set initial confidence = 1.0                                   │
└─────────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────────┐
│ 7. SAVE STATE                                                       │
│    - Save updated cells to disk                                     │
│    - Save lineage buffer to disk                                    │
│    - Log statistics                                                 │
└─────────────────────────────────────────────────────────────────────┘
```

### 4.2 Data Flow

#### Input Data

| Source | Fields Used |
|--------|-------------|
| ProbSevere | `id`, `centroid`, `bbox`, `max_refl`, `num_gates` |
| StormCast | `u`, `v` (velocity components) |
| Previous Scan | All tracked cell fields |

#### Output Data

| Field | Type | Description |
|-------|------|-------------|
| `id` | int | Cell identifier (may change on merge/split) |
| `centroid` | [lat, lon] | Current or predicted position |
| `bbox` | [[lat, lon], ...] | Polygon boundary |
| `max_refl` | float | Maximum reflectivity (dBZ) |
| `num_gates` | int | Number of radar gates |
| `tracking_mode` | str | "active" or "predicted" |
| `prediction_count` | int | Consecutive scans in prediction mode |
| `confidence` | float | Confidence score (0.0-1.0) |
| `event_type` | str | "ACTIVE", "MERGE", "SPLIT", "DISSIPATED" |
| `parent_ids` | List[int] | Parent cell IDs (for merge) |
| `split_from` | int | Parent cell ID (for split) |
| `kalman_state` | dict | Kalman filter state for debugging |

### 4.3 API Reference

#### StormCellTracker

```python
class StormCellTracker:
    """
    Tracks storm cells across scans with lineage detection and Kalman filtering.
    """
    
    def __init__(
        self,
        ps_old: Any,                    # Previous scan ProbSevere data
        ps_new: Any,                    # Current scan ProbSevere data
        io_manager: Any,                # IO manager for logging
        lineage_buffer: Optional[LineageBuffer] = None,
        overlap_threshold: float = 0.30,
        tracking_config: Optional[TrackingConfig] = None,
        assignment_config: Optional[AssignmentConfig] = None
    ):
        """Initialize the storm cell tracker."""
    
    def detect_lineage_events(
        self,
        old_cells: List[Dict],
        new_cells: List[Dict],
        stormcell_dir: Optional[Path] = None,
    ) -> LineageResult:
        """Detect merge and split events between cell sets."""
    
    def update_cells(
        self,
        entries: List[Dict],
        updated_data: List[Dict],
        timestamp: Optional[str] = None,
        dt_seconds: float = 120.0,
        lineage: Optional[LineageResult] = None,
    ) -> List[Dict]:
        """Update cells using lineage detection + Kalman continuity."""
    
    def get_lineage_buffer(self) -> Optional[LineageBuffer]:
        """Get the current lineage buffer."""
    
    def save_lineage_buffer(self, stormcell_dir: Path) -> bool:
        """Save lineage buffer to disk."""
```

#### LineageDetector

```python
class LineageDetector:
    """
    Detects merge and split events between storm cell sets.
    """
    
    def __init__(
        self,
        buffer: Optional[LineageBuffer] = None,
        overlap_threshold: float = 0.30,
        io_manager: Optional[Any] = None,
    ):
        """Initialize the lineage detector."""
    
    def detect(
        self,
        old_cells: List[Dict],
        new_cells: List[Dict],
    ) -> LineageResult:
        """Detect merge and split events."""
```

#### ConfidenceCalculator

```python
class ConfidenceCalculator:
    """
    Calculates confidence scores for Kalman-predicted storm cells.
    """
    
    def calculate(
        self,
        scans_predicted: int,
        time_predicted_seconds: float,
        velocity_variance: Optional[Tuple[float, float]] = None,
        position_uncertainty_km: Optional[Tuple[float, float]] = None
    ) -> float:
        """Calculate confidence score (0.0-1.0)."""
    
    def should_terminate(
        self,
        confidence: float,
        time_predicted_seconds: float,
        scans_predicted: int
    ) -> Tuple[bool, str]:
        """Determine if a predicted storm should be terminated."""
```

---

## Appendix A: Configuration Reference

### kalman.yaml Complete Example

```yaml
# Kalman Filter Configuration for Storm Cell Tracking

kalman_filter:
  process_noise:
    position: 0.1      # degrees
    velocity: 0.5      # m/s
    acceleration: 0.1  # m/s²
  measurement_noise:
    position: 0.5      # km

tracking:
  max_prediction_time_minutes: 6
  reacquisition_radius_km: 5.0
  confidence_threshold: 0.4
  confidence_decay_factor: 0.7

assignment:
  prefilter_radius_km: 16.0
  gating_threshold: 6.0
  min_gating_radius_km: 2.0
  weights:
    position: 1.0
    velocity_direction: 2.0
    size_similarity: 0.5
  method: hybrid
  covariance_regularization: 1e-6

termination:
  reflectivity_threshold_dbz: 30
  decay_hysteresis_scans: 3
  require_both_criteria: true

lineage:
  overlap_threshold: 0.30
  min_confirmations: 2
  max_pending: 100
  prune_after_scans: 5
```

---

## Appendix B: Troubleshooting

### Common Issues

#### Issue: Cells terminating too quickly

**Symptoms**: Cells disappear after brief detection dropouts.

**Solutions**:
- Increase `max_prediction_time_minutes`
- Decrease `confidence_threshold`
- Increase `confidence_decay_factor`

#### Issue: Cells persisting too long

**Symptoms**: Dissipated cells remain tracked for extended periods.

**Solutions**:
- Decrease `max_prediction_time_minutes`
- Increase `confidence_threshold`
- Decrease `confidence_decay_factor`
- Lower `reflectivity_threshold_dbz`

#### Issue: False merge/split events

**Symptoms**: Spurious merge or split detections.

**Solutions**:
- Increase `min_confirmations` in lineage buffer
- Increase `overlap_threshold`
- Check ProbSevere ID stability

#### Issue: Assignment failures

**Symptoms**: Valid detections not matching existing tracks.

**Solutions**:
- Increase `prefilter_radius_km`
- Increase `gating_threshold`
- Check Kalman filter initialization

---

## Appendix C: Performance Metrics

### Expected Performance

| Metric | Target | Notes |
|--------|--------|-------|
| Assignment time | <10ms | Per 100 cells |
| Lineage detection | <5ms | Per scan |
| Confidence calculation | <1ms | Per cell |
| Buffer save/load | <50ms | Per scan |

### Monitoring Points

```python
# Tracking statistics logged each scan
stats = {
    'matches': int,      # Direct overlap matches
    'merges': int,       # Confirmed merge events
    'splits': int,       # Confirmed split events
    'reacquired': int,   # Re-acquired from prediction
    'predicted': int,    # Entered prediction mode
    'new': int,          # New cell detections
    'terminated': int    # Terminated cells
}
```

---

*Document Version: 1.0*
*Last Updated: 2026-02-24*
*Authors: EdgeWARN Development Team*
