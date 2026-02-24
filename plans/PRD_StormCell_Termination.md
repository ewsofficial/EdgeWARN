# Product Requirements Document: Storm Cell Termination Handling

## Executive Summary

This document specifies the requirements for enhanced storm cell termination handling in the EdgeWARN-Core system. The implementation introduces reflectivity-based decay detection with hysteresis to improve the accuracy of storm dissipation identification, reducing false positives from temporary reflectivity fluctuations while ensuring timely removal of truly dissipated convection.

---

## 1. Meteorological Background

### 1.1 Convective Cell Lifecycle

A convective storm cell progresses through three distinct phases during its lifecycle:

1. **Cumulus Stage (Towering Cumulus)**: Characterized by dominant updrafts, increasing vertical development, and growing reflectivity cores. Cells in this stage exhibit rapidly increasing maximum reflectivity values and expanding precipitation footprints.

2. **Mature Stage**: The cell reaches equilibrium between updraft and downdraft. Maximum reflectivity typically exceeds 40 dBZ, with well-defined precipitation cores and organized storm structure. This is the primary warning-worthy phase.

3. **Dissipating Stage**: The downdraft dominates as the updraft weakens. Reflectivity values decrease, the precipitation core collapses, and the storm structure becomes disorganized. The cell may fragment or merge with adjacent outflow boundaries.

### 1.2 Reflectivity Decay Signatures

During the dissipating stage, radar reflectivity exhibits characteristic signatures:

- **Core Collapse**: Maximum reflectivity (Z_max) decreases as the updraft fails to sustain hydrometeor growth
- **Echo Top Descent**: The height of the 18 dBZ echo top lowers as vertical development ceases
- **Areal Expansion with Intensity Decrease**: The precipitation shield may expand while core intensity weakens (stratiform transition)
- **Fragmentation**: The organized reflectivity pattern breaks into multiple weaker echoes

### 1.3 Termination Criteria Considerations

Accurate storm termination detection must balance two competing requirements:

- **Sensitivity**: Detecting true dissipation promptly to clear warnings appropriately
- **Specificity**: Avoiding premature termination during temporary intensity fluctuations

Temporary reflectivity decreases can occur due to:
- Radar sampling variations (beam elevation, range effects)
- Precipitation loading cycles within the storm
- Brief updraft pulsations
- Attenuation through adjacent convection

---

## 2. Problem Statement

### 2.1 Current Limitations

The existing EdgeWARN-Core termination logic relies primarily on:

1. **Kalman Filter Prediction Mode**: Cells enter prediction when ProbSevere detection is lost, with termination after confidence decay or time limit (10 minutes)

2. **Spatial Overlap Termination**: Legacy `CellTerminator` removes cells highly covered (≥67%) by larger cells

3. **Lineage Dissipation**: Cells marked `DISSIPATED` when no overlap match exists

These approaches have limitations:

| Limitation | Impact |
|------------|--------|
| No reflectivity-based criteria | Weak cells may persist despite meteorological dissipation |
| Binary detection loss | Single-scan dropout triggers prediction mode unnecessarily |
| No decay monitoring | Gradual dissipation not detected until complete loss |
| Prediction mode overuse | Kalman prediction invoked for truly dissipated cells |

### 2.2 Desired Behavior

The system should:

1. Monitor reflectivity decay as a primary indicator of convective dissipation
2. Apply hysteresis to prevent premature termination from transient fluctuations
3. Integrate reflectivity decay with Kalman prediction for robust termination decisions
4. Require both reflectivity and prediction criteria to fail before termination

---

## 3. Requirements Specification

### 3.1 Functional Requirements

#### FR1: Reflectivity Decay Detection

The system SHALL monitor maximum reflectivity (max_refl) for each tracked storm cell and detect when values fall below the configurable threshold.

**Threshold**: 30 dBZ (configurable)
**Rationale**: Below 30 dBZ, convection has typically lost its severe potential. This threshold captures the transition from organized convection to weak echoes.

#### FR2: Hysteresis Implementation

The system SHALL require N consecutive scans below the reflectivity threshold before marking a cell for termination consideration.

**Hysteresis Count**: 3 consecutive scans (configurable)
**Rationale**: At typical 2-minute scan intervals, 3 scans provide 6 minutes of observation. This filters out transient fluctuations while detecting true decay.

#### FR3: Dual-Criterion Termination

The system SHALL terminate a storm cell only when BOTH criteria are met:

1. **Reflectivity Criterion**: max_refl < 30 dBZ for 3+ consecutive scans
2. **Prediction Criterion**: Cell in Kalman prediction mode with confidence below threshold OR time limit exceeded

**Rationale**: Requiring both criteria prevents:
- Premature termination of cells with temporary reflectivity drops but continued ProbSevere detection
- Extended prediction of cells that have clearly dissipated meteorologically

#### FR4: Decay State Tracking

The system SHALL maintain decay state information for each tracked cell:

| Field | Type | Description |
|-------|------|-------------|
| decay_scan_count | int | Consecutive scans below reflectivity threshold |
| decay_start_timestamp | str | ISO timestamp when decay monitoring began |
| decay_max_refl_history | List[float] | Recent max_refl values for analysis |

#### FR5: Decay Event Logging

The system SHALL log decay detection events with sufficient detail for post-analysis:

```
[CellDetection] Cell {id} entered decay monitoring (max_refl: {value} dBZ)
[CellDetection] Cell {id} decay count: {n}/{threshold} (max_refl: {value} dBZ)
[CellDetection] Cell {id} terminated: reflectivity decay + prediction exhaustion
```

### 3.2 Non-Functional Requirements

#### NFR1: Configuration

All termination parameters SHALL be configurable via the existing `config/kalman.yaml` file:

```yaml
termination:
  reflectivity_threshold_dbz: 30
  decay_hysteresis_scans: 3
  require_both_criteria: true
```

#### NFR2: Performance

Decay monitoring SHALL add no more than 5ms per cell per scan to processing time.

#### NFR3: Backward Compatibility

The implementation SHALL maintain backward compatibility with existing storm cell data structures. New fields SHALL be optional with sensible defaults.

#### NFR4: Observability

Decay state SHALL be included in cell data output for monitoring and debugging purposes.

---

## 4. State Machine Model

### 4.1 Cell Tracking States

```
                                    ┌─────────────────┐
                                    │                 │
                                    │    NEW CELL     │
                                    │                 │
                                    └────────┬────────┘
                                             │
                                             ▼
┌───────────────────────────────────────────────────────────────────┐
│                                                                   │
│   ┌─────────────┐      ProbSevere      ┌─────────────────┐       │
│   │             │ ◄──────────────────── │                 │       │
│   │   ACTIVE    │                      │   PREDICTED     │       │
│   │             │ ────────────────────► │                 │       │
│   └──────┬──────┘    ProbSevere lost   └────────┬────────┘       │
│          │                                        │               │
│          │ max_refl < 30 dBZ                      │               │
│          ▼                                        ▼               │
│   ┌─────────────┐      Both criteria      ┌─────────────────┐    │
│   │   DECAYING  │ ──────────────────────► │   TERMINATED    │    │
│   └─────────────┘      met                └─────────────────┘    │
│                                                                   │
└───────────────────────────────────────────────────────────────────┘
```

### 4.2 State Descriptions

| State | Description | Entry Condition | Exit Conditions |
|-------|-------------|-----------------|-----------------|
| ACTIVE | Normal tracking with ProbSevere detection | New cell or re-acquisition | ProbSevere lost → PREDICTED; max_refl < threshold → DECAYING |
| PREDICTED | Kalman prediction mode without ProbSevere | ProbSevere detection lost | Re-acquisition → ACTIVE; Both criteria met → TERMINATED |
| DECAYING | Reflectivity below threshold, monitoring decay | max_refl < threshold for 1 scan | max_refl ≥ threshold → ACTIVE; Both criteria met → TERMINATED |
| TERMINATED | Cell removed from tracking | Both decay + prediction criteria met | N/A - terminal state |

### 4.3 Transition Logic

```
State: ACTIVE
├── ProbSevere match AND max_refl ≥ threshold → remain ACTIVE
├── ProbSevere match AND max_refl < threshold → transition to DECAYING
├── ProbSevere lost AND max_refl ≥ threshold → transition to PREDICTED
└── ProbSevere lost AND max_refl < threshold → transition to DECAYING + PREDICTED

State: PREDICTED
├── Re-acquired → transition to ACTIVE
├── Confidence OK AND time OK → remain PREDICTED
├── max_refl < threshold for N scans AND confidence/time exhausted → TERMINATED
└── max_refl ≥ threshold → reset decay count, remain PREDICTED

State: DECAYING
├── max_refl ≥ threshold → reset decay count, transition to ACTIVE/PREDICTED
├── decay_count < N → remain DECAYING
├── decay_count ≥ N AND in prediction mode AND prediction exhausted → TERMINATED
└── decay_count ≥ N AND ProbSevere active → remain DECAYING (wait for prediction)
```

---

## 5. Integration with Existing Systems

### 5.1 Kalman Filter Module

The decay detection integrates with the existing Kalman filter tracking:

| Component | Integration Point |
|-----------|-------------------|
| `PredictionState` | Extended to include decay monitoring fields |
| `ConfidenceCalculator` | Decay factor incorporated into confidence calculation |
| `TrackingConfig` | Extended with termination configuration |

### 5.2 Lineage Detection

Decay termination is independent of lineage events (merge/split):

- **Merge**: If a cell is absorbed into another, decay state is irrelevant
- **Split**: Child cells inherit parent's decay count proportionally to overlap ratio
- **DISSIPATED**: Renamed/merged with TERMINATED state

### 5.3 Data Flow

```
┌──────────────────┐     ┌──────────────────┐     ┌──────────────────┐
│  ProbSevere      │     │  Storm Cell      │     │  Termination     │
│  Detection       │────►│  Tracker         │────►│  Decision        │
└──────────────────┘     └────────┬─────────┘     └──────────────────┘
                                  │
                                  ▼
                         ┌──────────────────┐
                         │  Decay Monitor   │
                         │  - Track max_refl│
                         │  - Count scans   │
                         │  - Apply hysteresis│
                         └──────────────────┘
```

---

## 6. Success Metrics

### 6.1 Validation Criteria

| Metric | Target | Measurement |
|--------|--------|-------------|
| False Termination Rate | < 2% | Cells terminated then re-acquired within 5 minutes |
| Detection Latency | < 8 minutes | Time from true dissipation to termination |
| Configuration Coverage | 100% | All parameters configurable via YAML |

### 6.2 Test Scenarios

1. **Transient Reflectivity Drop**: Cell drops below 30 dBZ for 1-2 scans, then recovers → should NOT terminate
2. **True Dissipation**: Cell gradually weakens over 6+ minutes while losing ProbSevere → should terminate
3. **Rapid Dissipation**: Cell drops below threshold and loses ProbSevere simultaneously → should terminate after hysteresis
4. **Prediction Mode Recovery**: Cell in prediction mode recovers with adequate reflectivity → should re-acquire

---

## 7. Implementation Phases

### Phase 1: Core Decay Detection
- Implement `DecayMonitor` class
- Extend `PredictionState` with decay fields
- Add configuration parameters

### Phase 2: Integration
- Integrate decay monitoring into `StormCellTracker.update_cells()`
- Implement dual-criterion termination logic
- Update state transitions

### Phase 3: Testing & Validation
- Unit tests for decay monitoring
- Integration tests with Kalman prediction
- Validation against historical storm data

---

## 8. Appendix: Meteorological References

### A. Reflectivity Thresholds

| Reflectivity (dBZ) | Convective Intensity |
|-------------------|---------------------|
| < 25 | Weak/non-convective |
| 25-30 | Moderate, developing |
| 30-40 | Moderate-strong |
| 40-50 | Strong |
| 50-60 | Severe |
| > 60 | Extreme/hail signature |

### B. Typical Dissipation Timescales

| Storm Type | Typical Dissipation Time |
|------------|-------------------------|
| Ordinary cell | 15-30 minutes |
| Multicell cluster | 30-60 minutes (individual cells) |
| Supercell | 60-120+ minutes |
| Pulse storm | 10-20 minutes |

### C. Radar Sampling Considerations

At typical S-band radar:
- Beam width: ~1°
- Range resolution: 250m-1km
- Volume scan time: 4-6 minutes (WSR-88D)
- MRMS composite: 1km resolution, 2-minute updates

---

*Document Version: 1.0*
*Last Updated: 2026-02-24*
